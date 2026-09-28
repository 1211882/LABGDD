from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path
from urllib.parse import quote

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config.settings import ConfigurationError, SparkSettings, load_spark_settings
from src.metrics.pipeline_metrics import PipelineMetrics
from src.spark.transformations import (
    BASE_COLUMNS,
    add_financial_features,
    add_sector,
    deduplicate_records,
    split_quality_records,
)

LOGGER = logging.getLogger(__name__)


def discover_parquet_files(
    input_path: str | Path,
    requested_symbols: list[str] | None = None,
) -> tuple[list[Path], list[str]]:
    """Find raw Parquet files for selected symbols, or all available symbols."""
    root = Path(input_path)
    if not root.exists():
        raise FileNotFoundError(f"Spark input path does not exist: {root}")

    requested = {symbol.upper() for symbol in requested_symbols or []}
    files: list[Path] = []
    discovered_symbols: set[str] = set()

    for file_path in sorted(root.glob("*/*.parquet")):
        symbol = file_path.parent.name.upper()
        if requested and symbol not in requested:
            continue
        files.append(file_path.resolve())
        discovered_symbols.add(symbol)

    if requested:
        missing = requested - discovered_symbols
        if missing:
            raise FileNotFoundError(
                f"No raw Parquet files found for symbols: {', '.join(sorted(missing))}"
            )
    if not files:
        raise FileNotFoundError(f"No raw Parquet files found under: {root}")

    return files, sorted(discovered_symbols)


def create_spark_session(settings: SparkSettings) -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName(settings.app_name)
        .master(settings.master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def prepare_raw_data(dataframe: DataFrame) -> DataFrame:
    missing = [column for column in BASE_COLUMNS if column not in dataframe.columns]
    if missing:
        raise ValueError(f"Raw dataset is missing columns: {missing}")

    return dataframe.select(
        F.col("timestamp").cast("timestamp").alias("timestamp"),
        F.upper(F.col("symbol")).alias("symbol"),
        F.col("open").cast("double").alias("open"),
        F.col("high").cast("double").alias("high"),
        F.col("low").cast("double").alias("low"),
        F.col("close").cast("double").alias("close"),
        F.col("volume").cast("long").alias("volume"),
    )


def transform_batch(
    dataframe: DataFrame,
    symbol_sectors: dict[str, str],
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Return processed rows, quarantined rows, and quality-valid pre-dedup rows."""
    prepared = add_sector(prepare_raw_data(dataframe), symbol_sectors)
    quality_valid, quality_invalid = split_quality_records(prepared)
    unique_valid, duplicates = deduplicate_records(quality_valid)
    processed = add_financial_features(unique_valid)
    quarantined = quality_invalid.unionByName(duplicates)
    return processed, quarantined, quality_valid


def run_batch(
    settings: SparkSettings,
    requested_symbols: list[str] | None = None,
) -> PipelineMetrics:
    full_configured_run = requested_symbols is None
    selected_symbols = requested_symbols or list(settings.symbol_sectors)
    files, discovered_symbols = discover_parquet_files(
        settings.input_path, selected_symbols
    )
    LOGGER.info(
        "Starting Spark batch: symbols=%s files=%s",
        ",".join(discovered_symbols),
        len(files),
    )
    input_size_bytes = sum(file_path.stat().st_size for file_path in files)
    spark = create_spark_session(settings)
    started_at = time.perf_counter()

    try:
        raw = spark.read.parquet(*[file_path.as_posix() for file_path in files]).cache()
        processed, quarantined, quality_valid = transform_batch(
            raw, settings.symbol_sectors
        )
        processed = processed.cache()
        quarantined = quarantined.cache()
        quality_valid = quality_valid.cache()

        input_row_count = raw.count()
        valid_row_count = quality_valid.count()
        invalid_row_count = input_row_count - valid_row_count
        duplicate_count = quarantined.filter(
            F.array_contains("validation_errors", "duplicate_symbol_timestamp")
        ).count()
        output_row_count = processed.count()
        processed_symbols = sorted(
            row.symbol
            for row in processed.select("symbol").distinct().collect()
            if row.symbol is not None
        )

        _write_partitioned(
            processed, settings.output_path, replace_all=full_configured_run
        )
        if invalid_row_count + duplicate_count > 0:
            _write_partitioned(
                quarantined,
                settings.invalid_output_path,
                replace_all=full_configured_run,
            )
        elif full_configured_run:
            _clear_partitioned_output(settings.invalid_output_path)

        duration = time.perf_counter() - started_at
        output_size_bytes = _directory_size(settings.output_path)
        metrics = PipelineMetrics(
            input_row_count=input_row_count,
            valid_row_count=valid_row_count,
            invalid_row_count=invalid_row_count,
            duplicate_count=duplicate_count,
            output_row_count=output_row_count,
            processing_duration_seconds=round(duration, 6),
            rows_per_second=round(input_row_count / duration, 3) if duration else 0.0,
            number_of_symbols=len(processed_symbols),
            processed_symbols=processed_symbols,
            input_size_bytes=input_size_bytes,
            output_size_bytes=output_size_bytes,
        )
        metrics.save(settings.metrics_path)
        LOGGER.info("Batch metrics: %s", json.dumps(metrics.to_dict(), sort_keys=True))
        return metrics
    finally:
        spark.stop()


def _write_partitioned(
    dataframe: DataFrame, output_path: Path, replace_all: bool = False
) -> None:
    if os.name == "nt" and not _windows_native_hadoop_available():
        _write_partitioned_with_pyarrow(dataframe, output_path, replace_all)
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    (
        dataframe.write.mode("overwrite")
        .option("partitionOverwriteMode", "static" if replace_all else "dynamic")
        .partitionBy("symbol")
        .parquet(output_path.resolve().as_posix())
    )


def _windows_native_hadoop_available() -> bool:
    hadoop_home = os.environ.get("HADOOP_HOME") or os.environ.get("hadoop.home.dir")
    return bool(hadoop_home and (Path(hadoop_home) / "bin" / "winutils.exe").is_file())


def _write_partitioned_with_pyarrow(
    dataframe: DataFrame, output_path: Path, replace_all: bool = False
) -> None:
    """Persist local Windows output when Hadoop native binaries are unavailable."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    root = output_path.resolve()
    root.mkdir(parents=True, exist_ok=True)
    _remove_child_directory(root, root / "_temporary")
    if replace_all:
        _clear_partitioned_output(root)
    LOGGER.warning(
        "winutils.exe is unavailable; using local PyArrow persistence for %s", root
    )

    symbols = [row.symbol for row in dataframe.select("symbol").distinct().collect()]
    for symbol in symbols:
        partition_value = "__HIVE_DEFAULT_PARTITION__" if symbol is None else quote(symbol, safe="")
        partition_dir = root / f"symbol={partition_value}"
        _remove_child_directory(root, partition_dir)
        partition_dir.mkdir(parents=True, exist_ok=True)

        condition = F.col("symbol").isNull() if symbol is None else F.col("symbol") == symbol
        partition_dataframe = dataframe.filter(condition).drop("symbol")
        partition = partition_dataframe.toPandas()
        arrow_schema = _spark_schema_to_arrow(partition_dataframe.schema)
        arrow_table = pa.Table.from_pandas(
            partition,
            schema=arrow_schema,
            preserve_index=False,
            safe=True,
        )
        pq.write_table(
            arrow_table,
            partition_dir / "part-00000.parquet",
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )

    (root / "_SUCCESS").write_text("", encoding="ascii")


def _spark_schema_to_arrow(schema):
    import pyarrow as pa
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        StringType,
        TimestampNTZType,
        TimestampType,
    )

    def convert(data_type):
        if isinstance(data_type, (TimestampType, TimestampNTZType)):
            return pa.timestamp("us")
        if isinstance(data_type, StringType):
            return pa.string()
        if isinstance(data_type, BooleanType):
            return pa.bool_()
        if isinstance(data_type, LongType):
            return pa.int64()
        if isinstance(data_type, IntegerType):
            return pa.int32()
        if isinstance(data_type, DoubleType):
            return pa.float64()
        if isinstance(data_type, FloatType):
            return pa.float32()
        if isinstance(data_type, ArrayType):
            return pa.list_(convert(data_type.elementType))
        raise TypeError(f"Unsupported Spark type for local Parquet output: {data_type}")

    return pa.schema(
        [pa.field(field.name, convert(field.dataType), field.nullable) for field in schema]
    )


def _remove_child_directory(root: Path, target: Path) -> None:
    resolved_root = root.resolve()
    resolved_target = target.resolve()
    if resolved_target.parent != resolved_root:
        raise ValueError(f"Refusing to remove path outside output directory: {target}")
    if resolved_target.exists():
        shutil.rmtree(resolved_target)


def _clear_partitioned_output(output_path: Path) -> None:
    root = output_path.resolve()
    if not root.exists():
        return
    for partition_dir in root.glob("symbol=*"):
        if partition_dir.is_dir():
            _remove_child_directory(root, partition_dir)
    success_file = root / "_SUCCESS"
    if success_file.is_file():
        success_file.unlink()


def _directory_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(file_path.stat().st_size for file_path in path.rglob("*") if file_path.is_file())


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Spark batch market processing.")
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="Optional symbols to process, separated by spaces. Defaults to all available.",
    )
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to the YAML configuration file.",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args = _parse_args()
    try:
        settings = load_spark_settings(args.config)
        run_batch(settings, args.symbols)
    except (ConfigurationError, FileNotFoundError, ValueError, RuntimeError) as exc:
        LOGGER.error("Spark batch failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
