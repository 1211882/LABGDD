from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
import time
import uuid
from dataclasses import replace
from pathlib import Path
from urllib.parse import quote

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from src.config.settings import (
    ConfigurationError,
    StructuredStreamingSettings,
    load_structured_streaming_settings,
)
from src.metrics.streaming_query_metrics import StructuredStreamingMetrics
from src.spark.batch_processor import (
    _spark_schema_to_arrow,
    _windows_native_hadoop_available,
)
from src.spark.streaming_transformations import (
    deduplicate_valid_stream,
    parse_kafka_market_bars,
    split_stream_records,
    validate_and_enrich_stream,
)

LOGGER = logging.getLogger(__name__)


def create_streaming_spark_session(
    settings: StructuredStreamingSettings,
) -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    if os.name == "nt" and not _windows_native_hadoop_available():
        raise RuntimeError(
            "Windows Structured Streaming checkpoints require a complete native "
            "Hadoop runtime (winutils.exe and hadoop.dll). Run the documented "
            "Docker Spark service on local Windows instead."
        )
    builder = (
        SparkSession.builder.appName(settings.app_name)
        .master(settings.master)
        .config("spark.jars.packages", settings.kafka_connector_package)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.ui.enabled", "false")
    )
    if os.environ.get("SPARK_IVY_DIR"):
        builder = builder.config("spark.jars.ivy", os.environ["SPARK_IVY_DIR"])
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(
    spark: SparkSession, settings: StructuredStreamingSettings
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.bootstrap_servers)
        .option("subscribe", settings.topic)
        .option("startingOffsets", settings.starting_offsets)
        .option("failOnDataLoss", str(settings.fail_on_data_loss).lower())
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
        .option("includeHeaders", "true")
        .load()
    )


def build_streaming_frames(
    kafka_stream: DataFrame,
    settings: StructuredStreamingSettings,
) -> tuple[DataFrame, DataFrame]:
    parsed = parse_kafka_market_bars(kafka_stream)
    validated = validate_and_enrich_stream(parsed, settings.symbol_sectors)
    valid, invalid = split_stream_records(validated)
    deduplicated = deduplicate_valid_stream(valid, settings.watermark_delay)
    return deduplicated, invalid


def run_structured_streaming(
    settings: StructuredStreamingSettings,
    *,
    available_now: bool = False,
    run_seconds: float | None = None,
) -> StructuredStreamingMetrics:
    spark = create_streaming_spark_session(settings)
    metrics = StructuredStreamingMetrics(
        kafka_topic=settings.topic,
        watermark_delay=settings.watermark_delay,
        checkpoint_path=str(settings.checkpoint_path),
        output_path=str(settings.output_path),
        invalid_output_path=str(settings.invalid_output_path),
        connector_package=settings.kafka_connector_package,
    )
    queries: dict[str, StreamingQuery] = {}
    failure: BaseException | None = None

    try:
        kafka_stream = read_kafka_stream(spark, settings)
        valid, invalid = build_streaming_frames(kafka_stream, settings)
        queries["valid"] = _start_query(
            valid,
            "financial-market-valid-stream",
            settings.output_path,
            settings.checkpoint_path / "valid",
            settings.trigger_interval,
            available_now,
            partition_by_symbol=True,
            metrics=metrics,
            stream_name="valid",
        )
        queries["invalid"] = _start_query(
            invalid,
            "financial-market-invalid-stream",
            settings.invalid_output_path,
            settings.checkpoint_path / "invalid",
            settings.trigger_interval,
            available_now,
            partition_by_symbol=False,
            metrics=metrics,
            stream_name="invalid",
        )

        if available_now:
            for query in queries.values():
                query.awaitTermination()
        elif run_seconds is not None:
            _wait_for_duration(queries, run_seconds)
        else:
            spark.streams.awaitAnyTermination()
        _raise_query_failures(queries)
    except BaseException as exc:
        failure = exc
    finally:
        for query in queries.values():
            if query.isActive:
                query.stop()
        progress = {
            name: list(query.recentProgress) for name, query in queries.items()
        }
        metrics.finish(
            progress,
            status="failed" if failure else "completed",
            error=str(failure) if failure else None,
        )
        metrics.save(settings.metrics_path)
        spark.stop()

    LOGGER.info(
        "Structured Streaming finished: status=%s input=%d valid=%d invalid=%d "
        "duplicates=%s duration_seconds=%.3f metrics=%s",
        metrics.status,
        metrics.input_events,
        metrics.valid_events,
        metrics.invalid_events,
        metrics.duplicate_events,
        metrics.duration_seconds,
        settings.metrics_path,
    )
    if failure:
        raise failure
    return metrics


def _start_query(
    dataframe: DataFrame,
    query_name: str,
    output_path: Path,
    checkpoint_path: Path,
    trigger_interval: str,
    available_now: bool,
    *,
    partition_by_symbol: bool,
    metrics: StructuredStreamingMetrics,
    stream_name: str,
) -> StreamingQuery:
    output_path.mkdir(parents=True, exist_ok=True)
    checkpoint_path.mkdir(parents=True, exist_ok=True)

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        batch.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            row_count = batch.count()
            symbols = [
                row.symbol
                for row in batch.select("symbol").where("symbol IS NOT NULL").distinct().collect()
            ]
            partitions = [
                int(row.kafka_partition)
                for row in batch.select("kafka_partition")
                .where("kafka_partition IS NOT NULL")
                .distinct()
                .collect()
            ]
            if row_count:
                _write_micro_batch(
                    batch, output_path, batch_id, partition_by_symbol
                )
            metrics.record_output(
                stream_name, int(batch_id), row_count, symbols, partitions
            )
        finally:
            batch.unpersist()

    writer = (
        dataframe.writeStream.queryName(query_name)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path.resolve().as_posix())
        .foreachBatch(write_batch)
    )
    writer = (
        writer.trigger(availableNow=True)
        if available_now
        else writer.trigger(processingTime=trigger_interval)
    )
    return writer.start()


def _write_micro_batch(
    dataframe: DataFrame,
    output_path: Path,
    batch_id: int,
    partition_by_symbol: bool,
) -> None:
    root = output_path.resolve()
    root.mkdir(parents=True, exist_ok=True)
    final_dir = root / f"batch_id={batch_id:020d}"
    if final_dir.exists():
        LOGGER.info("Micro-batch output already exists, skipping: %s", final_dir)
        return
    temporary_dir = root / f".batch-{batch_id:020d}-{uuid.uuid4().hex}"
    _verify_direct_child(root, temporary_dir)
    _verify_direct_child(root, final_dir)

    try:
        if os.name == "nt" and not _windows_native_hadoop_available():
            _write_micro_batch_with_pyarrow(
                dataframe, temporary_dir, partition_by_symbol
            )
        else:
            writer = dataframe.write.mode("overwrite")
            if partition_by_symbol:
                writer = writer.partitionBy("symbol")
            writer.parquet(temporary_dir.as_posix())
        temporary_dir.replace(final_dir)
    except BaseException:
        if temporary_dir.exists():
            shutil.rmtree(temporary_dir)
        raise


def _write_micro_batch_with_pyarrow(
    dataframe: DataFrame,
    output_dir: Path,
    partition_by_symbol: bool,
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyspark.sql import functions as F

    output_dir.mkdir(parents=True, exist_ok=False)
    if partition_by_symbol:
        symbols = [row.symbol for row in dataframe.select("symbol").distinct().collect()]
        for symbol in symbols:
            encoded = (
                "__HIVE_DEFAULT_PARTITION__"
                if symbol is None
                else quote(symbol, safe="")
            )
            partition_dir = output_dir / f"symbol={encoded}"
            partition_dir.mkdir()
            condition = (
                F.col("symbol").isNull()
                if symbol is None
                else F.col("symbol") == symbol
            )
            selected = dataframe.filter(condition).drop("symbol")
            table = pa.Table.from_pandas(
                selected.toPandas(),
                schema=_spark_schema_to_arrow(selected.schema),
                preserve_index=False,
                safe=True,
            )
            pq.write_table(
                table,
                partition_dir / "part-00000.parquet",
                coerce_timestamps="us",
                allow_truncated_timestamps=True,
            )
    else:
        table = pa.Table.from_pandas(
            dataframe.toPandas(),
            schema=_spark_schema_to_arrow(dataframe.schema),
            preserve_index=False,
            safe=True,
        )
        pq.write_table(
            table,
            output_dir / "part-00000.parquet",
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )
    (output_dir / "_SUCCESS").write_text("", encoding="ascii")


def _verify_direct_child(root: Path, target: Path) -> None:
    if target.resolve().parent != root.resolve():
        raise ValueError(f"Refusing to write outside streaming output: {target}")


def _wait_for_duration(queries: dict[str, StreamingQuery], seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        _raise_query_failures(queries)
        time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))


def _raise_query_failures(queries: dict[str, StreamingQuery]) -> None:
    for name, query in queries.items():
        exception = query.exception()
        if exception is not None:
            raise RuntimeError(f"Structured Streaming query {name} failed: {exception}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process Kafka market bars with Spark Structured Streaming."
    )
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--available-now", action="store_true")
    parser.add_argument("--run-seconds", type=float)
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"])
    parser.add_argument("--bootstrap-servers")
    parser.add_argument("--output-path", type=Path)
    parser.add_argument("--invalid-output-path", type=Path)
    parser.add_argument("--checkpoint-path", type=Path)
    parser.add_argument("--metrics-path", type=Path)
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args = _parse_args()
    if args.run_seconds is not None and args.run_seconds <= 0:
        LOGGER.error("--run-seconds must be greater than zero.")
        return 2
    if args.available_now and args.run_seconds is not None:
        LOGGER.error("Use either --available-now or --run-seconds, not both.")
        return 2

    try:
        settings = load_structured_streaming_settings(args.config)
        overrides = {
            "starting_offsets": args.starting_offsets,
            "bootstrap_servers": args.bootstrap_servers,
            "output_path": args.output_path,
            "invalid_output_path": args.invalid_output_path,
            "checkpoint_path": args.checkpoint_path,
            "metrics_path": args.metrics_path,
        }
        settings = replace(
            settings,
            **{key: value for key, value in overrides.items() if value is not None},
        )
        run_structured_streaming(
            settings,
            available_now=args.available_now,
            run_seconds=args.run_seconds,
        )
    except (ConfigurationError, FileNotFoundError, ValueError, RuntimeError) as exc:
        LOGGER.error("Spark Structured Streaming failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
