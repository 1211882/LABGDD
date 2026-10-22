from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
import time
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config.settings import (
    BatchAnalyticsSettings,
    ConfigurationError,
    load_batch_analytics_settings,
)
from src.metrics.batch_analytics_metrics import BatchAnalyticsMetrics
from src.spark.batch_processor import _directory_size, _spark_schema_to_arrow
from src.spark.streaming_analytics import (
    add_bar_return,
    add_trading_date,
    duration_minutes,
    filter_regular_session,
)

LOGGER = logging.getLogger(__name__)
def prepare_analytics_source(
    dataframe: DataFrame, regular_session_only: bool
) -> DataFrame:
    source = filter_regular_session(dataframe) if regular_session_only else dataframe
    return add_bar_return(add_trading_date(source))


def build_batch_symbol_analytics(
    dataframe: DataFrame,
    window_durations: tuple[str, ...],
    dataset_version: str,
) -> DataFrame:
    frames: list[DataFrame] = []
    for duration in window_durations:
        expected_count = duration_minutes(duration)
        frames.append(
            dataframe.groupBy(
                F.window("timestamp", duration), "trading_date", "symbol", "sector"
            )
            .agg(
                F.avg("close").alias("average_price"),
                F.min("close").alias("minimum_price"),
                F.max("close").alias("maximum_price"),
                F.min_by("close", "timestamp").alias("first_price"),
                F.max_by("close", "timestamp").alias("last_price"),
                F.sum("volume").alias("total_volume"),
                F.count(F.lit(1)).alias("event_count"),
                F.stddev_samp("bar_return").alias("volatility"),
            )
            .withColumn(
                "window_return",
                F.when(
                    F.col("first_price") > 0,
                    (F.col("last_price") - F.col("first_price"))
                    / F.col("first_price"),
                ),
            )
            .withColumn("window_duration", F.lit(duration))
            .withColumn("dataset_version", F.lit(dataset_version))
            .withColumn("expected_event_count", F.lit(expected_count))
            .withColumn(
                "coverage_ratio", F.col("event_count") / F.col("expected_event_count")
            )
        )
    return _union(frames).select(
        "dataset_version",
        "trading_date",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "window_duration",
        "symbol",
        "sector",
        "average_price",
        "minimum_price",
        "maximum_price",
        "first_price",
        "last_price",
        "total_volume",
        "event_count",
        "window_return",
        "volatility",
        "expected_event_count",
        "coverage_ratio",
    )


def build_batch_sector_analytics(
    dataframe: DataFrame,
    window_durations: tuple[str, ...],
    dataset_version: str,
) -> DataFrame:
    frames: list[DataFrame] = []
    for duration in window_durations:
        expected_count = duration_minutes(duration)
        frames.append(
            dataframe.groupBy(F.window("timestamp", duration), "trading_date", "sector")
            .agg(
                F.avg("close").alias("average_price"),
                F.min("close").alias("minimum_price"),
                F.max("close").alias("maximum_price"),
                F.sum("volume").alias("total_volume"),
                F.count(F.lit(1)).alias("event_count"),
                F.approx_count_distinct("symbol").alias("symbol_count"),
                F.stddev_samp("bar_return").alias("volatility"),
            )
            .withColumn("window_duration", F.lit(duration))
            .withColumn("dataset_version", F.lit(dataset_version))
            .withColumn("active_symbols", F.col("symbol_count"))
            .withColumn(
                "expected_event_count", F.lit(expected_count) * F.col("active_symbols")
            )
            .withColumn(
                "coverage_ratio", F.col("event_count") / F.col("expected_event_count")
            )
        )
    return _union(frames).select(
        "dataset_version",
        "trading_date",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "window_duration",
        "sector",
        "average_price",
        "minimum_price",
        "maximum_price",
        "total_volume",
        "event_count",
        "symbol_count",
        "active_symbols",
        "volatility",
        "expected_event_count",
        "coverage_ratio",
    )


def _union(frames: list[DataFrame]) -> DataFrame:
    if not frames:
        raise ValueError("At least one analytical window duration is required.")
    return reduce(lambda left, right: left.unionByName(right), frames)


def create_spark_session(settings: BatchAnalyticsSettings) -> SparkSession:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    spark = (
        SparkSession.builder.appName(settings.app_name)
        .master(settings.master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_processed_batch(spark: SparkSession, input_path: Path) -> DataFrame:
    root = input_path.resolve()
    files = sorted(root.glob("symbol=*/*.parquet"))
    if not files:
        raise FileNotFoundError(f"No processed Batch Parquet files found under: {root}")
    return spark.read.option("basePath", root.as_posix()).parquet(
        *[path.resolve().as_posix() for path in files]
    )


def run_batch_analytics(settings: BatchAnalyticsSettings) -> BatchAnalyticsMetrics:
    spark = create_spark_session(settings)
    started = time.perf_counter()
    version_root = settings.output_root / settings.dataset_version
    try:
        raw = read_processed_batch(spark, settings.input_path).cache()
        source = prepare_analytics_source(raw, settings.regular_session_only).cache()
        input_rows = raw.count()
        filtered_rows = source.count()
        symbols = source.select("symbol").distinct().count()
        sectors = source.select("sector").distinct().count()
        symbol_counts: dict[str, int] = {}
        sector_counts: dict[str, int] = {}
        missing_counts: dict[str, int] = {}
        coverage_by_duration: dict[str, dict[str, dict[str, float | int]]] = {}

        for duration in settings.window_durations:
            duration_key = duration.replace(" ", "_")
            symbol_frame = build_batch_symbol_analytics(
                source, (duration,), settings.dataset_version
            ).cache()
            sector_frame = build_batch_sector_analytics(
                source, (duration,), settings.dataset_version
            ).cache()
            symbol_counts[duration] = symbol_frame.count()
            sector_counts[duration] = sector_frame.count()
            expected_minutes = int(duration.split()[0])
            missing_counts[duration] = symbol_frame.filter(
                F.col("event_count") < expected_minutes
            ).count()
            coverage_by_duration[duration] = {
                "symbol": _coverage_summary(symbol_frame),
                "sector": _coverage_summary(sector_frame),
            }
            _write_frame(symbol_frame, version_root / "symbol" / f"duration={duration_key}")
            _write_frame(sector_frame, version_root / "sector" / f"duration={duration_key}")
            symbol_frame.unpersist()
            sector_frame.unpersist()

        duration = time.perf_counter() - started
        metrics = BatchAnalyticsMetrics(
            dataset_version=settings.dataset_version,
            generated_at_utc=BatchAnalyticsMetrics.timestamp(),
            regular_session_only=settings.regular_session_only,
            input_rows=input_rows,
            rows_after_session_filter=filtered_rows,
            number_of_symbols=symbols,
            number_of_sectors=sectors,
            symbol_windows=symbol_counts,
            sector_windows=sector_counts,
            symbol_windows_with_missing_minutes=missing_counts,
            coverage_by_duration=coverage_by_duration,
            processing_duration_seconds=round(duration, 6),
            rows_per_second=round(input_rows / duration, 3) if duration else 0.0,
            output_size_bytes=_directory_size(version_root),
            output_root=str(version_root),
        )
        metrics.save(settings.metrics_path)
        return metrics
    finally:
        spark.stop()


def _coverage_summary(dataframe: DataFrame) -> dict[str, float | int]:
    row = dataframe.agg(
        F.count(F.lit(1)).alias("total_windows"),
        F.sum(F.when(F.col("event_count") >= F.col("expected_event_count"), 1).otherwise(0)).alias("complete_windows"),
        F.avg("event_count").alias("average_event_count"),
        F.expr("percentile_approx(event_count, 0.5, 10000)").alias("median_event_count"),
        F.avg("coverage_ratio").alias("average_coverage_ratio"),
        F.min("coverage_ratio").alias("minimum_coverage_ratio"),
        F.expr("percentile_approx(coverage_ratio, 0.5, 10000)").alias("median_coverage_ratio"),
        F.sum(F.when(F.col("volatility").isNotNull(), 1).otherwise(0)).alias("valid_volatility_windows"),
    ).first()
    total = int(row.total_windows)
    complete = int(row.complete_windows or 0)
    valid_volatility = int(row.valid_volatility_windows or 0)
    return {
        "total_windows": total,
        "complete_windows": complete,
        "incomplete_windows": total - complete,
        "average_event_count": round(float(row.average_event_count or 0.0), 6),
        "median_event_count": int(row.median_event_count or 0),
        "average_coverage_ratio": round(float(row.average_coverage_ratio or 0.0), 6),
        "minimum_coverage_ratio": round(float(row.minimum_coverage_ratio or 0.0), 6),
        "median_coverage_ratio": round(float(row.median_coverage_ratio or 0.0), 6),
        "valid_volatility_windows": valid_volatility,
        "valid_volatility_ratio": round(valid_volatility / total, 6) if total else 0.0,
    }


def _write_frame(dataframe: DataFrame, output_path: Path) -> None:
    root = output_path.resolve()
    if root.exists():
        shutil.rmtree(root)
    root.parent.mkdir(parents=True, exist_ok=True)
    if os.name != "nt":
        dataframe.write.mode("overwrite").parquet(root.as_posix())
        return

    import pyarrow as pa
    import pyarrow.parquet as pq

    root.mkdir(parents=True)
    pandas_frame = dataframe.toPandas()
    table = pa.Table.from_pandas(
        pandas_frame,
        schema=_spark_schema_to_arrow(dataframe.schema),
        preserve_index=False,
        safe=True,
    )
    pq.write_table(table, root / "part-00000.parquet")
    (root / "_SUCCESS").write_text("", encoding="ascii")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run versioned Phase 6 Batch analytics.")
    parser.add_argument("--config", default="config/config.yaml")
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    args = _parse_args()
    try:
        metrics = run_batch_analytics(load_batch_analytics_settings(args.config))
        LOGGER.info("Phase 6 Batch analytics complete: %s", metrics.to_dict())
    except (ConfigurationError, FileNotFoundError, ValueError, RuntimeError) as exc:
        LOGGER.error("Phase 6 Batch analytics failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
