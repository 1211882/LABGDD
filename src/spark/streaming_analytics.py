from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def filter_regular_session(dataframe: DataFrame) -> DataFrame:
    """Keep 09:30-16:00 America/New_York bars with DST-aware conversion."""
    local_timestamp = F.from_utc_timestamp("timestamp", "America/New_York")
    minute = F.hour(local_timestamp) * 60 + F.minute(local_timestamp)
    return dataframe.filter((minute >= 9 * 60 + 30) & (minute < 16 * 60))


def add_bar_return(dataframe: DataFrame) -> DataFrame:
    """Add the close-to-open return of each source bar."""
    return dataframe.withColumn(
        "bar_return",
        F.when(F.col("open") > 0, (F.col("close") - F.col("open")) / F.col("open")),
    )


def add_trading_date(dataframe: DataFrame) -> DataFrame:
    return dataframe.withColumn(
        "trading_date",
        F.to_date(F.from_utc_timestamp("timestamp", "America/New_York")),
    )


def _union(frames: list[DataFrame]) -> DataFrame:
    return reduce(lambda left, right: left.unionByName(right), frames)


def duration_minutes(duration: str) -> int:
    return int(duration.split()[0])


def build_symbol_analytics(
    dataframe: DataFrame,
    window_durations: tuple[str, ...],
    dataset_version: str = "unversioned",
) -> DataFrame:
    source = add_bar_return(add_trading_date(dataframe))
    frames = []
    for duration in window_durations:
        expected_count = duration_minutes(duration)
        grouped = source.groupBy(
            F.window("timestamp", duration), "trading_date", "symbol", "sector"
        )
        frames.append(
            grouped.agg(
                F.avg("close").alias("average_price"),
                F.min("close").alias("minimum_price"),
                F.max("close").alias("maximum_price"),
                F.min_by("close", "timestamp").alias("first_price"),
                F.max_by("close", "timestamp").alias("last_price"),
                F.sum("volume").alias("total_volume"),
                F.count(F.lit(1)).alias("event_count"),
                F.stddev_samp("bar_return").alias("volatility"),
            )
            .withColumn("expected_event_count", F.lit(expected_count))
            .withColumn(
                "coverage_ratio", F.col("event_count") / F.col("expected_event_count")
            )
            .withColumn(
                "window_return",
                F.when(
                    F.col("first_price") > 0,
                    (F.col("last_price") - F.col("first_price")) / F.col("first_price"),
                ),
            )
            .withColumn("window_duration", F.lit(duration))
            .withColumn("dataset_version", F.lit(dataset_version))
        )
    return _union(frames).select(
        "dataset_version", "trading_date",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "window_duration", "symbol", "sector", "average_price", "minimum_price",
        "maximum_price", "first_price", "last_price", "total_volume", "event_count",
        "window_return", "volatility", "expected_event_count", "coverage_ratio",
    )


def build_sector_analytics(
    dataframe: DataFrame,
    window_durations: tuple[str, ...],
    dataset_version: str = "unversioned",
) -> DataFrame:
    source = add_bar_return(add_trading_date(dataframe))
    frames = []
    for duration in window_durations:
        expected_count = duration_minutes(duration)
        frames.append(
            source.groupBy(F.window("timestamp", duration), "trading_date", "sector")
            .agg(
                F.avg("close").alias("average_price"),
                F.min("close").alias("minimum_price"),
                F.max("close").alias("maximum_price"),
                F.sum("volume").alias("total_volume"),
                F.count(F.lit(1)).alias("event_count"),
                F.approx_count_distinct("symbol").alias("symbol_count"),
                F.stddev_samp("bar_return").alias("volatility"),
            )
            .withColumn("active_symbols", F.col("symbol_count"))
            .withColumn(
                "expected_event_count", F.lit(expected_count) * F.col("active_symbols")
            )
            .withColumn(
                "coverage_ratio", F.col("event_count") / F.col("expected_event_count")
            )
            .withColumn("window_duration", F.lit(duration))
            .withColumn("dataset_version", F.lit(dataset_version))
        )
    return _union(frames).select(
        "dataset_version", "trading_date",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "window_duration", "sector", "average_price", "minimum_price", "maximum_price",
        "total_volume", "event_count", "symbol_count", "active_symbols", "volatility",
        "expected_event_count", "coverage_ratio",
    )
