from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


BASE_COLUMNS = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
LOGICAL_EVENT_IDENTITY = ["symbol", "timestamp"]
PRICE_COLUMNS = ["open", "high", "low", "close"]
ROLLING_VOLATILITY_PERIODS = 30


def add_validation_columns(dataframe: DataFrame) -> DataFrame:
    """Flag data-quality failures while preserving every input record."""
    checks = [
        (F.col("timestamp").isNull(), "timestamp_is_null"),
        (F.col("symbol").isNull(), "symbol_is_null"),
        (F.col("open").isNull(), "open_is_null"),
        (F.col("high").isNull(), "high_is_null"),
        (F.col("low").isNull(), "low_is_null"),
        (F.col("close").isNull(), "close_is_null"),
        (F.col("volume").isNull(), "volume_is_null"),
        *[
            (
                F.isnan(column)
                | F.col(column).isin(float("inf"), float("-inf")),
                f"{column}_is_not_finite",
            )
            for column in PRICE_COLUMNS
        ],
        (F.col("high") < F.col("low"), "high_below_low"),
        (F.col("high") < F.col("open"), "high_below_open"),
        (F.col("high") < F.col("close"), "high_below_close"),
        (F.col("low") > F.col("open"), "low_above_open"),
        (F.col("low") > F.col("close"), "low_above_close"),
        (F.col("open") < 0, "open_is_negative"),
        (F.col("high") < 0, "high_is_negative"),
        (F.col("low") < 0, "low_is_negative"),
        (F.col("close") < 0, "close_is_negative"),
        (F.col("volume") < 0, "volume_is_negative"),
    ]
    errors = F.filter(
        F.array(*[F.when(condition, F.lit(message)) for condition, message in checks]),
        lambda error: error.isNotNull(),
    )
    return dataframe.withColumn("validation_errors", errors).withColumn(
        "is_valid", F.size("validation_errors") == 0
    )


def split_quality_records(dataframe: DataFrame) -> tuple[DataFrame, DataFrame]:
    validated = add_validation_columns(dataframe)
    return validated.filter(F.col("is_valid")), validated.filter(~F.col("is_valid"))


def deduplicate_records(dataframe: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Keep one deterministic record per symbol/timestamp and return duplicate extras."""
    identity_window = Window.partitionBy("symbol", "timestamp").orderBy(
        F.col("open").asc_nulls_last(),
        F.col("high").asc_nulls_last(),
        F.col("low").asc_nulls_last(),
        F.col("close").asc_nulls_last(),
        F.col("volume").asc_nulls_last(),
    )
    ranked = dataframe.withColumn("_duplicate_rank", F.row_number().over(identity_window))
    unique = ranked.filter(F.col("_duplicate_rank") == 1).drop("_duplicate_rank")
    duplicates = (
        ranked.filter(F.col("_duplicate_rank") > 1)
        .drop("_duplicate_rank")
        .withColumn(
            "validation_errors",
            F.array_union(
                F.col("validation_errors"), F.array(F.lit("duplicate_symbol_timestamp"))
            ),
        )
        .withColumn("is_valid", F.lit(False))
    )
    return unique, duplicates


def add_sector(dataframe: DataFrame, symbol_sectors: dict[str, str]) -> DataFrame:
    if not symbol_sectors:
        return dataframe.withColumn("sector", F.lit("Unknown"))

    mapping_items = []
    for symbol, sector in sorted(symbol_sectors.items()):
        mapping_items.extend([F.lit(symbol.upper()), F.lit(sector)])
    sector_map = F.create_map(*mapping_items)
    return dataframe.withColumn(
        "sector", F.coalesce(sector_map[F.upper(F.col("symbol"))], F.lit("Unknown"))
    )


def add_financial_features(dataframe: DataFrame) -> DataFrame:
    """Calculate per-symbol lagged and rolling financial features."""
    ordered = Window.partitionBy("symbol").orderBy("timestamp")
    result = (
        dataframe.withColumn("previous_close", F.lag("close").over(ordered))
        .withColumn("price_change", F.col("close") - F.col("previous_close"))
        .withColumn(
            "return_pct",
            F.when(
                F.col("previous_close") > 0,
                (F.col("close") - F.col("previous_close")) / F.col("previous_close"),
            ),
        )
        .withColumn(
            "log_return",
            F.when(
                (F.col("previous_close") > 0) & (F.col("close") > 0),
                F.log(F.col("close") / F.col("previous_close")),
            ),
        )
        .withColumn("previous_volume", F.lag("volume").over(ordered))
        .withColumn("volume_change", F.col("volume") - F.col("previous_volume"))
    )

    for periods in (5, 15, 30):
        rolling = ordered.rowsBetween(-(periods - 1), 0)
        result = result.withColumn(
            f"ma_{periods}",
            F.when(
                F.count("close").over(rolling) == periods,
                F.avg("close").over(rolling),
            ),
        )

    volatility_window = ordered.rowsBetween(-(ROLLING_VOLATILITY_PERIODS - 1), 0)
    return result.withColumn(
        "rolling_volatility_30",
        F.when(
            F.count("return_pct").over(volatility_window)
            == ROLLING_VOLATILITY_PERIODS,
            F.stddev_samp("return_pct").over(volatility_window),
        ),
    )
