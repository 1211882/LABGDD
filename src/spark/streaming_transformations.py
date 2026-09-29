from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.kafka.event_codec import SCHEMA_VERSION
from src.spark.transformations import (
    LOGICAL_EVENT_IDENTITY,
    add_sector,
    add_validation_columns,
)

MARKET_BAR_JSON_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("open", StringType(), True),
        StructField("high", StringType(), True),
        StructField("low", StringType(), True),
        StructField("close", StringType(), True),
        StructField("volume", LongType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)

VALID_STREAM_COLUMNS = [
    "timestamp",
    "symbol",
    "sector",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "schema_version",
    "kafka_key",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "processing_time",
    "validation_errors",
    "is_valid",
]

INVALID_STREAM_COLUMNS = VALID_STREAM_COLUMNS + [
    "raw_value",
    "corrupt_record",
]


def parse_kafka_market_bars(dataframe: DataFrame) -> DataFrame:
    """Decode Kafka metadata, schema headers, and the explicit market-bar JSON schema."""
    selected = dataframe.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").cast("int").alias("kafka_partition"),
        F.col("offset").cast("long").alias("kafka_offset"),
        F.col("timestamp").cast("timestamp").alias("kafka_timestamp"),
        F.col("headers"),
    )
    parsed = selected.withColumn(
        "payload",
        F.from_json(
            "raw_value",
            MARKET_BAR_JSON_SCHEMA,
            {
                "mode": "PERMISSIVE",
                "columnNameOfCorruptRecord": "_corrupt_record",
            },
        ),
    ).withColumn(
        "schema_version",
        F.expr(
            "CAST(try_element_at(filter(headers, header -> "
            "header.key = 'schema_version'), 1).value AS STRING)"
        ),
    )

    return parsed.select(
        "raw_value",
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "schema_version",
        F.to_timestamp(F.col("payload.timestamp")).alias("timestamp"),
        F.upper(F.trim(F.col("payload.symbol"))).alias("symbol"),
        F.col("payload.open").cast("double").alias("open"),
        F.col("payload.high").cast("double").alias("high"),
        F.col("payload.low").cast("double").alias("low"),
        F.col("payload.close").cast("double").alias("close"),
        F.col("payload.volume").cast("long").alias("volume"),
        F.col("payload._corrupt_record").alias("corrupt_record"),
    )


def validate_and_enrich_stream(
    dataframe: DataFrame,
    symbol_sectors: dict[str, str],
) -> DataFrame:
    """Apply protocol, shared OHLCV, key, schema, and sector validation."""
    protocol_checks = [
        (F.col("corrupt_record").isNotNull(), "malformed_json"),
        (F.col("schema_version").isNull(), "missing_schema_version"),
        (
            F.col("schema_version").isNotNull()
            & (F.col("schema_version") != F.lit(SCHEMA_VERSION)),
            "unsupported_schema_version",
        ),
        (
            F.col("kafka_key").isNull() | (F.length(F.trim("kafka_key")) == 0),
            "kafka_key_is_null",
        ),
        (
            F.col("kafka_key").isNotNull()
            & F.col("symbol").isNotNull()
            & (F.trim(F.col("kafka_key")) != F.col("symbol")),
            "kafka_key_payload_symbol_mismatch",
        ),
    ]
    protocol_errors = F.filter(
        F.array(
            *[
                F.when(condition, F.lit(message))
                for condition, message in protocol_checks
            ]
        ),
        lambda error: error.isNotNull(),
    )

    validated = add_validation_columns(
        dataframe.withColumn("protocol_errors", protocol_errors)
    )
    enriched = add_sector(validated, symbol_sectors)
    all_errors = F.array_union(
        F.col("protocol_errors"), F.col("validation_errors")
    )
    all_errors = F.when(
        F.col("sector") == "Unknown",
        F.array_union(all_errors, F.array(F.lit("unknown_symbol"))),
    ).otherwise(all_errors)

    return (
        enriched.withColumn("validation_errors", all_errors)
        .withColumn("is_valid", F.size("validation_errors") == 0)
        .withColumn("processing_time", F.current_timestamp())
        .drop("protocol_errors")
    )


def split_stream_records(dataframe: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = dataframe.filter(F.col("is_valid")).select(*VALID_STREAM_COLUMNS)
    invalid = dataframe.filter(~F.col("is_valid")).select(*INVALID_STREAM_COLUMNS)
    return valid, invalid


def deduplicate_valid_stream(dataframe: DataFrame, watermark_delay: str) -> DataFrame:
    """Deduplicate logical events while bounding state with the event-time watermark."""
    return dataframe.withWatermark("timestamp", watermark_delay).dropDuplicatesWithinWatermark(
        LOGICAL_EVENT_IDENTITY
    )
