from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.spark.streaming_transformations import (
    MARKET_BAR_JSON_SCHEMA,
    deduplicate_valid_stream,
    parse_kafka_market_bars,
    split_stream_records,
    validate_and_enrich_stream,
)


@pytest.fixture(scope="module")
def streaming_spark() -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("financial-market-streaming-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


KAFKA_TEST_SCHEMA = StructType(
    [
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True),
        StructField("topic", StringType(), False),
        StructField("partition", IntegerType(), False),
        StructField("offset", LongType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField(
            "headers",
            ArrayType(
                StructType(
                    [
                        StructField("key", StringType(), False),
                        StructField("value", BinaryType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)


def kafka_row(
    payload: dict[str, object] | str,
    *,
    key: str = "AAPL",
    version: str | None = "1",
    offset: int = 0,
) -> tuple:
    value = payload if isinstance(payload, str) else json.dumps(payload)
    headers = [] if version is None else [("schema_version", version.encode())]
    return (
        key.encode(),
        value.encode(),
        "market-bars-raw",
        0,
        offset,
        datetime(2026, 9, 29, 14, 31, tzinfo=timezone.utc),
        headers,
    )


def market_payload(symbol: str = "AAPL", **updates: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "timestamp": "2026-09-29T14:30:00Z",
        "symbol": symbol,
        "open": 100.0,
        "high": 102.0,
        "low": 99.0,
        "close": 101.0,
        "volume": 1200,
    }
    payload.update(updates)
    return payload


def test_market_bar_json_schema_is_explicit() -> None:
    assert MARKET_BAR_JSON_SCHEMA.fieldNames() == [
        "timestamp",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "_corrupt_record",
    ]


def test_parse_validate_reject_and_sector_enrichment(
    streaming_spark: SparkSession,
) -> None:
    missing_close = market_payload()
    missing_close.pop("close")
    rows = [
        kafka_row(market_payload(), offset=0),
        kafka_row(market_payload(high=98.0), offset=1),
        kafka_row(missing_close, offset=2),
        kafka_row(market_payload(), key="MSFT", offset=3),
        kafka_row(market_payload("UNKNOWN"), key="UNKNOWN", offset=4),
        kafka_row(market_payload(), version="2", offset=5),
        kafka_row("{not-json", offset=6),
        kafka_row(market_payload(), version=None, offset=7),
    ]
    source = streaming_spark.createDataFrame(rows, KAFKA_TEST_SCHEMA)
    parsed = parse_kafka_market_bars(source)
    validated = validate_and_enrich_stream(parsed, {"AAPL": "Technology"})
    valid, invalid = split_stream_records(validated)

    valid_rows = valid.collect()
    assert len(valid_rows) == 1
    assert valid_rows[0].sector == "Technology"
    assert valid_rows[0].schema_version == "1"
    times = valid.select(
        F.date_format("timestamp", "yyyy-MM-dd HH:mm:ss").alias("event_time"),
        F.date_format("kafka_timestamp", "yyyy-MM-dd HH:mm:ss").alias(
            "kafka_time"
        ),
    ).first()
    assert times.event_time == "2026-09-29 14:30:00"
    assert times.kafka_time == "2026-09-29 14:31:00"

    errors_by_offset = {
        row.kafka_offset: set(row.validation_errors) for row in invalid.collect()
    }
    assert "high_below_open" in errors_by_offset[1]
    assert "close_is_null" in errors_by_offset[2]
    assert "kafka_key_payload_symbol_mismatch" in errors_by_offset[3]
    assert "unknown_symbol" in errors_by_offset[4]
    assert "unsupported_schema_version" in errors_by_offset[5]
    assert "malformed_json" in errors_by_offset[6]
    assert "missing_schema_version" in errors_by_offset[7]


def test_watermark_deduplication_is_defined_on_event_time(
    streaming_spark: SparkSession,
) -> None:
    stream = (
        streaming_spark.readStream.format("rate")
        .option("rowsPerSecond", 1)
        .load()
        .selectExpr(
            "timestamp",
            "'AAPL' AS symbol",
            "'Technology' AS sector",
            "100.0 AS open",
            "101.0 AS high",
            "99.0 AS low",
            "100.5 AS close",
            "CAST(1000 AS BIGINT) AS volume",
            "'1' AS schema_version",
            "'AAPL' AS kafka_key",
            "'market-bars-raw' AS kafka_topic",
            "0 AS kafka_partition",
            "CAST(value AS BIGINT) AS kafka_offset",
            "timestamp AS kafka_timestamp",
            "current_timestamp() AS processing_time",
            "CAST(array() AS ARRAY<STRING>) AS validation_errors",
            "true AS is_valid",
        )
    )

    deduplicated = deduplicate_valid_stream(stream, "10 minutes")

    assert deduplicated.isStreaming
    logical_plan = deduplicated._jdf.queryExecution().logical().toString()
    assert "EventTimeWatermark" in logical_plan
    assert "DeduplicateWithinWatermark [symbol" in logical_plan
    assert "timestamp" in logical_plan
