from __future__ import annotations

import math
import os
import sys
from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.spark.batch_analytics import (
    build_batch_sector_analytics,
    build_batch_symbol_analytics,
    prepare_analytics_source,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[1]")
        .appName("phase6-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _frame(spark: SparkSession):
    rows = [
        (datetime(2026, 9, 1, 13, 34), "AAPL", "Technology", 104.0, 106.0, 103.0, 105.0, 30),
        (datetime(2026, 9, 1, 13, 30), "AAPL", "Technology", 100.0, 102.0, 99.0, 101.0, 10),
        (datetime(2026, 9, 1, 13, 32), "AAPL", "Technology", 101.0, 104.0, 100.0, 103.0, 20),
        (datetime(2026, 9, 1, 13, 34), "MSFT", "Technology", 200.0, 203.0, 199.0, 202.0, 40),
        (datetime(2026, 9, 1, 13, 34), "JPM", "Financial", 50.0, 51.0, 49.0, 50.5, 50),
    ]
    return spark.createDataFrame(
        rows, ["timestamp", "symbol", "sector", "open", "high", "low", "close", "volume"]
    )


def test_symbol_windows_match_phase5_formulas(spark: SparkSession) -> None:
    source = prepare_analytics_source(_frame(spark), False)
    result = build_batch_symbol_analytics(
        source, ("1 minute", "5 minutes", "15 minutes"), "v-test"
    )
    durations = {row.window_duration for row in result.select("window_duration").distinct().collect()}
    assert durations == {"1 minute", "5 minutes", "15 minutes"}

    row = result.filter("symbol = 'AAPL' AND window_duration = '5 minutes'").first()
    assert row.dataset_version == "v-test"
    assert str(row.trading_date) == "2026-09-01"
    assert row.window_start == datetime(2026, 9, 1, 13, 30)
    assert row.window_end == datetime(2026, 9, 1, 13, 35)
    assert row.first_price == pytest.approx(101.0)
    assert row.last_price == pytest.approx(105.0)
    assert row.minimum_price == pytest.approx(101.0)
    assert row.maximum_price == pytest.approx(105.0)
    assert row.average_price == pytest.approx(103.0)
    assert row.total_volume == 60
    assert row.event_count == 3
    assert row.window_return == pytest.approx((105.0 - 101.0) / 101.0)
    returns = [0.01, 2.0 / 101.0, 1.0 / 104.0]
    mean = sum(returns) / len(returns)
    expected = math.sqrt(sum((value - mean) ** 2 for value in returns) / 2)
    assert row.volatility == pytest.approx(expected)


def test_sector_windows_and_single_observation_null_volatility(spark: SparkSession) -> None:
    source = prepare_analytics_source(_frame(spark), False)
    rows = build_batch_sector_analytics(source, ("5 minutes",), "v-test").collect()
    by_sector = {row.sector: row for row in rows}
    assert set(by_sector) == {"Technology", "Financial"}
    assert by_sector["Technology"].event_count == 4
    assert by_sector["Technology"].symbol_count == 2
    assert by_sector["Financial"].event_count == 1
    assert by_sector["Financial"].symbol_count == 1
    assert by_sector["Financial"].volatility is None


def test_missing_minutes_are_not_synthesized(spark: SparkSession) -> None:
    source = prepare_analytics_source(_frame(spark), False)
    row = build_batch_symbol_analytics(source, ("5 minutes",), "v-test").filter(
        "symbol = 'AAPL'"
    ).first()
    assert row.event_count == 3


def test_trading_dates_are_new_york_dates_and_remain_separate(spark: SparkSession) -> None:
    rows = [
        ("2026-09-01 03:59:00", "AAPL", "Technology", 1.0, 1.0, 1.0, 1.0, 1),
        ("2026-09-01 04:00:00", "AAPL", "Technology", 2.0, 2.0, 2.0, 2.0, 1),
    ]
    frame = spark.createDataFrame(rows, ["timestamp", "symbol", "sector", "open", "high", "low", "close", "volume"]).withColumn(
        "timestamp", F.to_timestamp("timestamp")
    )
    source = prepare_analytics_source(frame, False)
    dates = {str(row.trading_date) for row in source.select("trading_date").collect()}
    assert dates == {"2026-08-31", "2026-09-01"}
    result = build_batch_symbol_analytics(source, ("15 minutes",), "v-test")
    assert result.select("trading_date").distinct().count() == 2


def test_regular_session_filter_is_dst_aware(spark: SparkSession) -> None:
    rows = [
        ("2026-01-05 14:29:00", "winter_pre"),
        ("2026-01-05 14:30:00", "winter_open"),
        ("2026-07-06 13:30:00", "summer_open"),
        ("2026-07-06 20:00:00", "summer_close"),
    ]
    frame = (
        spark.createDataFrame(rows, ["timestamp", "event_id"])
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("open", F.lit(1.0))
        .withColumn("close", F.lit(1.0))
    )
    kept = prepare_analytics_source(frame, True).select("event_id").collect()
    assert {row.event_id for row in kept} == {"winter_open", "summer_open"}


def test_batch_larger_windows_match_streaming_coverage_definitions(spark: SparkSession) -> None:
    source = prepare_analytics_source(_frame(spark), False)
    rows = build_batch_symbol_analytics(
        source, ("30 minutes", "60 minutes"), "v-test"
    ).filter("symbol = 'AAPL'").collect()
    by_duration = {row.window_duration: row for row in rows}
    assert by_duration["30 minutes"].window_start == datetime(2026, 9, 1, 13, 30)
    assert by_duration["60 minutes"].window_start == datetime(2026, 9, 1, 13, 0)
    assert by_duration["30 minutes"].expected_event_count == 30
    assert by_duration["60 minutes"].expected_event_count == 60
    assert by_duration["30 minutes"].event_count == 3
    assert by_duration["30 minutes"].coverage_ratio == pytest.approx(3 / 30)

    sector = build_batch_sector_analytics(source, ("60 minutes",), "v-test").filter(
        "sector = 'Technology'"
    ).first()
    assert sector.active_symbols == 2
    assert sector.expected_event_count == 120
    assert sector.event_count == 4
    assert sector.coverage_ratio == pytest.approx(4 / 120)
