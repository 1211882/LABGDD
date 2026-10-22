from __future__ import annotations

import math
import os
import sys
from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from src.spark.streaming_analytics import (
    build_sector_analytics,
    build_symbol_analytics,
    filter_regular_session,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = SparkSession.builder.master("local[1]").appName("phase5-tests").config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _frame(spark: SparkSession):
    rows = [
        (datetime(2026, 9, 1, 13, 30), "AAPL", "Technology", 100.0, 102.0, 99.0, 101.0, 10),
        (datetime(2026, 9, 1, 13, 32), "AAPL", "Technology", 101.0, 104.0, 100.0, 103.0, 20),
        (datetime(2026, 9, 1, 13, 34), "MSFT", "Technology", 200.0, 203.0, 199.0, 202.0, 30),
        (datetime(2026, 9, 1, 13, 34), "JPM", "Financial", 50.0, 51.0, 49.0, 50.5, 40),
    ]
    return spark.createDataFrame(rows, ["timestamp", "symbol", "sector", "open", "high", "low", "close", "volume"])


def test_symbol_window_metrics_include_missing_minutes(spark: SparkSession) -> None:
    result = build_symbol_analytics(_frame(spark), ("5 minutes",), "v-test")
    row = result.filter("symbol = 'AAPL'").first()
    assert row.dataset_version == "v-test"
    assert str(row.trading_date) == "2026-09-01"
    assert row.event_count == 2
    assert row.first_price == pytest.approx(101.0)
    assert row.last_price == pytest.approx(103.0)
    assert row.minimum_price == pytest.approx(101.0)
    assert row.maximum_price == pytest.approx(103.0)
    assert row.average_price == pytest.approx(102.0)
    assert row.total_volume == 30
    assert row.window_return == pytest.approx((103.0 - 101.0) / 101.0)
    returns = [0.01, (103.0 - 101.0) / 101.0]
    expected = math.sqrt(sum((value - sum(returns) / 2) ** 2 for value in returns))
    assert row.volatility == pytest.approx(expected)


def test_sector_aggregation_keeps_sectors_separate(spark: SparkSession) -> None:
    rows = build_sector_analytics(_frame(spark), ("5 minutes",)).collect()
    by_sector = {row.sector: row for row in rows}
    assert set(by_sector) == {"Technology", "Financial"}
    assert by_sector["Technology"].event_count == 3
    assert by_sector["Technology"].symbol_count == 2
    assert by_sector["Financial"].event_count == 1
    assert by_sector["Financial"].volatility is None


def test_regular_session_filter_excludes_pre_and_post_market(spark: SparkSession) -> None:
    rows = [
        (datetime(2026, 9, 1, 13, 29), "AAPL"),
        (datetime(2026, 9, 1, 13, 30), "AAPL"),
        (datetime(2026, 9, 1, 19, 59), "AAPL"),
        (datetime(2026, 9, 1, 20, 0), "AAPL"),
    ]
    frame = spark.createDataFrame(rows, ["timestamp", "symbol"])
    assert filter_regular_session(frame).count() == 2


def test_larger_windows_expose_observed_coverage(spark: SparkSession) -> None:
    frame = _frame(spark)
    symbol_rows = build_symbol_analytics(frame, ("30 minutes", "60 minutes")).filter(
        "symbol = 'AAPL'"
    ).collect()
    by_duration = {row.window_duration: row for row in symbol_rows}
    assert by_duration["30 minutes"].window_start == datetime(2026, 9, 1, 13, 30)
    assert by_duration["60 minutes"].window_start == datetime(2026, 9, 1, 13, 0)
    assert by_duration["30 minutes"].expected_event_count == 30
    assert by_duration["60 minutes"].expected_event_count == 60
    assert by_duration["30 minutes"].event_count == 2
    assert by_duration["30 minutes"].coverage_ratio == pytest.approx(2 / 30)

    sector = build_sector_analytics(frame, ("60 minutes",)).filter(
        "sector = 'Technology'"
    ).first()
    assert sector.active_symbols == 2
    assert sector.expected_event_count == 120
    assert sector.event_count == 3
    assert sector.coverage_ratio == pytest.approx(3 / 120)
