"""Exercise native Spark Gold transformations in the supported Linux image."""

import os
import sys
from datetime import datetime, timedelta
from decimal import Decimal

from labgdd.processing.expected_series_spark import (
    expected_plan_fingerprint,
    expected_series_frame,
)
from labgdd.processing.gold_batch import transform_individual_gold
from labgdd.research.universe import (
    CalendarProvenance,
    ResearchMembership,
    ResearchSession,
    ResearchUniverse,
    generate_expected_series,
)


def main() -> None:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("labgdd-gold-smoke")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    opening = datetime.fromisoformat("2026-09-14T13:30:00+00:00")
    closing = datetime.fromisoformat("2026-09-14T20:00:00+00:00")
    valid_from = datetime.fromisoformat("2026-01-01T00:00:00+00:00")
    valid_to = datetime.fromisoformat("2027-01-01T00:00:00+00:00")
    rows = [
        {
            "symbol": "AAPL",
            "sector": "Technology",
            "source": "alpaca",
            "feed": "iex",
            "timeframe": "1Min",
            "adjustment": "raw",
            "session_id": "2026-09-14",
            "session_open": opening,
            "session_close": closing,
            "calendar_provider": "exchange_calendars",
            "calendar_version": "4.11.2",
            "schedule_sha256": "a" * 64,
            "membership_version": "study-cohort-v1",
            "event_time": opening + timedelta(minutes=index),
            "bar_end": opening + timedelta(minutes=index + 1),
            "open": Decimal("100"),
            "close": Decimal("100"),
            "selected_observation_id": f"{index:064x}",
            "membership_valid_from": valid_from,
            "membership_valid_to": valid_to,
            "quality_status": "canonical",
        }
        for index in range(390)
    ]
    try:
        canonical = spark.createDataFrame(rows)
        result = transform_individual_gold(canonical, (5, 15, 30, 60))
        assert result.returns.count() == 390
        assert result.rolling.count() == 389 * 4
        assert result.fixed.count() == 77 + 25 + 12 + 6
        assert result.fixed.filter("not eligible").count() == 0
        memberships = tuple(
            ResearchMembership(
                symbol,
                "Technology",
                "study-cohort-v1",
                valid_from,
                valid_to,
                "declared_container_fixture",
                "container_fixture",
                "declared_research_assumption",
                "unverified",
                "unverified",
                "project_defined_unverified",
                ("extended_hours",),
                ("not_historical_evidence",),
            )
            for symbol in ("AAPL", "MSFT")
        )
        plan = generate_expected_series(
            ResearchUniverse("1.0.0", "study-cohort-v1", "study-cohort-v1", memberships),
            (ResearchSession("2026-09-14", opening, closing),),
            ("2026-09-14",),
            CalendarProvenance(
                "exchange_calendars",
                "4.11.2",
                "XNYS",
                "America/New_York",
                "2025.2",
                "a" * 64,
                "common_regular_session_benchmark",
            ),
        )
        authority = expected_series_frame(
            spark,
            plan,
            source="alpaca",
            feed="iex",
            timeframe="1Min",
            adjustment="raw",
        )
        integrated = transform_individual_gold(
            canonical,
            (5, 15, 30, 60),
            authority,
            approved_expected_plan_sha256=expected_plan_fingerprint(
                plan,
                source="alpaca",
                feed="iex",
                timeframe="1Min",
                adjustment="raw",
            ),
        )
        assert integrated.returns.count() == 780
        assert integrated.returns.filter("symbol = 'AAPL' and eligible").count() == 389
        assert (
            integrated.returns.filter("symbol = 'MSFT' and reason = 'missing_close'").count() == 390
        )
        assert integrated.rolling.count() == 389 * 4 * 2
        assert integrated.fixed.count() == (77 + 25 + 12 + 6) * 2
        print("gold container smoke passed: legacy=390/1556/120 expected=780/3112/240")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
