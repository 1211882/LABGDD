import os
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from math import isclose
from typing import Any

import pytest

from labgdd.analytics.individual import (
    ReferenceBar,
    ReferenceReturn,
    ReferenceWindow,
    calculate_individual_measurements,
)
from labgdd.processing.gold_batch import transform_individual_gold

OPEN = datetime(2026, 9, 14, 13, 30, tzinfo=UTC)
MEMBER_FROM = datetime(2026, 1, 1, tzinfo=UTC)
MEMBER_TO = datetime(2027, 1, 1, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark() -> Any:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("labgdd-gold-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def canonical(spark: Any, values: list[Decimal]) -> tuple[Any, tuple[ReferenceBar, ...]]:
    closes = [Decimal("100")]
    for value in values:
        closes.append(closes[-1] * value.exp())
    rows = []
    reference = []
    for index, close in enumerate(closes):
        event = OPEN + timedelta(minutes=index)
        rows.append(
            {
                "symbol": "AAPL",
                "sector": "Technology",
                "source": "alpaca",
                "feed": "iex",
                "timeframe": "1Min",
                "adjustment": "raw",
                "session_id": "2026-09-14",
                "session_open": OPEN,
                "session_close": OPEN + timedelta(minutes=len(closes)),
                "calendar_provider": "exchange_calendars",
                "calendar_version": "4.11.2",
                "schedule_sha256": "a" * 64,
                "membership_version": "study-cohort-v1",
                "event_time": event,
                "bar_end": event + timedelta(minutes=1),
                "open": close,
                "close": close,
                "selected_observation_id": f"{index:064x}",
                "membership_valid_from": MEMBER_FROM,
                "membership_valid_to": MEMBER_TO,
                "quality_status": "canonical",
            }
        )
        reference.append(ReferenceBar(event, close, close, MEMBER_FROM, MEMBER_TO))
    return spark.createDataFrame(rows), tuple(reference)


def test_spark_matches_independent_reference_and_counts(spark: Any) -> None:
    values = [Decimal("0.01"), Decimal("-0.01"), Decimal("0.01"), Decimal("-0.01"), Decimal("0.01")]
    frame, bars = canonical(spark, values)
    actual = transform_individual_gold(frame, (5,))
    expected = calculate_individual_measurements(
        bars, OPEN, OPEN + timedelta(minutes=6), horizons=(5,)
    )
    assert actual.returns.count() == 6
    assert actual.rolling.count() == 5
    assert actual.fixed.count() == 1
    spark_rolling = actual.rolling.filter("eligible").first()
    spark_fixed = actual.fixed.filter("eligible").first()
    assert spark_rolling is not None and spark_fixed is not None
    expected_rolling = expected.rolling[-1].value
    expected_variance = expected.fixed[0].realized_variance
    assert expected_rolling is not None and expected_variance is not None
    assert isclose(
        spark_rolling.rolling_return_volatility,
        expected_rolling,
        rel_tol=1e-10,
        abs_tol=1e-12,
    )
    assert isclose(
        spark_fixed.realized_variance,
        expected_variance,
        rel_tol=1e-10,
        abs_tol=1e-12,
    )


def test_missing_bar_retains_ineligible_fixed_anchor(spark: Any) -> None:
    frame, _ = canonical(spark, [Decimal("0.01")] * 10)
    frame = frame.filter("event_time != timestamp'2026-09-14 13:32:00'")
    actual = transform_individual_gold(frame, (5,))
    fixed = actual.fixed.orderBy("window_start").collect()
    assert len(fixed) == 2
    assert fixed[0].reason == "incomplete_fixed_window"
    assert fixed[1].eligible


def _series_rows(
    symbol: str,
    session_id: str,
    opening: datetime,
    minutes: int,
    *,
    missing: set[int] | None = None,
    membership_change: int | None = None,
    constant: bool = False,
) -> tuple[list[dict[str, object]], tuple[ReferenceBar, ...]]:
    missing = missing or set()
    closing = opening + timedelta(minutes=minutes)
    rows: list[dict[str, object]] = []
    reference: list[ReferenceBar] = []
    for index in range(minutes):
        if index in missing:
            continue
        event = opening + timedelta(minutes=index)
        close = Decimal("100") if constant else Decimal("100") + Decimal(index) / Decimal("100")
        valid_from = MEMBER_FROM
        valid_to = MEMBER_TO
        if membership_change is not None:
            boundary = opening + timedelta(minutes=membership_change)
            if index < membership_change:
                valid_to = boundary
            else:
                valid_from = boundary
        rows.append(
            {
                "symbol": symbol,
                "sector": "Technology",
                "source": "alpaca",
                "feed": "iex",
                "timeframe": "1Min",
                "adjustment": "raw",
                "session_id": session_id,
                "session_open": opening,
                "session_close": closing,
                "calendar_provider": "exchange_calendars",
                "calendar_version": "4.11.2",
                "schedule_sha256": session_id.replace("-", "").ljust(64, "0"),
                "membership_version": "study-cohort-v1",
                "event_time": event,
                "bar_end": event + timedelta(minutes=1),
                "open": close,
                "close": close,
                "selected_observation_id": f"{len(rows) + 1000:064x}",
                "membership_valid_from": valid_from,
                "membership_valid_to": valid_to,
                "quality_status": "canonical",
            }
        )
        reference.append(ReferenceBar(event, close, close, valid_from, valid_to))
    return rows, tuple(reference)


def _micros(value: datetime) -> int:
    return int(value.timestamp() * 1_000_000)


def _assert_number(actual: float | None, expected: float | None) -> None:
    assert (actual is None) == (expected is None)
    if actual is not None and expected is not None:
        assert abs(actual - expected) <= 1e-12 + 1e-10 * abs(expected)


def test_record_level_parity_all_horizons_temporal_cases_and_order(spark: Any) -> None:
    specifications: tuple[tuple[str, str, datetime, int, set[int], int | None, bool], ...] = (
        ("AAPL", "2026-09-14", datetime(2026, 9, 14, 13, 30, tzinfo=UTC), 390, set(), None, True),
        ("MSFT", "2026-11-27", datetime(2026, 11, 27, 14, 30, tzinfo=UTC), 210, {20}, None, False),
        ("AAPL", "2026-11-02", datetime(2026, 11, 2, 14, 30, tzinfo=UTC), 61, {0}, None, False),
        ("MSFT", "2026-03-09", datetime(2026, 3, 9, 13, 30, tzinfo=UTC), 61, set(), 30, False),
    )
    rows: list[dict[str, object]] = []
    expected_returns: dict[tuple[str, str, int], ReferenceReturn] = {}
    expected_rolling: dict[tuple[str, str, int, int], ReferenceWindow] = {}
    expected_fixed: dict[tuple[str, str, int, int], ReferenceWindow] = {}
    for symbol, session_id, opening, minutes, missing, change, constant in specifications:
        series_rows, bars = _series_rows(
            symbol,
            session_id,
            opening,
            minutes,
            missing=missing,
            membership_change=change,
            constant=constant,
        )
        rows.extend(series_rows)
        reference = calculate_individual_measurements(
            bars, opening, opening + timedelta(minutes=minutes)
        )
        for return_item in reference.returns:
            expected_returns[(symbol, session_id, _micros(return_item.event_time))] = return_item
        for rolling_item in reference.rolling:
            expected_rolling[
                (symbol, session_id, rolling_item.horizon, _micros(rolling_item.window_end))
            ] = rolling_item
        for fixed_item in reference.fixed:
            expected_fixed[
                (symbol, session_id, fixed_item.horizon, _micros(fixed_item.window_end))
            ] = fixed_item

    from pyspark.sql import functions as F

    first = transform_individual_gold(spark.createDataFrame(rows), (5, 15, 30, 60))
    second = transform_individual_gold(spark.createDataFrame(list(reversed(rows))), (5, 15, 30, 60))
    for left, right in (
        (first.returns, second.returns),
        (first.rolling, second.rolling),
        (first.fixed, second.fixed),
    ):
        assert left.exceptAll(right).limit(1).count() == 0
        assert right.exceptAll(left).limit(1).count() == 0

    actual_returns = first.returns.select(
        "symbol",
        "session_id",
        F.unix_micros("event_time").alias("event_us"),
        F.unix_micros("return_end").alias("end_us"),
        "expected_bars",
        "observed_bars",
        "intrabar_log_return",
        "close_to_close_log_return",
        "eligible",
        "reason",
    ).collect()
    assert len(actual_returns) == len(expected_returns)
    for row in actual_returns:
        expected_return = expected_returns[(row.symbol, row.session_id, row.event_us)]
        assert row.end_us == _micros(expected_return.return_end)
        assert row.expected_bars == 2
        if expected_return.reason == "missing_close":
            expected_observed = 0
        elif expected_return.reason in {"session_boundary", "nonconsecutive_bars"}:
            expected_observed = 1
        else:
            expected_observed = 2
        assert row.observed_bars == expected_observed
        assert row.eligible == expected_return.eligible and row.reason == expected_return.reason
        _assert_number(row.intrabar_log_return, expected_return.intrabar_log_return)
        _assert_number(row.close_to_close_log_return, expected_return.close_to_close_log_return)

    actual_rolling = first.rolling.select(
        "symbol",
        "session_id",
        "horizon",
        F.unix_micros("window_start").alias("start_us"),
        F.unix_micros("window_end").alias("end_us"),
        "expected_returns",
        "observed_returns",
        "rolling_return_volatility",
        "eligible",
        "reason",
    ).collect()
    assert len(actual_rolling) == len(expected_rolling)
    for row in actual_rolling:
        expected_rolling_window = expected_rolling[
            (row.symbol, row.session_id, row.horizon, row.end_us)
        ]
        assert row.start_us == _micros(expected_rolling_window.window_start)
        assert row.expected_returns == expected_rolling_window.expected_returns
        assert row.observed_returns == expected_rolling_window.observed_returns
        assert (
            row.eligible == expected_rolling_window.eligible
            and row.reason == expected_rolling_window.reason
        )
        _assert_number(row.rolling_return_volatility, expected_rolling_window.value)

    actual_fixed = first.fixed.select(
        "symbol",
        "session_id",
        "horizon",
        F.unix_micros("window_start").alias("start_us"),
        F.unix_micros("window_end").alias("end_us"),
        "expected_returns",
        "observed_returns",
        "realized_variance",
        "realized_volatility",
        "eligible",
        "reason",
    ).collect()
    assert len(actual_fixed) == len(expected_fixed)
    for row in actual_fixed:
        expected_fixed_window = expected_fixed[
            (row.symbol, row.session_id, row.horizon, row.end_us)
        ]
        assert row.start_us == _micros(expected_fixed_window.window_start)
        assert row.expected_returns == expected_fixed_window.expected_returns
        assert row.observed_returns == expected_fixed_window.observed_returns
        assert (
            row.eligible == expected_fixed_window.eligible
            and row.reason == expected_fixed_window.reason
        )
        _assert_number(row.realized_variance, expected_fixed_window.realized_variance)
        _assert_number(row.realized_volatility, expected_fixed_window.value)

    first_eligible_h5 = min(
        (
            row
            for row in actual_rolling
            if row.symbol == "AAPL"
            and row.session_id == "2026-09-14"
            and row.horizon == 5
            and row.eligible
        ),
        key=lambda row: row.end_us,
    )
    assert first_eligible_h5.start_us == _micros(datetime(2026, 9, 14, 13, 31, tzinfo=UTC))
    assert first_eligible_h5.end_us == _micros(datetime(2026, 9, 14, 13, 36, tzinfo=UTC))
