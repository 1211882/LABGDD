import os
import sys
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from labgdd.analytics.sector import (
    INCOMPLETE_POPULATION,
    MISSING_EXPECTED_MEMBER,
    MISSING_OBSERVED_BAR,
    OUTSIDE_ACQUISITION_SCOPE,
    UNVERIFIED_HISTORY,
    SectorMemberReturn,
    SectorPopulationMember,
    SectorThreshold,
    calculate_sector_measurements,
)
from labgdd.processing.sector_gold import (
    sector_population_schema,
    sector_return_schema,
    sector_threshold_schema,
    transform_sector_gold,
)

OPEN = datetime(2026, 9, 14, 13, 30, tzinfo=UTC)
FINGERPRINT = "a" * 64
PLAN = "b" * 64


@pytest.fixture(scope="module")
def spark() -> Any:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("labgdd-sector-gold-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def population(
    symbols: tuple[str, ...],
    *,
    minutes: int = 61,
    sector: str = "Technology",
    outside_scope: set[str] | None = None,
    verified: bool = True,
    membership_status: str = "full_session",
) -> tuple[SectorPopulationMember, ...]:
    outside_scope = outside_scope or set()
    return tuple(
        SectorPopulationMember(
            symbol,
            sector,
            "2026-09-14",
            OPEN,
            OPEN + timedelta(minutes=minutes),
            OPEN - timedelta(days=1),
            OPEN + timedelta(days=1),
            "study-cohort-v1",
            "study-cohort-v1",
            membership_status,
            "independently_verified" if verified else "declared_research_assumption",
            "verified" if verified else "unverified",
            "verified" if verified else "unverified",
            "independently_verified" if verified else "project_defined_unverified",
            symbol not in outside_scope,
            "sector-complete-population-v1",
            FINGERPRINT,
        )
        for symbol in symbols
    )


def returns(
    members: tuple[SectorPopulationMember, ...],
    values: Mapping[str, tuple[float, ...]],
    *,
    missing: dict[str, set[int]] | None = None,
) -> tuple[SectorMemberReturn, ...]:
    missing = missing or {}
    result = []
    for member in members:
        if not member.acquisition_scope:
            continue
        for minute, value in enumerate(values[member.symbol], start=2):
            if minute not in missing.get(member.symbol, set()):
                result.append(
                    SectorMemberReturn(
                        member.symbol,
                        member.sector,
                        member.session_id,
                        OPEN + timedelta(minutes=minute),
                        2,
                        True,
                        True,
                        value,
                        None,
                    )
                )
    return tuple(result)


def population_frame(spark: Any, members: tuple[SectorPopulationMember, ...]) -> Any:
    rows = [
        (
            row.symbol,
            row.sector,
            "alpaca",
            "iex",
            "1Min",
            "raw",
            row.session_id,
            row.session_open,
            row.session_close,
            row.membership_valid_from,
            row.membership_valid_to,
            row.membership_version,
            row.universe_version,
            row.session_membership_status,
            row.evidence_status,
            row.historical_listing_status,
            row.entity_identity_status,
            row.sector_classification_status,
            row.acquisition_scope,
            row.population_policy_version,
            row.research_reference_sha256,
            "synthetic_fixture",
            PLAN,
        )
        for row in members
    ]
    return spark.createDataFrame(rows, sector_population_schema())


def return_frame(spark: Any, rows: tuple[SectorMemberReturn, ...]) -> Any:
    return spark.createDataFrame(
        [
            (
                row.symbol,
                row.sector,
                row.session_id,
                row.return_end,
                row.observed_bars,
                row.canonical_valid,
                row.financial_eligible,
                row.value,
                row.reason,
            )
            for row in rows
        ],
        sector_return_schema(),
    )


def threshold_frame(spark: Any, rows: tuple[SectorThreshold, ...]) -> Any:
    return spark.createDataFrame(
        [(row.symbol, row.sector, row.horizon, row.realized_volatility_threshold) for row in rows],
        sector_threshold_schema(),
    )


def assert_number(actual: float | None, expected: float | None) -> None:
    assert (actual is None) == (expected is None)
    if actual is not None and expected is not None:
        assert abs(actual - expected) <= 1e-12 + 1e-10 * abs(expected)


def test_native_spark_matches_scalar_all_horizons_and_partition_orders(spark: Any) -> None:
    symbols = ("AAPL", "MSFT", "NVDA", "ORCL", "ADBE")
    members = population(symbols) + population(("XOM",), sector="Energy")
    values = {
        symbol: tuple(
            (symbol_index + 1) * (1 if minute % 3 else -2) * 0.0001 for minute in range(60)
        )
        for symbol_index, symbol in enumerate(symbols)
    }
    values["XOM"] = tuple((1 if minute % 2 else -1) * 0.0007 for minute in range(60))
    member_returns = returns(members, values)
    thresholds = tuple(
        SectorThreshold(member.symbol, member.sector, horizon, 0.0)
        for member in members
        for horizon in (5, 15, 30, 60)
    )
    expected = calculate_sector_measurements(members, member_returns, thresholds=thresholds)
    first = transform_sector_gold(
        population_frame(spark, members).repartition(2),
        return_frame(spark, member_returns).repartition(3),
        (5, 15, 30, 60),
        thresholds=threshold_frame(spark, thresholds),
    )
    second = transform_sector_gold(
        population_frame(spark, tuple(reversed(members))).repartition(3),
        return_frame(spark, tuple(reversed(member_returns))).repartition(2),
        (5, 15, 30, 60),
        thresholds=threshold_frame(spark, tuple(reversed(thresholds))),
    )
    assert first.windows.exceptAll(second.windows).limit(1).count() == 0
    assert second.windows.exceptAll(first.windows).limit(1).count() == 0
    from pyspark.sql import functions as F

    actual = {
        (row.sector, row.session_id, row.horizon, row.window_start_us, row.window_end_us): row
        for row in first.windows.select(
            "*",
            F.unix_micros("window_start").alias("window_start_us"),
            F.unix_micros("window_end").alias("window_end_us"),
        ).collect()
    }
    assert len(actual) == len(expected.windows) == 38
    numeric = (
        "sector_realized_variance",
        "sector_realized_volatility",
        "portfolio_realized_variance",
        "portfolio_realized_volatility",
        "mean_log_return_realized_volatility",
        "volatility_dispersion",
        "mean_pairwise_correlation",
        "volatility_concentration_hhi",
        "volatility_effective_breadth",
        "volatility_breadth",
    )
    for expected_window in expected.windows:
        row = actual[
            (
                expected_window.sector,
                expected_window.session_id,
                expected_window.horizon,
                int(expected_window.window_start.timestamp() * 1_000_000),
                int(expected_window.window_end.timestamp() * 1_000_000),
            )
        ]
        for field in numeric:
            assert_number(row[field], getattr(expected_window, field))
        assert row.expected_symbols == list(expected_window.expected_symbols)
        assert row.expected_members == expected_window.expected_members
        assert row.valid_pairs == expected_window.valid_pairs
        assert row.reason == expected_window.reason
    actual_pairs = {
        (
            row.sector,
            row.session_id,
            row.horizon,
            row.window_start_us,
            row.window_end_us,
            row.left_symbol,
            row.right_symbol,
        ): row
        for row in first.correlations.select(
            "*",
            F.unix_micros("window_start").alias("window_start_us"),
            F.unix_micros("window_end").alias("window_end_us"),
        ).collect()
    }
    assert len(actual_pairs) == len(expected.correlations)
    for expected_pair in expected.correlations:
        row = actual_pairs[
            (
                expected_pair.sector,
                expected_pair.session_id,
                expected_pair.horizon,
                int(expected_pair.window_start.timestamp() * 1_000_000),
                int(expected_pair.window_end.timestamp() * 1_000_000),
                expected_pair.left_symbol,
                expected_pair.right_symbol,
            )
        ]
        assert_number(row.value, expected_pair.value)
        assert row.eligible == expected_pair.eligible
        assert row.reason == expected_pair.reason


@pytest.mark.parametrize(
    ("case", "expected_reason"),
    (
        ("absent", MISSING_EXPECTED_MEMBER),
        ("interior", MISSING_OBSERVED_BAR),
        ("outside", OUTSIDE_ACQUISITION_SCOPE),
        ("unverified", UNVERIFIED_HISTORY),
        ("membership", INCOMPLETE_POPULATION),
    ),
)
def test_spark_missingness_and_population_reasons(
    spark: Any, case: str, expected_reason: str
) -> None:
    symbols = ("AAPL", "MSFT")
    members = population(
        symbols,
        minutes=6,
        outside_scope={"MSFT"} if case == "outside" else None,
        verified=case != "unverified",
        membership_status="membership_change_during_session"
        if case == "membership"
        else "full_session",
    )
    values = {"AAPL": (0.01,) * 5, "MSFT": (0.02,) * 5}
    member_returns = returns(
        members,
        values,
        missing={"MSFT": set(range(2, 7))}
        if case == "absent"
        else ({"MSFT": {4}} if case == "interior" else None),
    )
    result = transform_sector_gold(
        population_frame(spark, members), return_frame(spark, member_returns), (5,)
    )
    row = result.windows.first()
    assert row is not None
    assert not row.eligible and row.reason == expected_reason
    assert row.sector_realized_volatility is None
    assert row.portfolio_realized_volatility is None


def test_spark_rejects_duplicate_returns_and_wrong_types(spark: Any) -> None:
    members = population(("AAPL",), minutes=6)
    values = {"AAPL": (0.01,) * 5}
    member_returns = returns(members, values)
    duplicate = return_frame(spark, member_returns + (member_returns[0],))
    with pytest.raises(ValueError, match="duplicate individual return"):
        transform_sector_gold(population_frame(spark, members), duplicate, (5,))
    valid_frame = return_frame(spark, member_returns)
    wrong_type = valid_frame.withColumn("observed_bars", valid_frame.observed_bars.cast("long"))
    with pytest.raises(ValueError, match="invalid return schema or types"):
        transform_sector_gold(population_frame(spark, members), wrong_type, (5,))
