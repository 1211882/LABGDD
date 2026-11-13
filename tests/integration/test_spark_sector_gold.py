import os
import sys
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from labgdd.analytics.sector import (
    CONSTANT_SERIES,
    DECLARED_RESEARCH_POLICY,
    INCOMPLETE_POPULATION,
    MISSING_EXPECTED_MEMBER,
    MISSING_OBSERVED_BAR,
    OUTSIDE_ACQUISITION_SCOPE,
    SECTOR_ELIGIBILITY_POLICY_VERSION,
    UNVERIFIED_HISTORY,
    VERIFIED_HISTORICAL_POLICY,
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
    eligibility_policy: str = VERIFIED_HISTORICAL_POLICY,
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
            eligibility_policy,
            SECTOR_ELIGIBILITY_POLICY_VERSION,
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
            row.eligibility_policy,
            row.eligibility_policy_version,
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
    values: dict[str, tuple[float, ...]] = {
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
        assert row.eligibility_policy == expected_window.eligibility_policy
        assert row.eligibility_policy_version == expected_window.eligibility_policy_version
        assert row.output_identity_sha256 == expected_window.output_identity_sha256
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
        assert row.eligibility_policy == expected_pair.eligibility_policy
        assert row.eligibility_policy_version == expected_pair.eligibility_policy_version
        assert row.output_identity_sha256 == expected_pair.output_identity_sha256


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


def test_spark_constant_and_near_constant_correlation_parity_all_horizons(spark: Any) -> None:
    from math import inf, nextafter

    from pyspark.sql import functions as F

    sectors = {
        "Positive": (("POS1", "POS2"), 0.0025),
        "Negative": (("NEG1", "NEG2"), -0.0025),
        "Zero": (("ZER1", "ZER2"), 0.0),
    }
    members = tuple(
        member
        for sector, (symbols, _) in sectors.items()
        for member in population(symbols, sector=sector)
    )
    low = 1.0e-12
    high = nextafter(low, inf)
    near_members = population(("NAR1", "NAR2"), sector="Near")
    members += near_members
    values: dict[str, tuple[float, ...]] = {
        symbol: (constant,) * 60 for symbols, constant in sectors.values() for symbol in symbols
    }
    values["NAR1"] = tuple(low if index % 2 else high for index in range(60))
    values["NAR2"] = tuple(high if index % 2 else low for index in range(60))
    member_returns = returns(members, values)
    expected = calculate_sector_measurements(members, member_returns)
    first = transform_sector_gold(
        population_frame(spark, members).repartition(4),
        return_frame(spark, member_returns).repartition(2),
        (5, 15, 30, 60),
    )
    second = transform_sector_gold(
        population_frame(spark, tuple(reversed(members))).repartition(2),
        return_frame(spark, tuple(reversed(member_returns))).repartition(5),
        (5, 15, 30, 60),
    )
    assert first.windows.exceptAll(second.windows).count() == 0
    assert second.windows.exceptAll(first.windows).count() == 0
    actual = {
        (row.sector, row.horizon, row.window_start_us): row
        for row in first.windows.select(
            "*", F.unix_micros("window_start").alias("window_start_us")
        ).collect()
    }
    for scalar in expected.windows:
        row = actual[
            (scalar.sector, scalar.horizon, int(scalar.window_start.timestamp() * 1_000_000))
        ]
        assert row.output_identity_sha256 == scalar.output_identity_sha256
        if scalar.sector == "Near":
            assert row.correlation_reason is None
            assert_number(row.mean_pairwise_correlation, -1.0)
        else:
            assert row.correlation_reason == CONSTANT_SERIES
            assert row.mean_pairwise_correlation is None


def test_spark_policy_provenance_validation_and_identity(spark: Any) -> None:
    from pyspark.sql import functions as F

    symbols = ("AAPL", "MSFT")
    values = {"AAPL": (0.01, -0.01, 0.02, -0.02, 0.03), "MSFT": (0.005,) * 5}
    strict_members = population(symbols, minutes=6)
    declared_members = population(symbols, minutes=6, eligibility_policy=DECLARED_RESEARCH_POLICY)
    strict = transform_sector_gold(
        population_frame(spark, strict_members),
        return_frame(spark, returns(strict_members, values)),
        (5,),
    ).windows.first()
    declared = transform_sector_gold(
        population_frame(spark, declared_members),
        return_frame(spark, returns(declared_members, values)),
        (5,),
    ).windows.first()
    assert strict is not None and declared is not None
    assert strict.eligibility_policy == VERIFIED_HISTORICAL_POLICY
    assert declared.eligibility_policy == DECLARED_RESEARCH_POLICY
    assert strict.eligibility_policy_version == SECTOR_ELIGIBILITY_POLICY_VERSION
    assert strict.output_identity_sha256 != declared.output_identity_sha256
    unknown = population_frame(spark, strict_members).withColumn(
        "eligibility_policy", F.lit("unknown")
    )
    with pytest.raises(ValueError, match="invalid population authority"):
        transform_sector_gold(unknown, return_frame(spark, returns(strict_members, values)), (5,))
    missing = population_frame(spark, strict_members).drop("eligibility_policy")
    with pytest.raises(ValueError, match="invalid population schema or types"):
        transform_sector_gold(missing, return_frame(spark, returns(strict_members, values)), (5,))


def test_spark_threshold_validation_and_reason_precedence(spark: Any) -> None:
    from math import inf

    members = population(
        ("AAPL", "MSFT"),
        minutes=6,
        outside_scope={"MSFT"},
        verified=False,
        membership_status="membership_change_during_session",
    )
    values = {"AAPL": (0.01,) * 5, "MSFT": (0.02,) * 5}
    result = transform_sector_gold(
        population_frame(spark, members), return_frame(spark, returns(members, values)), (5,)
    )
    window = result.windows.first()
    assert window is not None
    assert window.reason == OUTSIDE_ACQUISITION_SCOPE

    complete = population(("AAPL",), minutes=6)
    complete_returns = returns(complete, {"AAPL": (0.01,) * 5})
    valid = SectorThreshold("AAPL", "Technology", 5, 0.0)
    for rows, match in (
        ((SectorThreshold("MSFT", "Technology", 5, 0.0),), "outside expected population"),
        ((valid, valid), "duplicate threshold"),
        ((valid, SectorThreshold("AAPL", "Technology", 5, 1.0)), "duplicate threshold"),
        ((SectorThreshold("AAPL", "Technology", 5, inf),), "invalid threshold"),
    ):
        with pytest.raises(ValueError, match=match):
            transform_sector_gold(
                population_frame(spark, complete),
                return_frame(spark, complete_returns),
                (5,),
                thresholds=threshold_frame(spark, rows),
            )
