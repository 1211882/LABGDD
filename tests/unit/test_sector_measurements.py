from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from math import exp, inf, isclose, log, nextafter, sqrt
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
    _output_identity,
    calculate_sector_measurements,
)

OPEN = datetime(2026, 9, 14, 13, 30, tzinfo=UTC)
FINGERPRINT = "a" * 64


def population(
    symbols: tuple[str, ...],
    *,
    sector: str = "Technology",
    minutes: int = 61,
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


def member_returns(
    members: tuple[SectorPopulationMember, ...],
    values: Mapping[str, tuple[float, ...]],
    *,
    missing: dict[str, set[int]] | None = None,
) -> tuple[SectorMemberReturn, ...]:
    missing = missing or {}
    rows: list[SectorMemberReturn] = []
    for member in members:
        if not member.acquisition_scope:
            continue
        for index, value in enumerate(values[member.symbol], start=2):
            if index in missing.get(member.symbol, set()):
                continue
            rows.append(
                SectorMemberReturn(
                    member.symbol,
                    member.sector,
                    member.session_id,
                    member.session_open + timedelta(minutes=index),
                    2,
                    True,
                    True,
                    value,
                    None,
                )
            )
    return tuple(rows)


def test_complete_five_member_sector_all_horizons_is_deterministic() -> None:
    symbols = ("AAPL", "MSFT", "NVDA", "ORCL", "ADBE")
    members = population(symbols)
    values = {
        symbol: tuple(
            (symbol_index + 1) * (1 if minute % 2 else -1) * 0.001 for minute in range(60)
        )
        for symbol_index, symbol in enumerate(symbols)
    }
    thresholds = tuple(
        SectorThreshold(symbol, "Technology", horizon, 0.0)
        for symbol in symbols
        for horizon in (5, 15, 30, 60)
    )
    first = calculate_sector_measurements(
        members, member_returns(members, values), thresholds=thresholds
    )
    second = calculate_sector_measurements(
        tuple(reversed(members)),
        tuple(reversed(member_returns(members, values))),
        thresholds=tuple(reversed(thresholds)),
    )
    assert first == second
    assert {
        horizon: sum(row.horizon == horizon for row in first.windows) for horizon in (5, 15, 30, 60)
    } == {5: 12, 15: 4, 30: 2, 60: 1}
    assert all(row.eligible and row.expected_members == 5 for row in first.windows)
    assert all(row.volatility_breadth == 1.0 for row in first.windows)
    assert all(row.expected_pairs == 10 and row.valid_pairs == 10 for row in first.windows)


def test_exact_portfolio_is_not_mean_log_proxy() -> None:
    members = population(("AAPL", "MSFT"), minutes=6)
    values = {"AAPL": (log(2.0),) * 5, "MSFT": (log(0.5),) * 5}
    window = calculate_sector_measurements(
        members, member_returns(members, values), horizons=(5,)
    ).windows[0]
    assert window.mean_log_return_realized_volatility == pytest.approx(0.0)
    expected_portfolio = sqrt(5) * abs(log(1.25))
    assert window.portfolio_realized_volatility == pytest.approx(expected_portfolio)
    assert exp(log(1.25)) == pytest.approx(1.25)


def test_missing_whole_member_and_interior_return_are_distinct() -> None:
    members = population(("AAPL", "MSFT"), minutes=6)
    values = {"AAPL": (0.01,) * 5, "MSFT": (0.02,) * 5}
    absent = calculate_sector_measurements(
        members,
        tuple(row for row in member_returns(members, values) if row.symbol != "MSFT"),
        horizons=(5,),
    ).windows[0]
    interior = calculate_sector_measurements(
        members,
        member_returns(members, values, missing={"MSFT": {4}}),
        horizons=(5,),
    ).windows[0]
    opening = calculate_sector_measurements(
        members,
        member_returns(members, values, missing={"MSFT": {2}}),
        horizons=(5,),
    ).windows[0]
    assert absent.reason == MISSING_EXPECTED_MEMBER
    assert interior.reason == MISSING_OBSERVED_BAR
    assert opening.reason == MISSING_OBSERVED_BAR
    assert absent.sector_realized_volatility is None
    assert interior.portfolio_realized_volatility is None


def test_ineligible_individual_return_is_not_used() -> None:
    members = population(("AAPL", "MSFT"), minutes=6)
    values = {"AAPL": (0.01,) * 5, "MSFT": (0.02,) * 5}
    rows = list(member_returns(members, values))
    target = rows[-1]
    rows[-1] = SectorMemberReturn(
        target.symbol,
        target.sector,
        target.session_id,
        target.return_end,
        2,
        True,
        False,
        None,
        "incomplete_return_window",
    )
    window = calculate_sector_measurements(members, tuple(rows), horizons=(5,)).windows[0]
    assert window.reason == "ineligible_individual_return"
    assert window.financially_eligible_members == 1
    assert window.sector_realized_variance is None


def test_scope_membership_and_historical_evidence_fail_closed() -> None:
    symbols = ("AAPL", "MSFT")
    scoped = population(symbols, minutes=6, outside_scope={"MSFT"})
    values = {"AAPL": (0.01,) * 5, "MSFT": (0.02,) * 5}
    scoped_window = calculate_sector_measurements(
        scoped, member_returns(scoped, values), horizons=(5,)
    ).windows[0]
    unverified = population(symbols, minutes=6, verified=False)
    unverified_window = calculate_sector_measurements(
        unverified, member_returns(unverified, values), horizons=(5,)
    ).windows[0]
    changed = population(symbols, minutes=6, membership_status="membership_change_during_session")
    changed_window = calculate_sector_measurements(
        changed, member_returns(changed, values), horizons=(5,)
    ).windows[0]
    assert scoped_window.reason == OUTSIDE_ACQUISITION_SCOPE
    assert scoped_window.excluded_members == 1
    assert unverified_window.reason == UNVERIFIED_HISTORY
    assert changed_window.reason == INCOMPLETE_POPULATION
    declared_window = calculate_sector_measurements(
        population(symbols, minutes=6, verified=False, eligibility_policy=DECLARED_RESEARCH_POLICY),
        member_returns(
            population(
                symbols,
                minutes=6,
                verified=False,
                eligibility_policy=DECLARED_RESEARCH_POLICY,
            ),
            values,
        ),
        horizons=(5,),
    ).windows[0]
    assert declared_window.eligible


def test_constant_series_keeps_intensity_but_nulls_ratios_and_correlations() -> None:
    members = population(("AAPL", "MSFT"), minutes=6)
    values = {"AAPL": (0.0,) * 5, "MSFT": (0.0,) * 5}
    result = calculate_sector_measurements(members, member_returns(members, values), horizons=(5,))
    window = result.windows[0]
    assert window.eligible and window.sector_realized_volatility == 0.0
    assert window.volatility_dispersion == 0.0
    assert window.volatility_concentration_hhi is None
    assert window.volatility_effective_breadth is None
    assert window.mean_pairwise_correlation is None
    assert window.concentration_reason == CONSTANT_SERIES
    assert window.correlation_reason == CONSTANT_SERIES
    assert result.correlations[0].reason == CONSTANT_SERIES


def test_multiple_sectors_and_population_dispersion_fixture() -> None:
    technology = population(("AAPL", "MSFT"), minutes=6)
    energy = population(("XOM",), sector="Energy", minutes=6)
    members = technology + energy
    values = {"AAPL": (3 / sqrt(5),) * 5, "MSFT": (4 / sqrt(5),) * 5, "XOM": (0.01,) * 5}
    result = calculate_sector_measurements(members, member_returns(members, values), horizons=(5,))
    technology_window = next(row for row in result.windows if row.sector == "Technology")
    energy_window = next(row for row in result.windows if row.sector == "Energy")
    assert technology_window.sector_realized_volatility == pytest.approx(sqrt(12.5))
    assert technology_window.volatility_dispersion == pytest.approx(0.5)
    assert technology_window.volatility_concentration_hhi == pytest.approx(337 / 625)
    assert technology_window.volatility_effective_breadth == pytest.approx(576 / 625)
    assert energy_window.expected_pairs == 0
    assert energy_window.mean_pairwise_correlation is None


def test_invalid_or_duplicate_inputs_are_rejected() -> None:
    members = population(("AAPL",), minutes=6)
    values = {"AAPL": (0.01,) * 5}
    rows = member_returns(members, values)
    with pytest.raises(ValueError, match="duplicate expected member"):
        calculate_sector_measurements(members + members, rows, horizons=(5,))
    with pytest.raises(ValueError, match="duplicate member return"):
        calculate_sector_measurements(members, rows + (rows[0],), horizons=(5,))
    assert isclose(rows[0].value or 0.0, 0.01)


@pytest.mark.parametrize("constant", (0.0, 0.0025, -0.0025))
@pytest.mark.parametrize("horizon", (5, 15, 30, 60))
def test_constant_correlations_are_null_for_every_horizon(constant: float, horizon: int) -> None:
    members = population(("AAPL", "MSFT"), minutes=horizon + 1)
    values = {symbol: (constant,) * horizon for symbol in ("AAPL", "MSFT")}
    result = calculate_sector_measurements(
        members, member_returns(members, values), horizons=(horizon,)
    )
    assert len(result.windows) == 1
    assert result.windows[0].correlation_reason == CONSTANT_SERIES
    assert result.windows[0].mean_pairwise_correlation is None
    assert result.correlations[0].reason == CONSTANT_SERIES
    assert result.correlations[0].value is None


@pytest.mark.parametrize("horizon", (5, 15, 30, 60))
def test_near_constant_but_varying_correlations_remain_defined(horizon: int) -> None:
    members = population(("AAPL", "MSFT"), minutes=horizon + 1)
    low = 1.0e-12
    high = nextafter(low, inf)
    left = tuple(low if index % 2 else high for index in range(horizon))
    right = tuple(high if index % 2 else low for index in range(horizon))
    result = calculate_sector_measurements(
        members,
        member_returns(members, {"AAPL": left, "MSFT": right}),
        horizons=(horizon,),
    )
    assert result.windows[0].correlation_reason is None
    assert result.windows[0].mean_pairwise_correlation == pytest.approx(-1.0)
    assert result.correlations[0].eligible
    assert result.correlations[0].value == pytest.approx(-1.0)


def test_eligibility_policy_is_validated_and_changes_output_identity() -> None:
    symbols = ("AAPL", "MSFT")
    values = {"AAPL": (0.01, -0.01, 0.02, -0.02, 0.03), "MSFT": values_for_policy()}
    strict_members = population(symbols, minutes=6)
    declared_members = population(symbols, minutes=6, eligibility_policy=DECLARED_RESEARCH_POLICY)
    strict = calculate_sector_measurements(
        strict_members, member_returns(strict_members, values), horizons=(5,)
    ).windows[0]
    declared = calculate_sector_measurements(
        declared_members, member_returns(declared_members, values), horizons=(5,)
    ).windows[0]
    assert strict.eligibility_policy == VERIFIED_HISTORICAL_POLICY
    assert declared.eligibility_policy == DECLARED_RESEARCH_POLICY
    assert strict.eligibility_policy_version == SECTOR_ELIGIBILITY_POLICY_VERSION
    assert strict.output_identity_sha256 != declared.output_identity_sha256
    assert len(strict.output_identity_sha256) == 64
    with pytest.raises(ValueError, match="unsupported eligibility policy"):
        calculate_sector_measurements(
            tuple(replace(row, eligibility_policy="") for row in strict_members),
            member_returns(strict_members, values),
            horizons=(5,),
        )
    with pytest.raises(ValueError, match="unsupported eligibility policy"):
        calculate_sector_measurements(
            tuple(replace(row, eligibility_policy="unknown") for row in strict_members),
            member_returns(strict_members, values),
            horizons=(5,),
        )


def values_for_policy() -> tuple[float, ...]:
    return (0.005, -0.003, 0.004, -0.002, 0.001)


@pytest.mark.parametrize(
    ("field", "changed"),
    (
        ("sector", "Energy"),
        ("session_id", "2026-09-15"),
        ("horizon", 15),
        ("window_index", 1),
        ("symbols", ("AAPL", "MSFT", "NVDA")),
        ("population_policy", "different-policy-version"),
        ("eligibility_policy", "verified_historical_applicability"),
        ("eligibility_policy_version", "different-eligibility-version"),
        ("research_reference", "b" * 64),
    ),
)
def test_window_identity_binds_contract_dimensions(field: str, changed: Any) -> None:
    # Encoding sensitivity only: unsupported policies still fail public validation.
    arguments: dict[str, Any] = dict(
        sector="Technology",
        session_id="2026-09-14",
        horizon=5,
        window_index=0,
        symbols=("AAPL", "MSFT"),
        population_policy="sector-complete-population-v1",
        eligibility_policy="declared_research",
        eligibility_policy_version="sector-eligibility-policy-v1",
        research_reference="a" * 64,
    )
    original = _output_identity(**arguments)
    arguments[field] = changed
    assert _output_identity(**arguments) != original


def test_scalar_threshold_validation_matches_population_contract() -> None:
    members = population(("AAPL",), minutes=6)
    rows = member_returns(members, {"AAPL": (0.01,) * 5})
    valid = SectorThreshold("AAPL", "Technology", 5, 0.0)
    calculate_sector_measurements(members, rows, horizons=(5,), thresholds=(valid,))
    for threshold, match in (
        (SectorThreshold("MSFT", "Technology", 5, 0.0), "outside expected population"),
        (SectorThreshold("AAPL", "Energy", 5, 0.0), "outside expected population"),
        (SectorThreshold("AAPL", "Technology", 7, 0.0), "invalid threshold"),
        (SectorThreshold("AAPL", "Technology", 5, inf), "invalid threshold"),
        (SectorThreshold("AAPL", "Technology", 5, float("nan")), "invalid threshold"),
        (SectorThreshold("AAPL", "Technology", 5, -1.0), "invalid threshold"),
    ):
        with pytest.raises(ValueError, match=match):
            calculate_sector_measurements(members, rows, horizons=(5,), thresholds=(threshold,))
    with pytest.raises(ValueError, match="duplicate threshold"):
        calculate_sector_measurements(
            members,
            rows,
            horizons=(5,),
            thresholds=(valid, replace(valid, realized_volatility_threshold=1.0)),
        )


def test_reason_precedence_is_normative() -> None:
    symbols = ("AAPL", "MSFT")
    values = {"AAPL": (0.01,) * 5, "MSFT": (0.02,) * 5}
    outside = population(
        symbols,
        minutes=6,
        outside_scope={"MSFT"},
        verified=False,
        membership_status="membership_change_during_session",
    )
    assert (
        calculate_sector_measurements(outside, member_returns(outside, values), horizons=(5,))
        .windows[0]
        .reason
        == OUTSIDE_ACQUISITION_SCOPE
    )
    membership = population(
        symbols,
        minutes=6,
        verified=False,
        membership_status="membership_change_during_session",
    )
    assert (
        calculate_sector_measurements(membership, tuple(), horizons=(5,)).windows[0].reason
        == INCOMPLETE_POPULATION
    )
    unverified = population(symbols, minutes=6, verified=False)
    assert (
        calculate_sector_measurements(unverified, tuple(), horizons=(5,)).windows[0].reason
        == UNVERIFIED_HISTORY
    )
