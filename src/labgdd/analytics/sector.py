"""Pure scalar reference for sector measurements in specification v2.0.0."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import combinations
from math import expm1, isfinite, log1p, sqrt

from labgdd.analytics.individual import HORIZONS

MISSING_EXPECTED_MEMBER = "missing_expected_member"
OUTSIDE_ACQUISITION_SCOPE = "member_outside_acquisition_scope"
MISSING_OBSERVED_BAR = "missing_observed_bar"
INELIGIBLE_INDIVIDUAL_RETURN = "ineligible_individual_return"
INCOMPLETE_WINDOW = "incomplete_calculation_window"
INSUFFICIENT_PAIRS = "insufficient_valid_pairs"
CONSTANT_SERIES = "constant_return_series"
UNVERIFIED_HISTORY = "unverified_historical_applicability"
INCOMPLETE_POPULATION = "incomplete_sector_population"
INCOMPLETE_CALIBRATION = "incomplete_threshold_calibration"
SECTOR_POPULATION_POLICY_VERSION = "sector-complete-population-v1"


@dataclass(frozen=True, slots=True)
class SectorPopulationMember:
    symbol: str
    sector: str
    session_id: str
    session_open: datetime
    session_close: datetime
    membership_valid_from: datetime
    membership_valid_to: datetime
    membership_version: str
    universe_version: str
    session_membership_status: str
    evidence_status: str
    historical_listing_status: str
    entity_identity_status: str
    sector_classification_status: str
    acquisition_scope: bool
    population_policy_version: str
    research_reference_sha256: str


@dataclass(frozen=True, slots=True)
class SectorMemberReturn:
    symbol: str
    sector: str
    session_id: str
    return_end: datetime
    observed_bars: int
    canonical_valid: bool
    financial_eligible: bool
    value: float | None
    reason: str | None


@dataclass(frozen=True, slots=True)
class SectorThreshold:
    symbol: str
    sector: str
    horizon: int
    realized_volatility_threshold: float


@dataclass(frozen=True, slots=True)
class SectorPairCorrelation:
    sector: str
    session_id: str
    horizon: int
    window_start: datetime
    window_end: datetime
    left_symbol: str
    right_symbol: str
    value: float | None
    eligible: bool
    reason: str | None


@dataclass(frozen=True, slots=True)
class SectorWindow:
    sector: str
    session_id: str
    horizon: int
    window_start: datetime
    window_end: datetime
    expected_symbols: tuple[str, ...]
    expected_members: int
    historically_verified_members: int
    acquisition_scope_members: int
    observed_members: int
    canonical_valid_members: int
    financially_eligible_members: int
    missing_members: int
    excluded_members: int
    expected_pairs: int
    valid_pairs: int
    membership_applicable: bool
    population_policy_version: str
    research_reference_sha256: str
    sector_realized_variance: float | None
    sector_realized_volatility: float | None
    portfolio_realized_variance: float | None
    portfolio_realized_volatility: float | None
    mean_log_return_realized_volatility: float | None
    volatility_dispersion: float | None
    mean_pairwise_correlation: float | None
    volatility_concentration_hhi: float | None
    volatility_effective_breadth: float | None
    volatility_breadth: float | None
    eligible: bool
    reason: str | None
    correlation_reason: str | None
    concentration_reason: str | None
    breadth_reason: str | None


@dataclass(frozen=True, slots=True)
class SectorMeasurements:
    windows: tuple[SectorWindow, ...]
    correlations: tuple[SectorPairCorrelation, ...]


def _utc_minute(value: datetime, field: str) -> None:
    if value.utcoffset() != UTC.utcoffset(value) or value.second or value.microsecond:
        raise ValueError(f"sector reference: {field} must be UTC minute aligned")


def _verified(member: SectorPopulationMember) -> bool:
    return (
        member.evidence_status == "independently_verified"
        and member.historical_listing_status == "verified"
        and member.entity_identity_status == "verified"
        and member.sector_classification_status == "independently_verified"
    )


def _membership_applicable(member: SectorPopulationMember) -> bool:
    return (
        member.session_membership_status == "full_session"
        and member.membership_valid_from <= member.session_open
        and member.membership_valid_to >= member.session_close
    )


def _pearson(left: tuple[float, ...], right: tuple[float, ...]) -> float | None:
    if len(left) != len(right) or len(left) < 3:
        return None
    left_mean = sum(left) / len(left)
    right_mean = sum(right) / len(right)
    left_centered = tuple(value - left_mean for value in left)
    right_centered = tuple(value - right_mean for value in right)
    left_sum = sum(value * value for value in left_centered)
    right_sum = sum(value * value for value in right_centered)
    if left_sum == 0.0 or right_sum == 0.0:
        return None
    return sum(x * y for x, y in zip(left_centered, right_centered, strict=True)) / sqrt(
        left_sum * right_sum
    )


def _validate_population(population: tuple[SectorPopulationMember, ...]) -> None:
    if not population:
        raise ValueError("sector reference: empty expected population")
    keys: set[tuple[str, str, str]] = set()
    symbol_sessions: set[tuple[str, str]] = set()
    for row in population:
        for value, field in (
            (row.session_open, "session open"),
            (row.session_close, "session close"),
            (row.membership_valid_from, "membership start"),
            (row.membership_valid_to, "membership end"),
        ):
            _utc_minute(value, field)
        texts = (
            row.symbol,
            row.sector,
            row.session_id,
            row.membership_version,
            row.universe_version,
            row.session_membership_status,
            row.evidence_status,
            row.historical_listing_status,
            row.entity_identity_status,
            row.sector_classification_status,
            row.population_policy_version,
        )
        if any(not value.strip() for value in texts):
            raise ValueError("sector reference: blank population field")
        if (
            row.session_close <= row.session_open
            or row.membership_valid_to <= row.membership_valid_from
        ):
            raise ValueError("sector reference: invalid temporal bounds")
        if row.population_policy_version != SECTOR_POPULATION_POLICY_VERSION:
            raise ValueError("sector reference: unsupported population policy")
        if len(row.research_reference_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in row.research_reference_sha256
        ):
            raise ValueError("sector reference: invalid research fingerprint")
        key = (row.symbol, row.sector, row.session_id)
        if key in keys:
            raise ValueError("sector reference: duplicate expected member")
        keys.add(key)
        symbol_session = (row.symbol, row.session_id)
        if symbol_session in symbol_sessions:
            raise ValueError("sector reference: symbol assigned to multiple sectors")
        symbol_sessions.add(symbol_session)


def _validate_returns(
    population: tuple[SectorPopulationMember, ...],
    returns: tuple[SectorMemberReturn, ...],
) -> dict[tuple[str, str, str, datetime], SectorMemberReturn]:
    population_keys = {(row.symbol, row.sector, row.session_id): row for row in population}
    by_key: dict[tuple[str, str, str, datetime], SectorMemberReturn] = {}
    for row in returns:
        _utc_minute(row.return_end, "return end")
        population_key = (row.symbol, row.sector, row.session_id)
        member = population_keys.get(population_key)
        if member is None:
            raise ValueError("sector reference: return outside expected population")
        if not member.acquisition_scope:
            raise ValueError("sector reference: observation outside acquisition scope")
        if not member.session_open + timedelta(minutes=1) < row.return_end <= member.session_close:
            raise ValueError("sector reference: return outside scheduled session")
        if row.observed_bars not in {0, 1, 2}:
            raise ValueError("sector reference: invalid observed-bar count")
        if row.canonical_valid and row.observed_bars != 2:
            raise ValueError("sector reference: canonical return requires two observed bars")
        if row.financial_eligible:
            if (
                row.observed_bars != 2
                or not row.canonical_valid
                or row.value is None
                or not isfinite(row.value)
                or row.reason is not None
            ):
                raise ValueError("sector reference: contradictory eligible return")
        elif row.value is not None or not row.reason:
            raise ValueError("sector reference: contradictory ineligible return")
        key = (*population_key, row.return_end)
        if key in by_key:
            raise ValueError("sector reference: duplicate member return")
        by_key[key] = row
    return by_key


def _primary_reason(
    members: tuple[SectorPopulationMember, ...],
    rows: dict[str, tuple[SectorMemberReturn | None, ...]],
    strict_historical: bool,
) -> str | None:
    if any(not member.acquisition_scope for member in members):
        return OUTSIDE_ACQUISITION_SCOPE
    if any(not _membership_applicable(member) for member in members):
        return INCOMPLETE_POPULATION
    if strict_historical and any(not _verified(member) for member in members):
        return UNVERIFIED_HISTORY
    if any(all(item is None for item in rows[member.symbol]) for member in members):
        return MISSING_EXPECTED_MEMBER
    if any(
        any(item is None or item.observed_bars < 2 for item in rows[member.symbol])
        for member in members
    ):
        return MISSING_OBSERVED_BAR
    if any(
        any(
            item is None or not item.canonical_valid or not item.financial_eligible
            for item in rows[member.symbol]
        )
        for member in members
    ):
        return INELIGIBLE_INDIVIDUAL_RETURN
    return None


def calculate_sector_measurements(
    population: tuple[SectorPopulationMember, ...],
    returns: tuple[SectorMemberReturn, ...],
    *,
    horizons: tuple[int, ...] = HORIZONS,
    thresholds: tuple[SectorThreshold, ...] = (),
    strict_historical: bool = True,
) -> SectorMeasurements:
    """Calculate complete-population fixed-window sector measurements without gap filling."""
    _validate_population(population)
    if (
        not horizons
        or len(horizons) != len(set(horizons))
        or any(h not in HORIZONS for h in horizons)
    ):
        raise ValueError("sector reference: unsupported or duplicate horizon")
    by_return = _validate_returns(population, returns)
    threshold_map: dict[tuple[str, str, int], float] = {}
    for threshold in thresholds:
        if (
            threshold.horizon not in HORIZONS
            or not isfinite(threshold.realized_volatility_threshold)
            or threshold.realized_volatility_threshold < 0
        ):
            raise ValueError("sector reference: invalid threshold")
        key = (threshold.symbol, threshold.sector, threshold.horizon)
        if key in threshold_map:
            raise ValueError("sector reference: duplicate threshold")
        threshold_map[key] = threshold.realized_volatility_threshold

    groups: dict[tuple[str, str], list[SectorPopulationMember]] = {}
    for member in population:
        groups.setdefault((member.sector, member.session_id), []).append(member)
    windows: list[SectorWindow] = []
    pair_rows: list[SectorPairCorrelation] = []
    for (sector, session_id), unordered_members in sorted(groups.items()):
        members = tuple(sorted(unordered_members, key=lambda item: item.symbol))
        first = members[0]
        shared = {
            (
                member.session_open,
                member.session_close,
                member.membership_version,
                member.universe_version,
                member.population_policy_version,
                member.research_reference_sha256,
            )
            for member in members
        }
        if len(shared) != 1:
            raise ValueError("sector reference: contradictory population authority")
        minutes = int((first.session_close - first.session_open).total_seconds() // 60)
        for horizon in horizons:
            for index in range((minutes - 1) // horizon):
                window_start = first.session_open + timedelta(minutes=1 + index * horizon)
                window_end = window_start + timedelta(minutes=horizon)
                ends = tuple(
                    window_start + timedelta(minutes=offset) for offset in range(1, horizon + 1)
                )
                member_rows = {
                    member.symbol: tuple(
                        by_return.get((member.symbol, sector, session_id, return_end))
                        for return_end in ends
                    )
                    for member in members
                }
                reason = _primary_reason(members, member_rows, strict_historical)
                expected = len(members)
                acquired = sum(member.acquisition_scope for member in members)
                verified = sum(_verified(member) for member in members)
                observed = sum(
                    all(
                        item is not None and item.observed_bars == 2
                        for item in member_rows[member.symbol]
                    )
                    for member in members
                )
                canonical = sum(
                    all(
                        item is not None and item.canonical_valid
                        for item in member_rows[member.symbol]
                    )
                    for member in members
                )
                eligible_members = sum(
                    all(
                        item is not None and item.financial_eligible
                        for item in member_rows[member.symbol]
                    )
                    for member in members
                )
                vectors: dict[str, tuple[float, ...]] = {}
                if reason is None:
                    for member in members:
                        vectors[member.symbol] = tuple(
                            item.value
                            for item in member_rows[member.symbol]
                            if item is not None and item.value is not None
                        )
                    if any(len(vector) != horizon for vector in vectors.values()):
                        reason = INCOMPLETE_WINDOW

                variances: dict[str, float] = {}
                sector_variance: float | None = None
                sector_volatility: float | None = None
                portfolio_variance: float | None = None
                portfolio_volatility: float | None = None
                mean_proxy_volatility: float | None = None
                dispersion: float | None = None
                hhi: float | None = None
                effective_breadth: float | None = None
                threshold_breadth: float | None = None
                concentration_reason: str | None = reason
                breadth_reason: str | None = reason
                if reason is None:
                    variances = {
                        symbol: sum(value * value for value in vector)
                        for symbol, vector in vectors.items()
                    }
                    volatilities = {symbol: sqrt(value) for symbol, value in variances.items()}
                    sector_variance = sum(variances.values()) / expected
                    sector_volatility = sqrt(sector_variance)
                    portfolio_returns = tuple(
                        log1p(sum(expm1(vectors[symbol][offset]) for symbol in vectors) / expected)
                        for offset in range(horizon)
                    )
                    portfolio_variance = sum(value * value for value in portfolio_returns)
                    portfolio_volatility = sqrt(portfolio_variance)
                    proxy_returns = tuple(
                        sum(vectors[symbol][offset] for symbol in vectors) / expected
                        for offset in range(horizon)
                    )
                    mean_proxy_volatility = sqrt(sum(value * value for value in proxy_returns))
                    mean_volatility = sum(volatilities.values()) / expected
                    dispersion = sqrt(
                        sum((value - mean_volatility) ** 2 for value in volatilities.values())
                        / expected
                    )
                    total_variance = sum(variances.values())
                    if total_variance > 0.0:
                        shares = tuple(value / total_variance for value in variances.values())
                        hhi = sum(value * value for value in shares)
                        concentration_reason = None
                        if expected >= 2:
                            effective_breadth = (1.0 - hhi) / (1.0 - 1.0 / expected)
                    else:
                        concentration_reason = CONSTANT_SERIES
                    member_thresholds = [
                        threshold_map.get((member.symbol, sector, horizon)) for member in members
                    ]
                    if all(value is not None for value in member_thresholds):
                        threshold_breadth = (
                            sum(
                                volatilities[member.symbol] > threshold
                                for member, threshold in zip(
                                    members, member_thresholds, strict=True
                                )
                                if threshold is not None
                            )
                            / expected
                        )
                        breadth_reason = None
                    else:
                        breadth_reason = INCOMPLETE_CALIBRATION

                expected_pairs = expected * (expected - 1) // 2
                valid_correlations: list[float] = []
                correlation_reason: str | None = reason
                for left, right in combinations(members, 2):
                    pair_reason = reason
                    value = None
                    if pair_reason is None:
                        value = _pearson(vectors[left.symbol], vectors[right.symbol])
                        if value is None:
                            pair_reason = CONSTANT_SERIES
                    if value is not None:
                        valid_correlations.append(value)
                    pair_rows.append(
                        SectorPairCorrelation(
                            sector,
                            session_id,
                            horizon,
                            window_start,
                            window_end,
                            left.symbol,
                            right.symbol,
                            value,
                            value is not None,
                            pair_reason,
                        )
                    )
                if reason is None:
                    if expected_pairs == 0:
                        correlation_reason = INSUFFICIENT_PAIRS
                    elif len(valid_correlations) != expected_pairs:
                        correlation_reason = CONSTANT_SERIES
                    else:
                        correlation_reason = None
                mean_correlation = (
                    sum(valid_correlations) / expected_pairs
                    if correlation_reason is None and expected_pairs
                    else None
                )
                windows.append(
                    SectorWindow(
                        sector,
                        session_id,
                        horizon,
                        window_start,
                        window_end,
                        tuple(member.symbol for member in members),
                        expected,
                        verified,
                        acquired,
                        observed,
                        canonical,
                        eligible_members,
                        expected - observed,
                        expected - acquired,
                        expected_pairs,
                        len(valid_correlations),
                        all(_membership_applicable(member) for member in members),
                        first.population_policy_version,
                        first.research_reference_sha256,
                        sector_variance,
                        sector_volatility,
                        portfolio_variance,
                        portfolio_volatility,
                        mean_proxy_volatility,
                        dispersion,
                        mean_correlation,
                        hhi,
                        effective_breadth,
                        threshold_breadth,
                        reason is None,
                        reason,
                        correlation_reason,
                        concentration_reason,
                        breadth_reason,
                    )
                )
    return SectorMeasurements(tuple(windows), tuple(pair_rows))
