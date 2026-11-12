"""Pure research-universe and expected symbol/session reference contracts."""

from dataclasses import dataclass
from datetime import UTC, datetime

from labgdd.domain.market_bar import valid_symbol


@dataclass(frozen=True, slots=True)
class ResearchMembership:
    symbol: str
    sector: str
    membership_version: str
    valid_from: datetime
    valid_to: datetime
    applicability_basis: str
    evidence_source: str
    evidence_status: str
    historical_listing_status: str
    entity_identity_status: str
    sector_classification_status: str
    exclusions: tuple[str, ...]
    limitations: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ResearchUniverse:
    contract_version: str
    universe_version: str
    membership_version: str
    memberships: tuple[ResearchMembership, ...]


@dataclass(frozen=True, slots=True)
class ResearchSession:
    label: str
    open: datetime
    close: datetime


@dataclass(frozen=True, slots=True)
class CalendarProvenance:
    provider: str
    version: str
    calendar: str
    timezone: str
    tzdata_version: str
    schedule_sha256: str
    applicability: str


@dataclass(frozen=True, slots=True)
class ExpectedSeries:
    symbol: str
    sector: str
    membership_version: str
    universe_version: str
    session_id: str
    session_open: datetime
    session_close: datetime
    membership_valid_from: datetime
    membership_valid_to: datetime
    session_membership_status: str
    membership_change_times: tuple[datetime, ...]
    applicability_basis: str
    evidence_source: str
    evidence_status: str
    historical_listing_status: str
    entity_identity_status: str
    sector_classification_status: str
    calendar_provider: str
    calendar_version: str
    calendar_name: str
    calendar_timezone: str
    tzdata_version: str
    schedule_sha256: str
    calendar_applicability: str


@dataclass(frozen=True, slots=True)
class ExpectedSeriesPlan:
    requested_sessions: tuple[str, ...]
    scheduled_sessions: tuple[str, ...]
    rows: tuple[ExpectedSeries, ...]
    calendar: CalendarProvenance


def _utc(value: datetime, field: str) -> None:
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"research universe: {field} must be UTC")


def validate_memberships(memberships: tuple[ResearchMembership, ...]) -> None:
    if not memberships:
        raise ValueError("research universe: empty membership reference")
    for row in memberships:
        _utc(row.valid_from, "membership start")
        _utc(row.valid_to, "membership end")
        if row.valid_to <= row.valid_from:
            raise ValueError("research universe: empty or reversed membership interval")
        required = (
            row.symbol,
            row.sector,
            row.membership_version,
            row.applicability_basis,
            row.evidence_source,
            row.evidence_status,
            row.historical_listing_status,
            row.entity_identity_status,
            row.sector_classification_status,
        )
        if any(not value.strip() for value in required) or not valid_symbol(row.symbol):
            raise ValueError("research universe: blank reference field")
        if row.evidence_status not in {"declared_research_assumption", "independently_verified"}:
            raise ValueError("research universe: unsupported evidence status")
        if row.historical_listing_status not in {"unverified", "verified"}:
            raise ValueError("research universe: unsupported listing status")
        if row.entity_identity_status not in {"unverified", "verified"}:
            raise ValueError("research universe: unsupported entity status")
        if row.sector_classification_status not in {
            "project_defined_unverified",
            "independently_verified",
        }:
            raise ValueError("research universe: unsupported sector status")
        if any(not value.strip() for value in (*row.exclusions, *row.limitations)):
            raise ValueError("research universe: blank exclusion or limitation")
    ordered = sorted(
        memberships,
        key=lambda row: (row.symbol, row.membership_version, row.valid_from, row.valid_to),
    )
    for left, right in zip(ordered, ordered[1:], strict=False):
        if (
            left.symbol == right.symbol
            and left.membership_version == right.membership_version
            and right.valid_from < left.valid_to
        ):
            raise ValueError("research universe: overlapping membership intervals")


def generate_expected_series(
    universe: ResearchUniverse,
    sessions: tuple[ResearchSession, ...],
    requested_sessions: tuple[str, ...],
    calendar: CalendarProvenance,
) -> ExpectedSeriesPlan:
    """Generate expected symbol/session identities without observed market data."""
    validate_memberships(universe.memberships)
    if universe.contract_version != "1.0.0":
        raise ValueError("research universe: unsupported contract version")
    if universe.membership_version != universe.universe_version:
        raise ValueError("research universe: contradictory universe versions")
    if any(row.membership_version != universe.membership_version for row in universe.memberships):
        raise ValueError("research universe: contradictory membership versions")
    if len(requested_sessions) != len(set(requested_sessions)):
        raise ValueError("research universe: duplicate requested session")
    calendar_fields = (
        calendar.provider,
        calendar.version,
        calendar.calendar,
        calendar.timezone,
        calendar.tzdata_version,
        calendar.schedule_sha256,
        calendar.applicability,
    )
    if any(not value.strip() for value in calendar_fields):
        raise ValueError("research universe: incomplete calendar provenance")
    if calendar.calendar != "XNYS" or calendar.timezone != "America/New_York":
        raise ValueError("research universe: unsupported calendar authority")
    session_by_label: dict[str, ResearchSession] = {}
    for session in sessions:
        _utc(session.open, "session open")
        _utc(session.close, "session close")
        if session.close <= session.open or session.label in session_by_label:
            raise ValueError("research universe: invalid or duplicate calendar session")
        session_by_label[session.label] = session
    scheduled = tuple(sorted(label for label in requested_sessions if label in session_by_label))
    rows: list[ExpectedSeries] = []
    by_symbol: dict[str, list[ResearchMembership]] = {}
    for membership in universe.memberships:
        if membership.membership_version == universe.membership_version:
            by_symbol.setdefault(membership.symbol, []).append(membership)
    for label in scheduled:
        session = session_by_label[label]
        for symbol in sorted(by_symbol):
            candidates = sorted(
                (
                    row
                    for row in by_symbol[symbol]
                    if row.valid_from < session.close and session.open < row.valid_to
                ),
                key=lambda row: row.valid_from,
            )
            opening = next(
                (row for row in candidates if row.valid_from <= session.open < row.valid_to),
                None,
            )
            if not candidates:
                continue
            selected = opening or candidates[0]
            changes = tuple(
                row.valid_from
                for row in candidates
                if row is not selected and session.open < row.valid_from < session.close
            )
            if opening is None:
                status = "partial_session" if not changes else "membership_change_during_session"
            elif changes:
                status = "membership_change_during_session"
            elif selected.valid_to < session.close:
                status = "partial_session"
            else:
                status = "full_session"
            rows.append(
                ExpectedSeries(
                    symbol,
                    selected.sector,
                    selected.membership_version,
                    universe.universe_version,
                    session.label,
                    session.open,
                    session.close,
                    selected.valid_from,
                    selected.valid_to,
                    status,
                    changes,
                    selected.applicability_basis,
                    selected.evidence_source,
                    selected.evidence_status,
                    selected.historical_listing_status,
                    selected.entity_identity_status,
                    selected.sector_classification_status,
                    calendar.provider,
                    calendar.version,
                    calendar.calendar,
                    calendar.timezone,
                    calendar.tzdata_version,
                    calendar.schedule_sha256,
                    calendar.applicability,
                )
            )
    return ExpectedSeriesPlan(
        tuple(sorted(requested_sessions)),
        scheduled,
        tuple(rows),
        calendar,
    )
