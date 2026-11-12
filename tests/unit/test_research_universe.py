from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from labgdd.adapters.calendar import xnys_snapshot
from labgdd.config import load_settings
from labgdd.processing.expected_series import generate_xnys_expected_series
from labgdd.research.universe import (
    CalendarProvenance,
    ResearchMembership,
    ResearchSession,
    ResearchUniverse,
    generate_expected_series,
    validate_memberships,
)
from labgdd.research_config import load_research_universe

ROOT = Path(__file__).resolve().parents[2]
WIDE_FROM = datetime(2026, 1, 1, tzinfo=UTC)
WIDE_TO = datetime(2027, 1, 1, tzinfo=UTC)


def membership(
    symbol: str,
    sector: str,
    valid_from: datetime = WIDE_FROM,
    valid_to: datetime = WIDE_TO,
) -> ResearchMembership:
    return ResearchMembership(
        symbol,
        sector,
        "study-cohort-v1",
        valid_from,
        valid_to,
        "declared_test_applicability",
        "deterministic_test_fixture",
        "declared_research_assumption",
        "unverified",
        "unverified",
        "project_defined_unverified",
        ("extended_hours",),
        ("not_historical_evidence",),
    )


def universe(*rows: ResearchMembership) -> ResearchUniverse:
    return ResearchUniverse("1.0.0", "study-cohort-v1", "study-cohort-v1", rows)


def provenance() -> CalendarProvenance:
    return CalendarProvenance(
        "exchange_calendars",
        "4.11.2",
        "XNYS",
        "America/New_York",
        "2025.2",
        "a" * 64,
        "common_regular_session_benchmark",
    )


def test_project_reference_preserves_30_symbols_and_six_groups() -> None:
    settings = load_settings(ROOT / "config/project.toml")
    loaded = load_research_universe(ROOT / "config/research_universe.toml", settings)
    assert loaded.universe.universe_version == "study-cohort-v1"
    assert len(settings.universe) == 6
    assert len(loaded.universe.memberships) == 30
    assert {row.sector for row in loaded.universe.memberships} == set(settings.universe)
    assert all(
        row.evidence_status == "declared_research_assumption" for row in loaded.universe.memberships
    )
    assert all(row.historical_listing_status == "unverified" for row in loaded.universe.memberships)
    assert all(row.entity_identity_status == "unverified" for row in loaded.universe.memberships)


def test_expected_relation_is_independent_of_absent_observations() -> None:
    settings = load_settings(ROOT / "config/project.toml")
    reference = load_research_universe(ROOT / "config/research_universe.toml", settings)
    snapshot = xnys_snapshot(date(2026, 9, 14), date(2026, 9, 14))
    plan = generate_xnys_expected_series(reference.universe, snapshot, ("2026-09-14",))
    observed_symbols: set[str] = set()
    observed_sessions: set[str] = set()
    assert observed_symbols == set() and observed_sessions == set()
    assert len(plan.rows) == 30
    assert {row.symbol for row in plan.rows} == {
        symbol for symbols in settings.universe.values() for symbol in symbols
    }
    assert plan.scheduled_sessions == ("2026-09-14",)
    assert all(row.session_open == datetime(2026, 9, 14, 13, 30, tzinfo=UTC) for row in plan.rows)
    assert all(row.session_close == datetime(2026, 9, 14, 20, 0, tzinfo=UTC) for row in plan.rows)


def test_variable_constituents_and_membership_effective_boundaries() -> None:
    opening = datetime(2026, 9, 14, 13, 30, tzinfo=UTC)
    closing = datetime(2026, 9, 14, 20, 0, tzinfo=UTC)
    session = ResearchSession("2026-09-14", opening, closing)
    rows = (
        membership("AAPL", "Technology", opening, closing),
        membership("MSFT", "Technology", WIDE_FROM, opening),
        membership("JPM", "Financials", opening, opening + timedelta(hours=1)),
        membership("NVDA", "Technology", opening + timedelta(minutes=1), closing),
    )
    plan = generate_expected_series(universe(*rows), (session,), (session.label,), provenance())
    assert [(row.symbol, row.session_membership_status) for row in plan.rows] == [
        ("AAPL", "full_session"),
        ("JPM", "partial_session"),
        ("NVDA", "partial_session"),
    ]


def test_membership_change_during_session_is_explicit() -> None:
    opening = datetime(2026, 9, 14, 13, 30, tzinfo=UTC)
    boundary = datetime(2026, 9, 14, 16, 0, tzinfo=UTC)
    closing = datetime(2026, 9, 14, 20, 0, tzinfo=UTC)
    plan = generate_expected_series(
        universe(
            membership("AAPL", "Technology", WIDE_FROM, boundary),
            membership("AAPL", "Research-Other", boundary, WIDE_TO),
        ),
        (ResearchSession("2026-09-14", opening, closing),),
        ("2026-09-14",),
        provenance(),
    )
    assert len(plan.rows) == 1
    assert plan.rows[0].sector == "Technology"
    assert plan.rows[0].session_membership_status == "membership_change_during_session"
    assert plan.rows[0].membership_change_times == (boundary,)


def test_overlapping_or_contradictory_memberships_are_rejected() -> None:
    left = membership("AAPL", "Technology", WIDE_FROM, datetime(2026, 7, 1, tzinfo=UTC))
    right = membership(
        "AAPL",
        "Research-Other",
        datetime(2026, 6, 1, tzinfo=UTC),
        WIDE_TO,
    )
    with pytest.raises(ValueError, match="overlapping membership intervals"):
        validate_memberships((left, right))


def test_holiday_dst_and_early_close_calendar_geometry() -> None:
    reference = universe(membership("AAPL", "Technology"))
    holiday = xnys_snapshot(date(2026, 9, 7), date(2026, 9, 7))
    holiday_plan = generate_xnys_expected_series(reference, holiday, ("2026-09-07",))
    assert holiday_plan.rows == () and holiday_plan.scheduled_sessions == ()

    dst = xnys_snapshot(date(2026, 3, 6), date(2026, 3, 9))
    dst_plan = generate_xnys_expected_series(reference, dst, ("2026-03-06", "2026-03-09"))
    assert [(row.session_id, row.session_open.hour) for row in dst_plan.rows] == [
        ("2026-03-06", 14),
        ("2026-03-09", 13),
    ]

    early = xnys_snapshot(date(2026, 11, 27), date(2026, 11, 27))
    early_plan = generate_xnys_expected_series(reference, early, ("2026-11-27",))
    assert early_plan.rows[0].session_open == datetime(2026, 11, 27, 14, 30, tzinfo=UTC)
    assert early_plan.rows[0].session_close == datetime(2026, 11, 27, 18, 0, tzinfo=UTC)


def test_generation_is_deterministic_across_reference_and_request_order() -> None:
    sessions = (
        ResearchSession(
            "2026-09-15",
            datetime(2026, 9, 15, 13, 30, tzinfo=UTC),
            datetime(2026, 9, 15, 20, 0, tzinfo=UTC),
        ),
        ResearchSession(
            "2026-09-14",
            datetime(2026, 9, 14, 13, 30, tzinfo=UTC),
            datetime(2026, 9, 14, 20, 0, tzinfo=UTC),
        ),
    )
    rows = (membership("MSFT", "Technology"), membership("AAPL", "Technology"))
    first = generate_expected_series(
        universe(*rows), sessions, ("2026-09-15", "2026-09-14"), provenance()
    )
    second = generate_expected_series(
        universe(*reversed(rows)),
        tuple(reversed(sessions)),
        ("2026-09-14", "2026-09-15"),
        provenance(),
    )
    assert first == second


def test_calendar_provenance_and_version_contradictions_fail_closed() -> None:
    session = ResearchSession(
        "2026-09-14",
        datetime(2026, 9, 14, 13, 30, tzinfo=UTC),
        datetime(2026, 9, 14, 20, 0, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="unsupported calendar authority"):
        generate_expected_series(
            universe(membership("AAPL", "Technology")),
            (session,),
            (session.label,),
            replace(provenance(), calendar="NOT-XNYS"),
        )
    with pytest.raises(ValueError, match="contradictory universe versions"):
        generate_expected_series(
            replace(universe(membership("AAPL", "Technology")), membership_version="other"),
            (session,),
            (session.label,),
            provenance(),
        )
    with pytest.raises(ValueError, match="contradictory membership versions"):
        generate_expected_series(
            universe(replace(membership("AAPL", "Technology"), membership_version="other")),
            (session,),
            (session.label,),
            provenance(),
        )
    with pytest.raises(ValueError, match="incomplete calendar provenance"):
        generate_expected_series(
            universe(membership("AAPL", "Technology")),
            (session,),
            (session.label,),
            replace(provenance(), schedule_sha256=""),
        )
