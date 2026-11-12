"""Narrow XNYS-calendar integration for the pure expected-series reference."""

from typing import cast

from labgdd.adapters.calendar import CalendarSnapshot
from labgdd.research.universe import (
    CalendarProvenance,
    ExpectedSeriesPlan,
    ResearchSession,
    ResearchUniverse,
    generate_expected_series,
)


def generate_xnys_expected_series(
    universe: ResearchUniverse,
    calendar: CalendarSnapshot,
    requested_sessions: tuple[str, ...],
) -> ExpectedSeriesPlan:
    metadata = calendar.metadata
    required = (
        "provider",
        "version",
        "calendar",
        "timezone",
        "tzdata_version",
        "schedule_sha256",
        "applicability",
    )
    if any(not isinstance(metadata.get(field), str) for field in required):
        raise ValueError("expected series: incomplete calendar provenance")
    provenance = CalendarProvenance(*(cast(str, metadata[field]) for field in required))
    sessions = tuple(ResearchSession(row.label, row.open, row.close) for row in calendar.sessions)
    return generate_expected_series(universe, sessions, requested_sessions, provenance)
