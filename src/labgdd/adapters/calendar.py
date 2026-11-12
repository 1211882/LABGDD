"""Versioned XNYS session snapshots; imports remain outside domain."""

import hashlib
import importlib.metadata
import importlib.resources
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import cast
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class Session:
    label: str
    open: datetime
    close: datetime


@dataclass(frozen=True)
class CalendarSnapshot:
    sessions: tuple[Session, ...]
    metadata: dict[str, object]

    def regular(self, event_time: datetime) -> bool:
        return any(
            s.open <= event_time and event_time + timedelta(minutes=1) <= s.close
            for s in self.sessions
        )

    def expected_minutes(self, start: datetime, end: datetime) -> int:
        return sum(
            max(0, int((min(s.close, end) - max(s.open, start)).total_seconds() // 60))
            for s in self.sessions
        )


def xnys_snapshot(start: date, end: date) -> CalendarSnapshot:
    import exchange_calendars as calendars

    if end < start:
        raise ValueError("calendar: reversed bounds")
    # Explicit bounds avoid the provider's rolling default date range.
    calendar = calendars.get_calendar(
        "XNYS",
        start=(start - timedelta(days=7)).isoformat(),
        end=(end + timedelta(days=7)).isoformat(),
        side="left",
    )
    resource = importlib.resources.files("tzdata").joinpath("zoneinfo/America/New_York")
    timezone_bytes = resource.read_bytes()
    with resource.open("rb") as stream:
        pinned_zone = ZoneInfo.from_file(stream, key="America/New_York")
    sessions = []
    for label in calendar.sessions_in_range(start.isoformat(), end.isoformat()):
        opening = cast(datetime, calendar.session_open(label).to_pydatetime()).astimezone(UTC)
        closing = cast(datetime, calendar.session_close(label).to_pydatetime()).astimezone(UTC)
        # Verify generated UTC offsets against the pinned tzdata distribution,
        # independent of which timezone search path the library used internally.
        for instant in (opening, closing):
            if (
                instant.astimezone(calendar.tz).utcoffset()
                != instant.astimezone(pinned_zone).utcoffset()
            ):
                raise ValueError("calendar: timezone provenance mismatch")
        sessions.append(Session(str(label.date()), opening, closing))
    rows = [
        {"session": s.label, "open": s.open.isoformat(), "close": s.close.isoformat()}
        for s in sessions
    ]
    fingerprint = hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()
    return CalendarSnapshot(
        tuple(sessions),
        {
            "provider": "exchange_calendars",
            "version": importlib.metadata.version("exchange_calendars"),
            "calendar": "XNYS",
            "timezone": "America/New_York",
            "minute_side": "left",
            "tzdata_version": importlib.metadata.version("tzdata"),
            "timezone_sha256": hashlib.sha256(timezone_bytes).hexdigest(),
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "schedule_sha256": fingerprint,
            "sessions": rows,
            "override_version": "none",
            "applicability": "common_regular_session_benchmark",
            "historical_listing_membership": "unverified_not_sector_enriched",
        },
    )
