from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from labgdd.adapters.calendar import xnys_snapshot
from labgdd.config import ConfigurationError, load_settings
from labgdd.historical_config import load_historical_request


@pytest.mark.parametrize(
    ("day", "opening", "closing"),
    [
        ("2026-03-06", "14:30", "21:00"),
        ("2026-03-09", "13:30", "20:00"),
        ("2026-10-30", "13:30", "20:00"),
        ("2026-11-02", "14:30", "21:00"),
        ("2026-11-27", "14:30", "18:00"),
    ],
)
def test_generated_dst_and_early_close(day: str, opening: str, closing: str) -> None:
    snapshot = xnys_snapshot(date.fromisoformat(day), date.fromisoformat(day))
    (session,) = snapshot.sessions
    assert session.open == datetime.fromisoformat(f"{day}T{opening}:00+00:00")
    assert session.close == datetime.fromisoformat(f"{day}T{closing}:00+00:00")
    assert snapshot.regular(session.open)
    assert snapshot.regular(session.close - timedelta(minutes=1))
    assert not snapshot.regular(session.close)
    assert not snapshot.regular(session.open - timedelta(minutes=1))


@pytest.mark.parametrize("day", ["2026-01-01", "2026-04-03", "2026-11-26", "2026-11-01"])
def test_exchange_holidays_and_weekend_have_no_expected_bars(day: str) -> None:
    assert not xnys_snapshot(date.fromisoformat(day), date.fromisoformat(day)).sessions


def test_calendar_fingerprint_repeatability_and_provenance() -> None:
    first = xnys_snapshot(date(2026, 11, 2), date(2026, 11, 3))
    second = xnys_snapshot(date(2026, 11, 2), date(2026, 11, 3))
    assert first.metadata == second.metadata
    assert first.metadata["version"] == "4.11.2"
    assert first.metadata["tzdata_version"] == "2025.2"
    assert (
        first.expected_minutes(
            datetime(2026, 11, 2, 14, 30, tzinfo=UTC), datetime(2026, 11, 2, 14, 36, tzinfo=UTC)
        )
        == 6
    )
    assert first.metadata["historical_listing_membership"] == "unverified_not_sector_enriched"


def test_configured_universe_applicability_is_not_historical_membership() -> None:
    root = Path(__file__).resolve().parents[2]
    settings = load_settings(root / "config/project.toml")
    request = load_historical_request(root / "config/historical.toml", settings)
    all_symbols = tuple(s for group in settings.universe.values() for s in group)
    replace(request, symbols=all_symbols).validate_universe(settings)
    with pytest.raises(ConfigurationError):
        replace(request, symbols=("UNKNOWN",)).validate_universe(settings)
