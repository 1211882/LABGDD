from datetime import UTC, datetime, timedelta
from decimal import Decimal

from scripts.validate_gold_evidence import _reference_maps


def test_reference_maps_treat_event_time_as_unique_within_each_symbol() -> None:
    session_open = datetime(2026, 9, 14, 13, 30, tzinfo=UTC)
    session_close = session_open + timedelta(minutes=1)
    rows = [
        {
            "symbol": symbol,
            "source": "alpaca",
            "feed": "iex",
            "timeframe": "1Min",
            "adjustment": "raw",
            "session_id": "2026-09-14",
            "event_time": session_open,
            "session_open": session_open,
            "session_close": session_close,
            "open": Decimal("100"),
            "close": Decimal("101"),
            "membership_valid_from": session_open,
            "membership_valid_to": session_close,
        }
        for symbol in ("AAPL", "MSFT")
    ]
    returns, rolling, fixed = _reference_maps(rows)
    assert len(returns) == 2
    assert {key[0][0] for key in returns} == {"AAPL", "MSFT"}
    assert len(rolling) == 0
    assert len(fixed) == 0
