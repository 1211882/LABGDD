from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from src.kafka.event_codec import (
    EVENT_FIELDS,
    MarketBarEventError,
    decode_market_bar,
    encode_market_bar,
)
from src.models.market_bar import MarketBar


def make_bar(**overrides: object) -> MarketBar:
    values: dict[str, object] = {
        "timestamp": datetime(2026, 9, 29, 14, 30, tzinfo=timezone.utc),
        "symbol": "aapl",
        "open": 100.0,
        "high": 102.0,
        "low": 99.0,
        "close": 101.5,
        "volume": 1200,
    }
    values.update(overrides)
    return MarketBar(**values)  # type: ignore[arg-type]


def test_event_codec_round_trip_has_required_contract() -> None:
    encoded = encode_market_bar(make_bar())
    payload = json.loads(encoded)
    decoded = decode_market_bar(encoded)

    assert set(payload) == EVENT_FIELDS
    assert payload["timestamp"] == "2026-09-29T14:30:00Z"
    assert decoded.symbol == "AAPL"
    assert decoded.close == 101.5


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"timestamp": datetime(2026, 9, 29, 14, 30)}, "timezone"),
        ({"high": 98.0}, "high"),
        ({"volume": -1}, "volume"),
        ({"close": float("nan")}, "finite"),
    ],
)
def test_event_codec_rejects_invalid_bars(
    overrides: dict[str, object], message: str
) -> None:
    with pytest.raises(MarketBarEventError, match=message):
        encode_market_bar(make_bar(**overrides))


def test_event_codec_rejects_missing_fields() -> None:
    with pytest.raises(MarketBarEventError, match="volume"):
        decode_market_bar('{"timestamp":"2026-09-29T14:30:00Z","symbol":"AAPL"}')
