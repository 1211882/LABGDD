from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from typing import Any

from src.models.market_bar import MarketBar

EVENT_TYPE = "market_bar"
SCHEMA_VERSION = "1"
EVENT_FIELDS = {"timestamp", "symbol", "open", "high", "low", "close", "volume"}


class MarketBarEventError(ValueError):
    """Raised when a Kafka market-bar event violates the event contract."""


def encode_market_bar(bar: MarketBar) -> bytes:
    """Validate and encode a market bar as compact UTF-8 JSON."""
    normalized = _validated_bar(bar)
    payload = {
        "timestamp": _format_timestamp(normalized.timestamp),
        "symbol": normalized.symbol,
        "open": normalized.open,
        "high": normalized.high,
        "low": normalized.low,
        "close": normalized.close,
        "volume": normalized.volume,
    }
    return json.dumps(payload, separators=(",", ":"), allow_nan=False).encode("utf-8")


def decode_market_bar(value: bytes | str) -> MarketBar:
    """Decode and validate a JSON market-bar event."""
    try:
        payload = json.loads(value)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
        raise MarketBarEventError("Event value must be valid UTF-8 JSON.") from exc
    if not isinstance(payload, dict):
        raise MarketBarEventError("Event value must be a JSON object.")

    missing = EVENT_FIELDS.difference(payload)
    if missing:
        raise MarketBarEventError(
            f"Event is missing required fields: {', '.join(sorted(missing))}."
        )

    try:
        timestamp = datetime.fromisoformat(str(payload["timestamp"]).replace("Z", "+00:00"))
        bar = MarketBar(
            timestamp=timestamp,
            symbol=str(payload["symbol"]),
            open=_number(payload["open"], "open"),
            high=_number(payload["high"], "high"),
            low=_number(payload["low"], "low"),
            close=_number(payload["close"], "close"),
            volume=_volume(payload["volume"]),
        )
    except (TypeError, ValueError) as exc:
        raise MarketBarEventError(f"Invalid market-bar field: {exc}") from exc
    return _validated_bar(bar)


def event_headers() -> list[tuple[str, bytes]]:
    return [
        ("event_type", EVENT_TYPE.encode("ascii")),
        ("schema_version", SCHEMA_VERSION.encode("ascii")),
    ]


def _validated_bar(bar: MarketBar) -> MarketBar:
    symbol = bar.symbol.strip().upper()
    if not symbol:
        raise MarketBarEventError("symbol must not be empty.")
    if bar.timestamp.tzinfo is None or bar.timestamp.utcoffset() is None:
        raise MarketBarEventError("timestamp must include a timezone.")

    try:
        prices = {
            "open": _number(bar.open, "open"),
            "high": _number(bar.high, "high"),
            "low": _number(bar.low, "low"),
            "close": _number(bar.close, "close"),
        }
        volume = _volume(bar.volume)
    except (TypeError, ValueError) as exc:
        raise MarketBarEventError(str(exc)) from exc
    if any(value < 0 for value in prices.values()):
        raise MarketBarEventError("OHLC prices must be non-negative.")
    if prices["high"] < max(prices["open"], prices["low"], prices["close"]):
        raise MarketBarEventError("high must be greater than or equal to OHLC values.")
    if prices["low"] > min(prices["open"], prices["high"], prices["close"]):
        raise MarketBarEventError("low must be less than or equal to OHLC values.")

    return MarketBar(
        timestamp=bar.timestamp.astimezone(timezone.utc),
        symbol=symbol,
        open=prices["open"],
        high=prices["high"],
        low=prices["low"],
        close=prices["close"],
        volume=volume,
    )


def _number(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{field} must be finite")
    return result


def _volume(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("volume must be an integer")
    result = int(value)
    if result != float(value) or result < 0:
        raise ValueError("volume must be a non-negative integer")
    return result


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
