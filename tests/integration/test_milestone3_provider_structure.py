"""Synthetic regression for mixed JSON numeric types observed in the live batch."""

from datetime import UTC, datetime
from decimal import Decimal

from labgdd.processing.historical import normalize_observation, parse_page


def test_mixed_integer_and_decimal_prices_in_one_provider_bar() -> None:
    # Synthetic values, not a copied real observation or credential fixture.
    response = (
        b'{"bars":{"AAPL":[{"t":"2026-09-14T13:30:00Z",'
        b'"o":150.125,"h":151,"l":149.75,"c":150,"v":10,"n":4,"vw":150.1}]},'
        b'"next_page_token":null}'
    )
    bars, token = parse_page(response)
    raw = bars["AAPL"][0]
    assert isinstance(raw, dict)
    assert isinstance(raw["o"], Decimal) and type(raw["h"]) is int
    bar = normalize_observation(raw, "AAPL", datetime(2026, 11, 3, tzinfo=UTC), "a" * 64)
    assert token is None
    assert bar.ohlcv == (Decimal("150.125"), Decimal("151"), Decimal("149.75"), Decimal("150"), 10)
    assert raw["n"] == 4 and raw["vw"] == Decimal("150.1")
    assert bar.event_time == datetime(2026, 9, 14, 13, 30, tzinfo=UTC)
    assert bar.source_event_id is None and bar.source_revision is None
