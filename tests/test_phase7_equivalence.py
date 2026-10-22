from __future__ import annotations

from src.equivalence.controlled_dataset import generate_bars
from src.equivalence.phase7 import _compare_rows


def test_controlled_dataset_contains_duplicates_and_bounded_disorder() -> None:
    bars = generate_bars()
    identities = [(bar.symbol, bar.timestamp) for bar in bars]
    assert len(bars) == 392
    assert len(set(identities)) == 390
    assert {bar.symbol for bar in bars} == {"AAPL", "MSFT", "JNJ", "JPM"}
    assert any(left.timestamp > right.timestamp for left, right in zip(bars, bars[1:]))


def test_comparator_classifies_small_rounding_difference() -> None:
    row = {
        "dataset_version": "phase7_controlled_v1",
        "trading_date": "2026-09-01",
        "window_start": "2026-09-01T13:30:00",
        "window_end": "2026-09-01T13:35:00",
        "window_duration": "5 minutes",
        "symbol": "AAPL",
        "sector": "Technology",
        "average_price": 100.0,
        "minimum_price": 99.0,
        "maximum_price": 101.0,
        "first_price": 99.5,
        "last_price": 100.5,
        "total_volume": 500,
        "event_count": 5,
        "window_return": 0.01,
        "volatility": 0.001,
        "expected_event_count": 5,
        "coverage_ratio": 1.0,
    }
    streaming = dict(row, average_price=100.0 + 1e-12)
    result = _compare_rows("symbol", "5 minutes", [row], [streaming])
    assert result["within_tolerance_rows"] == 1
    assert result["mismatched_rows"] == 0
    assert result["missing_rows"] == 0


def test_comparator_rejects_metric_mismatch() -> None:
    row = {
        "dataset_version": "phase7_controlled_v1",
        "trading_date": "2026-09-01",
        "window_start": "2026-09-01T13:30:00",
        "window_end": "2026-09-01T13:31:00",
        "window_duration": "1 minute",
        "sector": "Technology",
        "average_price": 100.0,
        "minimum_price": 100.0,
        "maximum_price": 100.0,
        "total_volume": 10,
        "event_count": 1,
        "symbol_count": 1,
        "active_symbols": 1,
        "volatility": None,
        "expected_event_count": 1,
        "coverage_ratio": 1.0,
    }
    result = _compare_rows("sector", "1 minute", [row], [dict(row, total_volume=11)])
    assert result["mismatched_rows"] == 1
