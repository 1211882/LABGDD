from __future__ import annotations

import pandas as pd
import pytest

from src.processing.normalization import (
    DataQualityError,
    normalize_bars,
    validate_market_bars,
)


def test_normalize_bars_maps_alpaca_fields_and_sorts_timestamps() -> None:
    bars = [
        {"t": "2026-09-01T09:31:00Z", "o": 2, "h": 3, "l": 2, "c": 3, "v": 20},
        {"t": "2026-09-01T09:30:00Z", "o": 1, "h": 2, "l": 1, "c": 2, "v": 10},
    ]

    dataframe = normalize_bars("aapl", bars)

    assert list(dataframe.columns) == [
        "timestamp",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    assert dataframe.loc[0, "symbol"] == "AAPL"
    assert dataframe.loc[0, "open"] == 1
    assert dataframe["timestamp"].is_monotonic_increasing


def test_validate_market_bars_rejects_empty_dataset() -> None:
    dataframe = pd.DataFrame(
        columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"]
    )

    with pytest.raises(DataQualityError, match="dataset is empty"):
        validate_market_bars(dataframe)


def test_validate_market_bars_rejects_invalid_ohlc_values() -> None:
    dataframe = normalize_bars(
        "AAPL",
        [
            {
                "t": "2026-09-01T09:30:00Z",
                "o": 10,
                "h": 8,
                "l": 9,
                "c": -1,
                "v": 100,
            }
        ],
    )

    with pytest.raises(DataQualityError) as exc_info:
        validate_market_bars(dataframe)

    message = str(exc_info.value)
    assert "high < low" in message
    assert "close contains negative values" in message


def test_validate_market_bars_accepts_valid_data() -> None:
    dataframe = normalize_bars(
        "AAPL",
        [
            {"t": "2026-09-01T09:30:00Z", "o": 1, "h": 2, "l": 1, "c": 2, "v": 10},
            {"t": "2026-09-01T09:31:00Z", "o": 2, "h": 3, "l": 2, "c": 3, "v": 20},
        ],
    )

    validate_market_bars(dataframe)
