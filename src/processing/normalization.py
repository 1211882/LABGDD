from __future__ import annotations

import logging
from typing import Any

import pandas as pd

LOGGER = logging.getLogger(__name__)

REQUIRED_COLUMNS = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]


class DataQualityError(ValueError):
    """Raised when normalized market data fails validation."""


def normalize_bars(symbol: str, bars: list[dict[str, Any]]) -> pd.DataFrame:
    """Normalize Alpaca bar records into the internal market bar schema."""
    rows = [
        {
            "timestamp": bar.get("t"),
            "symbol": symbol.upper(),
            "open": bar.get("o"),
            "high": bar.get("h"),
            "low": bar.get("l"),
            "close": bar.get("c"),
            "volume": bar.get("v"),
        }
        for bar in bars
    ]

    dataframe = pd.DataFrame(rows, columns=REQUIRED_COLUMNS)
    if not dataframe.empty:
        dataframe["timestamp"] = pd.to_datetime(dataframe["timestamp"], utc=True)
        numeric_columns = ["open", "high", "low", "close", "volume"]
        dataframe[numeric_columns] = dataframe[numeric_columns].apply(
            pd.to_numeric, errors="coerce"
        )
        dataframe = dataframe.sort_values("timestamp").reset_index(drop=True)

    return dataframe


def validate_market_bars(dataframe: pd.DataFrame) -> None:
    """Validate core data-quality expectations for normalized market bars."""
    problems: list[str] = []

    if dataframe.empty:
        problems.append("dataset is empty")

    missing_columns = [col for col in REQUIRED_COLUMNS if col not in dataframe.columns]
    if missing_columns:
        problems.append(f"missing columns: {missing_columns}")

    if problems:
        _raise_quality_error(problems)

    if dataframe["timestamp"].isna().any():
        problems.append("timestamp contains null values")

    if not dataframe["timestamp"].is_monotonic_increasing:
        problems.append("timestamps are not sorted")

    price_columns = ["open", "high", "low", "close"]
    for column in price_columns + ["volume"]:
        if dataframe[column].isna().any():
            problems.append(f"{column} contains null values")

    if (dataframe["high"] < dataframe["low"]).any():
        problems.append("found records where high < low")

    for column in ["open", "close", "volume"]:
        if (dataframe[column] < 0).any():
            problems.append(f"{column} contains negative values")

    if problems:
        _raise_quality_error(problems)


def _raise_quality_error(problems: list[str]) -> None:
    for problem in problems:
        LOGGER.error("Data quality validation problem: %s", problem)
    raise DataQualityError("; ".join(problems))
