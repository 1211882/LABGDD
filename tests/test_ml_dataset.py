from pathlib import Path

import pandas as pd
import pytest

from src.ml.dataset import build_dataset, quality_report, regular_session


def _frame() -> pd.DataFrame:
    return pd.DataFrame({
        "timestamp": pd.to_datetime(["2026-01-05T14:29:00Z", "2026-01-05T14:30:00Z", "2026-01-05T20:59:00Z", "2026-01-05T21:00:00Z"], utc=True),
        "symbol": ["AAPL"] * 4,
        "open": [1.0] * 4, "high": [1.1] * 4, "low": [0.9] * 4,
        "close": [1.0] * 4, "volume": [10] * 4,
    })


def test_regular_session_uses_new_york_half_open_bounds() -> None:
    result = regular_session(_frame())
    assert list(result["timestamp"].dt.minute) == [30, 59]
    assert result["trading_date"].tolist() == ["2026-01-05", "2026-01-05"]


def test_quality_report_counts_gaps_duplicates_and_violations() -> None:
    frame = regular_session(_frame()).copy()
    frame = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)
    frame.loc[1, "high"] = 0.5
    result = quality_report(frame, {"AAPL": "Technology"})
    assert result["duplicate_identities"] == 1
    assert result["ohlc_violations"] == 1
    assert result["missing_minutes_total"] == 387
    assert result["coverage_per_sector"] == {"Technology": 3}


def test_dataset_version_cannot_be_overwritten(tmp_path: Path) -> None:
    (tmp_path / "ml_dataset_v1").mkdir()
    with pytest.raises(FileExistsError, match="Immutable dataset"):
        build_dataset(dataset_version="ml_dataset_v1", start="2025-01-01", end="2026-01-01", root=tmp_path, config=Path("unused"))
