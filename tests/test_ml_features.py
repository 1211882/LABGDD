import numpy as np
import pandas as pd

from src.ml.features import engineer_features
from src.ml.sequences import build_sequences
from src.ml.splits import temporal_date_splits
from src.ml.targets import add_classes, add_future_targets, classify_returns


def bars(minutes=50, symbol="AAPL", date="2026-01-05"):
    timestamps = pd.date_range(f"{date} 14:30:00Z", periods=minutes, freq="min")
    close = np.arange(100, 100 + minutes, dtype=float)
    return pd.DataFrame({"timestamp": timestamps, "symbol": symbol, "sector": "Technology", "trading_date": date,
        "open": close, "high": close + 1, "low": close - 1, "close": close, "volume": np.arange(1000, 1000 + minutes)})


def test_features_are_backward_looking_and_reset_by_day() -> None:
    source = pd.concat([bars(), bars(date="2026-01-06")], ignore_index=True)
    original = engineer_features(source)
    changed = source.copy()
    changed.loc[49, "close"] = 9999
    recalculated = engineer_features(changed)
    pd.testing.assert_series_equal(original.loc[:48, "sma_30"], recalculated.loc[:48, "sma_30"])
    assert pd.isna(original.loc[50, "sma_30"])
    assert original.loc[29, "sma_30"] == np.mean(np.arange(100, 130))


def test_exact_future_targets_do_not_cross_gaps_or_days() -> None:
    source = pd.concat([bars(20), bars(20, date="2026-01-06")], ignore_index=True)
    source = source.drop(index=5).reset_index(drop=True)
    result = add_future_targets(source)
    assert pd.isna(result.loc[0, "future_return_5m"])
    assert pd.isna(result.loc[result["trading_date"].eq("2026-01-05")].iloc[-1]["future_return_5m"])
    assert result.loc[1, "future_return_5m"] == source.loc[5, "close"] / source.loc[1, "close"] - 1


def test_threshold_boundaries_are_neutral() -> None:
    values = pd.Series([-0.0006, -0.0005, 0, 0.0005, 0.0006, np.nan])
    assert classify_returns(values, .0005).tolist() == ["DOWN", "NEUTRAL", "NEUTRAL", "NEUTRAL", "UP", pd.NA]


def test_temporal_splits_are_disjoint_and_ordered() -> None:
    frame = pd.concat([bars(2, date=f"2026-01-{day:02d}") for day in range(1, 11)], ignore_index=True)
    result, metadata = temporal_date_splits(frame)
    assert set(result["split"]) == {"train", "validation", "test"}
    assert metadata["train"]["end"] < metadata["validation"]["start"] < metadata["test"]["start"]


def test_sequences_preserve_contiguous_symbol_day_boundaries() -> None:
    frame = pd.concat([bars(5), bars(5, "MSFT")], ignore_index=True)
    frame["target"] = "UP"
    values, labels, indices = build_sequences(frame, ["close"], "target", 3)
    assert values.shape == (6, 3, 1)
    assert len(labels) == len(indices) == 6
    assert all(np.diff(sequence[:, 0]).tolist() == [1, 1] for sequence in values)
