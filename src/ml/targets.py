from __future__ import annotations

import numpy as np
import pandas as pd

HORIZONS = (5, 15, 30)
CLASS_NAMES = ("DOWN", "NEUTRAL", "UP")
CLASS_TO_INDEX = {name: index for index, name in enumerate(CLASS_NAMES)}


def add_future_targets(dataframe: pd.DataFrame, horizons: tuple[int, ...] = HORIZONS) -> pd.DataFrame:
    frame = dataframe.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    lookup = frame[["symbol", "trading_date", "timestamp", "close"]]
    for horizon in horizons:
        future = lookup.copy()
        future["timestamp"] = future["timestamp"] - pd.Timedelta(minutes=horizon)
        future = future.rename(columns={"close": f"future_close_{horizon}m"})
        frame = frame.merge(future, on=["symbol", "trading_date", "timestamp"], how="left", validate="one_to_one")
        frame[f"future_return_{horizon}m"] = frame[f"future_close_{horizon}m"] / frame["close"] - 1
    return frame


def classify_returns(values: pd.Series, threshold: float) -> pd.Series:
    result = pd.Series(pd.NA, index=values.index, dtype="string")
    result.loc[values < -threshold] = "DOWN"
    result.loc[values.between(-threshold, threshold, inclusive="both")] = "NEUTRAL"
    result.loc[values > threshold] = "UP"
    return result


def threshold_analysis(dataframe: pd.DataFrame, candidates_bps: tuple[int, ...] = (2, 5, 10)) -> dict:
    result: dict[str, dict] = {}
    for horizon in HORIZONS:
        values = dataframe[f"future_return_{horizon}m"].dropna()
        candidate = {}
        for bps in candidates_bps:
            counts = classify_returns(values, bps / 10000).value_counts(normalize=True)
            candidate[str(bps)] = {name: float(counts.get(name, 0)) for name in CLASS_NAMES}
        result[f"{horizon}m"] = {
            "mean": float(values.mean()), "std": float(values.std()),
            "quantiles": {str(key): float(value) for key, value in values.quantile([.01, .05, .25, .5, .75, .95, .99]).items()},
            "candidate_thresholds_bps": candidate,
        }
    return result


def add_classes(dataframe: pd.DataFrame, threshold_bps: int = 5) -> pd.DataFrame:
    frame = dataframe.copy()
    for horizon in HORIZONS:
        frame[f"target_{horizon}m"] = classify_returns(frame[f"future_return_{horizon}m"], threshold_bps / 10000)
    return frame
