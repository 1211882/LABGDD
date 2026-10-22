from __future__ import annotations

import numpy as np
import pandas as pd


def build_sequences(dataframe: pd.DataFrame, feature_columns: list[str], target: str, lookback: int, max_sequences: int | None = None):
    candidates = []
    for _, group in dataframe.sort_values("timestamp").groupby(["symbol", "trading_date"], sort=False):
        group = group.reset_index()
        times = group["timestamp"]
        targets = group[target].to_numpy()
        for end in range(lookback - 1, len(group)):
            start = end - lookback + 1
            if times.iloc[end] - times.iloc[start] != pd.Timedelta(minutes=lookback - 1):
                continue
            if pd.isna(targets[end]):
                continue
            candidates.append((group, start, end))
    if max_sequences and len(candidates) > max_sequences:
        positions = np.linspace(0, len(candidates) - 1, max_sequences, dtype=int)
        candidates = [candidates[position] for position in positions]
    sequences, labels, indices = [], [], []
    for group, start, end in candidates:
        sequences.append(group.loc[start:end, feature_columns].to_numpy(dtype="float32"))
        labels.append(group.loc[end, target])
        indices.append(group.loc[end, "index"])
    return np.asarray(sequences, dtype="float32"), np.asarray(labels), np.asarray(indices)
