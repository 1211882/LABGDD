from __future__ import annotations

import pandas as pd


def temporal_date_splits(dataframe: pd.DataFrame, train_fraction: float = .70, validation_fraction: float = .15):
    dates = sorted(dataframe["trading_date"].unique())
    train_end = max(1, int(len(dates) * train_fraction))
    validation_end = max(train_end + 1, int(len(dates) * (train_fraction + validation_fraction)))
    mapping = {date: "train" for date in dates[:train_end]}
    mapping.update({date: "validation" for date in dates[train_end:validation_end]})
    mapping.update({date: "test" for date in dates[validation_end:]})
    frame = dataframe.copy()
    frame["split"] = frame["trading_date"].map(mapping)
    metadata = {
        name: {"start": subset[0], "end": subset[-1], "trading_days": len(subset)}
        for name, subset in {
            "train": dates[:train_end], "validation": dates[train_end:validation_end], "test": dates[validation_end:]
        }.items()
    }
    return frame, metadata
