from __future__ import annotations

import hashlib
import json
import os
import random
import time
from pathlib import Path

import numpy as np
import pandas as pd

from src.ml.features import FEATURE_COLUMNS

SEED = 42
TRAIN_SPLIT = "train"
VALIDATION_SPLIT = "validation"
TEST_SPLIT = "test"


def set_seeds(seed: int) -> None:
    os.environ["TF_DETERMINISTIC_OPS"] = "1"
    random.seed(seed); np.random.seed(seed)


def split_frame(frame: pd.DataFrame, target: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    usable = frame.loc[frame[target].notna()]
    train = usable.loc[usable.split.eq(TRAIN_SPLIT)].copy()
    validation = usable.loc[usable.split.eq(VALIDATION_SPLIT)].copy()
    test = usable.loc[usable.split.eq(TEST_SPLIT)].copy()
    if set(train.index) & set(validation.index) or set(train.index) & set(test.index) or set(validation.index) & set(test.index):
        raise ValueError("Split row overlap")
    if train.timestamp.max() >= validation.timestamp.min() or validation.timestamp.max() >= test.timestamp.min():
        raise ValueError("Temporal split order violation")
    return train, validation, test


def deterministic_tabular_subset(frame: pd.DataFrame, cap: int) -> pd.DataFrame:
    if len(frame) <= cap: return frame.sort_values(["timestamp", "symbol"])
    allocations = frame.groupby("symbol").size(); shares = np.maximum(1, np.floor(allocations / len(frame) * cap).astype(int))
    selected = []
    for symbol, group in frame.groupby("symbol", sort=True):
        group = group.sort_values("timestamp"); count = min(int(shares[symbol]), len(group))
        selected.append(group.iloc[np.linspace(0, len(group) - 1, count, dtype=int)])
    result = pd.concat(selected).sort_values(["timestamp", "symbol"])
    if len(result) < cap:
        remaining = frame.loc[~frame.index.isin(result.index)].sort_values(["timestamp", "symbol"])
        result = pd.concat([result, remaining.iloc[np.linspace(0, len(remaining) - 1, min(cap - len(result), len(remaining)), dtype=int)]])
    return result.sort_values(["timestamp", "symbol"]).iloc[:cap]


def build_sequences_fast(frame: pd.DataFrame, target: str, lookback: int, cap: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    candidates = []
    groups = []
    for _, group in frame.sort_values(["symbol", "trading_date", "timestamp"]).groupby(["symbol", "trading_date"], sort=False):
        values = group[FEATURE_COLUMNS].to_numpy(dtype="float32")
        targets = group[target].to_numpy()
        indices = group.index.to_numpy()
        times = pd.to_datetime(group.timestamp, utc=True).to_numpy(dtype="datetime64[m]")
        groups.append((values, targets, indices))
        group_id = len(groups) - 1
        if len(group) < lookback: continue
        gaps = np.diff(times).astype("timedelta64[m]").astype(int) != 1
        run_start = np.maximum.accumulate(np.where(np.r_[True, gaps], np.arange(len(group)), 0))
        valid = np.arange(len(group)) - run_start + 1 >= lookback
        valid &= ~pd.isna(targets)
        candidates.extend((group_id, int(end)) for end in np.flatnonzero(valid))
    if len(candidates) > cap:
        candidates = [candidates[index] for index in np.linspace(0, len(candidates) - 1, cap, dtype=int)]
    x = np.empty((len(candidates), lookback, len(FEATURE_COLUMNS)), dtype="float32")
    labels = []; indices = np.empty(len(candidates), dtype="int64")
    for row, (group_id, end) in enumerate(candidates):
        values, targets, source_indices = groups[group_id]
        x[row] = values[end - lookback + 1:end + 1]; labels.append(targets[end]); indices[row] = source_indices[end]
    return x, np.asarray(labels), indices


class TrialStore:
    COLUMNS = ["trial_id", "task", "model", "horizon", "hyperparameters_json", "random_seed", "training_rows", "validation_rows", "selection_metric", "selection_metric_value", "secondary_metrics_json", "training_duration_seconds", "status", "artifact_path", "error_message"]

    def __init__(self, path: Path): self.path = path

    def completed_ids(self) -> set[str]:
        if not self.path.exists(): return set()
        return set(pd.read_csv(self.path).query("status == 'COMPLETED'").trial_id)

    def append(self, row: dict) -> None:
        record = {column: row.get(column) for column in self.COLUMNS}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        existing = pd.read_csv(self.path) if self.path.exists() else pd.DataFrame(columns=self.COLUMNS)
        existing = existing.loc[existing.trial_id.ne(record["trial_id"])]
        temporary = self.path.with_suffix(".tmp")
        pd.concat([existing, pd.DataFrame([record])], ignore_index=True).to_csv(temporary, index=False)
        temporary.replace(self.path)

    def best(self, task: str, model: str, horizon: int, *, higher_is_better: bool) -> pd.Series:
        frame = pd.read_csv(self.path)
        eligible = frame.query("task == @task and model == @model and horizon == @horizon and status == 'COMPLETED'")
        if eligible.empty: raise ValueError(f"No completed trials for {task}/{model}/{horizon}")
        index = eligible.selection_metric_value.idxmax() if higher_is_better else eligible.selection_metric_value.idxmin()
        return eligible.loc[index]


def persist_best_config(path: Path, *, search_space: dict, trials: int, best: pd.Series, seed: int) -> dict:
    result = {"search_space": search_space, "trial_count": trials, "best_validation_metric": float(best.selection_metric_value),
              "selected_parameters": json.loads(best.hyperparameters_json), "selected_trial_id": best.trial_id, "seed": seed,
              "selected_using": "VALIDATION only", "test_data_used_for_selection": False}
    path.parent.mkdir(parents=True, exist_ok=True); path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def timed(callable_):
    start = time.perf_counter(); value = callable_(); return value, time.perf_counter() - start


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""): digest.update(block)
    return digest.hexdigest()
