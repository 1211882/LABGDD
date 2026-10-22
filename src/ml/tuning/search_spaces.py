from __future__ import annotations

import itertools
import random


SEARCH_SPACES = {
    "logistic": {"C": [.01, .1, .5, 1.0, 2.0, 5.0, 10.0], "class_weight": [None, "balanced"]},
    "random_forest_classifier": {"n_estimators": [100, 150, 200], "max_depth": [8, 12, 16, 24, None], "min_samples_split": [2, 5, 10, 20], "min_samples_leaf": [1, 2, 5, 10], "max_features": ["sqrt", "log2", .5, 1.0], "class_weight": [None, "balanced", "balanced_subsample"], "bootstrap": [True]},
    "ridge": {"alpha": [.0001, .001, .01, .1, 1.0, 10.0, 100.0]},
    "random_forest_regressor": {"n_estimators": [100, 150, 200], "max_depth": [8, 12, 16, 24, None], "min_samples_split": [2, 5, 10, 20], "min_samples_leaf": [1, 2, 5, 10], "max_features": ["sqrt", "log2", .5, 1.0], "bootstrap": [True]},
    "deep_classification": {"lookback": [30, 60, 90], "hidden_size": [32, 64, 128], "num_layers": [1, 2], "dropout": [0.0, .1, .2, .3], "learning_rate": [1e-4, 3e-4, 1e-3], "batch_size": [128, 256, 512], "weight_decay": [0.0, 1e-5, 1e-4], "patience": [3, 5, 8], "class_weight": [None, "balanced"]},
    "deep_regression": {"lookback": [30, 60, 90], "hidden_size": [32, 64, 128], "num_layers": [1, 2], "dropout": [0.0, .1, .2, .3], "learning_rate": [1e-4, 3e-4, 1e-3], "batch_size": [128, 256, 512], "weight_decay": [0.0, 1e-5, 1e-4], "patience": [3, 5, 8], "loss": ["mse", "huber"]},
}


def deterministic_candidates(name: str, budget: int, seed: int, *, horizon: int = 0, model_offset: int = 0) -> list[dict]:
    if name not in SEARCH_SPACES or budget < 1:
        raise ValueError(f"Invalid search request: {name=} {budget=}")
    space = SEARCH_SPACES[name]
    keys = list(space)
    combinations = [dict(zip(keys, values)) for values in itertools.product(*(space[key] for key in keys))]
    if name.startswith("deep_"):
        combinations = [item for item in combinations if item["num_layers"] > 1 or item["dropout"] == 0.0]
    rng = random.Random(seed + horizon * 101 + model_offset * 1009)
    rng.shuffle(combinations)
    return combinations[: min(budget, len(combinations))]


def validate_candidate(name: str, candidate: dict) -> None:
    expected = SEARCH_SPACES[name]
    if set(candidate) != set(expected):
        raise ValueError(f"Candidate keys do not match {name} search space")
    if any(candidate[key] not in values for key, values in expected.items()):
        raise ValueError(f"Candidate contains a value outside {name} search space")
    if name.startswith("deep_") and candidate["num_layers"] == 1 and candidate["dropout"] != 0:
        raise ValueError("One-layer recurrent candidates must use zero inter-layer dropout")
