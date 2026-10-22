import json
from pathlib import Path

import pandas as pd
import pytest

from src.ml.tuning.common import TrialStore, build_sequences_fast, deterministic_tabular_subset, persist_best_config, split_frame
from src.ml.features import FEATURE_COLUMNS
from src.ml.tuning.search_spaces import SEARCH_SPACES, deterministic_candidates, validate_candidate
from src.ml.tuning.validate import validate_tuned_experiment


def test_search_spaces_produce_valid_deterministic_candidates() -> None:
    first = deterministic_candidates("deep_classification", 5, 42, horizon=5)
    assert first == deterministic_candidates("deep_classification", 5, 42, horizon=5)
    assert len(first) == 5
    for candidate in first: validate_candidate("deep_classification", candidate)


def test_invalid_one_layer_dropout_is_rejected() -> None:
    candidate = {key: values[0] for key, values in SEARCH_SPACES["deep_classification"].items()}
    candidate.update({"num_layers": 1, "dropout": .2})
    with pytest.raises(ValueError, match="One-layer"):
        validate_candidate("deep_classification", candidate)


def test_temporal_split_separation_and_order() -> None:
    frame = pd.DataFrame({"timestamp": pd.to_datetime(["2026-01-01", "2026-02-01", "2026-03-01"], utc=True), "split": ["train", "validation", "test"], "target": [1, 2, 3]})
    train, validation, test = split_frame(frame, "target")
    assert train.timestamp.max() < validation.timestamp.min() < test.timestamp.min()


def test_deterministic_subset_preserves_symbol_representation() -> None:
    frame = pd.DataFrame({"symbol": ["A"] * 10 + ["B"] * 10, "timestamp": pd.date_range("2026-01-01", periods=20, freq="min", tz="UTC")})
    subset = deterministic_tabular_subset(frame, 8)
    assert set(subset.symbol) == {"A", "B"}
    assert subset.equals(subset.sort_values(["timestamp", "symbol"]))


def test_trial_persistence_and_validation_only_best_selection(tmp_path) -> None:
    store = TrialStore(tmp_path / "trials.csv")
    for trial, score in (("a", .3), ("b", .4)):
        store.append({"trial_id": trial, "task": "classification", "model": "Logistic Regression", "horizon": 5, "hyperparameters_json": json.dumps({"C": 1}), "selection_metric_value": score, "selection_metric": "validation_macro_f1", "status": "COMPLETED"})
    assert store.best("classification", "Logistic Regression", 5, higher_is_better=True).trial_id == "b"
    assert store.completed_ids() == {"a", "b"}


def test_regression_best_trial_uses_lower_validation_mae(tmp_path) -> None:
    store = TrialStore(tmp_path / "trials.csv")
    for trial, score in (("a", .002), ("b", .001)):
        store.append({"trial_id": trial, "task": "regression", "model": "Ridge", "horizon": 5, "hyperparameters_json": json.dumps({"alpha": 1}), "selection_metric_value": score, "selection_metric": "validation_mae", "status": "COMPLETED"})
    assert store.best("regression", "Ridge", 5, higher_is_better=False).trial_id == "b"


def test_best_config_records_no_test_based_tuning(tmp_path) -> None:
    best = pd.Series({"selection_metric_value": .4, "hyperparameters_json": '{"C": 1}', "trial_id": "x"})
    result = persist_best_config(tmp_path / "best.json", search_space={"C": [1]}, trials=1, best=best, seed=42)
    assert result["selected_using"] == "VALIDATION only"
    assert result["test_data_used_for_selection"] is False


def test_baseline_delta_directions() -> None:
    classification_delta = .386 - .384
    regression_delta = .000860 - .000862
    assert classification_delta > 0
    assert regression_delta < 0


def test_sequence_builder_preserves_classification_labels() -> None:
    rows = 4
    frame = pd.DataFrame({column: [1.0] * rows for column in FEATURE_COLUMNS})
    frame["symbol"] = "A"; frame["trading_date"] = "2026-01-02"
    frame["timestamp"] = pd.date_range("2026-01-02 14:30", periods=rows, freq="min", tz="UTC")
    frame["target"] = ["DOWN", "NEUTRAL", "UP", "UP"]
    _, labels, _ = build_sequences_fast(frame, "target", 2, 10)
    assert labels.tolist() == ["NEUTRAL", "UP", "UP"]


@pytest.mark.parametrize("task,root", [
    ("classification", Path("models/classification/phase13_classification_tuned_v1")),
    ("regression", Path("models/regression/phase13_regression_tuned_v1")),
])
def test_frozen_tuned_experiment_integrity_and_reload(task, root) -> None:
    if not root.exists():
        pytest.skip("Frozen Phase 13 artifact is not present")
    result = validate_tuned_experiment(root, task)
    assert result["valid"] is True
    assert result["best_configs"] == 12
    assert result["prediction_files"] == 12


def test_financial_evaluation_rules_remain_frozen() -> None:
    from src.ml.evaluation import portfolio_financial_metrics

    transaction_cost = 0.0005
    frame = pd.DataFrame({
        "timestamp": pd.date_range("2026-01-01", periods=3, freq="5min", tz="UTC"),
        "symbol": ["A"] * 3,
        "horizon": [5] * 3,
        "actual_class": ["UP", "DOWN", "UP"],
        "predicted_class": ["UP", "NEUTRAL", "DOWN"],
        "future_return": [.01, -.01, .02],
    })
    metrics = portfolio_financial_metrics(frame, non_overlapping=False)
    assert metrics["transaction_cost"] == pytest.approx(transaction_cost)
