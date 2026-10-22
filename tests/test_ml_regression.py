import hashlib
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pytest

from src.ml.regression_evaluation import error_distribution, regression_financial_metrics, regression_metrics, return_magnitude_bucket
from src.ml.targets import classify_returns


def test_regression_metric_formulas() -> None:
    actual = np.array([.01, -.01, 0.0])
    predicted = np.array([.008, -.012, 0.0])
    result = regression_metrics(actual, predicted)
    assert np.isclose(result["mae"], .004 / 3)
    assert np.isclose(result["mse"], 8e-6 / 3)
    assert np.isclose(result["rmse"], np.sqrt(8e-6 / 3))
    assert result["directional_accuracy"] == 1.0
    assert np.isclose(error_distribution(actual, predicted)["median_error"], -.002)


def test_zero_and_train_mean_baselines_do_not_use_test_statistics() -> None:
    train = pd.Series([.01, .03])
    test = pd.Series([1.0, 2.0])
    zero = np.zeros(len(test))
    mean = np.full(len(test), train.mean())
    assert zero.tolist() == [0.0, 0.0]
    assert mean.tolist() == [.02, .02]
    assert regression_metrics(test, mean)["pearson_correlation"] == 0.0
    assert regression_metrics(test, mean)["spearman_correlation"] == 0.0


def test_return_to_class_uses_same_five_basis_point_boundaries() -> None:
    result = classify_returns(pd.Series([-.0006, -.0005, .0005, .0006]), .0005)
    assert result.tolist() == ["DOWN", "NEUTRAL", "NEUTRAL", "UP"]


def test_return_magnitude_buckets() -> None:
    values = pd.Series([0, .0007, .002, .004, .006])
    assert return_magnitude_bucket(values).astype(str).tolist() == ["<5bps", "5-10bps", "10-25bps", "25-50bps", ">50bps"]


def test_regression_non_overlapping_financial_grid() -> None:
    frame = pd.DataFrame({"timestamp": pd.date_range("2026-01-05 14:30", periods=10, freq="min", tz="UTC"), "symbol": ["AAPL"] * 10, "predicted_return": [.001] * 10, "actual_return": [.001] * 10, "horizon": [5] * 10})
    assert regression_financial_metrics(frame)["signals"] == 10
    result = regression_financial_metrics(frame, non_overlapping=True)
    assert result["signals"] == 2
    assert result["overlapping_horizons"] is False


def test_frozen_regression_manifest_integrity_and_tabular_loading() -> None:
    root = Path("models/regression/phase11_regression_final_v1")
    if not root.exists():
        pytest.skip("Frozen local Phase 11 artifacts are not present")
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    for name, expected in manifest["checksums"].items():
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == expected
    assert joblib.load(root / "models" / "linear_regression_5m.joblib").n_features_in_ == 37
    assert joblib.load(root / "models" / "random_forest_regressor_5m.joblib").n_features_in_ == 37


def test_frozen_regression_neural_artifact_loading() -> None:
    path = Path("models/regression/phase11_regression_final_v1/models/lstm_5m.keras")
    if not path.exists():
        pytest.skip("Frozen local Phase 11 artifacts are not present")
    from tensorflow import keras
    model = keras.models.load_model(path)
    assert model.input_shape == (None, 60, 37)
    assert model.output_shape == (None, 1)
