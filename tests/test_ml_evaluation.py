import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

from src.ml.evaluation import classification_metrics, financial_metrics


def test_classification_metrics_include_balanced_and_macro_scores() -> None:
    actual = np.array(["DOWN", "NEUTRAL", "UP", "UP"])
    predicted = np.array(["DOWN", "UP", "UP", "NEUTRAL"])
    result = classification_metrics(actual, predicted)
    assert round(result["balanced_accuracy"], 6) == round((1 + 0 + .5) / 3, 6)
    assert len(result["confusion_matrix"]) == 3
    assert set(result["per_class"]) == {"DOWN", "NEUTRAL", "UP"}


def test_financial_metrics_apply_long_flat_policy_and_costs() -> None:
    frame = pd.DataFrame({
        "timestamp": pd.date_range("2026-01-01", periods=3, freq="min", tz="UTC"),
        "symbol": ["AAPL"] * 3, "predicted_class": ["UP", "UP", "DOWN"],
        "future_return": [.01, -.005, .02],
    })
    result = financial_metrics(frame, transaction_cost=.001)
    assert result["signals"] == 2
    assert result["turnover"] == 2
    assert result["directional_accuracy"] == .5


def test_scaler_uses_train_statistics_only() -> None:
    train = np.array([[0.0], [2.0]])
    test = np.array([[100.0]])
    scaler = StandardScaler().fit(train)
    assert scaler.mean_[0] == 1.0
    assert scaler.transform(test)[0, 0] == 99.0
