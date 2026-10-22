import json

import numpy as np
import pandas as pd

from src.ml.evaluation import portfolio_financial_metrics
from src.ml.harden_classification import _validate_probabilities
from src.ml.targets import CLASS_NAMES, CLASS_TO_INDEX


def test_canonical_class_and_probability_order() -> None:
    assert CLASS_NAMES == ("DOWN", "NEUTRAL", "UP")
    assert CLASS_TO_INDEX == {"DOWN": 0, "NEUTRAL": 1, "UP": 2}
    frame = pd.DataFrame({
        "probability_down": [.8, .1, .1], "probability_neutral": [.1, .8, .1],
        "probability_up": [.1, .1, .8], "predicted_class": list(CLASS_NAMES),
    })
    _validate_probabilities(frame)


def test_probability_order_mismatch_is_rejected() -> None:
    frame = pd.DataFrame({"probability_down": [.8], "probability_neutral": [.1], "probability_up": [.1], "predicted_class": ["UP"]})
    try:
        _validate_probabilities(frame)
    except ValueError as error:
        assert "order mismatch" in str(error)
    else:
        raise AssertionError("mismatched probability order was accepted")


def test_non_overlapping_financial_check_reduces_signal_grid() -> None:
    frame = pd.DataFrame({
        "timestamp": pd.date_range("2026-01-05 14:30", periods=10, freq="min", tz="UTC"),
        "symbol": ["AAPL"] * 10, "predicted_class": ["UP"] * 10,
        "future_return": [.001] * 10, "horizon": [5] * 10,
    })
    diagnostic = portfolio_financial_metrics(frame)
    non_overlapping = portfolio_financial_metrics(frame, non_overlapping=True)
    assert diagnostic["signals"] == 10
    assert non_overlapping["signals"] == 2
    assert non_overlapping["overlapping_horizons"] is False


def test_frozen_manifest_class_contract() -> None:
    manifest = json.loads(open("models/classification/phase10_classification_final_v1/manifest.json", encoding="utf-8").read())
    assert manifest["status"] == "frozen"
    assert manifest["class_order"] == list(CLASS_NAMES)
    assert manifest["probability_order"] == ["probability_down", "probability_neutral", "probability_up"]
