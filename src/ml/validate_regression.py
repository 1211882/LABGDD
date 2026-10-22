from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import joblib
import pandas as pd

from src.ml.features import FEATURE_COLUMNS
from src.ml.targets import HORIZONS


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate(root: Path) -> dict:
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    mismatches = [name for name, expected in manifest["checksums"].items() if not (root / name).is_file() or _sha256(root / name) != expected]
    if mismatches:
        raise ValueError(f"Checksum mismatch: {mismatches}")
    metrics = pd.read_csv(root / "metrics" / "regression_metrics_full.csv")
    if metrics.isna().any().any():
        raise ValueError("Non-finite full regression metrics")
    required = {"timestamp", "symbol", "sector", "horizon", "actual_return", "predicted_return", "prediction_error", "absolute_error", "squared_error", "model_name", "lookback", "dataset_version", "split", "actual_class_from_5bps", "predicted_class_from_5bps"}
    prediction_rows = 0
    for path in (root / "predictions").glob("*.parquet"):
        frame = pd.read_parquet(path)
        if not required.issubset(frame.columns):
            raise ValueError(f"Prediction schema mismatch: {path}")
        if frame.duplicated(["symbol", "timestamp"]).any():
            raise ValueError(f"Duplicate prediction keys: {path}")
        prediction_rows += len(frame)
    for horizon in HORIZONS:
        feature_scaler = joblib.load(root / "scalers" / f"feature_scaler_{horizon}m.joblib")
        target_scaler = joblib.load(root / "scalers" / f"target_scaler_{horizon}m.joblib")
        if feature_scaler.n_features_in_ != len(FEATURE_COLUMNS) or target_scaler.n_features_in_ != 1:
            raise ValueError(f"Scaler shape mismatch: {horizon}m")
        joblib.load(root / "models" / f"linear_regression_{horizon}m.joblib")
        joblib.load(root / "models" / f"random_forest_regressor_{horizon}m.joblib")
    from tensorflow import keras
    for horizon in HORIZONS:
        for kind in ("lstm", "gru"):
            keras.models.load_model(root / "models" / f"{kind}_{horizon}m.keras")
    return {"experiment_id": manifest["experiment_id"], "checksum_files": len(manifest["checksums"]), "prediction_files": len(list((root / "predictions").glob("*.parquet"))), "prediction_rows": prediction_rows, "model_artifacts_loaded": 12, "scalers_loaded": 6, "valid": True}


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the frozen Phase 11 experiment.")
    parser.add_argument("--root", type=Path, default=Path("models/regression/phase11_regression_final_v1"))
    args = parser.parse_args(); print(json.dumps(validate(args.root), indent=2)); return 0


if __name__ == "__main__": raise SystemExit(main())
