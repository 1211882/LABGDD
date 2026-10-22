from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from src.ml.evaluation import classification_metrics
from src.ml.targets import HORIZONS


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate(root: Path, features_path: Path, report_path: Path) -> dict:
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    checksum_errors = [relative for relative, expected in manifest["checksums"].items() if not (root / relative).is_file() or _sha256(root / relative) != expected]
    features_checksum_ok = _sha256(features_path) == manifest["features_checksum_sha256"]
    metrics = pd.read_csv(root / "metrics" / "classification_metrics_full.csv")
    metric_errors, duplicate_predictions = [], 0
    for path in sorted((root / "predictions").glob("*.parquet")):
        frame = pd.read_parquet(path)
        duplicate_predictions += int(frame.duplicated(["timestamp", "symbol", "horizon", "model_name"]).sum())
        row = metrics.loc[(metrics["model"] == frame["model_name"].iloc[0]) & (metrics["horizon"] == frame["horizon"].iloc[0])].iloc[0]
        calculated = classification_metrics(frame["actual_class"], frame["predicted_class"], frame[["probability_down", "probability_neutral", "probability_up"]].to_numpy())
        for field in ("accuracy", "balanced_accuracy", "macro_f1", "weighted_f1", "log_loss", "multiclass_brier_score"):
            if not np.isclose(float(row[field]), calculated[field], atol=1e-12):
                metric_errors.append(f"{path.name}:{field}")

    loaded_scalers = [path.name for path in sorted((root / "scalers").glob("*.joblib")) if joblib.load(path) is not None]
    loaded_tabular = [path.name for path in sorted((root / "models").glob("*.joblib")) if joblib.load(path) is not None]
    import tensorflow as tf
    loaded_neural = [path.name for path in sorted((root / "models").glob("*.keras")) if tf.keras.models.load_model(path, compile=False) is not None]

    features = pd.read_parquet(features_path, columns=["timestamp", "symbol", "trading_date", "close", *[f"future_close_{h}m" for h in HORIZONS], *[f"future_return_{h}m" for h in HORIZONS], *[f"target_{h}m" for h in HORIZONS]])
    target_examples = {}
    for horizon in HORIZONS:
        future_close, future_return, target = f"future_close_{horizon}m", f"future_return_{horizon}m", f"target_{horizon}m"
        normal = features.loc[features[future_return].notna()].iloc[100]
        session_end_index = features.groupby(["symbol", "trading_date"])["timestamp"].idxmax()
        end = features.loc[session_end_index].loc[lambda value: value[target].isna()].iloc[0]
        missing = features.loc[features[future_return].isna() & ~features.index.isin(session_end_index)].iloc[0]
        target_examples[f"{horizon}m"] = {
            "normal_recalculation_matches": bool(np.isclose(normal[future_return], normal[future_close] / normal.close - 1)),
            "missing_future": {"symbol": missing.symbol, "timestamp": missing.timestamp.isoformat(), "label_null": bool(pd.isna(missing[target]))},
            "end_of_session": {"symbol": end.symbol, "timestamp": end.timestamp.isoformat(), "label_null": bool(pd.isna(end[target]))},
        }
    result = {
        "validated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "experiment_id": manifest["experiment_id"], "status": "PASS" if not checksum_errors and features_checksum_ok and not metric_errors and not duplicate_predictions else "FAIL",
        "checksum_errors": checksum_errors, "features_checksum_ok": features_checksum_ok,
        "metric_recomputation_errors": metric_errors, "duplicate_prediction_business_keys": duplicate_predictions,
        "loaded_scalers": loaded_scalers, "loaded_tabular_models": loaded_tabular, "loaded_neural_models": loaded_neural,
        "target_examples": target_examples,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("models/classification/phase10_classification_final_v1"))
    parser.add_argument("--features", type=Path, default=Path("datasets/ml/ml_dataset_v1/features_v1/features_targets.parquet"))
    parser.add_argument("--report", type=Path, default=Path("reports/phase10_hardening_runtime_validation.json"))
    args = parser.parse_args(); result = validate(args.root, args.features, args.report); print(json.dumps(result, indent=2)); return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__": raise SystemExit(main())
