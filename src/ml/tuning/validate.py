from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import joblib
import pandas as pd


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_tuned_experiment(root: Path, task: str) -> dict:
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    mismatches = [
        name for name, expected in manifest["checksums"].items()
        if not (root / name).is_file() or _sha256(root / name) != expected
    ]
    if mismatches:
        raise ValueError(f"Checksum mismatch: {mismatches}")
    if manifest["selection_split"] != "validation":
        raise ValueError("Tuned experiment was not selected on validation")

    trials = pd.read_csv(root / "tuning_trials.csv")
    expected_metric = "validation_macro_f1" if task == "classification" else "validation_mae"
    completed = trials.query("status == 'COMPLETED'")
    if completed.empty or set(completed.selection_metric) != {expected_metric}:
        raise ValueError("Trial selection metric is inconsistent")

    configs = list((root / "best_configs").glob("*/*/best_config.json"))
    if len(configs) != 12:
        raise ValueError(f"Expected 12 best configs, found {len(configs)}")
    for path in configs:
        config = json.loads(path.read_text(encoding="utf-8"))
        if config.get("selected_using") != "VALIDATION only" or config.get("test_data_used_for_selection") is not False:
            raise ValueError(f"Invalid selection provenance: {path}")

    prediction_files = sorted((root / "predictions").glob("*.parquet"))
    duplicate_rows = 0
    prediction_rows = 0
    for path in prediction_files:
        frame = pd.read_parquet(path, columns=["symbol", "timestamp", "split", "experiment_version", "model_variant"])
        duplicate_rows += int(frame.duplicated(["symbol", "timestamp"]).sum())
        prediction_rows += len(frame)
        if set(frame["split"]) != {"test"} or set(frame["model_variant"]) != {"TUNED"}:
            raise ValueError(f"Invalid prediction provenance: {path}")
    if duplicate_rows:
        raise ValueError(f"Duplicate prediction rows: {duplicate_rows}")

    loaded_scalers = [path.name for path in sorted((root / "scalers").glob("*.joblib")) if joblib.load(path) is not None]
    loaded_tabular = [path.name for path in sorted((root / "models").glob("*.joblib")) if joblib.load(path) is not None]
    from tensorflow import keras
    loaded_neural = [path.name for path in sorted((root / "models").glob("*.keras")) if keras.models.load_model(path, compile=False) is not None]

    return {
        "experiment_id": manifest["experiment_id"],
        "task": task,
        "checksum_files": len(manifest["checksums"]),
        "completed_trials": len(completed),
        "best_configs": len(configs),
        "prediction_files": len(prediction_files),
        "prediction_rows": prediction_rows,
        "duplicate_prediction_rows": duplicate_rows,
        "loaded_scalers": loaded_scalers,
        "loaded_tabular_models": loaded_tabular,
        "loaded_neural_models": loaded_neural,
        "valid": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate an immutable Phase 13 tuned experiment.")
    parser.add_argument("root", type=Path)
    parser.add_argument("--task", choices=("classification", "regression"), required=True)
    args = parser.parse_args()
    print(json.dumps(validate_tuned_experiment(args.root, args.task), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
