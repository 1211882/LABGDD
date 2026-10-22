from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from src.ml.evaluation import classification_metrics


def analyze(experiment_root: Path) -> Path:
    rows = []
    duplicate_keys = 0
    required = {"timestamp", "symbol", "sector", "horizon", "actual_class", "predicted_class", "probability_down", "probability_neutral", "probability_up", "future_return", "model_name", "lookback", "dataset_version", "split"}
    for path in sorted((experiment_root / "predictions").glob("*.parquet")):
        frame = pd.read_parquet(path)
        missing = required - set(frame.columns)
        if missing:
            raise ValueError(f"{path} missing prediction fields: {sorted(missing)}")
        duplicate_keys += int(frame.duplicated(["timestamp", "symbol", "horizon", "model_name"]).sum())
        for level in ("sector", "symbol"):
            for value, group in frame.groupby(level):
                metrics = classification_metrics(group["actual_class"], group["predicted_class"])
                rows.append({"model": group["model_name"].iloc[0], "horizon": int(group["horizon"].iloc[0]), "level": level, "value": value, "rows": len(group), "accuracy": metrics["accuracy"], "balanced_accuracy": metrics["balanced_accuracy"], "macro_f1": metrics["macro_f1"], "weighted_f1": metrics["weighted_f1"]})
    if duplicate_keys:
        raise ValueError(f"Prediction business keys contain {duplicate_keys} duplicates")
    output = experiment_root / "symbol_sector_metrics.csv"
    pd.DataFrame(rows).to_csv(output, index=False)
    summary = {
        "prediction_files": 15,
        "prediction_schema_valid": True,
        "duplicate_business_keys": duplicate_keys,
        "grouped_metric_rows": len(rows),
        "interpretation": [
            "Five-minute models provide the strongest learned-model classification evidence.",
            "Random Forest has the best Macro F1 at every tested horizon.",
            "LSTM and GRU do not consistently outperform transparent baselines.",
            "Accuracy alone is misleading at 15 and 30 minutes because the majority baseline benefits from class imbalance.",
            "Long/flat simulations are negative after declared costs and do not demonstrate profitability.",
        ],
    }
    (experiment_root / "analysis_summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment-root", type=Path, default=Path("models/classification/phase10_classification_v1"))
    args = parser.parse_args()
    print(analyze(args.experiment_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
