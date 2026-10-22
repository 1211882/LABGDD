from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.ml.dataset import sha256
from src.ml.features import FEATURE_COLUMNS, FEATURE_VERSION, engineer_features, ml_ready
from src.ml.splits import temporal_date_splits
from src.ml.targets import HORIZONS, add_classes, add_future_targets, threshold_analysis

THRESHOLD_BPS = 5


def prepare(dataset_root: Path, output_root: Path) -> Path:
    if output_root.exists():
        raise FileExistsError(f"Immutable feature dataset already exists: {output_root}")
    temporary = output_root.parent / f".{output_root.name}.building"
    if temporary.exists():
        shutil.rmtree(temporary)
    source_manifest = json.loads((dataset_root / "manifest.json").read_text(encoding="utf-8"))
    source = pd.read_parquet(dataset_root / "processed" / "market_bars.parquet")
    featured = engineer_features(source)
    targeted = add_future_targets(featured)
    ready, warmup = ml_ready(targeted)
    ready, split_metadata = temporal_date_splits(ready)
    analysis = threshold_analysis(ready.loc[ready["split"] == "train"])
    ready = add_classes(ready, THRESHOLD_BPS)
    temporary.mkdir(parents=True)
    output_path = temporary / "features_targets.parquet"
    ready.to_parquet(output_path, index=False)
    distributions = {}
    for horizon in HORIZONS:
        column = f"target_{horizon}m"
        distributions[f"{horizon}m"] = {
            split: {
                ("MISSING" if pd.isna(key) else str(key)): int(value)
                for key, value in subset[column].value_counts(dropna=False).items()
            }
            for split, subset in ready.groupby("split")
        }
    metadata = {
        "features_version": FEATURE_VERSION,
        "dataset_version": source_manifest["dataset_version"],
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "feature_columns": FEATURE_COLUMNS,
        "feature_count": len(FEATURE_COLUMNS),
        "warmup": warmup,
        "target_horizons_minutes": list(HORIZONS),
        "target_formula": "(close at exact t+H - close at t) / close at t; same symbol and trading_date",
        "threshold_methodology": "fixed transaction-cost-aware threshold",
        "threshold_basis_points": THRESHOLD_BPS,
        "threshold_rationale": "Five basis points is an explicit reproducible movement filter and the declared one-way simulated transaction cost; candidate balances are reported rather than optimizing on test data.",
        "threshold_analysis_train_only": analysis,
        "splits": split_metadata,
        "class_distributions": distributions,
        "row_count": len(ready),
        "checksum_sha256": sha256(output_path),
        "leakage_controls": [
            "all rolling and exponentially weighted features are backward-looking",
            "feature histories reset by symbol and New York trading date",
            "future targets require an exact same-symbol same-day timestamp",
            "one date split is shared by all symbols",
            "scalers are fit on training rows only during model training",
        ],
    }
    (temporary / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    temporary.rename(output_root)
    return output_root / output_path.name


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate immutable Phase 10 features and targets.")
    parser.add_argument("--dataset-root", type=Path, default=Path("datasets/ml/ml_dataset_v1"))
    parser.add_argument("--output-root", type=Path, default=Path("datasets/ml/ml_dataset_v1/features_v1"))
    args = parser.parse_args()
    print(prepare(args.dataset_root, args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
