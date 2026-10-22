from __future__ import annotations

import argparse
from pathlib import Path

from src.ml.harden_classification import harden
from src.ml.train_classification import train


def main() -> int:
    parser = argparse.ArgumentParser(description="Reproduce Phase 10 from frozen features without downloading market data.")
    parser.add_argument("--features", type=Path, default=Path("datasets/ml/ml_dataset_v1/features_v1/features_targets.parquet"))
    parser.add_argument("--feature-metadata", type=Path, default=Path("datasets/ml/ml_dataset_v1/features_v1/metadata.json"))
    parser.add_argument("--work-output", type=Path, required=True, help="New immutable training output directory.")
    parser.add_argument("--final-output", type=Path, required=True, help="New immutable hardened output directory.")
    args = parser.parse_args()
    source = train(args.features, args.feature_metadata, args.work_output)
    final = harden(args.features, source, args.final_output)
    print(final)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
