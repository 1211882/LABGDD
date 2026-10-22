from __future__ import annotations

import argparse
from pathlib import Path

from src.ml.tuning.run_classification import EXPERIMENT_ID as CLASS_ID, tune_and_evaluate as tune_classification
from src.ml.tuning.run_regression import EXPERIMENT_ID as REG_ID, tune_and_evaluate as tune_regression


def main() -> int:
    parser=argparse.ArgumentParser(description="Run Phase 13 validation-only tuning followed by one final TEST evaluation.")
    parser.add_argument("--features",type=Path,default=Path("datasets/ml/ml_dataset_v1/features_v1/features_targets.parquet"));parser.add_argument("--metadata",type=Path,default=Path("datasets/ml/ml_dataset_v1/features_v1/metadata.json"));parser.add_argument("--tabular-trials",type=int,default=3);parser.add_argument("--neural-trials",type=int,default=2);parser.add_argument("--seed",type=int,default=42)
    args=parser.parse_args();class_root=tune_classification(args.features,args.metadata,Path(f"models/classification/{CLASS_ID}"),args.tabular_trials,args.neural_trials,args.seed,["logistic","random_forest","lstm","gru"],[5,15,30]);reg_root=tune_regression(args.features,args.metadata,Path(f"models/regression/{REG_ID}"),args.tabular_trials,args.neural_trials,args.seed,["ridge","random_forest","lstm","gru"],[5,15,30]);print(class_root);print(reg_root);return 0


if __name__=="__main__":raise SystemExit(main())
