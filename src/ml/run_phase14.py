from __future__ import annotations

import argparse
import os
from pathlib import Path

from src.ml.dataset import sha256


def main():
    parser = argparse.ArgumentParser(description="Volatility-aligned research with validation-based stopping and independent chronological model/horizon selection")
    parser.add_argument("--output", type=Path, default=Path("models/research/phase14_volatility_aligned_v3"))
    parser.add_argument("--processed", type=Path, default=Path("datasets/ml/ml_dataset_v1/processed/market_bars.parquet"))
    parser.add_argument("--features", type=Path, default=Path("datasets/ml/ml_dataset_v1/features_v1/features_targets.parquet"))
    parser.add_argument("--metadata", type=Path, default=Path("datasets/ml/ml_dataset_v1/features_v1/metadata.json"))
    parser.add_argument("--config", type=Path, default=Path("config/config.yaml"))
    parser.add_argument("--budget-minutes", type=float, default=None, help="Optional wall-clock limit; omitted by default so validation controls neural stopping")
    parser.add_argument("--prepared-data-from", type=Path, default=None, help="Reuse a completed, input-matched target cache from a preserved earlier experiment")
    parser.add_argument("--prepared-data-mode", choices=["reference", "copy"], default="reference", help="Use verified targets read-only without duplicating the dataset by default")
    parser.add_argument("--resume-prepared", action="store_true", help="Resume an existing prepared experiment before any model training or holdout evaluation")
    parser.add_argument("--resume-training", action="store_true", help="Reuse completed tuning fits in an interrupted experiment before final selection")
    parser.add_argument("--max-epochs", type=int, default=75)
    parser.add_argument("--search-candidates", type=int, default=8)
    parser.add_argument("--tabular-train-cap", type=int, default=240_000, help="Tuning only; final training uses every eligible row")
    parser.add_argument("--neural-train-cap", type=int, default=60_000, help="Tuning only; final training uses every eligible sequence for the selected lookback")
    parser.add_argument("--validation-cap", type=int, default=20_000, help="Tuning only; final early stopping uses the full eligible validation population")
    parser.add_argument("--retain-tuning-checkpoints", action="store_true", help="Retain intermediate tuning binaries in addition to all histories/metrics and final models")
    args = parser.parse_args()
    if args.resume_prepared and args.resume_training:
        parser.error("Choose only one resume mode")
    if args.max_epochs < 75: parser.error("Use a ceiling of at least 75 epochs")
    if args.budget_minutes is not None and args.budget_minutes <= 0: parser.error("An optional time limit must be positive")
    if min(args.search_candidates, args.tabular_train_cap, args.neural_train_cap, args.validation_cap) < 1:
        parser.error("Search budgets and row caps must be positive")
    for key in ("TF_NUM_INTRAOP_THREADS", "OMP_NUM_THREADS"):
        os.environ[key] = "2"
    os.environ["TF_NUM_INTEROP_THREADS"] = "1"
    os.environ["TF_DETERMINISTIC_OPS"] = "1"
    settings = {k: (str(v.resolve()) if isinstance(v, Path) else v) for k, v in vars(args).items() if k not in {"output", "resume_prepared", "resume_training"}}
    settings["input_checksums"] = {k: sha256(getattr(args, k)) for k in ("processed", "features", "metadata", "config")}
    settings["stopping_policy"] = "validation_primary_with_best_checkpoint"
    settings["time_cutoff_enabled"] = args.budget_minutes is not None
    settings["full_final_population"] = True
    settings["holdout_cap"] = None
    settings["population_policy"] = "large_sample_tuning_full_eligible_final_training_validation_test"
    from src.ml.volatility.experiment import Experiment
    Experiment(args.output, settings).run(resume_prepared=args.resume_prepared, resume_training=args.resume_training)


if __name__ == "__main__":
    main()
