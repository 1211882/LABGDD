# Phase 11 - Regression Experiment

## Scope and reproducibility

Regression is a secondary experiment; frozen Phase 10 classification remains the primary ML task. Phase 11 reuses `ml_dataset_v1`, `features_v1`, all 37 leakage-safe features, exact 5/15/30-minute same-symbol same-session targets, and the original chronological train/validation/test dates. No Alpaca data was downloaded and the Phase 10 freeze was not modified.

Zero-return, train-mean, Linear Regression, Random Forest Regressor, LSTM, and GRU were evaluated. Feature and neural-target scalers were fit on training data only. Recurrent models use exact contiguous 60-bar sequences capped deterministically at 120k/30k/30k observations. The Random Forest uses 40 trees and a deterministic 25% bootstrap sample per tree to bound local CPU runtime; all training rows remain eligible and all tabular test rows are evaluated.

## Main results

On each model's full test population, GRU had the lowest MAE: 0.000863 (5m), 0.001453 (15m), and 0.002020 (30m). These figures are not directly comparable with tabular full results because recurrent models use 30,000 contiguous sequence endpoints. On the common 30,000-key subset, zero return was best at 5m and 30m; Random Forest was best at 15m by only approximately 0.000001 MAE. All best common-subset R-squared values were close to or below zero.

Correlation was weak across horizons. Regression-derived classes did not match frozen direct classification: the best common-subset regression-derived Macro F1 remained materially below the corresponding Phase 10 Random Forest. LSTM and GRU therefore did not establish a useful deep-learning advantage.

## Financial diagnostics

The main diagnostic is long when predicted return exceeds +5 bps and flat otherwise, with 5 bps charged on each position change and equal-weight aggregation by timestamp. A secondary clock-grid check takes one decision every H minutes. Learned signals were sparse for several models and cost-adjusted overlapping results were generally negative. Isolated positive non-overlapping outcomes used very few signals and are not evidence of profitability.

## Interpretation and limitations

Low MAE largely reflects the concentration of intraday returns near zero. Near-zero or negative R-squared and weak correlation show that continuous return magnitude was not learned reliably out of sample. Direct classification remains more effective for directional decisions in this experiment. Results use Alpaca IEX data, one market year, one local machine, fixed compact model configurations, and diagnostic rather than executable trading assumptions. They do not demonstrate market predictability, profitability, or production-scale performance.

## Artifacts

The immutable experiment is `models/regression/phase11_regression_final_v1`. It contains row-level predictions, full/common metrics, target and error distributions, symbol/sector/magnitude analyses, financial diagnostics, direct-classification comparisons, persisted models/scalers/histories, publication figures, a manifest, and SHA-256 checksums.

Reproduce into a new output path with `python -m src.ml.run_phase11 --output models/regression/<new_id> --figures reports/figures/<new_id>`. Validate the canonical freeze with `python -m src.ml.validate_regression`.
