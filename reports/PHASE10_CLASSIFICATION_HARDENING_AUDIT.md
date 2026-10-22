# Phase 10 Classification Hardening Audit

## Findings before correction

### Critical

No target, split, scaler, or direct feature-leakage defect was found.

### High

- Tabular models were evaluated on every labelled test row while LSTM/GRU used a
  deterministic 30,000-sequence sample. The original headline table did not provide an
  identical-key comparison.
- The original financial calculation serially compounded overlapping predictions after
  sorting by symbol. That curve was not a coherent portfolio return and is superseded.

### Medium

- Class order existed in more than one module and lacked an explicit persisted contract.
- Aggregate metrics had not been independently rebuilt from every row-level prediction.
- Confidence, log loss, Brier score, common-subset metrics, and distribution-shift
  evidence were absent.
- Hyperparameters were predeclared but no search evidence was recorded. To avoid
  post-test tuning, hardening retains them unchanged and explicitly records that choice.
- Model/scaler checksums, software versions, feature formulas, and artifact resolution
  were incomplete in the original manifest.

### Low

- Learning curves and publication figures had not been generated.
- A market-aware baseline and non-overlapping financial sanity check were absent.

## Correctness and leakage validation

The canonical class order is `DOWN=0`, `NEUTRAL=1`, `UP=2`. Every persisted probability
vector was checked against this order and its decoded prediction. Target validation
recalculates `(close[t+H]-close[t])/close[t]`, requires an exact same-symbol/same-date
future timestamp, and confirms null labels for missing future bars and session endings.
Both threshold boundaries are NEUTRAL.

All 37 features are absent from the target/future column sets. Rolling and exponentially
weighted calculations are backward-looking and reset by symbol and trading date.
Sector features use only returns stamped at the current bar close. Scalers contain
training-split statistics only. Sequence construction enforces symbol, date, chronological,
and one-minute contiguity boundaries.

## Fair comparison and metrics

The frozen experiment reports two views:

- **Full model test set:** each model's legitimate available test population.
- **Common comparable test set:** intersection of `(symbol,timestamp)` keys across all
  six models for each horizon, yielding 30,000 rows per horizon.

All Accuracy, Balanced Accuracy, per-class Precision/Recall/F1, Macro/Weighted F1,
confusion matrices, ROC-AUC, log loss, and multiclass Brier scores are recomputed from
predictions. Rankings use Macro F1, then Balanced Accuracy, then Accuracy. Random Forest
remains first at every horizon in both the principal full-set ranking and the common-set
comparison.

## Confidence and calibration

Confidence buckets are `<=0.40`, `0.40-0.50`, `0.50-0.60`, and `>0.60`. Learned-model
log loss is approximately 1.04-1.11 and Brier score approximately 0.63-0.67, close to a
weak three-class probabilistic forecast. Confidence does not provide evidence strong
enough to justify post-hoc calibration. Raw probabilities are preserved unchanged.

## Distribution shift

At five minutes the train distribution is nearly balanced, while test NEUTRAL increases
to about 39%. At 15 and 30 minutes NEUTRAL is less frequent, and its share changes across
chronological splits. These changes support the documented non-stationarity limitation
and explain why Accuracy is not a suitable primary ranking metric.

## Financial evaluation

The primary output is explicitly named **DIAGNOSTIC SIGNAL EVALUATION** because horizon
returns overlap. Returns are now aggregated as equal-weight symbol returns per timestamp,
with five basis points charged on each symbol's position transitions. A secondary
clock-grid **NON-OVERLAPPING SANITY CHECK** evaluates at most one signal per horizon per
symbol. Both remain negative for the leading Random Forest model at every horizon. This
is not a trading-strategy backtest and provides no evidence of profitability.

## Frozen baseline

`models/classification/phase10_classification_final_v1` is the canonical immutable
classification baseline. Its manifest records data/features, class and probability
orders, splits, models, parameters, seed, software, policy, and SHA-256 checksums.
The original `phase10_classification_v1` is retained as source evidence rather than
deleted or silently replaced.
