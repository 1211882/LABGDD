# Phase 10 Classification Analysis

## Experimental basis

`ml_dataset_v1` freezes 2,276,418 Alpaca IEX one-minute regular-session bars for 30
symbols and six project-defined sectors. The requested period is 2025-10-01 through
2026-10-01 and the observed data cover 252 trading dates. Missing source minutes remain
missing; no prices or volumes were interpolated or forward-filled.

Feature generation produced 2,001,858 rows after excluding 274,560 indicator warm-up
rows. Every rolling history resets at the symbol and New York trading-date boundary.
Labels require an exact same-symbol, same-date observation at t+5, t+15, or t+30.

The fixed five-basis-point UP/NEUTRAL/DOWN boundary is transaction-cost-aware and was
selected before test evaluation. Candidate two, five, and ten basis-point class balances
and return quantiles are retained in feature metadata. Dates are split chronologically:
176 train, 38 validation, and 38 test trading days.

## Test comparison

| Model | Horizon | Accuracy | Balanced accuracy | Macro F1 |
|---|---:|---:|---:|---:|
| Majority | 5m | 0.3903 | 0.3333 | 0.1872 |
| Logistic Regression | 5m | 0.4241 | 0.3922 | 0.3651 |
| Random Forest | 5m | 0.4219 | **0.3976** | **0.3844** |
| LSTM | 5m | **0.4285** | 0.3877 | 0.3601 |
| GRU | 5m | 0.4280 | 0.3850 | 0.3499 |
| Majority | 15m | 0.3783 | 0.3333 | 0.1830 |
| Logistic Regression | 15m | 0.3272 | 0.3815 | 0.3090 |
| Random Forest | 15m | 0.3491 | **0.3854** | **0.3448** |
| LSTM | 15m | 0.3772 | 0.3391 | 0.3007 |
| GRU | 15m | **0.3798** | 0.3414 | 0.3031 |
| Majority | 30m | 0.4121 | 0.3333 | 0.1945 |
| Logistic Regression | 30m | 0.2827 | 0.3768 | 0.2762 |
| Random Forest | 30m | 0.3291 | **0.3795** | **0.3288** |
| LSTM | 30m | 0.4139 | 0.3396 | 0.3043 |
| GRU | 30m | **0.4145** | 0.3396 | 0.3022 |

Five minutes is the strongest horizon for learned classification. Random Forest has the
best Macro F1 at all horizons. The recurrent models do not establish an advantage over
the simpler models. Majority accuracy at 15 and 30 minutes illustrates why accuracy
cannot be interpreted without Balanced Accuracy, Macro F1, and class distributions.

## Fair-comparison hardening

The final baseline preserves each model's full legitimate test set and adds a common
30,000-row `(symbol,timestamp)` intersection per horizon. Random Forest remains first by
Macro F1 on the full and common populations. Metrics are recomputed directly from every
persisted prediction, and confidence, log loss, multiclass Brier score, class-shift,
symbol, and sector evidence are included in the frozen experiment.

## Financial interpretation

The declared policy is UP=long and NEUTRAL/DOWN=flat, with five basis points charged
when each symbol's position changes. The hardened calculation aggregates equal-weight
symbol returns by timestamp. Its primary output is explicitly a diagnostic signal
evaluation because horizon returns overlap. A secondary clock-grid check uses at most
one signal per horizon per symbol. Both variants remain negative at every horizon,
directional accuracy remains close to 50%, and there is no evidence of profitable
predictability. Neither is described as an executable trading-strategy backtest.

Per-symbol and per-sector Accuracy, Balanced Accuracy, Macro F1, and Weighted F1 are
stored in `symbol_sector_metrics.csv`. Performance is heterogeneous, but those subgroup
results do not alter the aggregate conclusion and should not be treated as independent
trials without multiple-testing controls.

## Limitations

IEX is not the consolidated US SIP feed, gaps differ by symbol, and the one-year period
contains only one market regime sample. Financial relationships are non-stationary.
LSTM/GRU training used deterministic caps of 120,000/30,000/30,000 contiguous sequences
for train/validation/test because execution was CPU-only on a local machine. The
experiment evaluates historical classification, not future live performance or profit.

The canonical baseline is `models/classification/phase10_classification_final_v1`; its
manifest and runtime validation establish artifact checksums, metric reproduction,
model/scaler loading, target samples, unique prediction keys, and software versions.
