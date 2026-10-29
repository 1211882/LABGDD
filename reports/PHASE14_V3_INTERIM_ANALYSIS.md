# Phase 14 V3 interim evidence review

Snapshot UTC: 2026-10-29T00:08:13.997035+00:00. This report is a fixed snapshot; the experiment continues.

The project has established historical Batch/Streaming processing and controlled comparison evidence. Phase 14 now studies emergent sector volatility directly. The strongest preliminary predictive evidence concerns continuous volatility, while emergence detection remains weak and unstable. No Phase 14 final holdout result or profitability conclusion is available.

## Execution and completeness

- 491 completed fits across 93 task/model/horizon jobs; all 279 reference fits (93 jobs x 3 chronological folds) are complete.
- 212 additional completed screening fits. Non-reference screening candidates currently have only fold 0 results; promotion, seed robustness, ablation and final full-population fits remain.
- 11 unresolved failed trial directories; all currently record ResourceInfeasible forest-memory guards. These configurations were rejected rather than trained on silently reduced populations.
- Current process PID 9084 was active and using approximately one CPU core during inspection. The current sector_regime/5m/GRU candidate 3 advanced to epoch 2. A preceding multi-hour log gap does not establish a deadlock; sleep, scheduling and slow computation have not been distinguished.
- Final selection marker, final manifest and final report do not yet exist. Historical TEST has been inspected in older phases; it is isolated from current selection but is not a newly untouched sample.

## Comparable three-fold reference results

The following table shows the lowest-mean-MAE or highest-mean-F1 reference model per task/horizon. It is descriptive, uses candidate 0 only, and does not apply the final one-standard-error selection rule. It is not a best-hyperparameter claim or a significance test. Tuning validation endpoints are common across models; training populations differ (neural cap 60,000; tabular cap 240,000). Different tasks/horizons have different eligible populations.

| Task | Horizon | Reference leader | Metric | Mean | Fold SD | Baseline skill |
|---|---:|---|---|---:|---:|---:|
| sector_rv | 5m | forest | MAE | 0.000377 | 0.000034 | +19.4% MAE reduction |
| sector_rv | 15m | gru | MAE | 0.000544 | 0.000085 | +15.5% MAE reduction |
| sector_rv | 30m | forest | MAE | 0.000700 | 0.000118 | +18.4% MAE reduction |
| company_rv | 5m | gru | MAE | 0.000420 | 0.000035 | +21.4% MAE reduction |
| company_rv | 15m | gru | MAE | 0.000519 | 0.000034 | +18.6% MAE reduction |
| company_rv | 30m | gru | MAE | 0.000620 | 0.000050 | +19.3% MAE reduction |
| sector_regime | 5m | forest | macro_F1 | 0.567312 | 0.025196 | +0.0533 F1 |
| sector_regime | 15m | lstm | macro_F1 | 0.609744 | 0.054575 | +0.0093 F1 |
| sector_regime | 30m | logistic | macro_F1 | 0.595976 | 0.025966 | -0.0206 F1 |
| company_regime | 5m | lstm | macro_F1 | 0.503333 | 0.010086 | +0.0410 F1 |
| company_regime | 15m | lstm | macro_F1 | 0.551593 | 0.009908 | +0.0261 F1 |
| company_regime | 30m | gru | macro_F1 | 0.590812 | 0.017096 | +0.0197 F1 |
| emergence | 5m | lstm | event_F1 | 0.169380 | 0.049673 | +0.0842 F1 |
| emergence | 15m | gru | event_F1 | 0.257700 | 0.147888 | +0.2232 F1 |
| emergence | 30m | gru | event_F1 | 0.133333 | 0.230940 | +0.1333 F1 |
| return_class | 5m | lstm | macro_F1 | 0.385469 | 0.003583 | +0.2093 F1 |
| return_class | 15m | lstm | macro_F1 | 0.362457 | 0.004737 | +0.1744 F1 |
| return_class | 30m | lstm | macro_F1 | 0.333174 | 0.005280 | +0.1326 F1 |
| return_reg | 5m | forest | MAE | 0.001042 | 0.000069 | -0.0% MAE reduction |
| return_reg | 15m | forest | MAE | 0.001785 | 0.000126 | -0.0% MAE reduction |
| return_reg | 30m | forest | MAE | 0.002527 | 0.000118 | -0.1% MAE reduction |

Baseline skill is the mean of per-fold comparisons on exact baseline-available validation keys: 1 - model MAE / baseline MAE for regression, and model F1 - baseline F1 for classification. It can differ from comparing the displayed full-validation mean against a baseline mean. Volatility baselines use past-window persistence; return regression uses zero; return classification uses its declared baseline.

## Interpretation

Continuous sector volatility is the clearest primary-target result: reference MAE improves approximately 15-19% versus persistence. Forest leads the descriptive mean at 5m and 30m; GRU leads 15m by only about 0.0000018 MAE versus forest, much smaller than fold-to-fold dispersion. There is no established neural advantage for sector volatility.

Company-volatility GRU references lead at all horizons, improving about 19-21% against persistence. Their MAE advantage over forest is approximately 1.3%, 4.0% and 6.1% at 5m/15m/30m. This supports continued neural evaluation for the supporting company task, subject to multiple seeds and final holdout confirmation.

Sector regime reference Macro F1 is approximately 0.57-0.61. At 30m all reference models underperform their comparable persistence baseline; moderate F1 alone does not establish added value. Company regimes show small positive neural improvements, approximately 0.02-0.04 F1.

Emergence event F1 is low: reference leaders average 0.169/0.258/0.133 at 5m/15m/30m. For 30m, validation contains only 19, 0 and 5 positive labelled windows across the three folds; these overlapping windows are not independent economic episodes. The zero-positive middle fold makes fold F1 especially uninformative. The GRU reference gets [0, 0, 0.4], so its mean does not demonstrate robust detection. Event precision, missed episodes and false alarms are essential; accuracy would be misleading.

Return regression remains weaker than zero return for every reference model/horizon. Return classification reference leaders reach approximately 0.385/0.362/0.333 Macro F1. These are current validation scores, not evidence of improvement over the frozen Phase 10 test results.

Across completed neural fits, median best epoch is 4 and median trained epochs is 10; no fit is flagged near the 75-epoch ceiling. Increasing the epoch ceiling is therefore not the present priority. Earlier best epochs followed by worsening validation support restoring the best checkpoint. The automated overfitting indicator is a learning-curve heuristic, not proof that every model is unusable.

Additional single-fold results include sector_regime/5m/LSTM candidate 3 Macro F1 0.607565, versus reference LSTM 0.571014 on that fold. It needs the other folds and seed checks; it must not replace the three-fold comparison yet.

## Earlier completed project evidence

Phase 8 completed 24 controlled local performance runs. At 100,000 input events, Batch mean end-to-end duration was 99.90s versus 650.70s Streaming; analytical row counts matched. Streaming latency in this producer-first replay includes queueing and startup, not steady-state live delivery. This supports the implemented processing comparison on the measured machine, not production-cluster scalability.

Frozen Phase 10 Random Forest test Macro F1 is 0.3844/0.3448/0.3288 at 5m/15m/30m, with no consistent recurrent advantage and negative declared cost-adjusted signal diagnostics. Frozen Phase 11 return regression generally fails zero return on common keys. Phase 14 addresses a more suitable volatility target; it does not erase these results.

Batch/Streaming historical analytics and the Phase 14 RV target use different volatility definitions. A live Phase 14 feature/target path has not been shown equivalent by the older analytics tests. The frozen dataset has 2,276,418 bars, 30 symbols, six sectors and 252 dates. Increasing volume means acquiring additional unique observations and market regimes, not counting Batch and Streaming copies twice.

## Remaining work and decisions

Complete independent candidate screening and three-fold promotion, then neural seeds and ablations. Preserve the isolated holdout until selection is frozen. Train final models on every eligible original-training row, use every eligible validation row for final epoch selection and evaluate every eligible TEST row. Validate predictions, population counts, financial diagnostics for return tasks, and monitoring metrics before publishing final model/horizon configurations.

Prioritize continuous sector RV as the strongest current monitoring output. Treat emergence as exploratory until episode-level precision/recall, false alarms and out-of-period stability are convincing. Keep persistence as a genuine competitor, particularly for 30m regimes. Reassess expensive configurations using measured fit times and available memory; do not alter the running search or claim a reliable completion date from the present evidence.

## Evidence files

- `reports/phase14_v3_interim_reference_metrics.csv`: all 93 reference model/horizon/task combinations, exact parameters, scores, fold dispersion, best epochs and accumulated fit duration.
- `models/research/phase14_volatility_aligned_v3/trials/**/result.json`: underlying saved results.
- `reports/PHASE14_RESEARCH_ALIGNMENT.md`: target, split, population and evaluation contract.
- `reports/PHASE10_CLASSIFICATION_ANALYSIS.md`, `reports/PHASE11_REGRESSION_ANALYSIS.md`, `reports/PHASE8_PERFORMANCE_ANALYSIS.md`: earlier completed evidence.
