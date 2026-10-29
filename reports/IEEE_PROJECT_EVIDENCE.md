# Project methods and evidence for the IEEE report

Prepared 2026-10-29. This is a documentation snapshot. See `ieee/project_snapshot.json` for precise collection time, environment and source hashes. The active experiment continues independently.

## Research questions and claim boundaries

| Question | Available evidence | Current boundary |
|---|---|---|
| RQ1: Do Batch and Streaming produce equivalent historical analytics on equivalent events? | Phase 7 equivalence artifacts and Phase 8 controlled comparison | Applies to tested definitions and workloads, not automatically to new Phase 14 RV features |
| RQ2: What are their local throughput, latency and resource trade-offs? | 24 Phase 8 measured runs: four workloads, two paths, three repeats | One machine; producer-first replay; queueing and startup included |
| RQ3: Can independently tuned models improve multi-horizon volatility forecasts against persistence? | Phase 14 completed reference validation and ongoing tuning | Final holdout and selected full-population models pending |
| RQ4: Can sector emergence be detected with useful precision, recall and false-alarm rates? | Exploratory label, prediction metrics and planned episode analysis | Rare positives and unstable fold results; reliable early detection not established |

## Architecture and operational scope

Historical Alpaca REST ingestion produces persisted OHLCV inputs for Spark Batch analytics. The streaming path uses Alpaca WebSocket ingestion, Kafka and Spark Structured Streaming with validation, deduplication, sector enrichment and event-time windows. Prometheus/Grafana support operational observation. Phase 14 training reads local prepared arrays and Parquet; its offline ML results do not require the live services to be active. Separate Power BI outputs are planned/generated only according to each experiment's validation status; no completed Phase 14 export is claimed.

Implementation entry points: `src/ml/run_phase14.py`, `src/ml/volatility/{data,targets,models,experiment,metrics,reporting}.py`. Earlier pipeline and benchmark entry points are indexed in the root README.

## Dataset, populations and temporal protocol

- Frozen source: `datasets/ml/ml_dataset_v1/processed/market_bars.parquet`; original features: `datasets/ml/ml_dataset_v1/features_v1/features_targets.parquet`.
- 2,276,418 actual one-minute bars, 30 symbols, six configured sectors, 252 observed dates, 2025-10-01 through 2026-10-01. Original features contain 2,001,858 rows after warm-up exclusions.
- Original chronological partitions: 176 train dates through 2026-06-12; 38 validation dates from 2026-06-15 through 2026-08-07; 38 TEST dates from 2026-08-10 through 2026-10-01.
- Three expanding validation folds: 2026-06-15–07-02, 2026-07-06–07-22, 2026-07-23–08-07. Each fold trains on dates preceding its validation interval.
- Original TEST was examined in earlier phases. It is excluded from current selection, but must not be called a newly untouched dataset.
- No price/volume/target interpolation. Missing derived features use train-fitted imputation; coverage indicators expose missingness. Complete exact future paths are required for RV; windows cannot cross sessions.
- Phase 14 features comprise 49 fields: original price/return/technical/time/sector features plus historical RV and coverage/context additions. Model sequence lookbacks are 15/30/60/90/120 exact minutes.
- Tuning caps: 240,000 tabular training rows, 60,000 neural endpoints, 20,000 validation endpoints. Actual eligible counts can be smaller, especially for sectors and long horizons. Tuning validation endpoints use common 120-bar eligibility across model families.
- Final policy: all eligible original-training rows; all eligible validation rows for best-epoch selection; all eligible TEST rows. Validation is not added to gradient/estimator fitting. Final full-population and identical-key comparison scores must both be reported.

## Target definitions

For exact observed prices in the same session:

```text
r(i,t+k) = log(P(i,t+k) / P(i,t+k-1))
RV(i,t,H) = sqrt(sum[k=1..H] r(i,t+k)^2), H in {5,15,30}
sector_RV(s,t,H) = sqrt(mean[i in fixed sector s] RV(i,t,H)^2)
```

RV is in H-minute return units, not annualized. Sector RV is root mean constituent variance, not investable portfolio volatility. All configured constituent windows must be valid. Regime thresholds are each entity/horizon's training 1/3 and 2/3 quantiles, refitted within each fold.

Emergence requires elevated sector RV, a strict majority of constituents above their own high-RV thresholds, positive average pairwise correlation above its training high quantile, and variance-share HHI no greater than its training high quantile. Undefined correlations remain undefined. This conjunction is an exploratory project definition, not a literature-validated crisis label. Return direction and return magnitude remain secondary targets; the neutral boundary is ±5 basis points.

## Independent model selection

Each task/model/horizon has its own deterministic candidate list. Defaults are references, not assumed optimal settings. Authoritative spaces and candidate values are `search_spaces.json` and `planned_candidates.json` in the experiment directory.

| Family | Tuned dimensions | Epoch treatment |
|---|---|---|
| LSTM / GRU | Architecture, lookback, hidden size, layers, learning rate, dropout, batch size, weight decay, patience, applicable class weighting | Ceiling 75; validation-primary early stopping; best checkpoint restored; exact best epoch saved |
| Random Forest | Trees, depth, split/leaf size, feature sampling, bootstrap/subsample and applicable class weighting | No epochs |
| Logistic Regression | C, L1/L2/elastic-net regularization, compatible solver and class weighting | No epochs |
| Ridge | Ten log-spaced alpha values | No epochs |
| Ordinary least squares | Single declared specification | No fabricated tuning grid |

All 93 jobs receive three-fold references. Additional candidates screen on the first fold; leading candidates are promoted to three folds. Selection uses validation means with a one-standard-error near-tie preference for lower fitted complexity. This is a bounded search, not a proof of global optimality. The strongest neural candidate per task/horizon is scheduled for seeds 42/43/44. Six feature-family ablations are scheduled on validation only. Current completion of these later stages must be checked rather than assumed.

Neural stopping monitors Macro F1 for multiclass tasks, event F1 for emergence and MAE in original units for regression. Training loss is recorded separately. The same validation interval supports stopping and candidate assessment; repeated search can produce optimistic validation scores. Three folds alone do not establish statistical significance.

## Metrics and financial interpretation

- Regression: MAE and secondary error, correlation and ranking diagnostics; compare with declared baselines on identical keys.
- Classification: fixed-class Macro F1, balanced accuracy, per-class metrics and probabilities. Emergence requires precision, recall, F1, false positives, missed events and false-positive rate.
- Monitoring: alarms per observed day, sector breakdowns, full truth episodes, detection coverage and first alerts. Current sampled tuning endpoints do not establish full-calendar operational false-alarm rates.
- Lead time: future-window completion is H minutes by construction; it is not demonstrated advance warning of an independently established onset. Delivery latency is not measured without ingestion timestamps.
- Financial diagnostics apply to return signals, with costs and non-overlapping decisions reported separately. A reduction in volatility MAE is not a trading return, and Sharpe/returns for a volatility model cannot be invented without a defined strategy.

## Completed findings and unfinished work

Phase 8: all 24 measured runs completed. At 100,000 events, mean end-to-end duration was 99.90 seconds Batch versus 650.70 seconds Streaming; matching analytical counts were observed. See `PHASE8_PERFORMANCE_ANALYSIS.md` for definitions and caveats.

Phase 10: frozen Random Forest test Macro F1 0.3844/0.3448/0.3288 at 5m/15m/30m; no consistent neural advantage; declared cost-adjusted signal diagnostics negative. Phase 11: common-key return regression generally fails the zero-return benchmark. These are preserved results, not superseded by current validation scores.

The separate interim Phase 14 review records 491 completed fits and all 279 reference folds. Reference sector-RV leaders reduce MAE against persistence by about 15–19%; company RV by about 19–21%. Emergence F1 is weak and unstable. These are validation findings only, descriptive reference leaders rather than final selected configurations. See the CSV for exact parameters, epochs, scores and durations.

The later documentation snapshot captured 492 completed fits with PID 17976 active. These counts refer to different observation times; the earlier interim review is intentionally preserved as a fixed snapshot.

Pending: remaining candidates, promotion, seeds, ablations, full final fits, frozen selections, full holdout evaluation, independent artifact/population validation, report and presentation exports. No completed manifest is currently claimed.

## Operational decisions and interruptions

| Stage | Decision / effect |
|---|---|
| Phase 13 | Stopped at user request; preserve remaining historical evidence |
| Phase 14 V1 | Original two-hour bounded pilot stopped when the user requested validation-controlled stopping |
| V1 cleanup | Duplicate prepared targets removed after matching checksums; obsolete evidence archived with cleanup manifest |
| Phase 14 V2 | Removed default time cutoff; completed 326 fits under smaller sample caps; preserved as a separate pilot |
| Phase 14 V3 | User selected larger tuning samples and full eligible final populations; read-only prepared-cache reference to V2 |
| Resource cleanup | Docker/monitoring stack and approved desktop applications stopped to free RAM; this is not evidence of an established speedup |
| First V3 interruption | Original process disappeared after 479 saved fits; cause not established from logs |
| Resume support | Added strict preselection resume validation, input/settings/plan checks and saved validation-artifact checks; incomplete fits restart |
| Migration discussion | Process stopped briefly at 491 completed fits, then restarted immediately when the user requested no stop; no transfer package or commit completed |
| Current instruction | Keep training running; document the project without changing scientific settings or stopping the process |

The source currently sets two intra-op/OMP threads and one inter-op thread; forest fits use two jobs. More RAM alone does not force full CPU usage. Native Windows training currently uses TensorFlow CPU. A different machine or library build can change timing, feasibility and floating-point outcomes; record that environment as a distinct execution segment.

## Reproducibility and artifact map

| Evidence | Location |
|---|---|
| Frozen Phase 10 | `models/classification/phase10_classification_final_v1/` |
| Frozen Phase 11 | `models/regression/phase11_regression_final_v1/` |
| Earlier controlled comparisons | `benchmarks/phase7_controlled_v1_equivalence.json`, `benchmarks/phase8_batch_streaming_performance.json` |
| Preserved V1 evidence | `models/research/phase14_volatility_aligned_v1/cleanup_manifest.json`, `stopped_run_evidence.zip` |
| Shared prepared data | `models/research/phase14_volatility_aligned_v2/data/`; do not delete while V3 references it |
| V3 settings, fingerprints, folds, plans | `models/research/phase14_volatility_aligned_v3/{request,data_reference,temporal_folds,planned_candidates}.json` |
| Saved fits | `trials/<task>/<horizon>m/<model>/candidate_XX/fold_N/seed_N/result.json` below V3 |
| Validation evidence | `validation_predictions.npy`, `validation_indices.npy`, neural histories and model-specific metadata in trial directories |
| Coverage | `training_coverage.csv`, `training_coverage_distribution.csv`; final per-model coverage files when final fitting completes |
| Live process/logs | `reports/phase14_v3_process_id.txt`, `phase14_v3_run.stdout.log`, `phase14_v3_run.stderr.log` |
| Resume source | `src/ml/run_phase14.py`, `src/ml/volatility/experiment.py`; `--resume-training` is pre-final-selection only |
| Verification | `tests/test_ml_volatility.py`, retained XML reports; latest observed test run: 28 passed before the latest resume |

Final checkpoints are retained. Intermediate tuning binaries can be deleted after validated predictions are stored, with original checksums retained; their absence does not mean that validation metrics were fabricated. Conversely, saved metrics alone do not replace prediction-level validation.

## Threats to validity and missing comparators

One year, one feed and a fixed small sector universe limit external validity. Sector membership and missingness influence which endpoints survive. Common 120-bar tuning endpoints favor continuous coverage and exclude much of the open/warm-up period. More raw rows do not guarantee more independent emergence events. Date-separated validation avoids cross-partition future target overlap, but overlapping windows inside a partition remain dependent.

HAR-RV, GARCH and foundation models are reviewed but not implemented/evaluated in this run. Full-population final fitting may change performance or expose memory failures. Some forest candidates are resource-infeasible under conservative memory estimates; report those as censored comparisons, not inferior measured accuracy. A long log gap does not identify whether the cause was sleep, scheduling or slow computation. Historical Batch/Streaming volatility definitions differ from new Phase 14 RV, so live feature parity requires a new test.

Do not claim causal contagion from correlation, profitable execution from diagnostic metrics, market-wide coverage from IEX data, or production readiness from local tests. A final chronological score after repeated historical experimentation requires cautious interpretation and, ideally, a later genuinely new evaluation period.
