# Phase 14: research-target alignment audit

## Scientific decision before optimization

The primary research objective is reproducible monitoring of **emergent sector
volatility**, with comparable Batch and Streaming processing. Return direction
and return magnitude remain **secondary ML experiments** and supporting
covariates. Company realized volatility is a supporting risk target; sector
volatility and coordinated emergence are the primary new monitoring targets.
The existing Phase 10/11 results remain valid evidence for their stated targets.

A return forecast estimates a conditional first moment. Volatility concerns
variation, approximately a conditional second moment. Positive and negative
returns can cancel while squared returns remain large. Therefore predicting
DOWN/NEUTRAL/UP or the endpoint return does not directly predict the intensity,
breadth, concentration, or synchronization of sector risk. Low return MAE near a
zero-return baseline is not evidence of successful volatility monitoring.

## Existing evidence and definition mismatch

Frozen Phase 10 Random Forest has the highest Macro F1 at 5/15/30m; recurrent
models have no demonstrated consistent advantage. Frozen Phase 11 common-key
results favor zero return at 5m/30m, with a very small forest advantage at 15m.
These are historical results, not outcomes of the new optimization.

The existing Batch/Streaming analytics use sample standard deviation of
close-to-open bar returns, and sector aggregation pools constituent observations.
The new target uses the square root of summed squared close-to-close log returns.
These are **different estimands**. Historical analytical equivalence remains
preserved; it does not establish equivalence of the new realized-volatility
analytics or a live ML feature path.

## New target contract

For observed close P(i,t) and exact same-session minute returns
r(i,t+k)=log(P(i,t+k)/P(i,t+k-1)):

    RV(i,t,H) = sqrt(sum(k=1..H) r(i,t+k)^2), H in {5,15,30}.

Every one of the H+1 prices must exist at its exact timestamp. No price or target
is interpolated or forward-filled. The target is in H-minute return units, not
annualized. Prices must be positive. Features contain observations at or before
t only. A prediction is available after the bar stamped t has closed and arrived;
without ingestion timestamps, operational delay is not measured.

Sector risk uses the fixed project constituent universe at each complete window:

    sector_RV(s,t,H) = sqrt(mean(i in s) RV(i,t,H)^2).

This is root mean stock realized variance, **not** the volatility of an investable
equal-weight sector portfolio. Report mean/median/dispersion of constituent RV
separately. Require all configured constituents for target validity; record
coverage and exclusions rather than silently changing sector membership.

LOW/NORMAL/HIGH regimes use the training 1/3 and 2/3 quantiles per symbol or
sector and horizon. Thresholds are fitted independently inside each chronological
fold and frozen before its validation window. Final thresholds use final training
data only. Degenerate thresholds are flagged rather than described as balanced.

For each sector/window report the high-RV fraction, mean signed and absolute
pairwise return correlation, return/RV dispersion, variance-share HHI, largest
variance share and top-two share. HHI uses shares RV_i^2/sum_j RV_j^2; broadness
is (1-HHI)/(1-1/N). Correlations with too few observations or zero variance remain
undefined. In particular, one-minute within-window correlation is not estimable.

An exploratory emergence event requires simultaneously: sector RV above its
training 2/3 quantile; a strict majority of constituents above their own training
high-RV thresholds; positive mean pairwise correlation above its training 2/3
quantile; and HHI no greater than its training 2/3 quantile. This is a transparent
conjunction with an auditable component vector, not an optimized weighted score
or an externally established economic event taxonomy. Sensitivity and rare-event
limitations must be reported. Undefined correlations do not become zero.

## Temporal and event evaluation contract

Use three expanding date folds, never shuffled cross-validation. Future targets
are contained inside their sessions, so date boundaries purge overlapping target
windows. Scalers, feature imputation, regime thresholds, and emergence thresholds
are fitted on each fold's past only. All candidates of a model/horizon share
validation endpoints. Training feature imputation never fills prices or targets;
observed-bar and coverage fields expose missingness.

Select by validation mean, assess fold dispersion, and use a declared near-tie
rule favoring smaller models. Neural trials use a 75-epoch ceiling, validation
early stopping and persisted best checkpoints. Record best epoch, trained epochs,
best validation loss, primary score, timing, and undertraining/overfitting flags.
Plain OLS has a single specification; no artificial tuning grid is invented.

Evaluate statistical prediction, financial signal diagnostics, and volatility
monitoring separately. Report persistence/train-mean baselines on identical keys.
For monitoring, include event precision/recall/F1, false positives, false positive
rate, missed events, alarms per observed trading day, and sector breakdowns.

Future-window labels alone do not reveal the instant an economic event begins.
Label a forecast's target-window start t+1 minute and completion t+H explicitly.
Report **window lead time**, adjusted only if measured delivery/processing latency
exists; do not present mechanically H-minute completion lead as demonstrated
early event-onset detection. Collapse overlapping high-state windows into episodes
for event detection rates and first-alert tables; provide the matching rule.

## Budget and preservation

The original v1 protocol used an authorized two-hour staged run. The user has
since removed that time limit: **v2 defaults to no wall-clock cutoff**. Neural
models stop after their configured patience without improvement in validation
Macro F1, event F1, or regression MAE, and reload the best checkpoint. Loss is
recorded separately; training loss alone does not control classifier stopping.
A 75-epoch safety ceiling remains, with undertraining flags where appropriate.
Finite candidate sets and representative sample caps remain explicit. An
optional time limit applies only when requested using `--budget-minutes`.

The v1 run, logs, and protocol source snapshot are preserved. V2 may copy its
completed target cache after verifying matching input checksums; it never edits
the old cache. Record actual tested candidates, untrained combinations, coverage,
folds, seeds, ablations, and failed comparisons explicitly. No globally optimal
model, causal result, profitability, or production readiness is assumed.

### Full-population final experiment (v3)

The user selected **large tuning samples followed by full-data final training,
validation and TEST**. New artifacts belong to `phase14_volatility_aligned_v3`.
V1 and the sample-capped V2 pilot remain preserved; V2 completed 326 fits before
the population-policy change. Their results are not mixed into the new search.

Tuning caps are 240,000 tabular rows, 60,000 recurrent endpoints, and 20,000
validation endpoints per fold, bounded by actual eligible observations. All
tuning candidates retain identical common 120-bar validation endpoints. Final
models use every valid training and validation observation for their chosen
lookback (all valid rows for tabular models), followed by every eligible TEST
observation. Each final coverage report and an independent population-count audit
must demonstrate the absence of final sample caps. The final validation period
selects neural epochs; it is not added to the training split. TEST remains isolated.

Final full-population metrics and identical-key common-120-bar metrics are
reported separately. Full-population scores from different model populations
must not be ranked as though their rows were identical. No additional raw bars
are manufactured by combining Batch/Streaming copies of the same observation.

Prepared targets are referenced read-only from V2 with complete file checksums,
not duplicated again. Preserve this shared target cache while V3 uses it. Fit
preprocessing uses exact train-only per-column medians and batched moments.
Tabular fit matrices are temporary disk-backed arrays; neural sequences are
generated batch by batch. The target-context cache is bounded to three entries.
All completed-trial metrics, histories, parameters and predictions are retained;
intermediate tuning model binaries may be removed after validation, retaining
their checksums. All final model binaries are preserved. A forest configuration
that exceeds the available-memory estimate is marked resource-infeasible rather
than silently using less training data. No time cutoff is enabled.

Preserve Phase 10,
Phase 11, Phase 13, recovered IDs, predictions, test results, and stopped work.
The previous Phase 13 v2 source implementation was absent from this checkout; preserve
whatever artifacts remain and inventory them without inventing missing results.
The existing historical TEST has already been examined; isolating it during this
selection does not make it a new untouched dataset. Power BI extensions are
separate and may include measured metrics only after artifact validation passes.

## Primary references

- [Realized variance construction and microstructure limitations](https://public.econ.duke.edu/~boller/Published_Papers/rfs_18.pdf).
- [Chronological expanding validation](https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.TimeSeriesSplit.html).
- [Logistic solver and regularization compatibility](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LogisticRegression.html).
