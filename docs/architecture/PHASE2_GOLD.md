# Phase 2 Milestone 2: individual-company Gold analytics

Date: 2026-11-03. Measurement authority is `MEASUREMENTS.md` v2.0.0 and temporal
authority is `DATA_CONTRACTS.md` v1.0.0. This milestone deliberately excludes all
sector, portfolio, correlation, emergence, streaming, machine-learning and serving
work.

## Input authority and lineage

`runners.gold` loads strict `config/gold.toml` and calls
`resolve_approved_silver(registry_path, data_root, dataset_version, run_id)` before
Spark starts. The request pins Silver dataset `silver-aapl-20260914-v1`, run
`aecb53c1-d7a8-40c7-939b-a6901f435ff5`, measurement v2.0.0 and horizons
5/15/30/60. Gold never scans for a newest directory. Every output row and the
terminal manifest retain the Silver dataset, run and manifest SHA-256. The
manifest also records deterministic configuration/source fingerprints and runtime
versions. Source completeness remains explicitly `unproven`.

## Calculations and eligibility

`analytics.individual.calculate_individual_measurements` is the engine-independent
reference. Decimal prices remain Decimal through natural-log ratio construction;
float64 begins at the measurement result. It emits scheduled slots and implements
intrabar `ln(close/open)`, same-session adjacent-close `ln(C[t]/C[t-1])`, trailing
sample standard deviation with denominator H-1, fixed realized variance `sum(r^2)`
and its square root.

`processing.gold_batch.transform_individual_gold` implements the same definitions
with native Spark SQL/window expressions. It generates the schedule from preserved
session bounds, so absent observations remain absent values rather than moving an
anchor. Overnight links, filling, zero substitution and incomplete final fixed
windows are prohibited. Rolling rows exist at every expected adjacent return end;
early rows are null with `insufficient_history`. Complete scheduled fixed anchors
are retained even when null, with counts and controlled reasons. Windows crossing
membership intervals are ineligible. The 390-bar acceptance geometry is 389
adjacent returns and 77/25/12/6 fixed windows for H=5/15/30/60.

## Storage and validation

Gold writes native Spark Parquet datasets `returns`, `rolling` and `fixed`,
partitioned by session and symbol without a global ordering or driver collection.
All timestamps are physical Parquet microsecond UTC values. Runs start in a Gold
staging namespace; Spark readback verifies counts and distributed logical equality,
while PyArrow verifies consistent physical schemas. Only after validation does the
adapter hash every artifact, write the terminal manifest and atomically rename the
directory. Failed runs publish only under `_failed`; a run UUID cannot overwrite
completed or failed evidence.

`scripts/validate_gold_evidence.py` separately rehashes Silver and Gold inventories,
reopens all three Parquet datasets and compares every eligible real rolling/fixed
value with the pure reference under `abs <= 1e-12 + 1e-10*abs(expected)`. It also
compares series identity, every temporal key, counts, eligibility, nullability and
reason codes exactly. The corrected reference uses `window_start=window_end-H`.

`config/gold_publications.toml` is the downstream authority. Consumers call
`resolve_approved_gold` with an explicit dataset and run UUID. Resolution verifies
terminal status, measurement and Silver identities, processing/configuration
fingerprints, Gold schema, contract-specific fingerprints, manifest checksum, every
artifact checksum and exact inventory; it rejects unregistered and superseded UUIDs.
Registry v1.1 stores multiple independent authorities while the resolver remains
backward-compatible with v1.0. No newest-directory rule exists.

## Optional expected-series population

The frozen publication used the original two-argument transform, which derives
series/session metadata from observed canonical rows. That path is unchanged.

Milestone 4 adds an optional third `expected_series` argument derived from the
versioned research reference and approved calendar schedule. It left-joins canonical
observations, retains wholly absent symbols/sessions and fails closed on conflicting
expectation or canonical evidence. The caller supplies an independently derived
approved-plan fingerprint; the relation's own digest is recomputed but is not trusted
by itself. The new `expected-series-left-join-v1` path was validated but not persisted
or made authoritative. See `PHASE2_EXPECTED_SERIES_SPARK.md`.

## Bounded real evidence

The approved run is `2c36a9ab-78ce-45a8-869d-8c0225267d17`, with manifest SHA-256
`cadbbcf50150b62ffc06aa348f503837c87281bbca2690d0179a788ec599d7ce`.
It derives only from the approved immutable 390-row AAPL/IEX/XNYS Silver run for
2026-09-14; no provider call is made. Expected output geometry is 390 return-slot
rows (389 eligible adjacent returns), 1,556 rolling rows (1,450 eligible after
horizon warm-up), and 120 fixed rows (all eligible).

## Separate expected-series publication candidate

Milestone 5 leaves the approved legacy publication and authority unchanged. A new
dedicated runner resolves the same approved Silver snapshot, independently builds
the 30-symbol `study-cohort-v1` XNYS plan and persists Gold schema v2.0.0 under
dataset `gold-study-cohort-20260914-expected-v1`. The bounded candidate run is
`fddf0d53-e48f-4176-b045-45fcc12f2b8b`, with 11,700 return slots, 46,680 rolling
rows and 3,600 fixed rows. Only AAPL has 390 canonical observations; the 11,310
other return slots preserve null observations and null financial values. Exact
AAPL legacy keys, reasons, eligibility and values were independently compared.

The immutable manifest retains `publication_status=pending_architecture_review` as
its creation-time state. Registry v1.1 subsequently approves the exact dataset/run,
Gold schema and fingerprints as a second authority without replacing AAPL. Expected
cohort membership, observed provider data,
canonical validity, financial eligibility, historical evidence and source
completeness remain separate. No missing minute is a verified historical absence.
ADR 011 records the accepted publication boundary. The candidate layout has one
file for each of 30 symbol partitions in each dataset; because those files are only
about 24–55 KB, future expected-series runs partition by session only. The reviewed
immutable candidate is not rewritten.

## Limits

The approved input covers one symbol and session. IEX coverage and correction
finality are not established; historical membership/listing evidence remains
unverified. The local rename protocol is not an object-store transaction. The Gold
registry provides selection governance, not distributed transaction semantics.
Sector analytics and all later phases remain not started.
