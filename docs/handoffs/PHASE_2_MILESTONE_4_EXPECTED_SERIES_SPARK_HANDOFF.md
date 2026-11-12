# LABGDD V2 – Development Handoff

**Phase:** Phase 2, Milestone 4 – Bounded Spark Expected-Series Integration
**Task:** Add an optional expected-population authority to individual Gold Spark processing
**Status:** Completed
**Date:** 2026-11-04
**Git branch and commit:** `main` at frozen Milestone 3 commit
`453e86be6b07b09342a4e473f15b512cc8b2a236`; Milestone 4 changes are uncommitted
for architecture review

## 1. Objective

Allow bounded Spark processing to distinguish expected research symbol/session
identities from observed canonical bars. Expected population must come from the
approved cohort/calendar reference, retain wholly absent symbols and sessions, and
never fabricate observations. Preserve the frozen AAPL Gold behavior and do not
publish or approve a new dataset.

## 2. Changes Implemented

`processing.expected_series_spark.expected_series_frame(...)` converts an approved
`ExpectedSeriesPlan` into an explicit-schema Spark DataFrame. It records observation
series identity, UTC session and membership bounds, universe/membership versions,
research evidence and verification statuses, full calendar provenance, expected
eligibility, deterministic plan/identity SHA-256 values and population policy
`expected-series-left-join-v1`.

Default `declared_research` mode preserves unverified evidence without claiming
historical verification. `strict_historical=True` marks rows ineligible with
`unverified_historical_applicability` unless evidence, listing and entity identity
are independently verified. Partial or changing membership is explicitly
ineligible with `membership_not_full_session`.

`transform_individual_gold(canonical, horizons, expected_series=None)` retains the
existing behavior when no authority is supplied. The optional path validates the
expected schema, approval marker, contract/policy versions, XNYS authority,
eligibility consistency, single plan fingerprint and identity uniqueness. Exact
duplicates are idempotent; conflicting calendar or membership definitions fail.

The new path explodes scheduled minute slots from expected session bounds and
left-joins canonical rows on symbol/source/feed/timeframe/adjustment/session/event
time. Canonical membership/calendar provenance must match the expectation. Missing
bars retain null prices and measurements with controlled reasons; no zero return,
gap compression, overnight link or reanchoring occurs.

## 3. Files Created or Modified

- `src/labgdd/processing/expected_series_spark.py`: explicit Spark authority schema,
  eligibility projection and deterministic fingerprints.
- `src/labgdd/processing/gold_batch.py`: optional authoritative-population left join
  while preserving the legacy two-argument path.
- `tests/integration/test_spark_expected_series.py`: six bounded Spark fixtures and
  approved AAPL parity.
- `scripts/smoke_gold_container.py`: Linux smoke for legacy and absent-symbol paths.
- `scripts/smoke_wheel.py`: clean-wheel import coverage for the new processing modules.
- `docs/decisions/010-expected-series-spark-population.md`: proposed ADR 010.
- `docs/architecture/PHASE2_EXPECTED_SERIES_SPARK.md`: integration contract/evidence.
- `docs/architecture/ARCHITECTURE.md`, `docs/architecture/PHASE2_GOLD.md`,
  `docs/architecture/RESEARCH_UNIVERSE.md`,
  `docs/architecture/IMPLEMENTATION_PLAN.md`, and `README.md`: bounded capability and
  limitations.
- `docs/handoffs/PHASE_2_MILESTONE_4_EXPECTED_SERIES_SPARK_HANDOFF.md`: this handoff.

No configuration request, runner, publisher, approved registry, or immutable
Bronze/Silver/Gold artifact was changed.

## 4. Architecture and Design Decisions

The expected relation is optional and explicit, preventing a silent change to the
approved AAPL processing path. The new schema/policy is versioned independently of
measurement v2.0.0 because formulas did not change; only population authority did.

Research expectation approval is distinct from verified listing applicability.
Expected rows keep evidence fields in every output. Strict mode fails closed rather
than interpreting an unverified symbol as a historical market absence.

The reference relation is bounded configuration-scale data, so validation uses
distributed `count` actions to reject invalid/contradictory definitions before the
financial plan is returned. Market data is never collected by production code.
Spark global row ordering is not promised; logical keys and fingerprints are
deterministic.

Persistence was deliberately not wired. Any future persisted result must use a new
dataset/configuration identity, retain the expected plan fingerprint and approved
Silver lineage, use established staging/Parquet/checksum publication, and remain
pending until separately approved.

## 5. Tests and Validation

Passed:

- `python -m ruff format --check .` and `python -m ruff check .`: clean.
- `python -m mypy`: strict typing passed for 61 source/test/script files.
- `python -m pytest tests/integration/test_spark_expected_series.py`: 6 passed in
  326.87 seconds.
- `python -m pytest tests/integration/test_spark_gold.py`: 3 passed in 121.73 seconds.
- `python -m pytest`: **245 passed in 692.04 seconds**.
- `python -m pip check`: no broken requirements.
- `python scripts/check_repository.py`: passed for 119 candidate files.
- `python -m build --no-isolation`: v0.4.0 sdist and wheel built successfully.
- `python scripts/smoke_wheel.py ...`: clean offline install, imports, configuration,
  module/console CLIs and dependency check passed.
- Fresh pinned Linux/amd64 image `labgdd-spark-batch:phase2-milestone4-review` built
  successfully with hash-locked dependencies.
- Silver container smoke: 4 canonical, 0 quarantine, 4 session fixtures.
- Gold container smoke: legacy 390/1,556/120 and expected-authority
  780/3,112/240 for one observed plus one absent symbol.

The six new tests cover complete AAPL, absent symbols, an entirely absent scheduled
session, multiple symbols/sectors, opening/interior gaps, all four horizons,
membership changes, strict historical mode, variable counts, holidays, DST, early
close, identical duplicates, calendar/membership contradictions, unapproved
expectations, deterministic fingerprints, and numeric Python/Spark parity.

The approved AAPL Silver input produced 390 AAPL return slots, 389 eligible adjacent
returns, 1,556 rolling rows, 1,450 eligible rolling rows and 120 eligible fixed
windows. All legacy columns, timestamps, boundaries, nullability, reasons and
financial values were exactly equal between the legacy and new AAPL paths.
Independent evidence validation rehashed seven Silver and seven Gold artifacts,
found zero temporal/key mismatches, and measured maximum absolute errors of
`2.6427936174102523e-16` for returns, `1.2723112494117395e-16` for rolling
volatility, `6.869437202232376e-19` for fixed variance and
`2.1640675362810669e-16` for fixed volatility.

On Windows, approved Parquet evidence was loaded read-only with bounded PyArrow
because native Hadoop directory enumeration fails. The transformations remained
native Spark. Pytest completed successfully but emitted the established external
temporary-directory cleanup warnings (`WinError 1392`/`145` and access denied).

## 6. Issues and Limitations

No new Gold publication exists, and the optional path is not exposed through a
runner/configuration. This prevents accidental promotion but means publication
lineage and physical Parquet validation for the extended schema remain future work.

The 29 absent symbols in the AAPL evidence fixture are declared research
expectations with unverified listing/entity evidence; they are not verified market
absences. IEX completeness and correction finality remain unproven. Historical
listing, entity, taxonomy and corporate-action evidence remains unavailable.

The bounded plan-to-DataFrame conversion is driver-local reference projection, not
a distributed-universe ingestion mechanism. No multi-node scale or performance
claim is made.

## 7. Scientific and Data Integrity

No financial formula, estimator, horizon, window anchor, temporal contract,
measurement version or immutable dataset changed. Missing closes remain null.
Return windows remain same-session and consecutive. Membership changes or
provenance mismatches are ineligible rather than silently resolved.

Expected membership, verified historical applicability, scheduled observations,
provider observations, canonical validity, financial eligibility, source
completeness and correction finality remain separate states.

## 8. Current Project Status

The project has frozen Bronze, Silver, individual Gold and research-universe
foundations. It now has a validated but unapproved optional Spark path that retains
expected symbols/sessions independently of observations and reproduces approved
AAPL behavior. It does not yet persist that extended schema or calculate sector
metrics.

## 9. Recommended Next Step

Perform architecture review of ADR 010, the extended Spark schema and reason-code
priority. If approved, define a separate immutable publication request and pending
registry workflow for one bounded expected-series Gold run. Do not begin sector
analytics automatically.

## 10. Review Questions

1. Approve `expected-series-left-join-v1` as the population-policy identity?
2. Approve default declared-research mode plus optional strict historical mode?
3. Approve the extended expected-series columns as the future persisted Gold schema
   boundary?

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
