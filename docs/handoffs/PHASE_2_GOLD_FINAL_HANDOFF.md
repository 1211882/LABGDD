# LABGDD V2 – Development Handoff

**Phase:** Phase 2 – Individual-company Gold finalization
**Task:** Gold final code-review remediation, publication authority and baseline freeze
**Status:** Completed
**Date:** 2026-11-03
**Git branch and commit:** `main`; implementation commit `8f2c69a8f0ebb5cfd209f6bb92a40d7606053300`; annotated tag `phase2-individual-gold-approved-v1` is the final approval reference

## 1. Objective

Correct the independent Python reference rolling-window boundary, prove full
record-level Python/Spark parity, establish explicit Gold publication authority,
harden failure-safe local publication, independently revalidate the existing real
AAPL Gold evidence, and freeze the accepted individual-company Gold milestone.
Sector analytics and all later infrastructure remained out of scope.

## 2. Changes Implemented

The root cause was an off-by-one expression in
`labgdd.analytics.individual.calculate_individual_measurements`. The reference used
`session_open + (end_index - horizon + 2) minutes`, which labeled every rolling
window start one minute late. It now uses `end_index - horizon + 1`, exactly
equivalent to `window_start = window_end - H minutes`. The production Spark
implementation already used the correct subtraction, so no stored Gold calculation
or timestamp changed. Fixed-window anchors were untouched.

`tests/integration/test_spark_gold.py` now compares independent Python and Spark
records across H={5,15,30,60}. It checks series identity, UTC window bounds,
expected/observed counts, eligibility, nullability, reason codes and numerical
values. Fixtures cover a complete 390-minute summer session, zero volatility, a
210-minute early close with an interior gap, a winter session missing its opening
bar, a post-DST session with a membership boundary, multiple symbols and reversed
input order. The first eligible AAPL H=5 window is exactly 13:31–13:36 UTC.

`labgdd.adapters.gold.resolve_approved_gold` and
`config/gold_publications.toml` implement explicit Gold authority. Resolution
requires dataset and run UUID and verifies terminal status, schema/measurement
version, input Silver identities, processing/configuration fingerprints, manifest
checksum, every artifact size/checksum and exact inventory. It rejects unregistered,
superseded, failed or tampered evidence; it never scans for a newest directory.

`GoldRun.finish` now refuses a complete publication unless `returns`, `rolling` and
`fixed` all exist with Parquet artifacts. Tests inject manifest-creation and rename
failures, partial output, duplicate UUIDs and artifact/manifest tampering. These are
local-filesystem guarantees only.

## 3. Files Created or Modified

- `src/labgdd/analytics/individual.py`: corrected rolling temporal boundary.
- `src/labgdd/adapters/gold.py`: approved-run resolver and complete-inventory gate.
- `config/gold_publications.toml`: sole approved Gold authority and superseded runs.
- `tests/integration/test_spark_gold.py`: systematic record/value/temporal parity.
- `tests/unit/test_individual_measurements.py`: explicit 13:31–13:36 boundary fixture.
- `tests/unit/test_gold_registry.py`: authority, supersession, failure and tamper tests.
- `tests/unit/test_gold_storage.py`: incomplete, manifest and rename failure tests.
- `scripts/validate_gold_evidence.py`: exact real-data record/temporal/value comparison.
- `docs/decisions/008-individual-gold-measurements.md`: ADR accepted with governance
  and expected-series limitation.
- `docs/architecture/PHASE2_GOLD.md`, `docs/architecture/IMPLEMENTATION_PLAN.md`,
  `README.md`: final architecture/status documentation.
- The complete earlier Gold implementation, configuration, runner, container smoke
  and tests were committed with the remediation as one reviewed milestone.

## 4. Architecture and Design Decisions

The existing immutable Gold run `2c36a9ab-78ce-45a8-869d-8c0225267d17` remains
the approved evidence. Independent validation proved its Spark window keys already
matched the corrected reference, so republishing identical data would have created
unnecessary evidence. Its manifest SHA-256 is
`cadbbcf50150b62ffc06aa348f503837c87281bbca2690d0179a788ec599d7ce`.

The registry supersedes development runs `e43ada7f-...` and `c6f9dc52-...` and does
not register the failed Windows attempt. Approval pins measurement v2.0.0, Silver
run `aecb53c1-d7a8-40c7-939b-a6901f435ff5`, Silver manifest SHA-256
`33209b03a5f5f6d39efea922376e13f08c59756aa5a7cc01e3cae367711933d6`,
processing fingerprint `61a13645c92841deb3564b79d27d7c9e0a8e20d348009f36e21a770dc4859778`
and configuration fingerprint
`a0bc3d8a7abe92a3b97226a730ab076a961206715b7746137f3a1719faf35094`.

Expected series are currently seeded by distinct canonical Silver series/session
metadata. That is correct for the bounded AAPL evidence because the series exists
and its approved session bounds generate all 390 slots. It cannot represent a
wholly absent symbol/session. Future full-cohort work must provide an approved
symbol/session expectation frame from versioned cohort/membership and calendar
evidence, then left-join observations. This task did not invent missing entities.

## 5. Tests and Validation

Passed after the workstation restart:

- `python -m ruff format --check .` – 54 files formatted.
- `python -m ruff check .` – passed.
- `python -m mypy` – strict typing passed for 54 source files.
- `python -m pytest` – **231 passed in 405.70 seconds**. Post-success cleanup warnings
  concerned a nonempty external pytest temporary directory; no assertion failed.
- `python -m pip check` – no broken requirements.
- `python scripts/check_repository.py` – passed for 103 candidates before this handoff.
- `python -m build --no-isolation` – v0.4.0 sdist and wheel built.
- `scripts/smoke_wheel.py` – clean installation/imports/CLIs/pip check passed.
- Gold Linux container smoke – `390/1556/120` passed.
- Silver Linux regression smoke – four canonical sessions, zero quarantine passed.
- Final image `labgdd-spark-batch:phase2-gold-final` –
  `sha256:730659f85820c3f4a47f0ea8e51ef6bdd8ee7e9a2efe2cade3e140f5c1558b7d`.

The blue-screen interruption terminated an earlier incomplete test process. No
result from that process was claimed; the entire 231-test suite and subsequent
gates were rerun from the durable repository state.

Independent real-data validation verified seven Silver and seven Gold artifacts,
390 expected/return rows, 389 eligible adjacent returns, 1,556 rolling rows, 1,450
eligible rolling rows and 120 eligible fixed windows. It found zero temporal or
record-key mismatches. Maximum absolute differences were:

- Returns: `2.6427936174102523e-16`.
- Rolling volatility: `1.2723112494117395e-16`.
- Fixed realized variance: `6.869437202232376e-19`.
- Fixed realized volatility: `2.1640675362810669e-16`.

All satisfy `abs(actual-expected) <= 1e-12 + 1e-10*abs(expected)`.

## 6. Issues and Limitations

The verified evidence is one AAPL IEX session, not full-cohort or multi-session
scale evidence. IEX venue coverage, provider correction finality and historical
listing/entity/sector membership remain unproven. Expected-series generation cannot
represent a wholly absent series until an approved expectation relation is added.
The local staging/rename protocol does not provide object-store transactionality.
Windows native Hadoop remains unsupported; Linux container execution is authoritative.
No distributed-cluster scale or recovery claim is made.

## 7. Scientific and Data Integrity

No approved measurement formula changed. The remediation corrected only a reference
window label to the already-approved `(a,b]` convention. Event time remains bar
interval start; return end remains the ending close. H returns require H+1 closes.
There is no overnight linking, filling, zero substitution, annualization, reanchoring,
implicit revision order or inferred historical membership. Fixed-window results and
all immutable Silver/Gold artifacts were preserved. Existing Gold numerical outputs
and timestamps were independently proven correct before registry approval.

## 8. Current Project Status

Phase 0, Bronze, approved Silver and individual-company Gold are operational within
their bounded evidence. Gold consumers can resolve exactly one reviewed run through
explicit registry authority. The project calculates individual returns, rolling
sample volatility, realized variance and realized volatility for H={5,15,30,60}.
Sector analytics, Kafka, Structured Streaming, ML, PostgreSQL, monitoring and Power
BI are not started.

## 9. Recommended Next Step

Review the frozen commit/tag and Gold registry. A future explicitly authorized
milestone should first define the approved expected symbol/session relation needed
for full-cohort processing. Do not begin sector calculations automatically.

## 10. Review Questions

None.

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
