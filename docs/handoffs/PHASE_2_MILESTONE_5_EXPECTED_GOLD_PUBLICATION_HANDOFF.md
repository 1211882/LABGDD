# LABGDD V2 – Development Handoff

**Phase:** Phase 2, Milestone 5 – Expected-Series Gold Publication

**Task:** Make the reviewed expected-series Spark path executable and persist one bounded, immutable Gold publication candidate

**Status:** Completed

**Date:** 2026-11-04
**Git branch and commit:** `main` at frozen baseline `e51c2d00dba881cf74efa9b8f23d93a42db5be5b`; annotated tag `phase2-expected-series-spark-approved-v1` resolves to that commit (tag object `4b9c38732068e4794211293d03eda61982ca52a2`). All Milestone 5 changes are intentionally uncommitted.

## 1. Objective

Turn the accepted optional expected-series Spark transformation into a separately
configured executable publication without changing the frozen legacy Gold runner or
approved Gold registry. The bounded execution had to resolve the approved AAPL
Silver run, independently generate the 30-symbol `study-cohort-v1` XNYS plan for
2026-09-14, preserve missing observations, write immutable native Spark Parquet,
validate checksums and physical schemas, prove exact AAPL parity, and remain pending
architecture review.

## 2. Changes Implemented

`labgdd.expected_gold_config.load_expected_gold_request()` now loads an exact-key,
side-effect-free request. It pins dataset/run authorities, research reference,
sessions, population/eligibility policies, measurement/schema versions and all four
horizons. Unknown keys, external paths, unsupported policies, duplicate/unsorted
dates and incompatible contracts fail closed.

`labgdd.processing.expected_gold.prepare_expected_gold()` resolves the explicitly
approved Silver run through the existing registry, loads the versioned research
universe, obtains the pinned XNYS snapshot, generates the plan independently of
observed bars and derives research, plan and configuration SHA-256 values before
Spark starts. `process_expected_gold()` reads only the approved canonical dataset,
uses the reviewed expected-series left join and v2.0.0 formulas, adds lineage, and
reuses `GoldRun` staging, readback, checksum and atomic local rename behavior. It
records exact counts/reasons and environment versions, labels source completeness
`unproven`, and publishes only with `pending_architecture_review`.

`labgdd.runners.expected_gold.main()` provides the separate
`labgdd-expected-gold` entry point. `--check` validates all authorities without
starting Spark or writing storage. The batch optional dependency/lock now includes
the already approved calendar and timezone closure required to construct the plan.

The first bounded execution failed closed because the physical validator expected a
nullable list element while native Spark correctly wrote
`list<element: timestamp[us, UTC] not null>`. The failed run
`fe38fb7a-5eb6-4f4c-ab59-ec6b39108423` remains immutable under `_failed`, with
`failure_stage=validate_outputs`; it was never reported complete. Validation was
corrected to require Spark's actual non-null element contract. The corrected run is
`fddf0d53-e48f-4176-b045-45fcc12f2b8b`.

## 3. Files Created or Modified

- `config/expected_gold.toml`: separate bounded publication request.
- `src/labgdd/expected_gold_config.py`: strict request model and loader.
- `src/labgdd/processing/expected_gold.py`: authority preparation, Spark execution,
  validation, manifest construction and immutable publication.
- `src/labgdd/runners/expected_gold.py`: dedicated CLI and read-only check mode.
- `src/labgdd/processing/gold.py`: opt-in physical expected-authority validation;
  the legacy call remains unchanged.
- `pyproject.toml`, `requirements-batch.lock`, `scripts/smoke_wheel.py`: entry point,
  hash-locked XNYS dependency closure and installed-package import coverage.
- `scripts/validate_expected_gold_evidence.py`: independent artifact, schema,
  geometry, nullability and legacy-AAPL parity validator.
- `tests/unit/test_expected_gold_config.py`: strict configuration, both explicit
  policies and no-write/no-Spark checks, including an existing-artifact inventory.
- `tests/integration/test_expected_gold_publication.py`: Linux publication,
  immutability, checksum, absent-series and failed-validation regressions.
- `docs/decisions/011-expected-series-gold-publication.md`: proposed publication
  decision; not accepted automatically.
- `README.md` and `docs/architecture/{ARCHITECTURE.md,IMPLEMENTATION_PLAN.md,PHASE2_EXPECTED_SERIES_SPARK.md,PHASE2_GOLD.md,RESEARCH_UNIVERSE.md}`: scope,
  candidate evidence and authority distinctions.
- This handoff document.

## 4. Architecture and Design Decisions

The new dataset is deliberately separate from the frozen individual Gold identity.
Input selection is by approved Silver dataset and run UUID; output selection is
never based on the newest directory. The checked-in `config/gold_publications.toml`
was not modified, so consumers cannot treat this candidate as approved.

The plan fingerprint is derived from the approved research plan outside the Spark
relation, then revalidated by the existing Milestone 4 boundary. Expected research
membership, observed provider data, canonical validity, financial eligibility,
historical evidence, source completeness and correction finality remain distinct.
Missing IEX bars remain null; no financial observation, return, price or zero is
fabricated. Gold schema v2.0.0 identifies the extended expected/provenance shape;
measurement formulas remain v2.0.0.

Publication remains a local same-filesystem staging/rename protocol. It validates
logical counts, physical UTC microsecond timestamps, readback, exact inventory and
SHA-256 values before terminal completion. It does not claim object-store
transactionality or multi-node Spark scale.

## 5. Tests and Validation

Passed commands and results:

- `.venv/Scripts/python -m ruff format --check .`: 67 files already formatted.
- `.venv/Scripts/python -m ruff check .`: passed.
- `.venv/Scripts/python -m mypy`: passed, 67 source files.
- `.venv/Scripts/python -m pytest`: 252 passed, 2 skipped in 855.90 seconds. The
  two skips are explicitly Linux-only publication tests on Windows. Pytest emitted
  cleanup warnings for a pre-existing damaged external Windows temporary directory;
  no test failed.
- Focused configuration/registry tests: 8 passed.
- `.venv/Scripts/python -m pip check`: no broken requirements.
- `.venv/Scripts/python -m build --no-isolation`: sdist and
  `labgdd_v2-0.4.0-py3-none-any.whl` built successfully.
- `.venv/Scripts/python scripts/smoke_wheel.py ...`: clean wheel installation,
  imports, CLIs and pip consistency passed with zero mandatory base dependencies.
- `.venv/Scripts/python scripts/check_repository.py`: passed, 128 candidates.
- `scripts/validate_expected_gold_evidence.py`: passed; verified 181 artifacts,
  manifest SHA-256 `985574b1701c879679090202e9ab39e99cb15291d550c26da625e6bdf026e59e`,
  physical schemas, counts, null absent values and exact AAPL keys/reasons/values.
- Cold `docker build` from the hash lock passed. Final image ID:
  `sha256:7f081a238c2cd46ec24333260b218c574c665a857cca974ab3c23faac47f2597`.
- Read-only container `labgdd-expected-gold --check`: passed.
- Linux native Silver smoke: passed, four DST/early-close session fixtures.
- Linux native Gold smoke: passed, legacy `390/1556/120` and expected-series
  `780/3112/240` fixture geometry.
- `git diff --check`, exact registry diff, ignore/tracking and blank-placeholder
  checks: passed. No data/build/environment file is tracked.

The real bounded publication produced 11,700 returns, 46,680 rolling rows and
3,600 fixed rows from 30 expected symbols and one session. It contains 390 canonical
AAPL slots, 390 observed slots and one observed symbol. Eligible counts are 389
returns, 1,450 rolling and 120 fixed. Reasons are: returns 389 eligible, 11,310
`missing_close`, one `session_boundary`; rolling 1,450 eligible, 42,050
`incomplete_return_window`, 3,180 `insufficient_history`; fixed 120 eligible and
3,480 `incomplete_fixed_window`. All 181 artifact checksums passed.

## 6. Issues and Limitations

The candidate is not approved and has no publication-registry entry. The production
image intentionally excludes pytest, so the two new publication test functions were
not run by pytest inside that image; equivalent native success/failure behavior was
exercised by the bounded run, preserved failed run, independent validator and Linux
smokes. The first failed run is retained as audit evidence and consumes local space.

Only one AAPL/IEX session contains observations. IEX is not consolidated coverage;
provider corrections and finality remain unproven. Historical listing, entity and
sector membership are research assumptions, not verified history. Multi-session,
multi-symbol observed acquisition, multi-node Spark and object-store publication
remain unverified. No downstream registry resolver exists for this pending dataset.

## 7. Scientific and Data Integrity

No financial formula or approved temporal contract changed. AAPL exactly reproduces
the frozen legacy 390 return rows, 1,556 rolling rows and 120 fixed rows, including
keys, boundaries, reasons, eligibility and finite values within the approved
tolerance. The other 29 cohort symbols are expected research identities with null
observations; their missingness is not evidence of historical absence. No minute is
filled and no missing observation contributes a financial value. Frozen Bronze,
Silver and approved Gold manifests retain their registered SHA-256 values.

## 8. Current Project Status

The project can now deterministically prepare and execute a bounded expected-series
Gold publication from approved Silver in native Linux Spark, preserving absent
series and complete lineage. One complete immutable candidate and one correctly
failed immutable audit run exist locally. The approved legacy Gold remains the only
registered authority. Sector analytics, broad acquisition, Kafka, streaming, ML,
serving and observability platforms have not started.

## 9. Recommended Next Step

Perform architecture review of ADR 011, the candidate manifest/schema, failed-run
evidence and exact AAPL parity. If accepted, define a separate explicit publication
registry decision for this dataset. Do not infer approval from directory existence
and do not begin sector analytics during that review.

## 10. Review Questions

1. Approve or reject ADR 011 and Gold schema v2.0.0 as the publication contract for
   expected-series individual measurements.
2. Decide whether the reviewed candidate run should receive an explicit approved
   registry entry or whether a fresh run from an accepted commit is required.

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
