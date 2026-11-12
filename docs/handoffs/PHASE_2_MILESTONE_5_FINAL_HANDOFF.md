# LABGDD V2 – Development Handoff

**Phase:** Phase 2, Milestone 5 – Expected-Series Gold Publication

**Task:** Final architecture review, independent Linux validation, publication governance and freeze

**Status:** Completed

**Date:** 2026-11-05

**Git branch and commit:** `main`. Candidate-generating source is preserved at
`cbf126beb732d3a2f43c4ffae3381a69a3a624f0`. The final approval commit and annotated
tag `phase2-expected-gold-publication-approved-v1` are created after this document is
committed; their exact object identities are recorded in the tag and final execution
report.

## 1. Objective

Perform the final correctness and architecture review of the bounded expected-series
Gold publisher, execute both previously skipped publication tests under Linux,
analyze its physical storage, establish backward-compatible multi-dataset Gold
authority, decide whether the existing candidate could be approved without
reprocessing, and freeze Milestone 5 without beginning sector analytics or another
phase.

## 2. Changes Implemented

The review found no financial, temporal, lineage or immutability defect in the
candidate. `prepare_expected_gold()` resolves the explicitly requested Silver
dataset/run through `resolve_approved_silver()`, independently loads the versioned
research reference and XNYS schedule, and derives plan, research and configuration
fingerprints before Spark starts. `process_expected_gold()` preserves extended
authority and observation provenance, validates that ineligible values remain null,
uses staged `GoldRun` publication and fails closed.

One governance limitation was concrete: Gold registry v1.0 held one authority and
`resolve_approved_gold()` hard-coded Gold schema `1.0.0`. Registry v1.1 now uses an
array of explicit authorities. Every entry pins dataset/run UUID, manifest, Gold
schema, measurement version, Silver lineage, source/configuration fingerprints,
creation-time publication status and optional contract fingerprints. The resolver
validates the entire authority inventory, explicit identity, manifest location and
SHA-256, metadata, exact artifact inventory/checksums and superseded-run exclusions.
It still reads the old v1.0 singleton format.

`config/gold_publications.toml` retains the original AAPL authority unchanged in
identity and adds the reviewed expected-series dataset as a second authority. The
candidate manifest remains immutable and truthfully retains
`pending_architecture_review` as its creation-time status; the registry is the later
external approval decision. No directory recency is used.

Storage review identified inefficient future partitioning, not candidate corruption.
The candidate has 90 tiny Parquet files because every dataset was partitioned by
session and symbol. Future expected-series writes now partition only by `session_id`;
the legacy Gold default remains session/symbol, and the existing candidate was not
rewritten. The Linux publication regression asserts the new layout.

## 3. Files Created or Modified

- `src/labgdd/adapters/gold.py`: backward-compatible v1.0/v1.1 authority parsing,
  multi-dataset selection, per-schema metadata and contract-fingerprint validation.
- `config/gold_publications.toml`: two independent approved authorities; the legacy
  AAPL entry remains intact and the expected-series candidate is explicit.
- `src/labgdd/processing/gold.py`: optional partition-column parameter; legacy
  behavior remains the default.
- `src/labgdd/processing/expected_gold.py`: session-only physical partitioning for
  future publications.
- `tests/unit/test_gold_registry.py`: v1.0 compatibility, v1.1 multi-authority,
  schema/contract mismatch, tampering, superseded and explicit-selection tests.
- `tests/integration/test_expected_gold_publication.py`: session-only layout
  regression in addition to immutability and failed-publication tests.
- `docker/spark-batch/Dockerfile.test` and `requirements-linux-test.lock`: dedicated,
  minimal hash-locked pytest layer; production Docker dependencies are unchanged.
- `docs/decisions/011-expected-series-gold-publication.md`: status changed from
  Proposed to Accepted with registry and provenance decisions.
- `README.md` and `docs/architecture/ARCHITECTURE.md`, `IMPLEMENTATION_PLAN.md`,
  `PHASE2_EXPECTED_SERIES_SPARK.md`, `PHASE2_GOLD.md`, `RESEARCH_UNIVERSE.md`: final
  approval, authority and storage documentation.
- This final handoff. The earlier Milestone 5 handoff remains preserved.

## 4. Architecture and Design Decisions

Authority is external and explicit. A complete run directory alone is never
authoritative. The registry requires dataset plus run UUID and supports independent
Gold schema versions without replacing a prior authority. An embedded publication
status records creation-time state; a later registry decision can approve that exact
immutable evidence while verifying the embedded value rather than rewriting it.

The existing candidate is approved without reprocessing. Its manifest source SHA-256
`9f41729c1aa6887f6f1bb418154f80c3d3846c5650d82dd19a0f736ca684e5e9`
was independently reconstructed from all 37 Python files in commit
`cbf126beb732d3a2f43c4ffae3381a69a3a624f0` and matched exactly. Configuration,
plan and research hashes also match the manifest. Later changes affect selection
governance and future file grouping only; they do not invalidate the already
validated rows, formulas, schemas or files.

The candidate's one-file-per-symbol layout is valid but inefficient. Session-only
partitioning is the smallest future correction: symbol remains a normal column and
can be filtered with Parquet statistics, while eliminating high-cardinality tiny
directories. The immutable candidate is grandfathered as reviewed evidence.

## 5. Tests and Validation

Actual passed results:

- Ruff format check: 67 files already formatted.
- Ruff lint: passed.
- Strict mypy: passed for 67 source files.
- Full Windows pytest: 254 passed, 2 platform-skipped in 1,000.25 seconds. The skips
  were only the two authoritative Linux publication tests. Pytest also reported a
  pre-existing damaged external Windows temporary-directory cleanup warning; no
  repository test failed.
- Dedicated Linux pytest layer on Python 3.11.18/Spark 4.3.0:
  `test_expected_gold_publication_is_immutable_extended_and_checksummed` passed;
  `test_expected_gold_failure_is_not_published_complete` passed. Final run: 2 passed,
  0 skipped in 70.56 seconds.
- Dependency consistency: `pip check` passed.
- Repository security/tracking check: passed for 131 candidates.
- Build: sdist and `labgdd_v2-0.4.0-py3-none-any.whl` succeeded.
- Clean-wheel smoke: installation, imports, module/console CLIs and pip consistency
  passed with zero mandatory base dependencies.
- Final production Docker build passed. Image:
  `sha256:8550e5b1181f7a7d2ebaa0cfeea04733bbc421e54556cac614a4ef6cd3b2db44`.
  It contains no pytest or development-test layer.
- Native Linux Silver smoke passed: four calendar/timestamp sessions.
- Native Linux Gold smoke passed: legacy `390/1556/120` and expected-series
  `780/3112/240` fixture geometry.
- Independent real-evidence validator passed: 181 artifacts, physical UTC schemas,
  manifest counts/nulls/checksums and exact AAPL key/reason/value parity.
- Actual registry resolution passed for both the legacy schema v1.0.0 authority and
  expected-series schema v2.0.0 authority.

Candidate storage analysis, excluding CRC sidecars and manifest/run JSON:

| Dataset | Parquet files | Total bytes | Median bytes | Largest bytes |
|---|---:|---:|---:|---:|
| `returns` | 30 | 906,420 | 29,365.5 | 54,985 |
| `rolling` | 30 | 903,012 | 29,735.5 | 40,847 |
| `fixed` | 30 | 734,509 | 24,426.5 | 26,305 |

Each dataset has 30 partitions of the form
`session_id=2026-09-14/symbol=<SYMBOL>`, exactly one Parquet file per partition.
Thus 90 tiny Parquet files and 90 Spark CRC sidecars account for 180 of the 181
manifest artifacts; `run.json` is the other artifact. This is unnecessary small-file
generation for approximately 2.54 MB of Parquet data, hence the future layout fix.

## 6. Issues and Limitations

The approved evidence still covers only one AAPL/IEX session; the other 29 expected
symbols are explicit null observations, not verified historical absences. IEX is not
consolidated coverage. Provider correction finality, historical listing, entity
identity and sector membership remain unverified. The local atomic rename protocol
is not an object-store transaction. Multi-node Spark, broad multi-symbol observed
acquisition and distributed-scale file sizing remain untested.

The candidate keeps its inefficient but valid 90-file layout. Future session-only
layout is Linux-tested only on bounded fixtures and requires scale evidence before
claiming an optimal target file size. No automatic compaction is introduced.

## 7. Scientific and Data Integrity

No mathematical formula, temporal contract, eligibility rule or expected-series
identity changed. The candidate still contains 11,700 returns, 46,680 rolling rows
and 3,600 fixed rows; 11,310 absent return slots have null observations and financial
values. AAPL remains exactly equal to approved legacy evidence: 390 return rows,
1,556 rolling rows and 120 fixed rows, with 389/1,450/120 eligible respectively.

Candidate manifest SHA-256 remains
`985574b1701c879679090202e9ab39e99cb15291d550c26da625e6bdf026e59e`.
The initial failed run remains isolated with status `failed`, stage
`validate_outputs`, and manifest SHA-256
`02885c1b45a1074ff7c77cd26af9149da7423c31bb0f45683f83004b3b27e0a8`.
No Bronze, Silver, legacy Gold or candidate artifact was modified.

## 8. Current Project Status

Phase 2 now has independently approved immutable Silver, legacy individual Gold and
expected-series individual Gold authorities. The latter preserves the full declared
30-symbol expected population while distinguishing missing observations from
financial eligibility. Registry v1.1 safely selects either dataset explicitly and
rejects unregistered, superseded, mismatched or tampered evidence. Sector analytics,
new acquisitions, streaming, ML, serving and monitoring remain not started.

## 9. Recommended Next Step

Stop at this freeze. Any next milestone requires explicit architecture authorization.
When authorized, scale/file-layout evidence should be gathered before broad
publication, and historical membership evidence remains a prerequisite for
historical sector claims.

## 10. Review Questions

None.

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
