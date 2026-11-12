# LABGDD V2 – Development Handoff

**Phase:** Phase 2, Milestone 4 – Bounded Spark Expected-Series Integration

**Task:** Final architecture hardening and freeze

**Status:** Completed

**Date:** 2026-11-04

**Git branch and commit:** `main`; frozen by annotated tag
`phase2-expected-series-spark-approved-v1`. The immutable commit and tag-object
hashes are recorded in the post-commit freeze report because a commit cannot contain
its own hash.

## 1. Objective

Harden the optional expected-series Spark integration before approval. The work had
to reject SQL-NULL and type-based validation bypasses, validate canonical Silver
observations before joining, keep ineligible financial values null, bind provenance
fingerprints to the approved research plan, preserve exact AAPL legacy parity, and
freeze the bounded milestone without publishing or modifying Bronze, Silver or Gold
evidence.

## 2. Changes Implemented

`labgdd.processing.expected_series_spark` now owns the exact non-nullable Spark
authority schema, population-policy constants, deterministic length-prefixed
identity encoding, plan fingerprint generation and projection from an approved
`ExpectedSeriesPlan`. Timestamps are fingerprinted as UTC epoch microseconds;
membership change arrays and row identities are encoded deterministically.
`expected_plan_fingerprint(...)` derives the caller's approved digest independently
of the externally supplied Spark relation.

`labgdd.processing.gold_batch._validated_expected_series(...)` treats an external
DataFrame as untrusted. It requires exact field types/nullability; rejects null or
blank mandatory values; validates enums, source/feed/timeframe/adjustment, XNYS
calendar authority, temporal bounds, membership geometry and eligibility policy;
derives the eligible state and reason from evidence; and rejects contradictory
logical definitions. Exact duplicate definitions remain idempotent. It recomputes
each identity and the complete plan digest, including the zero-row plan case, and
compares them with both the relation-carried values and the separately supplied
approved digest. A strict caller can require
`verified_historical_applicability`, preventing a boolean or policy-label downgrade
from bypassing listing/entity/evidence requirements.

`_validated_canonical(...)` now checks required fields and types, non-null/nonblank
values, Alpaca/IEX/1Min/raw identity, XNYS calendar provider, identifiers, accepted
quality states, minute alignment, session and membership bounds, exact one-minute
`bar_end`, positive prices and canonical logical-key uniqueness. Duplicate or
conflicting observations fail; none are arbitrarily selected.

`transform_individual_gold(...)` retains its original two-argument behavior. Its
optional expected-series path requires `approved_expected_plan_sha256`, validates
canonical input before the left join, keeps observation existence separate from
financial eligibility, and assigns controlled reasons for missing, historically
ineligible, membership-invalid or provenance-inconsistent slots. Intrabar,
close-to-close, rolling and fixed financial values remain null when their underlying
expectation or observation is ineligible.

## 3. Files Created or Modified

- `src/labgdd/processing/expected_series_spark.py`: typed expected authority and
  deterministic fingerprint boundary.
- `src/labgdd/processing/gold_batch.py`: fail-closed expected/canonical validation
  and optional left-join calculations.
- `tests/integration/test_spark_expected_series.py`: absent-series, all-horizon,
  strict-mode, fingerprint, schema, canonical and AAPL parity regressions.
- `scripts/smoke_gold_container.py` and `scripts/smoke_wheel.py`: Linux Spark and
  installed-package coverage.
- `docs/decisions/010-expected-series-spark-population.md`: Accepted decision.
- `docs/architecture/PHASE2_EXPECTED_SERIES_SPARK.md`, `PHASE2_GOLD.md`,
  `RESEARCH_UNIVERSE.md`, `ARCHITECTURE.md`, `IMPLEMENTATION_PLAN.md`, and
  `README.md`: architecture, authority, scope and usage documentation.
- `docs/handoffs/PHASE_2_MILESTONE_4_EXPECTED_SERIES_SPARK_HANDOFF.md`: preserved
  pre-hardening execution record.
- `docs/handoffs/PHASE_2_MILESTONE_4_FINAL_HANDOFF.md`: this final record.

## 4. Architecture and Design Decisions

ADR 010 is Accepted. Expected population remains an optional interface in the
modular monolith; no runner or publication registry uses it automatically. The
legacy Gold path and approved Gold publication are unchanged. The relation cannot
self-attest merely by carrying 64-character strings: the caller must provide the
digest derived from its approved plan, after which Spark recomputes row and relation
digests. This is deterministic provenance validation, not cryptographic signing.

The expected relation is the population authority, while canonical Silver remains
the observation authority. Their calendar and membership provenance must agree.
Missingness, observation presence, financial eligibility, historical evidence,
source completeness and correction finality remain separate states. No driver-side
collection was added to financial processing; bounded Python construction is used
only to project the already bounded research plan.

## 5. Tests and Validation

Final passed checks:

- `.venv/Scripts/python -m ruff format --check .`: 61 files formatted.
- `.venv/Scripts/python -m ruff check .`: clean.
- `.venv/Scripts/python -m mypy`: strict typing passed for 61 files.
- `.venv/Scripts/python -m pytest tests/integration/test_spark_gold.py -q`: 3 passed
  in 159.67 seconds.
- `.venv/Scripts/python -m pytest -q`: **248 passed in 833.76 seconds**.
- `.venv/Scripts/python -m pip check`: no broken requirements.
- `scripts/check_repository.py`: 119 candidate files passed credential, forbidden
  path, ignore/tracking and reference-audit integrity checks before this final
  handoff was added; the staged form is rerun at freeze.
- `python -m build --no-isolation`: v0.4.0 sdist and wheel built.
- `scripts/smoke_wheel.py ...0.4.0...`: clean offline install, imports, CLIs and
  dependency check passed.
- `scripts/validate_gold_evidence.py`: seven Silver and seven Gold artifacts
  rehashed; 390 return, 1,556 rolling and 120 fixed rows; zero temporal/key
  mismatches. Maximum absolute errors were `2.6427936174102523e-16` (returns),
  `1.2723112494117395e-16` (rolling), `6.869437202232376e-19` (variance), and
  `2.1640675362810669e-16` (fixed volatility).
- Fresh Linux image `sha256:1f80b0c23e8839d44f699f1fb8b5a6529cd093ce5ec8f350cebaa147dbe90ab0`:
  Silver smoke passed with 4 canonical, 0 quarantine, 4 sessions; Gold smoke passed
  with legacy 390/1,556/120 and expected 780/3,112/240.

One focused regression run failed while authoring the zero-row test because the
fixture attempted an invalid empty research universe. The fixture was corrected to
retain a valid membership authority with zero expected rows; that test then passed,
and the complete 248-test suite was rerun successfully. Pytest emitted post-success
Windows cleanup warnings (`WinError 1392/145`) for a damaged/nonempty external temp
directory; no assertion or repository artifact failed.

## 6. Issues and Limitations

The optional path is not exposed by a runner and has no persisted Gold publication.
The declared 30-symbol cohort still lacks independently verified historical listing,
entity and sector applicability. IEX completeness and provider correction finality
remain unproven. The fingerprint boundary detects inconsistency but is not a signer
or protection against a malicious caller controlling both the plan and approval
argument. Windows temporary-storage health should be checked separately because of
the cleanup warnings; it did not affect the verified repository outputs.

## 7. Scientific and Data Integrity

Measurement specification v2.0.0, timestamps, session anchors and all four horizons
are unchanged. Exact AAPL parity remains 390 return slots (389 eligible), 1,556
rolling rows (1,450 eligible), and 120 eligible fixed windows, with identical legacy
columns, boundaries, nullability, reasons and values. No synthetic bar, zero fill,
gap compression, overnight link or inferred historical membership was introduced.
Frozen Bronze, Silver and Gold artifacts and their publication registries were not
modified.

## 8. Current Project Status

The project can generate an approved expected symbol/session relation independently
of observed bars and apply it to bounded native Spark Gold calculations while
retaining fully absent symbols and sessions. It can fail closed on malformed
expectations, forged fingerprints, duplicate canonical keys and contradictory
provenance. This milestone produced validation evidence only; it did not publish a
new dataset or start sector analytics.

## 9. Recommended Next Step

Architecture-review this frozen milestone. Only after explicit approval, define a
separate bounded publication milestone with a new dataset/configuration identity and
reviewed expected-plan lineage. Do not reuse or mutate the existing approved Gold
publication.

## 10. Review Questions

None. ADR 010 and the bounded validation scope are ready for review.

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
