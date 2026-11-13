# LABGDD V2 – Development Handoff

**Phase:** Phase 2, Milestone 7 – Multi-Symbol Publication Governance
**Task:** Activate the reviewed Bronze/Silver/Gold authorities and freeze the milestone
**Status:** Completed
**Date:** 2026-11-12
**Git branch and commit:** `main`; final commit and annotated tag identities are recorded by Git and in the final execution response because a tracked handoff cannot contain the hash of its own commit.

## 1. Objective

Execute the explicitly authorized four-step publication plan against the exact
reviewed candidate identities, verify every authority after each mutation, accept the
multi-layer governance architecture, and freeze Milestone 7 without regenerating
data, calling Alpaca, changing measurements, or starting Sector Gold.

## 2. Changes Implemented

The previously reviewed governance implementation is now active. Bronze publication
authority is external to acquisition completion and resolves exact acquisition UUIDs,
source/schema/calendar fingerprints and immutable raw inventories. Silver registry
v1.1 supports multiple exact authorities while retaining valid v1.0 compatibility
and verifying ordered Bronze lineage. Gold registry v1.1 now contains four independent
authorities spanning schema v1 and v2.

`review_publication_plan()` remains read-only and hash-binds the approval report to
the plan and proposed registries. `approve_reviewed_step()` requires an exact step,
dataset and UUID, checks the current registry precondition and upstream authority,
and uses flushed temporary files plus atomic `os.replace`. Exact repeats are
idempotent; divergent state fails closed. `labgdd-publications` exposes the review
and approve operations without interactive prompts or newest-directory discovery.

The scalar Individual Gold evidence validator now evaluates each symbol/source/feed/
timeframe/adjustment/session series independently, allowing the five-symbol candidate
to be compared with the approved pure-Python formulas without weakening key checks.

## 3. Exact Activation Results

All steps ran separately and succeeded in the authorized order:

1. Bronze `iex-1min-pilot-msft-nvda-jpm-xom-20260914-v1`, acquisition
   `ae7fecdc-d082-478d-b26d-b3e7e5db9600`. Active registry SHA-256:
   `87872458f765223dfbd3619c3ab054584f7967889d645a5053c1c28f0f89c439`.
2. Silver `silver-pilot-five-symbol-20260914-v1`, run
   `f9d4bcc6-5a51-4c37-8d74-86cf9cee1ca2`. Active registry SHA-256:
   `969309c9d1df647095351cd7069f6be19c97d09940cd07f816f2bcf71affdc2a`.
3. Individual Gold `gold-pilot-five-symbol-20260914-individual-v1`, run
   `310941bd-7ff9-4e25-971a-febec753c9e5`. Intermediate Gold registry SHA-256:
   `b12ac0b18f842457fd71ddc7051ad872a2f6c3b7109b6a19e530488d4a227f4f`.
4. Expected Gold `gold-study-cohort-20260914-multisymbol-expected-v1`, run
   `39ab367c-c6d2-480d-8c94-f35f767ed289`. Final Gold registry SHA-256:
   `2f9aaf217ead86fea588dc6fb25f4faa9e4ceb8ed832545218ecf9ac95badf51`.

After every step, all authorities in the resulting registry resolved and verified
their manifests/artifacts. Upstream registry hashes remained unchanged, and no orphan
`.pending` file was present.

## 4. Final Authority Inventories

- Bronze: AAPL acquisition `f2345e41-da10-420f-8b60-6079a9df2006` and four-symbol
  acquisition `ae7fecdc-d082-478d-b26d-b3e7e5db9600`.
- Silver: AAPL run `aecb53c1-d7a8-40c7-939b-a6901f435ff5` and five-symbol run
  `f9d4bcc6-5a51-4c37-8d74-86cf9cee1ca2`.
- Gold: legacy AAPL Individual `2c36a9ab-78ce-45a8-869d-8c0225267d17`, AAPL-based
  Expected `fddf0d53-e48f-4176-b045-45fcc12f2b8b`, five-symbol Individual
  `310941bd-7ff9-4e25-971a-febec753c9e5`, and multi-symbol Expected
  `39ab367c-c6d2-480d-8c94-f35f767ed289`.

Candidate manifest SHA-256 values remain Bronze `7b1b3ce7...f6a3`, Silver
`f0ff790c...15f8`, Individual Gold `c9ecea60...ad0`, and Expected Gold
`d97b0e48...05b2`. Every declared file size/checksum and exact inventory passed.

## 5. Files Created or Modified

Core changes are in `src/labgdd/adapters/bronze.py`,
`src/labgdd/adapters/silver.py`, `src/labgdd/publication_governance.py`, and
`src/labgdd/runners/publications.py`. Active registries are
`config/bronze_publications.toml`, `config/silver_publications.toml`, and
`config/gold_publications.toml`. The proposed/intermediate registries, activation
plan and JSON review report remain auditable exact evidence.

`docs/decisions/013-multilayer-publication-governance.md` records the accepted
decision. `README.md`, `docs/architecture/ARCHITECTURE.md`, and
`docs/architecture/MILESTONE6_PUBLICATION_MIGRATION.md` now describe activated
authority. Tests cover legacy compatibility, tampering, supersession, ambiguity,
cross-layer dependencies, review immutability, atomic failure recovery and migration
idempotence.

## 6. Tests and Validation

Actual final results:

- Ruff format: 74 files already formatted; passed. Ruff lint: passed.
- Strict mypy: passed for 74 source files.
- Full Windows pytest: **270 passed, 2 platform-skipped** in 819.21 seconds.
- Dedicated activated-state Linux publication tests: **2 passed, 0 skipped** in
  53.34 seconds using a 159-file package containing no `.env`.
- Focused governance/evidence tests: **21 passed** in 1.79 seconds.
- `pip check`: passed.
- Sdist and `labgdd_v2-0.4.0-py3-none-any.whl`: built successfully.
- Clean-wheel installation, package imports, module/console CLI and dependency smoke:
  passed.
- Repository security validation: passed for 161 final candidate files.
- Individual Gold scalar parity: 1,861 canonical records; 1,950 returns/1,787
  eligible; 7,780 rolling/5,117 eligible; 600 fixed/468 eligible; zero temporal/key
  mismatches; all errors within approved tolerance.
- Expected Gold validation: 11,700 returns, 46,680 rolling, 3,600 fixed, 30 symbols,
  1,861 observed slots, 9,839 missing slots and exact frozen AAPL parity.

## 7. Architecture and Scientific Integrity

ADR 013 accepts external multi-layer authority and layer-ordered atomic activation.
No formula, horizon, temporal key, eligibility rule, research cohort, source data,
Parquet artifact or immutable manifest changed. Missing observations were not filled.
The 25 intentionally unrequested symbols remain distinct from the 89 provider-response
absences for JPM/XOM.

This remains a one-session, five-symbol IEX pilot. IEX is venue-specific, provider
correction finality is unproven, and historical entity/listing/sector membership is
not independently verified. Publication approval does not convert those assumptions
into scientific facts or authorize sector analytics.

## 8. Security and Limitations

`.env` remains ignored and untracked; neither `.env` nor `.env.example` was modified.
No credential value was read, printed, staged or committed. The credentials exposed
through prior IDE context still require rotation before any authenticated Alpaca
operation. No network request occurred in Milestone 7.

Atomic replacement is a local same-filesystem guarantee, not an object-store or
distributed catalog transaction. A hard interruption before replacement may leave an
orphan pending file that must be inspected manually, but cannot publish a partial
active registry.

## 9. Current Project Status

The five-symbol Bronze, Silver, Individual Gold and full-cohort Expected-Series Gold
evidence are now explicitly authoritative alongside the frozen AAPL evidence. The
governance implementation, activation records, tests and documentation are frozen by
the Milestone 7 commit and annotated tag recorded in Git. Sector Gold, streaming,
Kafka, ML, serving and monitoring remain not started.

## 10. Recommended Next Step

Rotate the exposed Alpaca credentials. Then perform architecture review of the
published five-symbol evidence and define a separate, evidence-aware proposal for any
future milestone. Do not begin Sector Gold or another acquisition automatically.

## 11. Review Questions

None. The explicitly authorized publication activation completed without a blocking
defect.

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
