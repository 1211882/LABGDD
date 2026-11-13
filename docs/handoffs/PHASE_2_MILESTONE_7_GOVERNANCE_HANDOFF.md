# LABGDD V2 – Development Handoff

**Phase:** Phase 2, Milestone 7 – Multi-Symbol Publication Governance
**Task:** Implement, review and validate the inactive multi-layer publication proposal
**Status:** Completed
**Date:** 2026-11-11
**Git branch and commit:** `main` at frozen baseline `e54e7261c1ffe5af63f4ef8814e18ce804f0d0ae` (`phase2-multisymbol-pilot-approved-v1`); all Milestone 7 changes intentionally uncommitted

## 1. Objective

Implement the minimum fail-closed governance migration needed to make the four
Milestone 6 candidates ready for a later explicit publication decision. The task had
to add Bronze authority, extend Silver to multiple independent authorities, reuse
Gold registry v1.1, verify the immutable evidence and separate read-only review from
registry mutation. It did not authorize activation of proposed registries, data
regeneration, new Alpaca requests, or Sector Gold.

## 2. Changes Implemented

`resolve_approved_bronze()` now requires an exact registered dataset/acquisition UUID
and verifies manifest identity, source contract, schema/identity versions, calendar
schedule, package-source fingerprint, exact inventory, sizes, checksums and physical
observation schema. A complete acquisition remains non-authoritative without this
external registry.

Silver registry v1.1 supports multiple `[[authority]]` entries while preserving valid
v1.0 singleton registries. Selection remains exact by dataset and run UUID. V1.1 pins
Silver schema, resolution/source contracts, request/configuration SHA-256, processing
source and ordered Bronze lineage. Duplicate identities, superseded runs, incorrect
lineage, unregistered runs, tampered artifacts and extra files fail closed.

`publication_governance.py` implements two distinct operations. `review` validates
the complete ordered Bronze → Silver → Individual Gold → Expected Gold proposal and
writes a structured report with no active-registry mutation. `approve` requires an
exact reviewed step, dataset and UUID, rechecks proposal/report hashes and upstream
active authority, then atomically replaces one registry. A repeated exact activation
is idempotent; contradictory current state is rejected. The CLI is exposed as
`labgdd-publications` with non-interactive `review` and `approve` subcommands.

The existing Individual Gold evidence validator was corrected after it exposed a
real validation defect: it treated event time as globally unique. It now groups by
symbol/source/feed/timeframe/adjustment/session before scalar parity, preserving the
approved formulas and tolerance.

## 3. Files Created or Modified

- `src/labgdd/adapters/bronze.py`: explicit Bronze authority resolver.
- `src/labgdd/adapters/silver.py`: backward-compatible Silver v1.1 parser/resolver.
- `src/labgdd/publication_governance.py` and
  `src/labgdd/runners/publications.py`: review/approval workflow and CLI.
- `config/bronze_publications.proposed.toml`,
  `config/silver_publications.proposed.toml`,
  `config/gold_publications.individual.proposed.toml`, and
  `config/gold_publications.expected.proposed.toml`: inactive exact registry states.
- `config/publication_governance_milestone7.toml`: ordered activation plan with
  current/proposed registry SHA-256 preconditions.
- `scripts/validate_gold_evidence.py` and `scripts/__init__.py`: multi-symbol scalar
  evidence validation and typed test import.
- `tests/unit/test_bronze_registry.py`, `test_publication_governance.py`,
  `test_validate_gold_evidence.py`, and `test_silver_registry.py`: authority,
  migration, tampering, dependency, atomicity, idempotence and compatibility tests.
- `docs/architecture/ARCHITECTURE.md`,
  `MILESTONE6_PUBLICATION_MIGRATION.md`,
  `PHASE2_MILESTONE7_APPROVAL_REVIEW.json`, and
  `PHASE2_MILESTONE7_CANDIDATE_REVIEW.md`: contracts and auditable review evidence.
- `pyproject.toml`: publication-governance console entry point.

Active `config/silver_publications.toml` and `config/gold_publications.toml` were not
modified. No `config/bronze_publications.toml` active registry was created.

## 4. Architecture and Governance Decisions

Authority inventory at completion is:

| Layer | Active authority | Inactive proposed authority |
|---|---|---|
| Bronze | None | AAPL Bronze plus the four-symbol acquisition |
| Silver | Legacy AAPL only, registry v1.0 | AAPL preserved plus five-symbol Silver, v1.1 |
| Gold | Legacy AAPL Individual plus AAPL-based Expected Gold | Both preserved, then five-symbol Individual and multi-symbol Expected Gold |

The proposal is split into four registry states so approval is layer-ordered and each
new financial dataset is selected independently. Gold schema v1 and v2 remain
separate. `authorized_bounded_validation` remains validation-only and cannot satisfy
the approved resolver. Proposed files are inert because no runner selects them by
name or directory recency. Approval metadata is exact in the proposals, but becomes
effective only if the user later authorizes the matching activation steps.

## 5. Candidate Review Outcomes

The structured review passed all four candidates and every preserved authority:

- Bronze acquisition `ae7fecdc-d082-478d-b26d-b3e7e5db9600`, manifest
  `7b1b3ce751ca50ded0fca940f029347f1a8f4bb80c5dc5212f2624a616e3f6a3`.
- Silver run `f9d4bcc6-5a51-4c37-8d74-86cf9cee1ca2`, manifest
  `f0ff790cfe0200e94c407010cfc4f2a8bfe93110d580f751b9f934095f9a15f8`.
- Individual Gold run `310941bd-7ff9-4e25-971a-febec753c9e5`, manifest
  `c9ecea6009363f589bb57365acbccd6454fe8e122c3d737fbe3fd71363badad0`.
- Expected Gold run `39ab367c-c6d2-480d-8c94-f35f767ed289`, manifest
  `d97b0e48c8463b6a97aa58e576e7f24998b4246f496fabaa286982177ab605b2`.

All declared inventories, sizes and hashes passed. Silver lineage includes the
unchanged AAPL Bronze manifest `048b6815...` and new Bronze manifest. Parquet time
fields are UTC microsecond timestamps. Active Silver and Gold registry SHA-256 values
remain `0abdbc9e...460b` and `2293259c...27a5` respectively.

## 6. Tests and Validation

Actual results:

- `ruff format --check .`: 74 files already formatted; passed.
- `ruff check .`: passed.
- strict `mypy`: passed for 74 source files.
- full Windows `pytest`: **270 passed, 2 skipped** in 895.56 seconds. The only skips
  were the two supported-Linux publication tests. A known damaged external pytest
  temp directory emitted cleanup warnings; no test failed.
- dedicated secret-free Linux container: both skipped tests passed, **2 passed,
  0 skipped** in 74.02 seconds. The package contained 158 explicit files and no
  `.env`; the repository and credentials were not mounted.
- focused authority/governance suite: 20 passed; validator regression: passed.
- `pip check`: passed.
- sdist and `labgdd_v2-0.4.0-py3-none-any.whl`: built successfully.
- clean-wheel installation, imports, module/console CLI and dependency smoke: passed.
- `scripts/check_repository.py`: passed for 158 candidate files.
- independent Individual Gold scalar validator: passed, 1,861 canonical, 1,950
  returns/1,787 eligible, 7,780 rolling/5,117 eligible, 600 fixed/468 eligible,
  zero key/temporal mismatches, and all errors within tolerance.
- independent Expected Gold validator: passed all 7 artifacts, 11,700 returns,
  46,680 rolling, 3,600 fixed, 30-symbol population and frozen AAPL parity.

`.env` is ignored and untracked and was never read by the implementation. No secret,
runtime data, build output or immutable candidate artifact is included in Git changes.

## 7. Scientific and Data Integrity

Counts independently remain AAPL 390/0 missing, MSFT 390/0, NVDA 390/0, JPM
371/19 and XOM 320/70. Silver contains 1,861 unique canonical keys; no bar was filled
or replaced with zero. Expected Gold contains 25 intentionally unrequested symbols,
9,750 unrequested return slots and zero observed bars for those symbols, distinct
from requested-symbol provider absence.

No measurement formula, horizon, eligibility rule, temporal contract, immutable
manifest or publication was changed. Historical listing/entity/sector membership is
still an unverified research assumption. IEX is venue-specific, provider correction
finality is unproven, and missing JPM/XOM bars do not prove exchange-wide absence.

## 8. Issues and Limitations

The candidates remain non-authoritative until explicit activation. Local atomic
replacement is not a cross-filesystem/object-store transaction. Bronze v1 still
lacks a fully reconstructible provider page/row locator beyond its preserved raw
response evidence. Multi-node Spark and production-scale governance are untested.

Credential text was visible in the IDE context supplied to the assistant. No value
was read from disk, logged, copied or committed, but the exposed Alpaca credential
pair should be rotated before future authenticated acquisition.

## 9. Current Project Status

Milestone 7 governance code and exact inactive proposals are implemented and fully
validated but intentionally uncommitted. Milestone 6 remains the frozen Git baseline.
All four candidate datasets remain pending; active authorities and immutable data are
unchanged. No sector analytics or later phase has started.

## 10. Exact Actions Requiring Authorization

Explicit user authorization is required before running `labgdd-publications approve`
for these steps, in order: `bronze_multisymbol`, `silver_multisymbol`,
`gold_individual_multisymbol`, and `gold_expected_multisymbol`, each with the exact
dataset and UUID recorded above. Authorization would create the active Bronze
registry, migrate active Silver to v1.1, then append the two Gold authorities in two
atomic operations. It would not regenerate data. No approval command was run here.

## 11. Recommended Next Step

Architecture-review the uncommitted diff, proposal documents and approval report.
Rotate the exposed Alpaca credentials. If the evidence and governance design are
accepted, explicitly authorize the four ordered activation commands in a separate
execution. Do not begin Sector Gold before that decision.

## 12. Review Questions

1. Approve or reject the proposed external Bronze authority contract.
2. Approve or reject the exact four-step registry activation plan.
3. Confirm credential rotation before any future authenticated provider request.

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
