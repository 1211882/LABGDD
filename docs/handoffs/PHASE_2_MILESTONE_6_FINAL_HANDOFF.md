# LABGDD V2 – Development Handoff

**Phase:** Phase 2, Milestone 6 – Multi-Symbol Historical Pilot
**Task:** Final architecture review, evidence audit, governance decision and freeze
**Status:** Completed
**Date:** 2026-11-06
**Git branch and commit:** `main`; frozen by annotated tag `phase2-multisymbol-pilot-approved-v1`. Because a tracked file cannot contain the hash of the commit that contains itself, exact commit and tag-object identities are recorded by the annotated tag and the final execution report.

## 1. Objective

Independently review the `authorized_bounded_validation` boundary, verify the real
five-symbol Bronze-to-Gold evidence, confirm financial and temporal completeness,
accept ADR 012 only if the system remains fail-closed, and freeze the implementation
without approving candidate datasets or beginning Sector Gold.

## 2. Changes Implemented

The architecture audit found no concrete correctness defect. The bounded-validation
mechanism remains an explicit opt-in configured by `silver_input_authority`. Normal
Gold requests default to `approved`; `resolve_approved_silver()` rejects the
candidate registry status. `resolve_bounded_validation_silver()` requires the exact
`authorized_bounded_validation` status and shares the approved resolver's strict
dataset/run identity, request/source fingerprint, manifest SHA-256, artifact size,
checksum and inventory validation. Neither path discovers filesystem directories.

ADR 012 is accepted for candidate execution only. This separates acquisition
authorization, candidate validation authority, immutable completed evidence and
publication approval. The existing approved Silver and Gold registries are unchanged.
The Milestone 6 validation registry remains isolated under `config/milestone6/`,
and no candidate was registered as authoritative.

`docs/architecture/MILESTONE6_PUBLICATION_MIGRATION.md` specifies the unexecuted safe
migration. Silver publication requires a backward-compatible multi-authority
registry that preserves the existing AAPL authority. Gold registry v1.1 can later
append explicit candidates after independent review. No manifest may be rewritten
and no directory may be selected by recency.

## 3. Files Created or Modified

- `src/labgdd/adapters/silver.py`: fail-closed approved and bounded-validation
  resolvers plus explicit dispatcher.
- `src/labgdd/gold_config.py`, `src/labgdd/expected_gold_config.py`: validated,
  optional authority mode; Expected Gold also validates acquisition-scope symbols.
- `src/labgdd/processing/gold.py`, `src/labgdd/processing/expected_gold.py`, and
  `src/labgdd/runners/gold.py`: resolve and persist the selected authority boundary.
- `config/historical_milestone6.toml`, `config/silver_milestone6.toml`, and
  `config/milestone6/`: exact requests and isolated validation authority.
- `tests/unit/test_silver_registry.py`, `tests/unit/test_gold_config.py`, and
  `tests/unit/test_expected_gold_config.py`: opt-in, separation, invalid-authority,
  duplicate-scope and outside-cohort regressions.
- `docker/spark-batch/Dockerfile.test`: configurable test base image; production
  image remains free of pytest/development dependencies.
- `docs/decisions/012-bounded-candidate-silver-validation.md`: Accepted.
- `docs/architecture/PHASE2_MULTISYMBOL_VALIDATION.md` and
  `docs/architecture/MILESTONE6_PUBLICATION_MIGRATION.md`: reviewed evidence and
  separate future approval procedure.
- `README.md`, the earlier authorization/development handoffs, and this final handoff.

Immutable candidate data remain under ignored `data/` paths and are not committed.

## 4. Architecture and Design Decisions

Implementation approval does not imply dataset approval. A `complete` manifest
means the bounded publisher completed and verified its output; it is not publication
authority. The milestone tag approves code, configuration contracts, tests and the
bounded pilot procedure only.

Gold schema v1 is unchanged. Its candidate pending state is represented externally
by non-registration; a future Gold registry entry may use
`manifest_publication_status = "not_recorded"`, consistent with the existing
approved schema-v1 authority. Expected-Series Gold retains immutable
`pending_architecture_review` metadata. No formula, eligibility rule, temporal key,
Bronze/Silver/Gold schema or frozen artifact changed.

The 25 cohort symbols outside the acquisition scope are preserved as independently
expected but intentionally unrequested. They are distinct from provider absence
for requested JPM and XOM. Acquisition scope is machine-readable manifest metadata.

## 5. Tests and Validation

Actual final results:

- Independent evidence audit: passed. All four candidate manifests and every
  declared artifact size/SHA-256/exact inventory verified.
- Acquisition audit: one page, one HTTP attempt, only MSFT/NVDA/JPM/XOM; AAPL reused
  from acquisition `f2345e41-da10-420f-8b60-6079a9df2006`.
- XNYS audit: 13:30–20:00 UTC, 390 scheduled minutes per symbol.
- Bronze/Silver audit: exactly 1,861 unique common keys; no fill, zero substitution,
  quarantine row, duplicate group or conflict group.
- Scalar parity: 10,330 five-symbol financial rows compared for H=5/15/30/60;
  exact keys/timestamps/nulls/reasons and tolerance
  `1e-12 + 1e-10*abs(expected)` passed.
- Ruff format: 68 files already formatted. Ruff lint: passed.
- Strict mypy: passed for 68 source files.
- Full Windows pytest: **260 passed, 2 platform-skipped** in 876.58 seconds. The
  only skips were the two authoritative Linux publication tests. A known damaged
  external Windows pytest temporary directory emitted cleanup warnings; no test failed.
- Dedicated Linux publication tests: **2 passed, 0 skipped** in 56.30 seconds:
  `test_expected_gold_publication_is_immutable_extended_and_checksummed` and
  `test_expected_gold_failure_is_not_published_complete`.
- Linux Silver smoke: canonical=4, quarantine=0, sessions=4.
- Linux Gold smoke: legacy `390/1556/120`; expected `780/3112/240`.
- `pip check`: passed. Sdist and wheel build: passed.
- Clean-wheel install/import/module CLI/console CLI/pip smoke: passed.
- `scripts/check_repository.py`: passed for 144 candidate files.
- Credential policy: `.env` ignored and untracked; `.env.example` placeholders blank;
  no credential value printed, staged or committed.

Candidate manifest SHA-256 values:

- Bronze: `7b1b3ce751ca50ded0fca940f029347f1a8f4bb80c5dc5212f2624a616e3f6a3`.
- Silver: `f0ff790cfe0200e94c407010cfc4f2a8bfe93110d580f751b9f934095f9a15f8`.
- Individual Gold: `c9ecea6009363f589bb57365acbccd6454fe8e122c3d737fbe3fd71363badad0`.
- Expected-Series Gold: `d97b0e48c8463b6a97aa58e576e7f24998b4246f496fabaa286982177ab605b2`.

## 6. Issues and Limitations

Provider correction finality remains unproven, and Bronze v1 cannot independently
reconstruct every page/row identity locator. IEX is venue-specific, not consolidated
market coverage. Local atomic rename is not an object-store transaction. Multi-node
Spark and production-scale sizing remain untested.

The candidate datasets remain pending. Silver registry v1.0 cannot add an independent
authority safely without the separately documented multi-authority migration. No
such migration was implemented in this freeze.

## 7. Scientific and Data Integrity

The acquired counts are AAPL 390 reused, MSFT 390, NVDA 390, JPM 371 and XOM 320.
JPM has 19 scheduled minutes absent; XOM has 70. Those provider-response absences
were neither filled nor replaced with zero and do not prove exchange-wide absence.

Individual Gold remains mathematically consistent:

- Returns: 1,950 total / 1,787 eligible.
- Rolling: 7,780 total / 5,117 eligible.
- Fixed: 600 total / 468 eligible.
- AAPL: 389 eligible returns, 1,450 rolling, 120 fixed.
- MSFT: 389 / 1,450 / 120.
- NVDA: 389 / 1,450 / 120.
- JPM: 352 / 511 / 72.
- XOM: 268 / 256 / 36.

Expected-Series Gold remains 11,700 returns, 46,680 rolling and 3,600 fixed rows.
All 25 intentionally unrequested symbols have null observations and financial
values; they are not provider failures. Missing JPM/XOM minutes propagate to
`nonconsecutive_bars`, incomplete rolling windows and incomplete fixed windows.

The one-session five-symbol pilot is not a validated sector result. Historical
entity/listing/sector membership is unverified, and the taxonomy is project-defined.
No sector metric, correlation, concentration, breadth or emergence event was computed.

## 8. Current Project Status

Milestone 6 implementation is frozen on `main` and identified by annotated tag
`phase2-multisymbol-pilot-approved-v1`. Phase 0, earlier Phase 2 tags, frozen
publications and approved registries remain unchanged. The system can reproducibly
process this bounded real multi-symbol snapshot through candidate Gold while
preserving explicit authority and missingness. Candidate financial datasets remain
non-authoritative.

## 9. Recommended Next Step

Stop at this freeze. A future request may independently review and execute the
publication migration. Sector Gold must not begin until explicit authorization and
the unresolved historical-membership evidence requirements are addressed.

## 10. Review Questions

None. ADR 012 and the implementation milestone are accepted; candidate publication
approval is deliberately deferred to a separate governance action.

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
