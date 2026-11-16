# LABGDD V2 - Milestone 9 development handoff

## 1. Executive Summary

Task: implement a read-only approved Expected-Series Gold to Sector Gold integration.
Status: Completed under the user's explicit Windows-wait waiver. Local validation
and remote Ubuntu CI passed; current Windows execution remains pending. Reuses accepted scalar
and native Spark engines without a publication writer or new financial formulas.
ADR 015 remains Proposed. The user explicitly authorized proceeding without waiting
for Windows CI; this does not constitute Windows success or architecture approval.

## 2. Repository and Git State

Canonical repository C:\Users\Deus\Documents\LABGDD; branch main; initial task
HEAD a08a5f7d6fa1feaf25d83f26e120a39af43c1725. Portability correction
268cc9c0cbce25062dd1b828316d01f7a99c6dcc and LF/CRLF fixture follow-up
bdc11581076fcf5ba339c58ee5999485211067da were committed and pushed normally,
preserving all prior ancestry. Origin https://github.com/1211882/LABGDD.git.
Milestone 9 implementation 55efcb3ebfd882e9d8e2745a8a32c9471346b50a was committed
with feat(sector-gold): integrate approved expected-gold inputs, pushed normally and
verified through ls-remote. Final documentation-only commit records these results;
its self-referential SHA is reported in the final response. Working tree was clean
after implementation push. No approval tags or history rewriting.

## 3. Initial State and Dependencies

VERIFIED accepted ADR 014 and Milestone 8 numerical/policy fixes are present.
Read AGENTS.md, CI handoff, ADRs 009-011/014 and current measurement/data contracts.
Existing approved registries select the multi-symbol Gold pilot. Python 3.11.9,
locked Spark 4.3.0/PyArrow/calendar tooling reused; no project dependencies added.
The initial checkout was clean. A temporary test image required Git for repository
hygiene tests; this was added only to a separate validation image.

## 4. Implementation Details

processing.approved_sector.project_approved_expected_gold accepts registry paths,
exact dataset/UUID/manifest digest, settings, research reference and eligibility
policy. It calls resolve_approved_gold and resolve_approved_silver, validates lineage,
research hash, pinned calendar provenance, regenerated expected-plan digest, physical
Parquet types, complete scheduled authority grid, canonical provenance, financial
nullability, acquisition scope and manifest coverage counts. Duplicate, missing or
contradictory slots fail. It never recomputes an upstream return.

The typed ApprovedSectorProjection exposes independent population, existing return
and coverage relations. calculate_approved_sector_gold owns that read path and
invokes the reused internal native engine, binding immutable input identities and
fingerprints onto windows/pairs. Public transform_sector_gold remains synthetic-only;
no caller-provided frame or ApprovedGold object can activate the public real path.

## 5. Architecture and Data Flow

Explicit approved Gold -> verified upstream/research/calendar/plan -> distributed
population and return projection -> existing native sector calculations. Physical
read checks and scalar aggregate coverage values reach the driver; production code
does not collect financial rows or use Python UDFs. Synthetic tests collect bounded
rows solely for independent scalar validation. Lazy Spark reads rely on immutable
storage; integrity is checked again before releasing projected relations.

## 6. Scientific and Mathematical Contracts

Preserve measurement 2.0.0, horizons 5/15/30/60, exact equal-weight portfolio return
log1p(mean(expm1(r))), RMS intensity, population dispersion, all-pair correlation,
HHI variance-share denominators and effective breadth (1-HHI)/(1-1/N). Accepted
stable correlation and sorted pair accumulation are reused. No filling/reanchoring.
Primary precedence remains scope, complete membership, historical verification,
absent member, missing bar, ineligible individual return. Strict downstream policy
may null unverified declared inputs, but a strict input cannot be downgraded.

## 7. Data Contracts and Provenance

VERIFIED real input gold-study-cohort-20260914-multisymbol-expected-v1,
run 39ab367c-c6d2-480d-8c94-f35f767ed289, manifest
d97b0e48c8463b6a97aa58e576e7f24998b4246f496fabaa286982177ab605b2;
schema/measurement 2.0.0. Gold resolver verified exact artifact checksums.
Approved upstream Silver is resolved independently. External registry approval is
distinct from the original manifest's preserved pending_architecture_review label.

Preserve sector-complete-population-v1, sector-eligibility-policy-v1 and
sector-output-identity-v1. Add source-bound approved_input_identity_sha256 using
sector-approved-input-v1, original output identity, dataset, UUID and manifest hash.
Output lineage includes source/configuration hashes, schema, measurement, research,
plan and Silver identities. No registry or immutable publication was modified.

## 8. Files Changed

- src/labgdd/processing/approved_sector.py: file-owning authority/projection API.
- src/labgdd/processing/sector_gold.py: internal verified-input engine entry,
  mandatory null checks and trusted reader nullability handling; public API unchanged.
- tests/integration/test_approved_sector.py: isolated synthetic approved publications,
  scalar/Spark and replay tests, semantic/integrity failures and optional real pilot.
- docs/decisions/015-approved-gold-sector-projection.md: Proposed boundary ADR.
- docs/architecture/PHASE2_APPROVED_SECTOR_INTEGRATION.md and ARCHITECTURE.md:
  integration, counts, limitations and authority documentation.
- This handoff: validation, Git and continuation evidence.
- docs/handoffs/TASK_CI_TEST_PORTABILITY_REMEDIATION_HANDOFF.md: record eventual
  successful Ubuntu/Windows portability results without concealing earlier failures.

## 9. Validation Evidence

VERIFIED focused Linux suite with read-only real evidence: 15 passed in 78.45 seconds,
including the actual approved pilot and checksum verification. After final replay and
manifest-summary checks, complete Linux suite with frozen data mounted read-only:
332 passed, no skips, in 592.01 seconds. Dataset-free full suite: 330 passed,
2 expected optional-evidence skips in 438.17 seconds. Production financial rows were
never collected; bounded test collections provided independent scalar parity.

Executed python -m ruff format --check . (80 files), python -m ruff check .,
python -m mypy --cache-dir <temporary> (80 files), python -m pip check,
python -m build --no-isolation and python scripts/smoke_wheel.py
dist/labgdd_v2-0.4.0-py3-none-any.whl --config config/project.toml: all passed.
Build and wheel installation ran from a secret-free tracked-file copy. Full pytest
used network-disabled Linux Docker, user 10001, explicit Python entrypoint and
-m pytest -q -p no:cacheprovider. Repository scanner and git diff --check passed;
repeat after final handoff/staging.

VERIFIED source-commit CI run https://github.com/1211882/LABGDD/actions/runs/43982258745:
Ubuntu 24.04 job 133701415778 succeeded, 330 passed/2 optional-real skips in 270.66
seconds; package build, clean-wheel smoke, CLI and repository security all passed
(178 candidates). Windows job 133701416184 was still running pytest at final
inspection. The user twice explicitly directed continuation without waiting for
Windows; no final Windows count or success is claimed for Milestone 9.

An initial Linux container launch lacked a nested mountpoint; creating it only in
the disposable copy corrected the launch. First integration execution rejected
Parquet-widened array nullability. Type checks now tolerate reader nullability only
while complete authority values still match the regenerated grid, including null
elements. A loop-variable typing conflict was corrected. No failure was suppressed.

Portability run 43934206966: Ubuntu 313 passed/1 skipped; Windows 308 passed/3
skipped/3 failed due copied-request CRLF hashes in fixture setup. Follow-up fixture
hash binding and explicit LF/CRLF regressions passed 9 focused tests, then were
pushed in bdc11581. Follow-up run 43937276053 passed both workflows: Ubuntu 316
passed/1 skipped in 186.35 seconds; Windows 314 passed/3 skipped in 1251.90 seconds.
Build, clean-wheel smoke, CLI and security gates passed on both (173 candidates).
User explicitly waived waiting for Windows before Milestone 9 work; it subsequently
passed independently.

## 10. Quantitative Results

VERIFIED real-pilot assertions: 30 population rows across six sectors; 11,700 Gold
scheduled slots; 1,861 observed bars; 9,839 missing slots. AAPL/MSFT/NVDA each 390,
JPM 371, XOM 320. There are 25 intentionally unrequested symbols, distinct from
19/70 missing scheduled bars within requested JPM/XOM. Projection yields 720 sector
windows and 7,200 pair rows; all complete-sector measurements are null/ineligible.
Opening bars remain coverage slots but are excluded from adjacent-return vectors.
Synthetic two-member fixture supplies complete and gap-affected windows for all
horizons, strict historical nullability and scalar/Spark parity (1e-12 absolute,
1e-10 relative tolerance; exact keys/nulls/reasons).

## 11. Architecture Decisions

ADR 015 Proposed; ADR 014 unchanged. Reject a public approved-label switch or
caller-supplied authoritative frame. Reuse strict resolvers, exact file identities
and independently regenerated population. Separate source-bound identity supplements
the accepted output contract. This is an in-process trusted-code boundary, not a
Python security sandbox. No calibration authority is introduced.

## 12. Security and Governance

No Alpaca requests, credentials exposed, publication activation, authority migration,
dataset changes or financial formula changes. Real data mounted read-only in Linux;
all synthetic publication writes stay in temporary fixtures. Source/configuration
tracking excludes data, Parquet, caches and builds. LABGDD_NEW untouched.

## 13. Problems and Limitations

Research classification is declared, not independently verified historical listing,
entity or sector membership. IEX is venue-specific; absent bars do not prove
exchange-wide absence. Correction finality remains unproven. No sector is complete
in the real pilot. Threshold calibration, detection, persistence and actual Sector
Gold publication remain excluded. Native approved Parquet integration tests execute
on supported Linux; their Windows skips do not prove native Windows file handling.
Current Windows CI remains unverified pending completion, explicitly deferred by user.

## 14. Current Project State

Read-only approved input integration implemented, committed and pushed; full
real/synthetic and portable Linux checks plus remote Ubuntu workflow passed. Windows
is pending under user waiver. ADR 015 remains Proposed; no real Sector Gold
artifacts or approval tags exist.

## 15. Recommended Next Step

Architecture review of Proposed ADR 015 is the primary next task: examine exact
authority resolution, source-bound identity, independent population, strict-policy
nullability, replay tests and scientific limitations. Acceptance requires explicit
review, not passing tests. Record the eventual Windows job result when available.
Exclude publication, acquisition, calibration and another milestone unless authorized.

## 16. Decisions Requiring Approval

ADR 015 acceptance and any future Sector Gold publication remain separate review
decisions. The user authorized implementation/normal push, not publication approval.

## 17. Next-Agent Continuation Instructions

Use canonical LABGDD; read AGENTS.md, ADR 014/015 and this handoff. Reuse exact
registered pilot evidence; never discover newest directories or re-acquire data.
Optional real tests skip only if the exact external run is absent, never when
present-invalid. Retain strict source verification and complete populations.
Do not touch LABGDD_NEW, immutable publications, registries or financial contracts.
Do not start another milestone. Stop for unresolved integrity or scope conflicts.

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
