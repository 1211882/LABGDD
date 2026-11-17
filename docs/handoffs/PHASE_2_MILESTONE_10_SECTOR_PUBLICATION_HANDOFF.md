# LABGDD V2 - Milestone 10 Sector Publication Handoff

Date: 2026-11-17. Evidence is VERIFIED unless otherwise labelled.

## 1. Executive Summary

Phase 2 Milestone 10 implements an immutable Sector Gold publication foundation.
Status: Completed; final local validation passed. The public API publishes only explicitly
marked synthetic upstream evidence, reusing the accepted approved-input calculation
boundary and existing local GoldRun finalization. Real publication and registry
activation remain unavailable. ADR 016 is Proposed; ADRs 014/015 are unchanged.
Measurement 2.0.0 and complete-population scientific contracts are preserved.

## 2. Repository and Git State

Canonical repository: C:\Users\Deus\Documents\LABGDD; branch main; origin
https://github.com/1211882/LABGDD.git. Starting HEAD:
`d9686dd4f5850569808ed10c9fe656028a55495b`, initially clean. No unrelated changes
were present. Implementation commit `6b6ee182651e96cbf7467ce06cb45ae395528a87`,
`feat(sector-gold): implement immutable publication foundation`, was created after
all gates passed and pushed normally from the starting baseline. Git ls-remote
verified origin/main equals that commit; the working tree was clean. This report
then received a documentation-only update containing actual CI/push evidence.
Its enclosing documentation-commit SHA and final remote verification are recorded
in the final response because a commit cannot contain its own SHA.
No approval tag, force-push, rebase or history
rewrite is authorized. Historical LABGDD_NEW remains untouched.

## 3. Initial State and Dependencies

Read AGENTS.md, accepted ADRs 014/015, the final Milestone 9 handoff, calculation
engines, GoldRun, Gold/Silver/Bronze authority mechanisms, existing manifests,
publication governance and tests. Baseline workflow 43994961692 was directly checked:
both Ubuntu and Windows completed successfully. This is baseline evidence, not CI
proof for the new implementation. Windows waiting is waived by user preference.
Reuse locked Spark 4.3.0, PyArrow 21.0.0 and exchange_calendars 4.11.2. Local Python
is 3.11.9; dedicated Linux validation uses 3.11.18. No dependencies were added or
production Docker images modified. Real evidence is optional and read-only.

## 4. Implementation Details

`SectorPublicationRequest` and `load_sector_publication_request` validate exact
configuration keys, portable owned references, dataset identifier, UUID, manifest
hash, eligibility policy, schema version and synthetic evidence kind. There is no
real mode or threshold configuration. Unknown keys and policies fail closed.

`publish_synthetic_sector_gold` accepts file-owning references, never arbitrary
financial frames. `_source` resolves exact approved Gold and Silver, binds metadata
to fingerprinted bytes, checks research bytes and requires a synthetic marker in
the input manifest itself. A request flag cannot relabel the real pilot. The API
then invokes Milestone 9's verified snapshot/native calculation path.

`GoldRun(..., sector_outputs=True)` preserves default Gold behavior while using
the separate sector_gold namespace and mandatory windows/correlations outputs.
Native Spark writes compressed Parquet, validates source and readback, rechecks
authority/processing fingerprints and finalizes a pending candidate atomically.
Existing completed, failed or staged UUIDs cannot be silently reused.

`inspect_sector_candidate` verifies exact manifest hash, source authority, schemas,
independent expected grid, population evidence, lineage, nullability, identities,
pair completeness, counts, reason summaries and physical checksums. It rechecks
inventory after inspection and returns metadata, not approval or lazy financial
frames. It recomputes through the accepted native engine and compares every
inherited provenance/financial column. This is separate from independent scalar
mathematical verification, exercised by the parity tests.

## 5. Architecture and Data Flow

Approved synthetic Expected-Series Gold -> verified Milestone 9 snapshot -> native
sector calculations -> exclusive staging -> schema/physical/scientific validation
and readback -> immutable completed synthetic candidate. Independent architecture
review and future external registry approval remain separate, unimplemented stages.

The existing local manifest/checksum/rename lifecycle is reused. Handled failures
retain isolated _failed evidence. Filesystem failure or process interruption can
retain staging; neither constitutes completion or authority. Inspect and use a new
UUID rather than overwriting or automatically resuming an interrupted attempt.
No object-store atomicity or distributed transaction guarantee is claimed.

## 6. Scientific and Mathematical Contracts

Preserve H={5,15,30,60}, measurement 2.0.0, sector-complete-population-v1,
sector-eligibility-policy-v1, sector-output-identity-v1 and source-bound identity.
For expected N, q_i=sum(r_i^2); sector RMS=sqrt(sum(q_i)/N). Exact portfolio log
return is log1p(sum(expm1(r_i))/N), with realized volatility sqrt(sum(p_t^2)).
The mean-log proxy remains separate. Dispersion uses population denominator N.
Pearson requires complete horizon vectors and all N(N-1)/2 expected pairs; stable
min==max constant detection and deterministic summation remain unchanged.
HHI=sum((q_i/sum(q))^2); effective breadth=(1-HHI)/(1-1/N), requiring N>=2 and
positive total variance. No observed-survivor denominator or gap filling is allowed.

Primary precedence remains outside acquisition scope, incomplete sector population,
unverified historical applicability, missing expected member, missing observed bar,
then ineligible individual return. Metric-specific undefined states are separate.
Threshold breadth is preserved as a field but stays null without calibration
authority. No calibration authority or emergence detection was implemented.

## 7. Data Contracts and Provenance

Separate Sector schema 1.0.0 and sector-calculation-v1. Windows preserve all accepted
metrics/counts/policies/boundaries; pairs retain full population/source provenance.
Publication counts/indices are int64; financial doubles are not recast. Explicit
schemas fix ordered names/types. Parquet nullability widening does not relax
mandatory-value checks. Physical timestamps must be UTC microseconds.

New sector-pair-identity-v1 hashes the source-bound window identity plus ordered
left/right symbols. Existing window/source identities are unchanged. Manifest records
source and sector fingerprints separately, exact Gold/Silver identities and digests,
research/plan/calendar hashes, runtime versions, bounded population/evidence/coverage,
schemas, row/eligibility/reason counts, artifact sizes/hashes and pending status.

All published test candidates are synthetic. The real input used only for refusal
and inherited read-only regressions is gold-study-cohort-20260914-multisymbol-expected-v1,
run 39ab367c-c6d2-480d-8c94-f35f767ed289, manifest
`d97b0e48c8463b6a97aa58e576e7f24998b4246f496fabaa286982177ab605b2`.
No real Sector dataset/run was created or approved.

## 8. Files Changed

- src/labgdd/sector_publication_config.py: strict synthetic publication request.
- src/labgdd/processing/sector_publication.py: orchestration, schemas, validation,
  pending candidate creation and read-only inspection.
- src/labgdd/adapters/gold.py: opt-in Sector namespace/output inventory; defaults unchanged.
- tests/integration/test_approved_sector.py: reusable synthetic evidence factory;
  explicit synthetic marker, configurable fixture sectors, original tests retained.
- tests/integration/test_sector_publication.py: lifecycle, all-horizon scalar parity,
  replay, strict policy, corruption, invalid counts/eligibility, missing windows/pairs,
  failed writes and real-input refusal.
- tests/unit/test_sector_publication_config.py: configuration/storage/rename-failure tests.
- tests/__init__.py and tests/integration/__init__.py: explicit shared test-package imports;
  excluded from installed application packages.
- scripts/smoke_wheel.py: isolated imports for both new Sector modules.
- docs/decisions/016-sector-gold-publication.md: Proposed decision/future registry contract.
- docs/architecture/PHASE2_SECTOR_PUBLICATION.md, ARCHITECTURE.md and README.md:
  API, lifecycle, authority, scientific and scope documentation.
- This handoff: execution evidence and safe continuation.

No active configuration registry, frozen artifact or dependency lock changed.
Generated datasets, builds, logs and caches stay outside Git.

## 9. Validation Evidence

Passed: nine configuration/storage/UUID unit tests (0.23s); initial synthetic lifecycle
suite, four passed (260.48s). The latter preceded additional hardening and is not
presented as final full-suite proof. Final complete Linux suite: 359 passed, no
skips, four pinned-Spark warnings, 924.47s. This includes native recomputation,
resealed-provenance refusal, registry preservation and read-only real-input refusal.
Executed python -m pytest -q -p no:cacheprovider against the final source in the
network-disabled dedicated Linux image with frozen evidence mounted read-only.
Earlier full runs passed 356 tests (897.67s) and 358 tests (843.46s) before the final
hardening; additional invalid-return and explicit registry/parity runs passed one
test each (120.24s and 159.65s). Final full-suite evidence supersedes those revisions.

Passed commands: python -m ruff check --no-cache .; strict python -m mypy
--cache-dir <temporary> (86 files); python -m pip check; python -m build
--no-isolation <secret-free tracked-file copy>; python scripts/smoke_wheel.py
<copy>/dist/labgdd_v2-0.4.0-py3-none-any.whl --config config/project.toml.
The clean wheel imports the new modules with zero mandatory runtime dependencies.
Offline project CLI and ingestion --check passed. Repository scanner passed 187
candidates before adding this report; the final scanner passed 188 candidates.
Staged inspection and diff checks are repeated before commit. Linux tests run as
UID 10001 with network disabled; frozen data is
mounted read-only only for the full suite/optional real checks.

Intermediate corrections: Ruff formatting/import issues and strict typing identified
heterogeneous schema-map types and fixture import/variable shadowing; corrected.
A strengthened lifecycle test failed after 169.81s because runtime Spark schemas
retained stricter nullability than the declared publication contract. Manifest schemas
now use the explicit versioned contract; mandatory values, physical types and grid
checks remain enforced. This failure was not suppressed. Pinned Spark emits a
toJSON future-return-type warning for bounded reference serialization; no financial
rows are collected through that path.
Review also identified that resealed artifacts could claim inconsistent provenance
while retaining valid keys/counts. Inspection now recomputes every inherited value
and provenance field through the accepted native boundary; an adversarial changed-feed
fixture verifies rejection. Output UUID normalization and early reference bounds
close malformed/oversized execution requests before publication work.

Implementation CI [44139229661](https://github.com/1211882/LABGDD/actions/runs/44139229661),
Ubuntu job 134237106744, completed successfully: 356 passed, three skipped,
four warnings, 463.25s. Skips were optional real multi-symbol Gold projection,
real-input publication refusal and approved AAPL Silver evidence absent from the
clean checkout; all ran in the no-skip local suite. Formatting, lint, strict typing,
dependencies, build, new-module clean-wheel imports, offline CLIs and security
(188 candidates) all succeeded. Windows was not awaited and no success is claimed
for its new run. Any documentation-only follow-up CI is separate and is reported
at its actual observed status in the final response, never predicted here.

## 10. Quantitative Results

Synthetic fixture: three five-member sectors, one XNYS session, all four horizons.
Declared candidate: 360 windows, 240 eligible; 3,600 pairs, 1,200 eligible. Energy
has zero variance/undefined ratios and correlation; Technology is complete;
Financials has a wholly absent V and one missing JPM bar. Strict historical candidate:
zero eligible windows/pairs; unverified evidence is not upgraded.

Observed final-source synthetic candidate in the disposable test container:
run a4958e09-cfb9-472d-8913-a19952a6b5d9, manifest SHA-256
`f601ffbca581d5875e6bb446ebb22ef777cbb95aced19e77687319303fe48e9b`.
One windows Parquet file, 59,955 bytes; one correlations file, 194,430 bytes.
Strict run 9002e0c1-afd3-47f3-8660-799581867f66, manifest
`9d2e399a195ee46d68c767c880dadd94668aaccb08d99779c3d7855a0a5d8e3d`.
Both record processing-source fingerprint
`75725248ba2f1ea5b0e5cdb245018118beb6850b565affac645a67f860951f7e`,
independently matched to canonical source bytes and the exact staged Git source tree.
Their synthetic input Gold manifest
is `58a0bcd224ac26715f2c78181a168f5c2fd77577ddfed1583e7ba333fd5d180b`.
The invalid-individual-return fixture produces 236 eligible windows and four
ineligible_individual_return windows across the four horizons, with null metrics.
These are temporary synthetic execution evidence, not retained market publications;
never register them as real authority. Logical replay is independent of filenames,
run UUIDs and binary Parquet size. Scalar parity uses absolute 1e-12/relative 1e-10.

## 11. Architecture Decisions

ADR 016 remains Proposed. Reuse GoldRun instead of a second storage lifecycle;
separate namespace/schema instead of changing frozen Individual/Expected Gold.
Keep the public path synthetic-only with a fingerprinted input marker; no real
publisher or Sector approval CLI is exposed. A future separate Sector registry must
bind exact manifest/version/fingerprints/upstream authority, reject superseded or
unregistered runs, preserve prior authority and use explicit content-bound approval.
No registry file or activation entry is created in this milestone.

## 12. Security and Governance

Zero Alpaca requests. No credentials read from .env, printed, committed or included
in reports. .env remains ignored and untracked. All source mutations in tests affect
temporary synthetic fixtures. Frozen datasets and active registries remain unchanged.
Completed candidate status is distinct from external approval; candidates remain
pending. GitHub fetch/push are authorized only as normal fast-forwards after gates.
No force-push or approval tag is permitted.

## 13. Problems and Limitations

No blocking defect remains in the validated bounded scope. The foundation is bounded to 1,000
reference population rows and the inherited 256 MiB input snapshot. Stop Spark
sessions to release session-local checkpoint blocks; block loss fails closed.
Shared-filesystem executor accessibility is required. Privileged in-process mutation,
object-store commits, durable checkpoint recovery and large campaigns are not solved.
Synthetic markers are trusted only through explicit local registry/manifests, not
cryptographic signing. Native candidate recomputation is not an independent
mathematical implementation; scalar parity and independent review remain necessary.
One-session IEX coverage and declared research membership cannot establish verified
historical sectors or provider correction finality. The real pilot has no complete
five-member sector and is deliberately not publishable through this API.

## 14. Current Project State

Synthetic immutable candidate creation is implemented; acceptance/real publication
remain deferred. Existing approved Bronze, Silver, Individual and Expected-Series
Gold authorities stay intact. No Sector authority exists. Original 30-symbol,
six-sector research cohort is unchanged. No later milestone has started.

## 15. Recommended Next Step

Independent review of Proposed ADR 016 and the synthetic publication foundation,
including boundary/physical/scientific/failure evidence. Acceptance requires no
unresolved blocking defect and verified quality gates. Keep real publication,
registry activation, acquisitions, calibration and later infrastructure excluded.

## 16. Decisions Requiring Approval

ADR 016 acceptance, any real Sector publication API, Sector registry implementation
and activation, and scaling beyond bounded local execution require separate approval.
Successful tests or an immutable directory provide none of those permissions.

## 17. Next-Agent Continuation Instructions

Work only in canonical LABGDD/main; read AGENTS.md, ADRs 014-016, this report and
PHASE2_SECTOR_PUBLICATION.md. Inspect the final commit/remote evidence supplied with
this report. Recreate synthetic fixtures; do not rely on deleted container artifacts.
Run python -m pytest -q, Ruff, strict mypy, pip check, build, clean-wheel and security
checks, including Linux Spark lifecycle tests. Missing optional real evidence may
skip; present-invalid evidence must fail. Preserve independent expected populations,
full pair counts, reason precedence, source-bound identities and null ineligible
metrics. Never modify LABGDD_NEW, frozen data or approved registries to pass tests.
Do not publish real Sector Gold, acquire data or advance phases automatically.

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
