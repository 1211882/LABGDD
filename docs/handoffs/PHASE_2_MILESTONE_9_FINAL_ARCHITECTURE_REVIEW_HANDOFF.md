# LABGDD V2 - Milestone 9 Final Architecture Review Handoff

Date: 2026-11-17. Evidence below is VERIFIED unless explicitly qualified.

## 1. Executive Summary

Phase 2, Milestone 9: independently review approved Expected-Series Gold to Sector
Gold calculations. Status: Completed; required local validation passed.
Three concrete defects were corrected: lazy source substitution, contradictory
current-close/observed-count eligibility, and rejection of a valid upstream
equivalent-duplicate quality state. Measurement 2.0.0 formulas remain unchanged.
No Sector Gold publication, acquisition or subsequent milestone was authorized.
ADR 015 is Accepted for this bounded read-only integration, without publication approval.

## 2. Repository and Git State

Canonical repository: `C:\Users\Deus\Documents\LABGDD`; branch `main`.
Starting HEAD: `ba516cd7a48d8b8ac732602faf64a78c9a22f57e`, initially clean.
Origin: `https://github.com/1211882/LABGDD.git`. A fetch confirmed the same
`origin/main` and zero divergence. No unrelated changes were present.
The authorized review commit and normal fast-forward push follow validation.
This report cannot contain its own enclosing commit SHA; final HEAD, push result
and verified remote SHA must be recorded in the accompanying final response.
No tag is requested or created; no previous history or tags are rewritten.
Historical `C:\Users\Deus\Documents\LABGDD_NEW` is untouched.

## 3. Initial State and Dependencies

Inherited Milestone 9 implementation `55efcb3ebfd882e9d8e2745a8a32c9471346b50a`
and documentation baseline above. Reviewed AGENTS.md, ADRs 014/015, measurement
and data contracts, the previous Milestone 9 handoff, Gold/Silver resolvers,
research/expected-plan contracts, approved-sector projection, native/scalar sector
engines and regression tests. Reused dependencies; none added. Local Windows
Python is 3.11.9; dedicated Linux validation uses Python 3.11.18, Spark 4.3.0,
PyArrow 21.0.0. Calendar dependency is exchange_calendars 4.11.2. GitHub CI pins
Python 3.11.9. The temporary test image includes Git without changing production
images or their dependencies.

## 4. Implementation Details

High-severity finding: two source checksum checks did not protect later lazy Spark
actions. `processing.approved_sector._verified_returns_snapshot` now binds parsed
metadata to the exact hashed manifest bytes, copies only manifested return Parquet
files into a private temporary directory, verifies copied byte counts/SHA-256,
checks physical schemas/UTC timestamps, and eagerly materializes a disk-only Spark
local checkpoint. Temporary copies are removed before frames return. Source
replacement afterward cannot alter calculations; mutation during capture fails.
The copy is bounded to 256 MiB and streams bytes without collecting financial rows.

Medium-severity findings: `project_approved_expected_gold` could accept an eligible
return with a missing current close if manifest coverage was resealed accordingly,
and accepted only `canonical` although upstream Gold also permits
`canonical_equivalent_duplicate`. Current-close presence must now agree with
observed counts, eligibility requires a current close, and both valid quality
states are accepted while unknown/null observed quality fails.

## 5. Architecture and Data Flow

Explicit Gold dataset/UUID/required manifest digest -> approved resolver -> approved
Silver lineage -> independent research/calendar/plan regeneration -> verified
bounded byte snapshot -> native Spark validation/projection -> existing sector
engine. Gold authority/inventory is checked again before returning. Public APIs
accept neither arbitrary financial frames nor caller-constructed authority objects.
The existing public frame API remains synthetic-only. There is no newest-directory
selection, writer, threshold-calibration input or publication activation.

## 6. Scientific and Mathematical Contracts

Preserve measurement 2.0.0 and H={5,15,30,60}. For N expected members,
q_i=sum_t(r_i,t^2), sector RMS volatility=sqrt(sum_i(q_i)/N). Exact equal-weight
rebalanced portfolio returns are log1p(sum_i(expm1(r_i,t))/N); portfolio volatility
is sqrt(sum_t(portfolio_return_t^2)). The mean-log-return proxy remains separate.
Dispersion is the population standard deviation of sqrt(q_i), divided by N.
Pearson requires complete H-vectors and all N(N-1)/2 pairs for its headline mean.
Constant detection uses min==max; translated/scaled centering preserves small
representable variation without an epsilon cutoff. Pair accumulation is ordered.
HHI=sum_i((q_i/sum_j(q_j))^2); effective breadth=(1-HHI)/(1-1/N), requiring N>=2
and positive total variance. Undefined metric states remain separate.

No observed-survivor denominators or fabricated minutes are permitted. Primary
reason precedence is `member_outside_acquisition_scope`,
`incomplete_sector_population`, `unverified_historical_applicability`,
`missing_expected_member`, `missing_observed_bar`, `ineligible_individual_return`.
Strict policy cannot manufacture history or downgrade strict source evidence.

## 7. Data Contracts and Provenance

Actual approved input: `gold-study-cohort-20260914-multisymbol-expected-v1`, run
`39ab367c-c6d2-480d-8c94-f35f767ed289`, manifest SHA-256
`d97b0e48c8463b6a97aa58e576e7f24998b4246f496fabaa286982177ab605b2`.
Upstream Silver: `silver-pilot-five-symbol-20260914-v1`, run
`f9d4bcc6-5a51-4c37-8d74-86cf9cee1ca2`, manifest
`f0ff790cfe0200e94c407010cfc4f2a8bfe93110d580f751b9f934095f9a15f8`.
Research hash: `84bf9ab5f33ac3cda6eb096ba35b3342de32560e251c504af8158251195b02e3`;
plan: `05da9a6a55b74c17665d530e26a171f2a19ab8f2dbacd505fc3b2cac2880972e`;
schedule: `b3a8f97e6d7e8a40c5c04682498b046f03f91c156174f63863b8c8092b2cfd48`.
Upstream processing-source hash:
`53f848a18cd536524488cab101df39e082600b50e8771c172540f40458bed0f5`;
configuration: `65cc05f6f6977d21a98861b030ad32e6c663577d21fb9c2028cee97982cff386`.
All four approved Gold and both approved Silver authorities independently resolved
with inventory/checksum verification; no registry changed.

Preserve `sector-output-identity-v1`, `sector-complete-population-v1` and
`sector-eligibility-policy-v1`. Base identity binds sector/session/horizon/window,
sorted expected symbols, policy/version and research hash. Additional identity is
SHA256 of `(sector-approved-input-v1|base identity|dataset|UUID|manifest hash)`.
Windows and pairs retain input source/configuration/schema/measurement/plan/research
and Silver lineage. Pair logical identity additionally requires left/right symbols;
the shared window hash alone is not a unique pair key. Outputs explicitly remain
`calculation_only_not_published`.

## 8. Files Changed

- `src/labgdd/processing/approved_sector.py`: snapshot and semantic corrections.
- `tests/integration/test_approved_sector.py`: corruption/replacement, snapshot
  limit, lineage/calendar/fingerprint/quality failures, source identity and output
  lineage assertions; synthetic publications remain isolated.
- `docs/decisions/015-approved-gold-sector-projection.md`: integrity boundary,
  failure semantics and acceptance decision.
- `docs/architecture/PHASE2_APPROVED_SECTOR_INTEGRATION.md`: verified-read contract.
- `docs/architecture/ARCHITECTURE.md`: final integration status.
- This handoff: review evidence and continuation instructions.

No frozen schemas, formulas, configuration registries, datasets or dependencies
changed. Build outputs, caches, test copies and logs are temporary/ignored, unstaged.

## 9. Validation Evidence

Passed: initial focused network-disabled Linux run, `python -m pytest
tests/integration/test_approved_sector.py -q -p no:cacheprovider`: 22 passed,
138.24s, including real pilot and post-verification replacement. Subsequent final
regressions are included in the final complete run: `python -m pytest -q
-p no:cacheprovider`, 343 passed, no skips, 588.02s. This ran in the dedicated
Linux container with all frozen data mounted read-only and network disabled.
The only later test change strengthens source corruption to retain file size.
Final `python -m pytest tests/integration/test_approved_sector.py -k source_mutation
-q -p no:cacheprovider` passed: four tests, 22 deselected, 67.37s, in a network-disabled
Linux container with no real datasets mounted. These prove digest-based rejection,
detached post-projection replay and bounded snapshot refusal.

Passed: `python -m ruff format --check --no-cache .` (80 files),
`python -m ruff check --no-cache .`, `python -m mypy --cache-dir <temporary>`
(80 files), `python -m pip check`, `python scripts/check_repository.py`,
`git diff --check`, offline check CLI and ingestion `--check`.
Passed: `python -m build --no-isolation <temporary tracked-file copy>`;
`python scripts/smoke_wheel.py <copy>/dist/labgdd_v2-0.4.0-py3-none-any.whl
--config config/project.toml` with PYTHONPATH cleared. Wheel source bytes match
the reviewed module. The smoke installs offline in a new venv outside the checkout.

Intermediate issues: default Ruff cache writes were denied by sandbox boundaries;
explicit no-cache checks passed. Mypy caught an imported-symbol export in the test;
the test now imports the defining adapter directly. Ruff import ordering was fixed.
The first wheel smoke inherited PYTHONPATH/editable metadata and skipped installation;
clearing that test-process override produced a genuine successful clean install.
An intermediate full Linux run was deliberately stopped to include the final
quality-state correction; it is not reported as passing. No failures were suppressed.

Source CI [43982258745](https://github.com/1211882/LABGDD/actions/runs/43982258745)
completed successfully: Ubuntu 330 passed/2 optional-evidence skips, 270.66s;
Windows 314 passed/18 skips, 988.04s. Baseline main CI
[43983493020](https://github.com/1211882/LABGDD/actions/runs/43983493020)
also completed successfully: Ubuntu 330/2, 197.78s; Windows 314/18, 1271.96s.
Job conclusions and test summaries were retrieved directly. Windows skips include
the Linux-only approved-Parquet suite; they are not Windows execution evidence for
that suite. The user explicitly waived waiting for Windows for this final review.
Any new review-commit CI is a separate run, reported in the final response; a pending
Windows job must never be described as passed.

## 10. Quantitative Results

Real evidence preserves 30 symbols, six declared sectors, 390 scheduled minutes,
11,700 Gold slots, 1,861 observations and 1,787 eligible upstream returns.
AAPL/MSFT/NVDA have 390 bars each, JPM 371, XOM 320. Missing slots=9,839:
9,750 deliberately unrequested plus 89 requested-symbol gaps (JPM 19, XOM 70).
The upstream returns snapshot contains one 149,769-byte Parquet file.
All 720 sector windows and 7,200 pair rows retain complete populations and null
financial metrics with outside-scope precedence. Synthetic two-member integration
compares 120 windows to Python with absolute 1e-12/relative 1e-10 tolerance.
Existing all-horizon tests exercise constants, next-representable variation, exact
portfolio returns, HHI/breadth, missing members and exact partition-order replay.

## 11. Architecture Decisions

ADR 014 is preserved. ADR 015 is Accepted after full validation and focused corruption checks.
Choose a bounded byte snapshot plus eager disk checkpoint over repeated checks
of lazy source paths. Avoid new services, signing, registries or financial schemas.
Sector publication remains separately governed; calculation acceptance grants no
publication permission. No approval tag is created.

## 12. Security and Governance

No Alpaca calls, acquisition, calibration, financial publication or registry writes.
Frozen data was mounted read-only into a network-disabled, unprivileged container.
Mutations affect only copied synthetic fixtures. `.env` is ignored and untracked;
its values were not read or reported. Credential values were never printed.
Security scanning includes secret assignments, forbidden paths and historical audit
integrity (179 candidate files); exact staged bytes are checked again before commit.
GitHub queries/fetch/push
are within explicit authorization; no force-push or history rewrite is permitted.

## 13. Problems and Limitations

No blocking correctness defect remains within the reviewed scope. Local checkpoints are
session-scoped, not durable recovery: block loss must fail rather than reread
original paths. Stop Spark sessions to release resources. Snapshot bytes must be
accessible to executors; larger/distributed deployment requires separate review.
The boundary does not sandbox malicious Python code or privileged filesystem/Spark
access. It does not continually reauthorize already-returned frames after future
registry changes; future publication must resolve authority anew.
One-session, five-symbol IEX data cannot establish complete sectors, consolidated
coverage, historical listing/classification validity or provider correction finality.

## 14. Current Project State

Approved upstream evidence remains intact and calculable through an explicit,
read-only boundary. Synthetic calculations verify numerical behavior; real pilot
calculations remain wholly sector-ineligible. No Sector Gold dataset or authority
has been published. No later milestone is started.

## 15. Recommended Next Step

After this review and CI evidence are accepted, separately review a bounded Sector
Gold publication design with explicit immutable identity, authority revalidation,
processing provenance and failure-safe publication. Require independent approval;
exclude acquisitions, sector-value claims from this incomplete pilot, calibration
and later services. Do not implement automatically.

## 16. Decisions Requiring Approval

Sector publication and larger/distributed snapshot execution require separate review
and authorization. No approval of either is inferred from ADR 015 acceptance.

## 17. Next-Agent Continuation Instructions

Use canonical LABGDD/main and the final commit recorded with this report. Read
AGENTS.md, ADRs 014/015, `processing/approved_sector.py`, `analytics/sector.py`,
`processing/sector_gold.py`, the integration documentation and this handoff.
Reuse exact approved manifests above; never choose the newest directory.
Run `python -m pytest -q`, strict typing/security/build/wheel checks and the Linux
Spark regressions in a dedicated environment. Optional real evidence may be absent;
present-invalid evidence must fail. Verify latest CI for the exact review commit.
Never modify LABGDD_NEW, frozen artifacts or registries to make tests pass. Do not
collect production financial rows, fill gaps, weaken complete populations, publish
Sector Gold or progress to another milestone without authorization.

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
