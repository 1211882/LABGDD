# LABGDD V2 – Development Handoff

**Phase:** Phase 2, Milestone 6 – Multi-Symbol Historical Acquisition and End-to-End Validation
**Task:** Authorized Alpaca acquisition and pending-review Bronze → Silver → Individual Gold → Expected-Series Gold validation
**Status:** Completed
**Date:** 2026-11-06
**Git branch and commit:** `main` at approved baseline `c085197e8762190501ffac6d898414d96563ace7`; all Milestone 6 changes are intentionally uncommitted.

## 1. Objective

Execute the explicitly authorized four-symbol Alpaca request, reuse immutable AAPL
Bronze evidence, and validate the resulting five-symbol pipeline through native
Linux Spark Silver, Individual Gold and full-cohort Expected-Series Gold. Preserve
financial, temporal, missingness, provenance and publication-authority contracts;
leave every new publication pending review and do not start sector analytics.

## 2. Changes Implemented

The existing Alpaca adapter executed exactly one read-only request for MSFT, NVDA,
JPM and XOM. Credentials were loaded from the ignored root `.env` only into that
process and were never printed, logged or stored in request metadata. The request
used raw IEX 1Min data for `[2026-09-14T13:30:00Z, 2026-09-14T20:00:00Z)`, provider
inclusive end `19:59:59.999999Z`, limit 10,000, ascending order, 15-second timeout,
three attempts per page and five-page ceiling. It completed with one page and one
HTTP attempt.

New Bronze acquisition `ae7fecdc-d082-478d-b26d-b3e7e5db9600` contains 1,471
observations: MSFT 390, NVDA 390, JPM 371 and XOM 320. All symbols start at 13:30
and end at 19:59 UTC. Coverage is 100%, 100%, 95.1282% and 82.0513%, respectively;
89 scheduled symbol-minutes are absent. The manifest reports zero invalid,
duplicate or conflicting logical keys. Original provider fields `t,o,h,l,c,v,n,vw`
and the response page are retained. AAPL was not requested again; acquisition
`f2345e41-da10-420f-8b60-6079a9df2006` supplied its existing 390 observations.

Native Linux Silver candidate `f9d4bcc6-5a51-4c37-8d74-86cf9cee1ca2` combined the
two explicit manifests. It produced 1,861 unique canonical records, no quarantine,
no duplicate/conflict groups and 89 missing of 1,950 scheduled records. The run has
physical UTC microsecond timestamps and verified artifact inventory.

The standard Gold boundary could not consume an unapproved Silver directory. A
small opt-in authority mode, `authorized_bounded_validation`, was added. It performs
the same identity, fingerprint, manifest and artifact verification as approved
resolution but uses a separate registry and is rejected by
`resolve_approved_silver()`. Existing configurations default to `approved`.

Individual Gold candidate `310941bd-7ff9-4e25-971a-febec753c9e5` produced 1,950
return slots, 7,780 rolling rows and 600 fixed windows. Eligible totals are 1,787,
5,117 and 468. Expected-Series Gold candidate
`39ab367c-c6d2-480d-8c94-f35f767ed289` produced 11,700 returns, 46,680 rolling rows
and 3,600 fixed rows for all 30 research symbols. Its manifest explicitly records
the five acquisition-scope symbols; the 25 unrequested symbols retain null
observations and financial values.

## 3. Files Created or Modified

- `config/historical_milestone6.toml` and `config/silver_milestone6.toml`: exact
  acquisition and two-manifest Silver requests.
- `config/milestone6/{silver_publications.toml,gold.toml,expected_gold.toml}`:
  isolated bounded-validation authority and Gold requests; no approved registry changed.
- `src/labgdd/adapters/silver.py`: separate approved and bounded-validation resolvers.
- `src/labgdd/{gold_config.py,expected_gold_config.py}`: explicit input-authority
  selection; expected configuration validates acquisition-scope symbols.
- `src/labgdd/processing/{gold.py,expected_gold.py}` and
  `src/labgdd/runners/gold.py`: consume the explicit authority mode and persist it;
  Expected Gold persists acquisition scope.
- `tests/unit/test_{silver_registry,gold_config,expected_gold_config}.py`: fail-closed
  authority and scope regressions.
- `docker/spark-batch/Dockerfile.test`: parameterized test base image without adding
  test dependencies to production.
- `docs/decisions/012-bounded-candidate-silver-validation.md`,
  `docs/architecture/PHASE2_MULTISYMBOL_VALIDATION.md`, `README.md`, and this handoff.

Ignored immutable evidence was created under `data/{bronze,silver,gold}` with the
run identities above. Existing approved data and `config/{silver,gold}_publications.toml`
were not modified.

## 4. Architecture and Design Decisions

Approved publication authority remains external and unchanged. The candidate
resolver is explicit, status-specific and fail-closed; it cannot select by directory
recency and cannot satisfy an approved lookup. ADR 012 remains **Proposed** pending
review. Gold candidates are not in the approved Gold registry. Expected-Series
Gold uses session-only partitioning; Individual Gold retains its established
session/symbol layout.

The expected population remains independent of observations. Acquisition scope is
manifest metadata, not a financial eligibility override. Declared-research policy
allows the bounded financial comparison while preserving unverified historical
membership fields. No bars, prices, returns or missing minutes were synthesized.

## 5. Tests and Validation

Passed commands and evidence:

- Ruff formatting and lint: passed; final source formatted.
- Strict mypy: passed for 68 source files.
- Full Windows pytest: **260 passed, 2 skipped** in 852.79 seconds. The only skips
  were the two Linux-only publication tests. A pre-existing damaged external pytest
  temp-directory cleanup warning occurred; no repository test failed.
- Dedicated Linux publication tests:
  `test_expected_gold_publication_is_immutable_extended_and_checksummed` and
  `test_expected_gold_failure_is_not_published_complete`: **2 passed, 0 skipped**
  in 52.14 seconds. Two earlier container invocations failed before test assertions
  because the first lacked the config mount and the second used non-executable
  `/tmp`; the corrected invocation passed.
- `pip check`: passed. Sdist and `labgdd_v2-0.4.0-py3-none-any.whl` built.
- Clean-wheel installation/import/CLI/pip smoke: passed with zero mandatory base dependencies.
- `scripts/check_repository.py`: passed for 142 candidate files; `.env` remains ignored/untracked.
- Production Linux Silver smoke: canonical=4, quarantine=0, sessions=4.
- Production Linux Gold smoke: legacy `390/1556/120`, expected `780/3112/240`.
- All Bronze/Silver/Gold manifest artifact sizes and SHA-256 values verified.
- Independent pure-Python comparison: 10,330 requested-symbol financial rows matched
  exact keys/nulls/reasons and approved numeric tolerance. Individual/Expected
  parity and AAPL legacy parity passed.

Manifest SHA-256 values: Bronze
`7b1b3ce751ca50ded0fca940f029347f1a8f4bb80c5dc5212f2624a616e3f6a3`;
Silver `f0ff790cfe0200e94c407010cfc4f2a8bfe93110d580f751b9f934095f9a15f8`;
Individual Gold `c9ecea6009363f589bb57365acbccd6454fe8e122c3d737fbe3fd71363badad0`;
Expected Gold `d97b0e48c8463b6a97aa58e576e7f24998b4246f496fabaa286982177ab605b2`.

Execution durations were 2.405 seconds acquisition, 28.860 seconds Silver, 35.269
seconds Individual Gold and 50.624 seconds Expected Gold. Parquet storage was:
Bronze 2 files/134,473 bytes; Silver 3/188,331; Individual Gold 15/402,707; Expected
Gold 3/290,317. These bounded local measurements are not scale projections.

## 6. Issues and Limitations

JPM lacks 19 scheduled bars and XOM lacks 70; absence is not filled and is not proof
of exchange-wide missingness. IEX is venue-specific. Provider correction finality
and Bronze v1 independent page/row identity reconstruction remain unproven. The
local atomic rename protocol is not an object-store transaction. Multi-node Spark
and production-scale sizing are untested. Individual Gold has no embedded
`publication_status`; its pending state is established by non-registration and the
isolated validation input authority. Architecture should decide whether future
schema-v1 candidates need an additive manifest status before approval.

## 7. Scientific and Data Integrity

Measurement specification v2.0.0, return/window anchors, UTC semantics and financial
formulas did not change. Gaps propagate to controlled null/reason states. For fully
covered AAPL, MSFT and NVDA, each has 389/390 eligible adjacent returns and all 120
fixed windows eligible. JPM has 352 eligible returns and 72/120 eligible fixed
windows; XOM has 268 eligible returns and 36/120 eligible fixed windows. The full
expected relation has 9,839 missing-close slots, including intentionally unrequested
symbols, and does not label them provider failures.

The six-sector/30-symbol taxonomy remains project-defined. Historical entity,
listing and sector membership are unverified. This five-symbol, one-session pilot
is not evidence for sector volatility, correlation, concentration, breadth or
emergence conclusions.

## 8. Current Project Status

The approved baseline remains commit `c085197e8762190501ffac6d898414d96563ace7`.
Milestone 6 now has reproducible real multi-symbol Bronze and native Linux
Silver/Gold candidate evidence, all immutable and unregistered. Source, tests,
configuration and documentation are uncommitted. No approved registry, prior
publication, tag or credential file changed. Sector Gold and later systems are not started.

## 9. Recommended Next Step

Perform architecture review of ADR 012, the four candidate manifests, gap semantics
and the Individual Gold pending-status limitation. If accepted, freeze Milestone 6
in a separate commit and explicitly decide publication authority; do not begin
Sector Gold until that review is complete.

## 10. Review Questions

1. Accept or reject `authorized_bounded_validation` as a distinct, non-publication
   authority boundary for candidate end-to-end runs.
2. Decide whether Individual Gold schema v1 candidate manifests require an explicit
   immutable `publication_status` field before any future approval.

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
