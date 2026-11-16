# LABGDD V2 - CI correction handoff

## 1. Executive Summary

Status: Blocked by pre-existing test dependence on machine-local evidence. CI environment setup and wheel selection corrected without application changes. Original run 43924758939 failed because Ubuntu 26.04 lacks Python 3.11.9 builds. New run reached pytest on both platforms.

## 2. Repository and Git State

Canonical repository: C:\Users\Deus\Documents\LABGDD. Branch main; starting commit 8656823c5288b011af6e3039c9bfbf888a0168d3; initial tree clean. Origin https://github.com/1211882/LABGDD.git. Correction commit d8e4b1facc0b6be56dcae5e8572071a681976962 was pushed by normal fast-forward; ls-remote verified the exact remote SHA. A subsequent local documentation commit records the blocker; its self-referential SHA is supplied in the final response. No tags or history rewrites.

## 3. Initial State and Dependencies

VERIFIED workflow used ubuntu-latest, Python 3.11.9 and an obsolete 0.2.0 wheel filename. Package version is 0.4.0. Official Python versions manifest confirms Ubuntu 24.04 x64 availability. No project dependency changes.

## 4. Implementation Details

Pin Linux to ubuntu-24.04; retain windows-latest and Python 3.11.9. Correct smoke wheel filename to labgdd_v2-0.4.0-py3-none-any.whl. Upgrade checkout to v7.0.1 and setup-python to v7.1.0 after checking official latest release metadata and action.yml Node.js 24 declarations.

## 5. Architecture and Data Flow

Only CI orchestration changes. Preserve all 14 existing steps, including installation, Ruff, mypy, pytest, dependency consistency, build, wheel smoke, CLI checks and security validation.

## 6. Scientific and Mathematical Contracts

Unchanged. Measurement v2.0.0, UTC semantics, population eligibility and financial calculations are unaffected.

## 7. Data Contracts and Provenance

No dataset, manifest, fingerprint, registry or publication changes. Historical LABGDD_NEW repository untouched.

## 8. Files Changed

.github/workflows/checks.yml: runner, action versions and wheel filename. This handoff: task evidence and continuation state. No source changes.

## 9. Validation Evidence

VERIFIED local Ruff check and scripts/check_repository.py passed (172 candidates including this document). git diff --check passed. PyYAML 6.0.3 installed only into a temporary validation directory; parsed with BaseLoader to preserve the Actions on key. YAML, triggers, OS matrix, Python pin, wheel/package version and unchanged 14-step count verified. Initial validation wrongly assumed 15 steps; corrected to comparison against baseline, which has 14.

VERIFIED run https://github.com/1211882/LABGDD/actions/runs/43927781980: Ubuntu failed, Windows cancelled by matrix fail-fast. Both passed environment setup, dependency installation, formatting, lint and typing and reached pytest. Ubuntu pytest: 307 passed, 2 failed in 166.64 seconds. Subsequent build, wheel and security steps were skipped. Logs downloaded through the GitHub job logs API using existing Git authentication internally without displaying credentials; unauthenticated download initially returned HTTP 403.

## 10. Quantitative Results

Two OS targets, 14 preserved steps, four workflow line replacements. Ubuntu collected 309 tests: 307 passed, 2 failed. Windows was cancelled; no final Windows test count is claimed. No market data or financial results generated.

## 11. Architecture Decisions

Pin supported Ubuntu rather than change the scientific Python baseline. Use verified stable Node.js 24 actions. No ADR changes required.

## 12. Security and Governance

Only authorized GitHub reads/push and temporary parser installation. No credentials read or printed; no Alpaca requests or publication activation. Stage only workflow and handoff.

## 13. Problems and Limitations

Success criterion remains unmet. test_approved_aapl_silver_parity_without_publishing fails at resolve_approved_silver with manifest fingerprint mismatch because ignored local data is not present in checkout; its missing-file skip occurs too late. test_configuration_check_does_not_start_spark_or_write expects return 0 but gets 2 because prepare_expected_gold requires that same local Silver authority. git ls-files data returns no tracked datasets. Changing test fixtures/evidence dependencies requires work beyond the bounded workflow correction; no tests were skipped or weakened and no frozen evidence was added to Git.

## 14. Current Project State

Workflow correction pushed and verified. Environment setup fixed; complete CI remains failing due to two tests requiring local evidence. Windows outcome cancelled. Application and scientific state unchanged.

## 15. Recommended Next Step

Authorize a separate bounded test-portability task: make the unit check use isolated fixture authority and define optional real-evidence integration behavior before invoking the strict resolver. Preserve strict checksum checks for present evidence. Then rerun both complete platform jobs. Exclude publication changes, dataset commits and financial changes.

## 16. Decisions Requiring Approval

Approval is required for test-fixture and evidence-handling changes beyond this workflow correction. Do not bypass tests, weaken authority verification or commit datasets simply to obtain green CI.

## 17. Next-Agent Continuation Instructions

Read AGENTS.md and this handoff in canonical LABGDD. Verify main, HEAD, clean tree and origin; reuse the commit-triggered Actions run. Do not modify LABGDD_NEW, datasets or financial contracts. Do not start another milestone.

**Review status:** BLOCKED

**Next phase:** NOT STARTED
