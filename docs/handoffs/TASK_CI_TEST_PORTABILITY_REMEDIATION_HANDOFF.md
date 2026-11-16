# LABGDD V2 - Test portability remediation

## 1. Executive Summary

Task: make tests portable across clean CI checkouts while preserving strict publication authority. Status: Completed. Both platform workflows passed after the LF/CRLF fixture correction. Only test code changed; no scientific or production behavior changes.

## 2. Repository and Git State

Canonical repository C:\Users\Deus\Documents\LABGDD; branch main. Starting HEAD a08a5f7d6fa1feaf25d83f26e120a39af43c1725 preserves the prior local documentation commit. Origin https://github.com/1211882/LABGDD.git. Correction commit 268cc9c0cbce25062dd1b828316d01f7a99c6dcc pushed normally after fetch and ancestry verification; ls-remote matched. Final documentation commit is self-referential and its SHA will be given in the final response. No tags or history rewriting.

## 3. Initial State and Dependencies

Previous CI run 43927781980 reported 307 passing tests and two failures on Ubuntu; Windows cancelled. Both failures depended on ignored local Silver evidence absent from checkout. Existing Python 3.11.9, Spark, PyArrow and locked development tooling reused without dependency changes.

## 4. Implementation Details

tests/integration/test_spark_expected_series.py adds _optional_approved_aapl: check the exact registered AAPL run directory before calling resolve_approved_silver. Only total run absence skips; present directories, partial runs and symlinks reach the unchanged strict resolver. No resolver exception is converted to a skip. The original financial parity assertions remain unchanged.

tests/unit/test_expected_gold_config.py uses temporary project, request and research configuration. Only processing.expected_gold.resolve_silver_input is mocked as the external authority boundary. Real request validation, research plan generation and runner check flow execute. Import and socket guards reject Spark/network access; inventory equality and absent data directory assert no writes. Invalid policy returns 2 before authority resolution; valid check returns 0.

## 5. Architecture and Data Flow

No production modifications. Optional external evidence is distinguished from mandatory controlled test fixtures. Resolver authentication, hashes, checksum inventory and approval selection are unchanged.

## 6. Scientific and Mathematical Contracts

Measurement v2.0.0 formulas, eligibility, population, timestamps, tolerances and original Python/Spark AAPL parity assertions are unchanged.

## 7. Data Contracts and Provenance

Real optional identity remains silver-aapl-20260914-v1, run aecb53c1-d7a8-40c7-939b-a6901f435ff5. Registry and actual immutable evidence unchanged. Temporary invalid evidence is synthetic regression data only and cannot become publication authority.

## 8. Files Changed

tests/integration/test_spark_expected_series.py: optional evidence boundary and missing/invalid regressions. tests/unit/test_expected_gold_config.py: isolated check-mode fixtures and guards. docs/handoffs/TASK_CI_TEST_PORTABILITY_REMEDIATION_HANDOFF.md: this report. No production source, workflow, registry or datasets changed.

## 9. Validation Evidence

VERIFIED focused pytest selection configuration_check or optional_aapl: 6 passed, 14 deselected in 4.44 seconds. Present real AAPL parity test: 1 passed in 207.63 seconds; Windows emitted nonfatal post-exit cleanup Access denied. Ruff format: 78 files formatted; Ruff lint, strict mypy (78 files), pip check, repository security (173 candidates including this document) and git diff --check passed. Build and clean-wheel smoke passed in a secret-free tracked-file copy. Full Linux suite runs network-disabled with no developer data directory; interrupted initial session had no retrievable final summary and is not counted as passed. A rerun reported 311 passed, 1 skipped and 2 environment failures in 397.42 seconds because the local test image lacked Git. Git was installed into a separate temporary validation image, never the production image; an initial entrypoint mismatch was corrected with explicit python -m pytest. All 5 Git/architecture tests then passed in 0.32 seconds; complete rerun pending. Remote results pending.

Final complete network-isolated Linux rerun with Git available: 313 passed, 1 skipped in 358.52 seconds. Only the absent real AAPL publication was skipped; synthetic integration and publication tests executed. The tracked-file copy initially had no data directory and no developer datasets were mounted.

VERIFIED GitHub Actions run https://github.com/1211882/LABGDD/actions/runs/43934206966 tests correction commit 268cc9c0cbce25062dd1b828316d01f7a99c6dcc. Ubuntu 24.04 job 133534374839 succeeded: 313 passed, 1 expected absent-evidence skip in 237.28 seconds; build, clean-wheel smoke, CLI and repository security steps succeeded (173 candidates). Windows job 133534374423 failed: 308 passed, 3 skipped, 3 failed in 789.56 seconds. The three invalid-manifest regression cases hit request fingerprint mismatch first because Windows checkout changed the copied request to CRLF. The temporary fixture registry now binds to the copied request's actual bytes, with explicit LF/CRLF variants; the real registry is unchanged. Focused rerun: 9 passed, 14 deselected in 3.31 seconds. Follow-up remote execution pending. User subsequently authorized starting Milestone 9 without waiting for Windows; that waiver does not establish Windows success.

## 10. Quantitative Results

Final VERIFIED run https://github.com/1211882/LABGDD/actions/runs/43937276053 on bdc11581076fcf5ba339c58ee5999485211067da passed both workflows. Ubuntu: 316 passed, 1 expected optional-real skip in 186.35 seconds. Windows: 314 passed, 3 expected skips in 1251.90 seconds (two Linux publication cases and the optional real AAPL evidence case). Build, clean-wheel smoke, CLI and repository security checks passed on both; 173 candidates. This supersedes the pending status recorded above without concealing the earlier failures.

Four new evidence regressions: absent, empty run, invalid JSON manifest and fingerprint-mismatching manifest; one additional invalid-configuration parameter case. Financial record counts are unchanged; no acquisition occurred.

## 11. Architecture Decisions

Do not catch resolver failures or weaken production check mode. Mock only the external authority boundary for the unit test; preserve strict resolution for all present integration evidence. No ADR changes.

## 12. Security and Governance

No credentials, real datasets, ignored environments or caches staged. No Alpaca access or publication activation. Existing GitHub authorization covers normal push and CI inspection. Historical LABGDD_NEW untouched.

## 13. Problems and Limitations

Clean CI cannot validate the optional real publication; synthetic Spark tests remain mandatory. Real evidence still requires independent integrity validation and is not committed. CI success remains pending until both platform jobs finish.

## 14. Current Project State

Bounded test changes implemented, committed and pushed; complete remote workflows passed on both operating systems. User subsequently authorized Milestone 9, whose separate handoff records its implementation and evidence.

## 15. Recommended Next Step

Portability task is complete. Continue only the separately authorized Milestone 9 work and preserve the absent-versus-invalid evidence boundary.

## 16. Decisions Requiring Approval

None within authorized test-portability scope. Stop for unrelated application or scientific defects.

## 17. Next-Agent Continuation Instructions

Use canonical LABGDD only. Read AGENTS.md and this handoff. Preserve a08a5f7d ancestry and all immutable evidence. Never skip present-invalid evidence, weaken resolver checks, add datasets, access Alpaca or advance milestones. Record actual remote outcomes before declaring success.

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
