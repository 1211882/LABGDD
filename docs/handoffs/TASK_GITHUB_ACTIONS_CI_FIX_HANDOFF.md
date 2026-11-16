# LABGDD V2 - CI correction handoff

## 1. Executive Summary

Status: Partial pending remote execution. Correct CI environment setup and wheel selection without application changes. Original run 43924758939 failed because Ubuntu 26.04 lacks Python 3.11.9 builds.

## 2. Repository and Git State

Canonical repository: C:\Users\Deus\Documents\LABGDD. Branch main; starting commit 8656823c5288b011af6e3039c9bfbf888a0168d3; initial tree clean. Origin https://github.com/1211882/LABGDD.git. Commit and push identities will be recorded after execution. No tags or history rewrites authorized.

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

VERIFIED local Ruff check and scripts/check_repository.py passed (171 candidates before this document). git diff --check passed. PyYAML 6.0.3 installed only into a temporary validation directory; parsed with BaseLoader to preserve the Actions on key. YAML, triggers, OS matrix, Python pin, wheel/package version and unchanged 14-step count verified. Initial validation wrongly assumed 15 steps; corrected to comparison against baseline, which has 14. Remote job outcomes remain pending and must not be called passed yet.

## 10. Quantitative Results

Two OS targets, 14 preserved steps, four workflow line replacements. No market data or financial results generated.

## 11. Architecture Decisions

Pin supported Ubuntu rather than change the scientific Python baseline. Use verified stable Node.js 24 actions. No ADR changes required.

## 12. Security and Governance

Only authorized GitHub reads/push and temporary parser installation. No credentials read or printed; no Alpaca requests or publication activation. Stage only workflow and handoff.

## 13. Problems and Limitations

CI success requires actual completion on both platforms. Additional failures must be diagnosed from actual logs and fixed only within authorized CI scope.

## 14. Current Project State

Local configuration validated; remote execution pending. Application and scientific state unchanged.

## 15. Recommended Next Step

Push the bounded correction after fast-forward verification; monitor both jobs to terminal status and record actual failures or success.

## 16. Decisions Requiring Approval

None within the authorized CI scope. Stop if resolving a failure requires application or scientific changes.

## 17. Next-Agent Continuation Instructions

Read AGENTS.md and this handoff in canonical LABGDD. Verify main, HEAD, clean tree and origin; reuse the commit-triggered Actions run. Do not modify LABGDD_NEW, datasets or financial contracts. Do not start another milestone.

**Review status:** NEEDS ATTENTION

**Next phase:** NOT STARTED
