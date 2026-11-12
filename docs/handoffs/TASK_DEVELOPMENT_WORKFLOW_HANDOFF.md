### LABGDD V2 – Development Handoff

**Phase:** Phase 0 — development governance  
**Task:** Persist the development workflow and mandatory handoff requirements  
**Status:** Completed  
**Date:** 2026-11-02 (Europe/Lisbon; supplied client date)  
**Git branch and commit:** `main`; no commits exist. Existing project files remain uncommitted.

#### 1. Objective

Make the supplied workflow apply to future repository tasks and establish a persistent, self-contained handoff for this update. Scope is documentation only; no implementation phase is advanced.

#### 2. Changes Implemented

Saved the supplied instructions verbatim as root `AGENTS.md`. This gives future coding agents a repository-level instruction source for incremental inspection, scoped implementation, architectural explanation, appropriate tests, executed validation, final diff review and complete handoffs.

The policy requires English Markdown reports with ten sections, actual command results, explicit scientific-integrity statements, branch/commit identification, and review/next-phase markers. Substantial phases use `docs/handoffs/PHASE_<NUMBER>_HANDOFF.md`; smaller tasks use `docs/handoffs/TASK_<SHORT_NAME>_HANDOFF.md`. Existing handoffs must be preserved. Every final response must include the full report rather than only a link.

Added README links to the workflow and handoff directory. Created this task handoff; previous architecture and validation documents were preserved.

#### 3. Files Created or Modified

- `AGENTS.md` — created; verbatim repository-wide development and reporting instructions.
- `README.md` — modified; two navigation links added under “Design and next work.”
- `docs/handoffs/TASK_DEVELOPMENT_WORKFLOW_HANDOFF.md` — created; persistent report for this documentation task.

#### 4. Architecture and Design Decisions

Use one root instruction file rather than adding tooling, hooks or dependencies. The README links to that authority without duplicating its policy. No modules, classes, functions, configuration values, dependency boundaries, persistence behavior or runtime interfaces changed. The new workflow requires explicit instruction before proceeding to another phase.

#### 5. Tests and Validation

Actual commands and results for this task:

- **Passed:** `git -c safe.directory=C:/Users/Deus/Documents/LABGDD_NEW symbolic-ref --short HEAD` returned `main`.
- **Failed as expected:** `git -c safe.directory=C:/Users/Deus/Documents/LABGDD_NEW rev-parse --verify HEAD` reported that no revision exists, confirming there is no commit to identify.
- **Passed:** the Python documentation check reproduced below verified that `AGENTS.md` exactly matches the supplied attachment, the README modification consists only of the two intended links, the handoff has all ten required sections and closing markers, and the preserved reference audit retains its expected SHA-256.
- **Failed, then corrected:** the first README comparison used the older built source archive, which predates an existing smoke-test instruction. After accounting for that pre-task line, the comparison passed; no README implementation change was needed.
- **Not executed:** unit/integration tests, formatting, linting, type checking, builds and installation checks. This task changes Markdown only; runtime code, configuration and dependencies are unchanged. No test was skipped within an executed suite.

Documentation check command: `.venv/Scripts/python -`, with a standard-library script reading the attachment and changed Markdown files, asserting the conditions above, and printing `Documentation checks passed`. The script also checked final Markdown whitespace and emitted the README diff for review.

#### 6. Issues and Limitations

No new runtime issue is identified by this documentation-only change. Instructions depend on future agents following `AGENTS.md`; no enforcement service or hook was introduced. The repository has no committed baseline, so final review used direct content comparisons and the existing Git status rather than relying on `git diff` for untracked files. Existing uncommitted project files were preserved. This task does not retrospectively generate a new Phase 0 implementation handoff or rerun its earlier checks.

#### 7. Scientific and Data Integrity

No scientific or data semantics changed. Financial formulas, temporal semantics, data-quality policies, batch/streaming specifications, dataset/model versions and reproducibility settings remain unchanged. No dataset, experiment, credential or legacy source was imported or modified. Reference-audit integrity was checked directly.

#### 8. Current Project Status

The existing Phase 0 foundation contains an installable Python package with immutable `MarketBar` validation, `load_settings`, structured logging and an offline configuration-check CLI, plus architecture contracts, ADRs and tooling. The previous validation report records 78 passing tests and successful build/install checks; these are historical results, not reruns in this task. Acquisition, distributed processing, ML and serving remain unimplemented. This execution verified documentation only.

#### 9. Recommended Next Step

Review Phase 0 contracts and unresolved calendar, historical-membership, adjustment, correction-ordering and lateness decisions. Once explicitly instructed, begin Phase 1 historical ingestion. No Phase 1 work was performed.

#### 10. Review Questions

None for this workflow-persistence task. Existing Phase 0 design questions remain documented for their separate review.

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
