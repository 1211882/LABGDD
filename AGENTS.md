# LABGDD V2 - Development Workflow and Mandatory Handoff Policy

These instructions apply to every future development task, implementation phase, refactoring, bug fix, and architecture change in this repository.

The canonical development repository is `C:\Users\Deus\Documents\LABGDD`.
`C:\Users\Deus\Documents\LABGDD_NEW` is the historical V2 provenance repository;
do not modify it unless separately and explicitly authorized.

## 1. Development Workflow

Work incrementally and preserve the established architecture.

For every task:

1. Inspect the relevant existing implementation.
2. Identify the objective and scope.
3. Explain significant architectural decisions.
4. Implement only the requested changes.
5. Add or update appropriate tests.
6. Execute relevant validation checks.
7. Review the final diff for unintended changes.
8. Produce a complete handoff report.

Do not automatically proceed to the next phase.

Do not introduce unnecessary abstractions, dependencies, services, or architectural complexity.

Preserve data contracts, scientific reproducibility, and existing validated behavior.

Implementation, validation, architecture acceptance, execution authorization and
publication approval are separate states and must never be conflated.

## 2. Mandatory End-of-Task Handoff

Every task must end with a comprehensive, self-contained English Markdown handoff:

1. Persisted under `docs/handoffs/` without overwriting an earlier handoff.
2. Reproduced in full in the final Codex response.

Use `docs/handoffs/PHASE_<NUMBER>_<DESCRIPTIVE_NAME>_HANDOFF.md` for a substantial
phase or milestone and `docs/handoffs/TASK_<SHORT_NAME>_HANDOFF.md` for a bounded or
smaller task. A summary or link is not a substitute for the complete response.

The persisted document and final response must contain the same substantive facts
and allow another engineer or agent to continue without prior chat history. A
genuinely irrelevant section may say `Not Applicable` with a short reason; material
information must never be omitted.

Use all of these sections in order:

### 1. Executive Summary

Record the project, phase/milestone, exact task and objectives, completion status
(`Completed`, `Partial`, `Blocked` or `Failed`), principal technical/scientific
outcomes, important architecture decisions and pending approvals.

### 2. Repository and Git State

Record canonical path, branch, starting/final commits, current HEAD, working tree,
remote name/URL, local commit and push status, remote branch and verified remote
commit, tags and exact targets, and unrelated or pre-existing changes. Never confuse
a local commit with a remote push. If a commit cannot contain its own SHA, state that
limitation and record the SHA in the final response.

### 3. Initial State and Dependencies

Explain inherited implementation, frozen/approved baselines, governing contracts and
ADRs, required datasets/manifests, dependencies, runtime versions, prerequisites,
initial blockers and whether each material fact was independently verified.

### 4. Implementation Details

Explain what changed, old/new behavior, rationale, affected functions, classes,
interfaces and configuration, validation boundaries, failure behavior and
compatibility. Do not merely list filenames.

### 5. Architecture and Data Flow

Describe relevant components, responsibilities, dependencies, processing boundaries,
input/output authority, data movement, persistence, immutable evidence and failure
recovery or fail-closed behavior.

### 6. Scientific and Mathematical Contracts

When applicable, state exact formulas, versions, horizons, population/eligibility
policies, temporal semantics, missingness, reason precedence, denominators, numerical
tolerances and Python/native Spark parity. If none changed, say so explicitly.

### 7. Data Contracts and Provenance

Record affected schemas, dataset/run identities, manifest/artifact SHA-256 values,
source/configuration fingerprints, UTC/calendar provenance, lineage, research
membership limitations, publication status and authority. Mark non-applicable or
uninspected evidence explicitly.

### 8. Files Changed

List every relevant added, modified and removed repository-relative file with its
responsibility. Identify generated/ignored files separately and whether Git excluded
them.

### 9. Validation Evidence

List actual commands and outcomes: passed, failed, skipped or not executed. Include
counts, durations and relevant versions for tests, formatting, lint, strict typing,
dependencies, builds, clean installs, security, Linux/container execution,
Python/Spark parity and physical-data checks. Record material intermediate failures
and their corrections. Never claim an unexecuted check passed.

### 10. Quantitative Results

When applicable, record counts, coverage, eligibility, missingness, reason
distributions, file counts/sizes, performance and numerical errors/tolerances.
Otherwise use `Not Applicable` with a reason.

### 11. Architecture Decisions

Record ADR status, accepted/rejected alternatives, trade-offs, compatibility,
deferred decisions and the distinction between implemented proposals and approvals.

### 12. Security and Governance

Record credential/environment handling, network authorization, immutable artifacts,
publication authority, registry effects, Git tracking, secret scans, incidents and
destructive-operation authorization. Never include credential values.

### 13. Problems and Limitations

Record defects and severity, correctness/scientific risks, technical debt,
environment limits, validation gaps, unverified assumptions and regressions.

### 14. Current Project State

Distinguish implemented, executed/tested, accepted and published capabilities.
Identify authoritative datasets, frozen milestones, pending candidates and work left.
Never infer approval from a directory, successful run or passing test.

### 15. Recommended Next Step

Give one primary task with rationale, prerequisites, bounded scope, acceptance
criteria, exclusions and required authorization. Do not perform it automatically.

### 16. Decisions Requiring Approval

List unresolved decisions, options, consequences and authorization requirements. If
none exist, state `None`.

### 17. Next-Agent Continuation Instructions

Give the canonical path/baseline, essential files, commands to rerun, evidence to
reuse, invariants, pitfalls and stop conditions. Explicitly prohibit modification of
the provenance repository and automatic phase progression.

End every handoff with:

**Review status:** READY FOR REVIEW / NEEDS ATTENTION / BLOCKED

**Next phase:** NOT STARTED

## 3. Evidence Integrity and Handoff Consistency

Use these labels when they materially clarify evidence status:

- **VERIFIED** - directly checked through an executed command, inspected file or
  validated artifact during the task.
- **REPORTED** - supplied by a prior handoff, another agent or external report but
  not independently verified during the task.
- **ASSUMED** - necessary context that was not independently established.
- **PROPOSED** - suggested but not implemented or approved.
- **BLOCKED** - awaiting a requirement, authorization or external state change.

Never fabricate tests, commits, hashes, identities, approvals or execution results.
Record material failures and corrections. Distinguish facts from estimates and
scientific interpretations. The persisted and final handoffs must contain the same
substantive information; unavoidable self-referential Git facts must be explained.

Use repository-relative paths and concrete interfaces. Never include credentials.
Avoid generic success claims and unnecessary terminal output. Substantial reports
should generally be 800-1,500 words, but evidence integrity and completeness take
precedence over length.

## 4. LABGDD-Specific Safeguards

Preserve these constraints unless the user explicitly authorizes a reviewed change:

- Frozen Bronze, Silver and Gold publications and immutable artifacts.
- External publication authority, registry selection and supersession contracts;
  never select the newest filesystem directory automatically.
- Financial measurement specification v2.0.0 and its distinct estimands.
- Research-universe versioning and historical listing/entity/sector-membership
  limitations; declared membership is not independently verified history.
- UTC event-time semantics, exchange-calendar provenance, DST and early closes.
- Python scalar/native Spark parity for applicable financial calculations.
- Immutable manifests, checksums, source/configuration fingerprints and lineage.
- Separate implementation, validation, execution authorization and publication
  approval states.
- Explicit authorization before authenticated Alpaca requests, publication
  activation, destructive operations or material external side effects.
- No secrets in logs, diffs, reports, commits or generated artifacts.
- No automatic progression to a later milestone or phase.

Do not modify approved registries, frozen datasets, formulas, temporal contracts or
publication authority merely to make a test pass. Stop and report conflicts requiring
architecture review or broader authorization.

## 5. Final Review Gate

Before finishing any task, verify and report:

- Requested scope and stop boundaries were respected.
- Relevant checks ran and their actual outcomes are recorded.
- Existing contracts and authority were preserved or explicitly reviewed.
- New dependencies and abstractions are justified.
- Exact diff/staged inventory excludes credentials, `.env`, datasets, models,
  caches, build products, generated runtime artifacts and unrelated files.
- Remaining risks and approval requirements are explicit.
- The handoff is understandable without repository or conversation access.
- Working tree, commit, tag and push state are precise.

Never proceed automatically to the next phase without explicit instruction.
