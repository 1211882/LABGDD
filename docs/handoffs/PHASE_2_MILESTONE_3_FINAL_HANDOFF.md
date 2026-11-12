# LABGDD V2 – Development Handoff

**Phase:** Phase 2, Milestone 3 – Research Universe and Expected-Series Foundation
**Task:** Architecture review, validation and approved baseline freeze
**Status:** Completed
**Date:** 2026-11-03
**Git branch and commit:** `main`; frozen by annotated tag
`phase2-research-universe-approved-v1`. The immutable commit and tag target are
reported by the freeze operation and final copy-paste handoff because a commit
cannot embed its own content-derived hash.

## 1. Objective

Review the complete Milestone 3 implementation, correct any genuine contract
defect, execute all repository and container validation, and freeze the approved
research-universe foundation without starting Spark integration, multi-symbol
acquisition or sector analytics.

## 2. Changes Implemented

The review accepted the versioned `study-cohort-v1` contract and pure expected
symbol/session design. `config/research_universe.toml` declares the bounded
2026-09-14 applicability interval as a project research assumption. It carries
unverified listing/entity status, project-defined-unverified sector status,
evidence source, exclusions and limitations while reusing the unchanged mapping in
`config/project.toml`.

`src/labgdd/research/universe.py` defines immutable `ResearchMembership`,
`ResearchUniverse`, `ResearchSession`, `CalendarProvenance`, `ExpectedSeries` and
`ExpectedSeriesPlan` contracts. `generate_expected_series(...)` consumes only the
reference universe, explicit requested sessions and resolved calendar data. It has
no observed-bar input and creates no prices, volumes or synthetic observations.

One fail-closed issue was identified during review: memberships carrying a version
different from the universe authority could previously be validated and then
silently omitted. The generator now rejects mixed membership versions. It also
rejects blank calendar-provenance fields. Regression assertions were added to
`tests/unit/test_research_universe.py`.

`src/labgdd/processing/expected_series.py` remains only a narrow adapter from the
existing XNYS `CalendarSnapshot`; it is not connected to the Silver or Gold
pipelines. ADR 009 was changed from Proposed to Accepted after validation.

## 3. Files Created or Modified

- `config/research_universe.toml`: bounded cohort and evidence declaration.
- `src/labgdd/research/__init__.py`: research-reference package boundary.
- `src/labgdd/research/universe.py`: immutable reference contracts and pure,
  deterministic expected-series generator.
- `src/labgdd/research_config.py`: strict TOML loader tied to the existing project
  taxonomy.
- `src/labgdd/processing/expected_series.py`: independently testable XNYS adapter.
- `tests/unit/test_research_universe.py`: cohort, absence, calendar, determinism and
  fail-closed provenance tests.
- `docs/architecture/RESEARCH_UNIVERSE.md`: expected/observed/completeness and
  scientific-evidence contract.
- `docs/decisions/009-research-universe-and-expected-series.md`: accepted ADR 009.
- `docs/architecture/ARCHITECTURE.md`,
  `docs/architecture/IMPLEMENTATION_PLAN.md`, `README.md`: current bounded capability.
- `docs/handoffs/PHASE_2_MILESTONE_3_RESEARCH_UNIVERSE_HANDOFF.md`: original
  implementation handoff, preserved.
- `docs/handoffs/PHASE_2_MILESTONE_3_FINAL_HANDOFF.md`: this review/freeze record.

## 4. Architecture and Design Decisions

The original six labels and 30 symbols remain exactly owned by
`config/project.toml`; no GICS or invented historical taxonomy was introduced.
Half-open membership intervals intersect explicit XNYS regular sessions, preserving
UTC bounds, membership evidence and calendar/tzdata/schedule provenance.

Requested holidays and weekends remain visible as requested but unscheduled.
Scheduled sessions generate expected identities even when all observations are
absent. A membership beginning or ending within a session is explicit; overlapping
same-symbol/version intervals and contradictory versions fail closed.

Frozen Bronze, Silver and Gold artifacts and publication registries remain
unchanged. No unapproved Spark integration was introduced. Future integration must
convert `ExpectedSeriesPlan.rows` into an expected Spark relation and left-join
canonical observations under a separately reviewed milestone.

## 5. Tests and Validation

Passed:

- `.venv/Scripts/python -m ruff format --check .`: 59 files formatted.
- `.venv/Scripts/python -m ruff check .`: passed.
- `.venv/Scripts/python -m mypy`: strict typing passed for 59 source files.
- `.venv/Scripts/python -m pytest tests/unit/test_research_universe.py`: 8 passed.
- `.venv/Scripts/python -m pytest`: **239 passed in 424.96 seconds**.
- `.venv/Scripts/python -m pip check`: no broken requirements.
- `.venv/Scripts/python scripts/check_repository.py`: passed for 113 candidate files
  before the final handoff was added; rerun at the final staging gate.
- `.venv/Scripts/python -m build --no-isolation`: v0.4.0 sdist and wheel built.
- `scripts/smoke_wheel.py`: clean offline wheel installation, imports, CLIs,
  configuration, ingestion check and dependency check passed.
- Fresh Linux/amd64 image build from pinned base digests and hash-locked batch
  dependencies: passed.
- Silver container smoke: 4 canonical rows, 0 quarantine rows and 4 session fixtures
  covering winter, DST, summer and early close.
- Gold container smoke: 390 returns, 1,556 rolling rows and 120 fixed windows.

Pytest emitted post-success Windows cleanup warnings for a damaged external
temporary path (`WinError 1392`/`145`); the suite exit code was zero and no assertion
failed.

## 6. Issues and Limitations

Historical listing dates, stable entity identifiers, ticker changes, mergers,
corporate actions and official taxonomy-effective dates remain unverified. The
cohort is selected and subject to selection and survivorship bias. XNYS is a common
regular-session benchmark, not proof every symbol was historically listed there.
Provider coverage and correction finality remain separate unproven claims.

Applicability is currently limited to the approved one-day study. The expected
relation is not yet consumed by Silver or Gold, so a wholly absent symbol is not yet
reported by those pipelines. No distributed-scale evidence or sector metric exists.

## 7. Scientific and Data Integrity

No financial formula, measurement version, return definition, temporal contract,
adjustment policy or immutable dataset changed. Expected identities never fill
prices. Future complete-sector calculations must keep missing expected constituents
in coverage denominators and return null when complete membership is not eligible.

The approved AAPL Silver and Gold behavior remained covered by local Spark and
container tests. No Alpaca request or new dataset publication occurred.

## 8. Current Project Status

LABGDD V2 now has frozen Phase 0, historical Bronze, canonical bounded Silver,
individual-company bounded Gold, and the independently validated research-universe
reference. It can deterministically describe expected `study-cohort-v1`
symbol/session identities without observed data. Spark integration, full-universe
processing and sector analytics remain unimplemented.

## 9. Recommended Next Step

After separate approval, define a bounded Spark integration milestone that
left-joins canonical observations to the expected relation and proves the frozen
AAPL outputs remain identical. Do not begin multi-symbol acquisition or sector
measurements automatically.

## 10. Review Questions

None for this freeze. Any Spark expected-series integration remains a separate
architecture decision.

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
