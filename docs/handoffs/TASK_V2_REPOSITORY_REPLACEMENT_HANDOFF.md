# LABGDD V2 – Development Handoff

**Phase:** Repository migration; no new development phase

**Task:** Commit the copied LABGDD_NEW contents as the current LABGDD project

**Status:** Completed

**Date:** 2026-11-12

**Git branch and commit:** `main`; replacement commit titled `chore: replace legacy project with LABGDD V2`, parent `223ec8fa7e68e6c2f0b678032e206ca45ee84684`. The replacement commit includes this document; its resulting identifier is available from `git log -1`.

## 1. Objective

Make the V2 files already copied by the user from LABGDD_NEW into `C:\Users\Deus\Documents\LABGDD` the committed working baseline of the existing repository. Remove the legacy project from the current snapshot without rewriting Git history.

## 2. Changes Implemented

Staged the existing V2 source, configuration, tests, dependency locks, container definitions, CI workflow and documentation, together with the legacy deletions already present in the working directory. No application implementation was authored or refactored during this task.

Removed `.env` and `data/` from the Git index while retaining local files. The copied `.gitignore` excludes credentials, datasets, environments, build outputs and caches. The existing `scripts/check_repository.py` scanner verified staged content without displaying credential values. The final snapshot contains 155 tracked files, including this handoff, with no legacy source paths outside `src/labgdd/`. The replacement stages 30,738 deletions relative to the old parent commit, largely legacy generated evidence and artifacts.

## 3. Files Created or Modified

- `src/labgdd/`: copied V2 domain, adapters, processing, research, publication governance and CLI implementation.
- `config/`: copied explicit project, acquisition, research-universe and publication configurations.
- `tests/unit/` and `tests/integration/`: copied V2 validation suites.
- `pyproject.toml` and `requirements-*.lock`: copied package version 0.4.0 and locked environments.
- `.github/workflows/checks.yml`, `docker/`, and `scripts/`: copied automation, container and validation tooling.
- `README.md`, `AGENTS.md`, and `docs/`: copied V2 documentation and development instructions.
- `.gitignore` and `.gitattributes`: copied exclusion rules and byte-preservation rule for the reference audit.
- `docs/handoffs/TASK_V2_REPOSITORY_REPLACEMENT_HANDOFF.md`: newly written migration report.
- `.env` and `data/`: removed from version control, not deleted locally.
- Legacy source, tests, configurations, reports, datasets and model artifacts: recorded existing removals.

## 4. Architecture and Design Decisions

Used a normal replacement commit on `main`, preserving repository history. This is a transition to the copied V2 implementation, not a claim of backward compatibility with the legacy project. No dependencies, financial formulas, publication authorities or runtime interfaces were changed beyond the files already supplied by the user. The copied CLI interfaces include `labgdd`, `labgdd-ingest`, `labgdd-silver`, `labgdd-gold`, `labgdd-expected-gold`, and `labgdd-publications`.

## 5. Tests and Validation

Executed in the current Windows workspace:

- **Passed:** `python scripts/check_repository.py --staged` — 154 index files before adding this handoff; final scan covers 155.
- **Passed:** `python scripts/check_repository.py` — 154 candidate files before this handoff.
- **Passed:** `.venv/Scripts/python -m ruff format --check .` — 72 files formatted.
- **Passed:** `.venv/Scripts/python -m ruff check .`.
- **Passed:** `.venv/Scripts/python -m mypy` — 72 source files.
- **Passed:** `.venv/Scripts/python -m pytest -q tests/unit` — 207 passed in 9.92 seconds.
- **Incomplete / stopped:** `.venv/Scripts/python -m pytest -q` — stopped during lengthy Spark integration execution; no complete-suite result is claimed.
- **Passed:** `.venv/Scripts/python -m pip check`.
- **Passed:** `.venv/Scripts/python -m build --no-isolation` — source distribution and version 0.4.0 wheel built.
- **Passed:** `.venv/Scripts/python scripts/smoke_wheel.py dist/labgdd_v2-0.4.0-py3-none-any.whl --config config/project.toml` — offline clean installation, imports, CLIs and dependency consistency.
- **Passed:** `.venv/Scripts/python -m labgdd.runners.check --config config/project.toml` and `.venv/Scripts/python -m labgdd.runners.ingest --check`.
- **Failed:** `git diff --cached --check` — 745 inherited whitespace findings in copied Markdown, including CRLF reference evidence and intentional Markdown hard breaks. Historical documents were preserved.
- **Passed:** `git -c core.whitespace=cr-at-eol,-blank-at-eol diff --cached --check`.
- **Passed:** index inspection found no legacy source paths; `.env` is ignored and remains locally present.
- **Not executed:** remote CI, live acquisition, complete Spark validation or a push.

## 6. Issues and Limitations

The copied CI workflow refers to wheel version 0.2.0, whereas `pyproject.toml` builds 0.4.0. This existing mismatch needs a separate correction before expecting remote CI to pass. Full Spark integration validation remains incomplete. Copy completeness against the original LABGDD_NEW directory was not independently checked.

The previous commit contains nonempty credential assignments in `.env`. Removing the file from the new snapshot does not remove those historical values; rotate the affected credentials. No history rewrite or remote push was performed. No credentials are included in this handoff.

## 7. Scientific and Data Integrity

No scientific or data semantics were edited during this migration. Legacy-to-V2 scientific equivalence is not asserted. Local datasets remain outside version control; a fresh clone alone does not reproduce previously acquired evidence. The reference audit checksum passed the existing scanner.

## 8. Current Project Status

V2 is the committed working baseline. Unit tests, static checks, dependency consistency, packaging and CLI smoke behavior were verified locally. End-to-end Spark processing and copied historical experiment results were not revalidated. Documentation describes bounded Bronze/Silver/Gold capabilities; this task does not approve new datasets or start sector analytics.

## 9. Recommended Next Step

Correct the existing CI wheel-version mismatch and complete Spark integration validation before publishing or starting another development milestone. Neither action is automatically started here.

## 10. Review Questions

None for this replacement commit. Any future deletion of historical commits requires separate authorization.

**Review status:** NEEDS ATTENTION

**Next phase:** NOT STARTED
