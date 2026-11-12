# LABGDD V2 – Development Handoff

**Phase:** Phase 2, Milestone 3 – Research universe and expected-series foundation
**Task:** Define expected symbol/session membership independently of observed bars
**Status:** Completed
**Date:** 2026-11-03
**Git branch and commit:** `main` at approved Gold baseline `0525c84a8ab72b24cf6340eaac0a898cf1db6eab`; Milestone 3 changes are uncommitted for architecture review

## 1. Objective

Establish a versioned research-cohort contract and deterministic expected
symbol/session relation that does not use observed market bars to decide which
companies or sessions should exist. Preserve the existing six groups, 30 symbols
and `study-cohort-v1` identity while separating project assumptions from verified
historical evidence. Do not calculate sector metrics or alter frozen publications.

## 2. Changes Implemented

`config/research_universe.toml` declares the `study-cohort-v1` reference contract,
the same one-day applicability used by the approved 2026-09-14 Silver study, its
project-assumption basis, evidence source/status, unverified listing/entity status,
project-defined sector status, exclusions and limitations. The loader combines
this metadata with the existing `config/project.toml` taxonomy, avoiding a second
duplicated symbol mapping.

`labgdd.research.universe` adds immutable contracts for `ResearchMembership`,
`ResearchUniverse`, `ResearchSession`, `CalendarProvenance`, `ExpectedSeries` and
`ExpectedSeriesPlan`. `generate_expected_series` accepts only universe, session and
calendar references; it has no observed-data input. It deterministically produces
one expected symbol/session row per membership intersecting the session, preserves
UTC and provenance, identifies partial or changing intra-session membership and
rejects same-version overlaps.

`labgdd.processing.expected_series.generate_xnys_expected_series` is a narrow hook
that converts the existing pinned `CalendarSnapshot` to the pure reference types.
It requires XNYS/America/New_York authority. Weekends/holidays remain requested but
unscheduled, while a scheduled session is generated even if all observed data is
absent. No price, volume, bar or synthetic financial observation is created.

## 3. Files Created or Modified

- `config/research_universe.toml`: cohort applicability/evidence declaration.
- `src/labgdd/research/universe.py`: pure contracts, validation and generator.
- `src/labgdd/research/__init__.py`: research-reference package boundary.
- `src/labgdd/research_config.py`: strict configuration loader.
- `src/labgdd/processing/expected_series.py`: narrow XNYS snapshot adapter.
- `tests/unit/test_research_universe.py`: cohort, absence, membership and calendar tests.
- `docs/architecture/RESEARCH_UNIVERSE.md`: scientific and integration contract.
- `docs/decisions/009-research-universe-and-expected-series.md`: proposed ADR 009.
- `docs/architecture/ARCHITECTURE.md`, `docs/architecture/IMPLEMENTATION_PLAN.md`,
  `README.md`: accurate current capability and scope.
- This handoff.

No Bronze, Silver or Gold manifest, artifact, registry or financial function was
modified.

## 4. Architecture and Design Decisions

The project taxonomy remains `study-cohort-v1`; no GICS substitution or invented
history was introduced. The declared interval is 2026-09-14 UTC only, matching the
approved bounded evidence. It is explicitly not a listing date, permanent cohort
period, official classification interval or corporate-action history.

The pure generator is reusable without Spark, network or filesystem access. The
calendar integration is separate because calendar libraries remain adapters. A
membership active at open but ending before close is `partial_session`; a later
adjacent interval beginning during the session creates
`membership_change_during_session` with explicit boundary instants. Overlapping
same-symbol/version definitions fail closed.

Frozen pipelines are intentionally not rewired. Silver still uses its existing
request-scoped memberships, and Gold still derives slots from canonical series
metadata for its approved bounded run. The next integration boundary is to convert
`ExpectedSeriesPlan.rows` into a Spark expected relation and left-join observations.
That migration needs separate approval and must prove the approved AAPL result is
unchanged. Future sector consumers must use expected membership for denominators;
missing or ineligible expected constituents make complete-membership statistics
null.

## 5. Tests and Validation

Passed:

- `python -m ruff format --check .` – 59 files formatted.
- `python -m ruff check .` – passed.
- `python -m mypy` – strict typing passed for 59 source files.
- `python -m pytest` – **239 passed in 4250.46 seconds**.
- Focused research/config/calendar/architecture tests – 52 passed.
- `python -m pip check` – no broken requirements.
- `python scripts/check_repository.py` – passed for 113 candidates including this handoff.
- `python -m build --no-isolation` – v0.4.0 sdist and wheel built.
- Clean wheel installation/imports/CLIs/dependency check – passed.

The new eight-test suite verifies 30 symbols, six groups, variable constituent
counts, completely absent observed symbols/sessions, membership effective bounds,
intra-session changes, overlapping contradictions, XNYS holiday behavior, DST,
early close, deterministic ordering and calendar/version provenance.

The real 2026-09-14 reference produced 30 rows across six groups. AAPL retained
13:30–20:00 UTC, `declared_research_assumption` and unverified listing status.
Independent revalidation of approved Gold remained 390 return rows, 389 eligible
adjacent returns, 1,556 rolling rows, 1,450 eligible rolling rows, 120 fixed windows
and zero temporal/key mismatches.

Docker was not rerun because no Spark transformation, publisher, container or
runtime dependency changed. The complete local suite still executed all existing
Spark Gold and Silver integration tests. Pytest emitted post-success cleanup warnings
for a damaged/nonempty external Windows temporary directory; no assertion failed.

## 6. Issues and Limitations

Historical listing, entity, sector-effective-date, ticker-change, merger and
corporate-action evidence remains unverified. The cohort is selected and subject to
selection/survivorship bias. XNYS is a common-session benchmark, not proof every
symbol was historically listed there. IEX/source correction completeness remains a
separate unproven claim.

The current declared interval covers only the approved study day. Full-cohort or
multi-period work requires reviewed applicability evidence or an explicitly labeled
new research assumption. The expected relation is not yet a Silver/Gold Spark input;
therefore existing pipelines cannot yet report a wholly absent full-cohort symbol.

## 7. Scientific and Data Integrity

No financial formula, temporal definition, measurement version, adjustment policy
or immutable dataset changed. Expected membership is now explicitly distinct from
observed provider data, valid canonical observations, eligible return windows,
complete future sector windows and source/correction finality. Expected rows never
manufacture prices. A missing expected member must remain in future expected counts
and force complete-sector measurements to null rather than disappearing from the
denominator.

## 8. Current Project Status

The project has frozen bounded Bronze, Silver and individual-company Gold evidence,
plus an independently tested research-universe and expected-series reference. It can
generate deterministic expected rows for explicitly requested XNYS sessions without
observed bars. It cannot yet process the complete cohort through acquisition or
Gold, and no sector financial metric exists.

## 9. Recommended Next Step

Review ADR 009 and the reference contract. If approved, define a separate bounded
integration milestone that converts the expected plan into a Spark relation and
left-joins canonical data while proving frozen AAPL outputs remain identical. Do
not begin multi-symbol acquisition or sector calculations automatically.

## 10. Review Questions

1. Approve the one-day declared applicability as the correct initial scope for
   `study-cohort-v1` expected-series integration?
2. Approve `ExpectedSeriesPlan` as the future Spark expected-relation boundary?

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
