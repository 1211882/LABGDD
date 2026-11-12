### LABGDD V2 – Development Handoff

**Phase:** Phase 0 freeze and Phase 1 milestone 1  
**Task:** Freeze the approved foundation; implement bounded historical acquisition and immutable local Bronze  
**Status:** Completed for the first Phase 1 milestone; ready for review  
**Date:** 2026-11-03 (Europe/Lisbon)  
**Git branch and commit:** `main`; Phase 1 implementation `38f566dab9b9d427bcef2fb0398c145cab63fb92`. This handoff is committed separately after implementation. Phase 0 baseline `81be7794ffa4c197e866f3fc89d147287abb3731`, annotated tag `phase0-approved-v1`.

#### 1. Objective

Resume the interrupted freeze without treating its false positive as evidence of leaked credentials. Inspect baseline candidates, correct/test the scanner, validate and commit/tag Phase 0, then implement the authorized small Phase 1 milestone using mocked Alpaca responses. No live acquisition campaign or Phase 2 work was performed.

#### 2. Changes Implemented

**Freeze:** `scripts/check_repository.py` scans line by line with horizontal whitespace only, checks safe/unsafe synthetic fixtures, empty credential templates, forbidden paths, index contents and audit integrity without printing values. All baseline validation passed, 45 approved files were committed, the annotated tag was created and the working tree was confirmed clean before Phase 1.

**Acquisition:** `HistoricalRequest`/`load_historical_request` validate symbols against the configured cohort, UTC minute-aligned `[start,end)` bounds, a 31-day milestone limit, explicit raw IEX/1Min and bounded timeout/attempt/page settings. `AlpacaREST` obtains authentication through `Credentials.from_environment`, uses fixed-host HTTPS with redirects rejected, paginates through orchestration, and bounds retries for 429/transient HTTP/transport failures. Retry-After supports seconds/dates; invalid or excessive delays fail safely. Response size is capped at 16 MiB.

**Calendar:** `xnys_snapshot` generates explicit-date schedules and stores provider, pinned timezone and schedule fingerprints. Actual generated fixtures cover holidays, both DST transitions, early close and regular-session endpoints. Configured cohort membership is checked; historical listing/industry membership is explicitly unverified.

**Bronze:** `acquire_historical` captures exact response bytes in Parquet before JSON/envelope/domain validation, then archives every observation projection with validity/reason/session classification. Invalid rows, duplicate observations and conflicting values are retained. Diagnostics do not select revisions. Repeated tokens/pages and page limits fail with preserved partial evidence.

`BronzeAcquisition` exclusively creates a UUID directory and artifacts, flushes writes and atomically publishes a no-replace terminal manifest using a hard link. Manifests contain parameters, UTC times, versions/source fingerprint, calendar provenance, coverage, counts, controlled failure codes and artifact sizes/SHA-256. `complete` means exhausted pagination, not complete market data. Failed acquisitions retain prior pages; absent terminal manifests indicate incomplete execution.

#### 3. Files Created or Modified

Phase 0 additions: `.gitattributes`, `scripts/check_repository.py`, `tests/unit/test_repository_checks.py`, `docs/handoffs/PHASE_0_HANDOFF.md`. Existing approved foundation files formed the initial commit; historical evidence was not edited.

Phase 1 created:

- `src/labgdd/historical_config.py`, `config/historical.toml` — validated request and six-minute/two-symbol example.
- `src/labgdd/adapters/__init__.py`, `src/labgdd/adapters/alpaca.py`, `src/labgdd/adapters/calendar.py`, `src/labgdd/adapters/bronze.py` — transport, calendar and storage boundaries.
- `src/labgdd/processing/__init__.py`, `src/labgdd/processing/historical.py` — raw-first acquisition orchestration and identity/validation helpers.
- `src/labgdd/runners/ingest.py` — `labgdd-ingest`, including credential-free/network-free `--check`.
- `tests/unit/test_historical_adapter.py`, `tests/unit/test_calendar_adapter.py`, `tests/integration/test_historical_acquisition.py` — adapter/calendar and real-Parquet mocked integration tests.
- `docs/architecture/PHASE1_INGESTION.md`, `docs/decisions/006-bronze-acquisition-publication.md`, `docs/handoffs/PHASE_1_HANDOFF.md` — detailed contracts, publication ADR and this report.

Phase 1 modified `pyproject.toml`, `requirements-dev.lock`, `scripts/lock_tools.py`, `scripts/smoke_wheel.py`, `.github/workflows/checks.yml`, `.env.example`, `README.md`, `src/labgdd/observability/logging.py`, `docs/architecture/ARCHITECTURE.md`, `docs/architecture/DATA_CONTRACTS.md`, `docs/architecture/IMPLEMENTATION_PLAN.md`, `docs/decisions/002-environment-and-configuration.md` and `docs/decisions/005-calendar-and-raw-price-policy.md` for optional dependencies, safe events, installation checks and current architecture status.

#### 4. Architecture and Design Decisions

Preserved the modular monolith and standard-library domain. Package version advances to 0.2.0; base imports retain zero mandatory dependencies. Optional `ingestion` pins exchange_calendars 4.11.2, PyArrow 21.0.0, NumPy 2.3.4, pandas 2.3.3 and tzdata 2025.2, with transitive hashes in the development lock. No SDK or HTTP framework was required.

Map project exclusive end to the provider's inclusive end minus one microsecond; preserve explicit feed/adjustment/as-of parameters. The provider documents inclusive bounds and continuation tokens in [Alpaca historical bars](https://docs.alpaca.markets/us/reference/stockbars).

Keep exact response bytes authoritative; readable observation JSON converts Decimal values to strings. Observation identities hash versioned length-prefixed acquisition/page/symbol/row/body-digest locators, not arrival order or maximum prices. Independent acquisitions get new identities; original stored locators reproduce IDs. No source revision ordering is invented.

Local atomic publication requires hard-link support. Cleanup failure after successful publication may leave a harmless pending alias but cannot misreport a committed acquisition as uncommitted. Object storage, automatic resume and power-loss durability are outside this milestone.

#### 5. Tests and Validation

Executed on Windows, CPython 3.11.9. Unless another interpreter is shown, commands use `.venv/Scripts/python`:

| Executed command | Result |
|---|---|
| `scripts/check_repository.py` and `scripts/check_repository.py --staged` | Passed candidate/index checks; no credentials/local artifacts staged; audit checksum preserved |
| `-m ruff format --check .` | Passed; Phase 1 has 30 Python files |
| `-m ruff check .` | Passed |
| `-m mypy` | Passed strict local typing; 30 files |
| `-m pytest` | Phase 0: 128 passed; final Phase 1: 194 passed, no skips, 4.67 seconds |
| `-m pip check` | Passed |
| `-m pip install --no-build-isolation --no-deps -e '.[ingestion]'` | Passed |
| `-m build --no-isolation` | Passed; sdist and wheel built from sdist |
| `scripts/smoke_wheel.py dist/labgdd_v2-0.2.0-py3-none-any.whl --config config/project.toml` | Passed offline clean base-wheel imports, module/console CLI and ingestion `--check` without optional dependencies |
| `-m labgdd.runners.ingest --check` | Passed; no credentials/network/storage creation |
| `-m pip download --disable-pip-version-check --require-hashes --only-binary=:all: --dest .venv/phase1-wheels -r requirements-dev.lock` | Passed hash-verified wheel download |
| `.venv-phase1/Scripts/python -m pip install --disable-pip-version-check --no-index --find-links .venv/phase1-wheels --require-hashes -r requirements-dev.lock` | Passed fresh offline locked installation |
| `.venv-phase1/Scripts/python -m pip install --force-reinstall --no-index --no-deps dist/labgdd_v2-0.2.0-py3-none-any.whl` | Passed |
| `.venv-phase1/Scripts/python -m pytest` | 194 passed against installed wheel; no skips, 4.95 seconds |
| `.venv-phase1/Scripts/python -m pip check` and `-m labgdd.runners.ingest --check` | Passed |

Phase 1 adds 66 tests: 35 mocked acquisition/persistence integration, 20 REST/auth/retry cases and 11 generated-calendar/applicability cases. Coverage includes missing minutes, invalid OHLCV, duplicate JSON keys/observations/pages, partial failure, checksums, immutable outputs and terminal-publication failures.

Resolved development failures: scanner matching of quoted synthetic fixtures; Git staging normalized audit line endings until file-specific `-text` attributes/restaging preserved original bytes; strict mypy caught a redirect-method annotation and unused suppression, both corrected. No failure was bypassed. Final checks passed. Exact staged lists/diffs were reviewed, and the implementation tree was clean after each commit. Linux/remote CI, authenticated Alpaca behavior and live acquisition were not executed.

#### 6. Issues and Limitations

XNYS is a common-session benchmark, not verified listing/sector history for all 30 securities. Historical applicability and IEX entitlements remain explicit future validation gates before scientific sector-enriched use. Mocked provider behavior does not establish live subscription access or production reliability. Memory tracks observed logical keys within bounded requests; no scalability benchmark is claimed. Interrupted-run recovery, correction resolution and object-store publication remain unimplemented. The named-assignment scanner is not an exhaustive detector of every secret encoding.

#### 7. Scientific and Data Integrity

Bar schema 1.0.0, temporal contracts, measurement specification 2.0.0 and exact-portfolio/proxy separation are unchanged. Raw IEX remains explicit; no implicit filling, source adjustment, deduplication or revision selection occurs. Source completeness is always recorded as unproven, independently of pagination completion. No legacy source/datasets/models or experiments were imported or modified. Audit SHA-256 remains `553d34b93258fe35bd061956b616f30c72e3c8759419a4cfd849920d071eb069`; baseline commit/tag were not amended or moved.

#### 8. Current Project Status

Verified: immutable Phase 0 baseline; installable Phase 1 adapter/configuration/calendar/Bronze/CLI; mocked end-to-end Parquet acquisition and failure behavior; clean base and optional-environment installations. No live market request, large acquisition, Silver, Spark, Kafka, analytics engine, ML, PostgreSQL, Prometheus/Grafana or Power BI was implemented or run.

#### 9. Recommended Next Step

Review this first Phase 1 milestone, especially terminal-state meanings, local publication assumptions and historical applicability. Only after explicit review should further ingestion validation or a tiny authenticated smoke acquisition be authorized. Do not begin Phase 2 automatically.

#### 10. Review Questions

- Accept `complete` as pagination exhaustion with separate missing/invalid/conflict coverage, and the local hard-link publication protocol?
- Which authoritative listing/entity reference and study interval should establish historical cohort applicability before sector-enriched analysis?

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
