# LABGDD V2

Big Data Pipeline for the Analysis of Emergent Sector Volatility in Financial
Markets. Independent Master's-level Data Engineering research foundation.

Phase 0 is frozen at `phase0-approved-v1` (commit
`81be7794ffa4c197e866f3fc89d147287abb3731`). Phase 1 milestone 1 adds bounded
Alpaca historical REST acquisition, XNYS calendar snapshots and immutable Bronze
Parquet persistence, including one reviewed live AAPL session. No broad acquisition
campaign was run. Phase 2 adds bounded Spark Batch Bronze-to-Silver processing
and v2.0.0 individual-company Gold returns/volatility for explicitly approved
Silver evidence. Sector analytics and later infrastructure are not started.
Milestone 3 defines the versioned research cohort and expected XNYS symbol/session
relation independently of observed bars. Milestone 4 adds a bounded optional Spark
left-join path. Milestone 5 publishes and independently approves that path under a
separate immutable dataset without replacing the frozen individual Gold authority.
Milestone 6 validates a five-symbol end-to-end pilot under an accepted bounded
candidate authority. Milestone 7 independently reviews and explicitly publishes its
Bronze, Silver, Individual Gold and Expected-Series Gold evidence through exact
multi-authority registries. Milestone 8 adds synthetic-only scalar and native Spark
Sector Gold calculation foundations with explicit complete-population semantics; it
does not publish or calculate real sector results.
The supplied reference audit is preserved byte-for-byte.

Publication selection is explicit through `config/bronze_publications.toml`,
`config/silver_publications.toml` and `config/gold_publications.toml`; unregistered,
failed or superseded directories are never selected by recency. Silver and Gold
registry v1.1 support multiple explicit dataset/run authorities, and Silver remains
able to read the original v1.0 singleton contract.

## Setup (PowerShell, Python 3.11)

```powershell
python -m venv .venv
.venv/Scripts/python -m pip install --require-hashes -r requirements-dev.lock
.venv/Scripts/python -m pip install --no-build-isolation --no-deps -e .
.venv/Scripts/labgdd --config config/project.toml
```

Linux: replace `.venv/Scripts/python` with `.venv/bin/python` and the CLI path
likewise. No activation is necessary. Initial verified interpreter: CPython 3.11.9;
see `.python-version` and the lock policy in ADR 002. The base package has zero mandatory runtime
dependencies; the locked development environment includes optional ingestion and
Spark Batch dependencies. A wheel installation does not include the repository's configuration;
pass an explicit path when running from another directory.

## Checks

```powershell
.venv/Scripts/python -m ruff format --check .
.venv/Scripts/python -m ruff check .
.venv/Scripts/python -m mypy
.venv/Scripts/python -m pytest
.venv/Scripts/python -m pip check
.venv/Scripts/python -m build --no-isolation
.venv/Scripts/python scripts/smoke_wheel.py dist/labgdd_v2-0.4.0-py3-none-any.whl --config config/project.toml
```

The ignore/tracking tests use Git. If this is a downloaded folder without `.git`,
run `git init` once in this new project. No commit, remote or legacy repository
is required. Tests exercise immutable bars, invalid prices/OHLC/volume/time,
version/revision metadata, variable membership, path ownership, configuration
failures, logging payload suppression, CLI status, domain dependencies, audit
integrity and secret-path ignore rules.

Configuration is `config/project.toml`: paths resolve relative to that file;
storage must remain inside the project's dedicated `data/` directory. Loading
does not create storage directories. No automatic `.env` loading occurs. `.env.example` is a
credential-free reference for ingestion. Keep actual secrets in the process
environment; never stage `.env` or force-add ignored credentials. Logging emits
allowlisted events with UTC timestamps and run UUIDs; it drops arbitrary messages,
exception text and extra fields by design. Extend the event schema deliberately.

## Design and next work

- [Development workflow and mandatory handoff](AGENTS.md)
- [Task and phase handoffs](docs/handoffs/)
- [Architecture and risks](docs/architecture/ARCHITECTURE.md)
- [Data and temporal contracts](docs/architecture/DATA_CONTRACTS.md)
- [Versioned measurement specification](docs/architecture/MEASUREMENTS.md)
- [Phased acceptance plan](docs/architecture/IMPLEMENTATION_PLAN.md)
- [Architecture decisions](docs/decisions/)
- [Executed validation report](docs/architecture/PHASE0_VALIDATION.md)
- [Phase 2 individual-company Gold](docs/architecture/PHASE2_GOLD.md)
- [Research universe and expected-series contract](docs/architecture/RESEARCH_UNIVERSE.md)
- [Bounded Spark expected-series integration](docs/architecture/PHASE2_EXPECTED_SERIES_SPARK.md)
- [Pending multi-symbol validation](docs/architecture/PHASE2_MULTISYMBOL_VALIDATION.md)
- [Milestone 6 publication migration](docs/architecture/MILESTONE6_PUBLICATION_MIGRATION.md)
- [Sector Gold calculation foundation](docs/architecture/PHASE2_SECTOR_GOLD.md)
- [Proposed expected-series Gold publication decision](docs/decisions/011-expected-series-gold-publication.md)
- [Phase 0 architecture-review remediation](docs/handoffs/TASK_PHASE0_ARCHITECTURE_REVIEW_HANDOFF.md)

Implemented packages include domain, configuration, observability, adapters,
acquisition orchestration, native Spark processing, individual-company analytics,
synthetic Sector Gold references and runners. Real Sector Gold publication,
research/serving and deployment remain future work.
The initial six-sector/30-symbol mapping is a study cohort; historical membership
and calendar applicability must be established before sector-enriched analysis.

The architecture review added a pure consecutive-close window validator and
independent scalar arithmetic fixtures. Phase 1 adds the calendar adapter;
production financial analytics now cover only the bounded individual-company Gold
measurements described in `PHASE2_GOLD.md`.
Measurement specification v2.0.0 distinguishes the exact rebalanced portfolio
from a mean-log-return proxy. Raw IEX is the initial adjustment policy.
`allowed_lateness_minutes` (default 10) is validated configuration for future
processing, not a completeness guarantee or an active finalizer. ADR 005 selects
exchange_calendars/XNYS. Phase 1 tests generate real provider schedules and
verify offsets against pinned tzdata; original domain fixtures remain independent.

Refresh tooling intentionally: edit `scripts/lock_tools.py`'s direct pins, run it
with network access, review the resulting transitive version/hash changes, install
the lock in a fresh environment and rerun checks. Existing lock versions are
retained unless the script is invoked with `--upgrade`. CI tests Windows and Linux.
No old datasets, environments, output paths or Docker volumes are reused.

## Spark Batch Silver (Phase 2 Milestone 1)

Java 21 and the hash-locked development environment are required. Validate the
configured immutable Bronze input without starting Spark, then run the bounded job:

```powershell
.venv/Scripts/python -m labgdd.runners.silver --check
.venv/Scripts/labgdd-silver --config config/project.toml --request config/silver.toml
```

The checked-in request is restricted to the approved AAPL session and declares a
one-day research-cohort applicability interval; it is not historical listing or
industry evidence. Outputs are immutable staged Silver runs with canonical,
quarantine, duplicate evidence and a terminal manifest. The supported native
Parquet runtime is the pinned non-root Linux image; no unofficial Windows Hadoop
binary is used. Exact build, synthetic-smoke and bounded real-run PowerShell
commands are in `docs/architecture/PHASE2_BATCH.md`. This proves local Spark
correctness, not multi-node scale or object-store transactionality.
Downstream code must resolve an explicitly requested approved run through
`config/silver_publications.toml`; it must never select the newest directory.

## Historical acquisition (Phase 1 milestone 1)

The setup above installs the full hash-locked test/ingestion environment. For a
standalone wheel, install `labgdd-v2[ingestion]` with the reviewed lock/constraints;
base installation supports configuration checks without the optional libraries.

```powershell
.venv/Scripts/python -m labgdd.runners.ingest --check
.venv/Scripts/python scripts/check_repository.py
```

Review `config/historical.toml` before a deliberately invoked acquisition. It is a
six-minute, two-symbol example with explicit raw IEX/1Min and UTC `[start,end)`.
Actual acquisition requires process environment variables `ALPACA_API_KEY` and
`ALPACA_API_SECRET`; `.env` is never loaded automatically. Once configured, run:

```powershell
.venv/Scripts/labgdd-ingest --config config/project.toml --request config/historical.toml
```

This command contacts Alpaca; it was not run against the live service during this
milestone. Failures return nonzero status without printing response bodies or
credentials. Logs identify the run UUID. Inspect
`data/bronze/dataset=<dataset-version>/acquisition=<run-UUID>/manifest.json` for
terminal status, coverage, calendar provenance and artifact checksums. `complete`
means pagination exhausted, not that every expected market observation exists.
Invalid/duplicate observations and original response bytes remain archived.
No completion manifest means the run is incomplete; existing outputs are never
silently overwritten or automatically deleted/resumed.

XNYS is a common-session benchmark, not proof of each security's historical
listing or sector membership. That limitation is explicit in metadata; do not
interpret Bronze as historical sector-enriched data. Local storage requires
hard-link support for atomic terminal-manifest publication.

- [Phase 1 ingestion contract and limitations](docs/architecture/PHASE1_INGESTION.md)
- [Phase 1 development handoff](docs/handoffs/PHASE_1_HANDOFF.md)
