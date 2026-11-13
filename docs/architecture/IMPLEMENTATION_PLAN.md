# Implementation and acceptance plan

Phase 0 is approved and frozen at `phase0-approved-v1`. Phase 1 milestone 1 is
authorized and implemented for review; see PHASE1_INGESTION.md and the Phase 1
handoff for executed evidence. Phase 2 Milestone 1 is implemented for review. No phase is accepted
based solely on documentation or historical tests.

| Phase | Deliverable | Independent acceptance and automated checks |
|---|---|---|
| 0 Foundation | Installable pure-Python domain/config/logging/CLI; contracts and ADRs | Clean wheel install/import, Ruff format/lint, strict mypy, unit tests, dependency guard, secret-path prevention, preserved audit hash; explicit-bound temporal and independent scalar arithmetic fixtures |
| 1 Historical ingestion | REST pagination/retries; immutable Bronze Parquet and acquisition manifests | Mock HTTP tests: multi-page, repeated token, 429 Retry-After, bounded retries/timeouts, malformed payload, partial failure; atomic write/restart/hash tests; opt-in authenticated smoke test with redacted credentials |
| 2 Spark Batch | Silver, bounded individual/expected Gold and Sector Gold foundation | Native Linux Silver/Gold publication, multi-dataset authority and v2.0.0 individual measurements are approved for bounded evidence. Synthetic-only scalar/native-Spark Sector Gold calculations preserve complete expected populations and exact parity; no real sector result or Sector Gold publication exists. |
| 3 Kafka/Streaming | WebSocket original/updated bars, retained events, replay and checkpoints | Protocol/correction fixtures, reconnect/backpressure, out-of-order and duplicate replay, restart with coupled checkpoint/output IDs, late-event cutoff, no-progress finalization; exact Batch/Streaming keys/nulls/counts and declared float tolerance |
| 4 Observability | Direct metrics, live heartbeat, Prometheus/Grafana | Collector tests, service crash/stale heartbeat detection, missing telemetry distinguished from zero, run identity/lag checks; historical artifacts do not masquerade as live health |
| 5 ML | Dataset/features, statistical and classical baselines; separate inference | Chronological split/purge tests, train-only preprocessing, horizon/as-of checks, seeded repeatability, frozen selection, prediction/evaluation persistence and interrupted-stage recovery/fault injection |
| 6 Serving | Requirement-driven finalized serving and Power BI model | Keys/revisions/migration and idempotent publication tests, null/coverage correctness, totals reconciled to Gold, report refresh/relationships verified |
| 7 Benchmarking | Reproducible correctness/recovery/scale experiments | Seeded workload and manifest, warm-up separated, repeated runs, throughput/event and processing latency, resource/state growth, failure recovery and increasing universe/skew; no extrapolation beyond tested hardware |

## Phase 1 entry gates

Review clarified v1 bar/time contracts and v2 measurement definitions. Integrate
and pin the proposed exchange_calendars/XNYS provider (ADR 005), validate explicit
study-date applicability rather than inventing historical membership dates, and
verify authenticated IEX entitlements. Initial adjustment policy is explicitly raw.
Select/pin the HTTP and Parquet
libraries with compatibility evidence. Define acquisition interval semantics and
page identity, bounded retry budgets and disk ownership before coding persistence.
Do not acquire or convert any legacy dataset without a reviewed import scope.

## Remaining policy acceptance fixtures

Phase 1: generated calendar UTC grids must match reviewed holidays/weekends,
both DST changes, 13:00 early closes, historical exceptions and supported bounds;
persist calendar/tzdb/schedule provenance. Confirm raw request parameters and
entity/symbol-as-of mapping. Preserve repeated-acquisition identity and raw bytes.

Phase 2: correction resolver must cover identical cross-transport payloads,
conflicting REST snapshots, original/updated bars, comparable revision namespaces,
same-ID corruption, chain forks/cycles/dangling links and order-independent null
quarantine. Financial functions must map the scalar fixtures to actual outputs,
including exact portfolio versus mean-log proxy, missing constituents, N=1,
undefined HHI/correlation, threshold boundaries and calibration-ID separation.

Phase 3: implement the distinct watermark, lateness, capture cutoff and bounded
completion policies in DATA_CONTRACTS.md. Test idle/uninitialized required inputs,
no synthetic finalization bars, empty terminal partitions, correction cutoffs,
new publication revisions and exact accepted-input parity. None of these future
engine behaviors is claimed by Phase 0's pure-domain tests.

## Environment policy

The development lock targets Python 3.11 on Windows/Linux. Phase 2 additionally
has a minimal linux/amd64 batch lock and digest-pinned Docker image for local
Spark 4.3.0/Java 21 correctness, with distinct read-only Bronze, writable Silver
and temporary paths. Kafka connectors, cluster execution and orchestration remain
future reviewed work. New tests appear with actual capabilities rather than empty
suite directories.
