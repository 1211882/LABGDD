# Phase 2 Milestone 1: Spark Batch and canonical Silver

Date: 2026-11-03. Scope is Bronze-to-Silver only. Phase 0 remains frozen and all
Phase 1 Bronze evidence is immutable.

## Pipeline and contracts

`silver_config.load_silver_request` validates V2-owned Bronze paths, output and
policy versions, and a half-open membership interval. `verify_bronze_manifest`
checks terminal status, exact inventory, sizes, SHA-256, observation schema and
calendar-document equality before Spark starts.

`transform_observations` performs fixed-schema reading; raw timestamp/OHLCV
normalization; raw numeric precision, scale, magnitude and no-rounding checks;
XNYS and membership enrichment; identity/duplicate/conflict classification;
conservative resolution; and expected-minute generation. Same-session calendar
provenance must agree across acquisitions. Expected minutes are de-duplicated
across overlapping inputs. Unknown/ineligible symbols cannot enter canonical.

Canonical records retain UTC event/bar-end timestamps, decimal128(26,8) OHLC,
int64 volume, source/feed/timeframe/adjustment, selected and contributing IDs,
membership applicability, session/calendar provenance, quality status, complete
input lineage, and schema/policy/dataset versions. No correction order is invented.
Missing minutes are metrics rather than synthetic bars. Provider completeness is
`unproven`.

## Staged native Spark publication

The CLI uses only native Spark DataFrame writes. Canonical uses Hive partitions
`session_date=<date>/feed=<feed>` and a `maxRecordsPerFile` cap; quarantine and
duplicate evidence use independent Parquet datasets. No result DataFrame is
collected into Python. Global output ordering is intentionally absent.

Each run begins under `_staging/run=<UUID>`. Before publication, the orchestrator
validates the exact top-level inventory, physical files, row counts, Spark logical
schemas, distributed decoded-value equality, and explicit PyArrow physical types.
All timestamps must be microsecond UTC Parquet values. The adapter then hashes
every artifact, writes the terminal manifest, and renames the directory to the
completed or failed namespace. The manifest is never considered complete while
the directory remains in staging. This same-filesystem protocol is not an
object-store transaction and does not guarantee crash cleanup.

Completion is not downstream authority. Gold or any other consumer must request a
specific dataset and run UUID through `resolve_approved_silver`. The external
registry `config/silver_publications.toml` identifies the sole approved run and
pins its manifest, request and source fingerprints. Directory modification time,
lexical order and discovery of a newer `run=<UUID>` are never selection rules.

Cached result frames are unpersisted in `finally`. Failed manifests contain only
controlled stage and exception-class diagnostics; payloads and exception text are
not logged. The bounded PyArrow path exists only for Windows tests, attaches UTC
explicitly, and cannot be silently selected by the runner.

## Supported Linux runtime (PowerShell)

The build pins both base images by linux/amd64 manifest digest and installs only
hash-locked PySpark, Py4J, PyArrow and package-build dependencies. It runs as
UID/GID 10001 and contains no credentials or source data.

```powershell
docker build --platform linux/amd64 --tag labgdd-spark-batch:phase2-hardened --file docker/spark-batch/Dockerfile .
docker run --rm --name labgdd-silver-smoke --entrypoint python labgdd-spark-batch:phase2-hardened scripts/smoke_silver_container.py
docker run --rm --name labgdd-silver-real `
  --mount type=bind,source=${PWD}\data\bronze,target=/opt/labgdd/data/bronze,readonly `
  --mount type=bind,source=${PWD}\data\silver,target=/opt/labgdd/data/silver `
  --tmpfs /tmp/labgdd-spark:rw,nosuid,size=1g,mode=1770,uid=10001,gid=10001 `
  labgdd-spark-batch:phase2-hardened
```

The synthetic smoke spans four XNYS fixtures: winter, the spring DST transition,
summer and an early close. It validates all canonical/session/membership timestamp
fields as UTC microseconds and replaces `DataFrame.collect` with a failing function,
proving the normal publisher does not use that API. This runtime is local Spark
only; executor-host, distributed filesystem, scale and failure-recovery claims
require separate evidence.

## Real bounded evidence and comparison

Input Bronze manifest SHA-256 remains
`048b6815a618b2779568ec6b10d15c0d71d4462f4d1282175508283765e8b9d0`.
The hardened Linux run is:

`data/silver/dataset=silver-aapl-20260914-v1/run=aecb53c1-d7a8-40c7-939b-a6901f435ff5/manifest.json`

It processed 390 existing AAPL raw IEX observations: 390 canonical, zero
quarantine, zero duplicate/conflict groups and zero missing of 390 scheduled XNYS
minutes. All artifact hashes and sizes verified. Its terminal manifest SHA-256 is
`33209b03a5f5f6d39efea922376e13f08c59756aa5a7cc01e3cae367711933d6`.
No Alpaca request ran.

The hardened run matches the previous run on all 390 observation IDs, OHLCV values,
quality classifications, input lineage and coverage. It deliberately does not
match the previous physical/logical timestamps: the bounded Windows compatibility
writer had interpreted Spark's naive UTC datetimes in the Europe/Lisbon host zone,
shifting the prior Parquet values from the Bronze/XNYS interval
13:30–20:00 UTC to 14:30–21:00 UTC. Bronze raw `t`, Bronze `event_time` and stored
XNYS bounds all independently confirm 13:30–20:00 UTC. The prior immutable run
`0c822d2b-...` is preserved but superseded as invalid Silver evidence. Run
`ed452342-...` proved native values but used timezone-implicit INT96 and is also
preserved but superseded. Run `ee5196e5-...` used correct UTC microseconds but
preceded the final test-only compatibility patch. The final hardened run uses the
exact reviewed source and explicit UTC microseconds.

The Phase 2 finalization decision approves `aecb53c1-...` and records
`0c822d2b-...` and `ed452342-...` as superseded without changing their immutable
files. Unregistered intermediate runs are not selectable.

## Limits

Historical listing/entity/sector membership remains unverified. IEX is
venue-specific and source completeness/correction finality remain unproven.
Bronze v1 cannot support cryptographic reconstruction of observation IDs from each
row. `BRONZE_V2_IDENTITY_PROPOSAL.md` defines the separate migration gate required
before making that stronger claim or combining the full universe across many
acquisitions. Local rename semantics do not generalize to object stores. Gold,
streaming, Kafka and later infrastructure are not started.
