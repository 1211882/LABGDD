### LABGDD V2 – Development Handoff

**Phase:** Phase 2 — Milestone 1 hardening
**Task:** Architecture hardening and distributed-native Silver persistence
**Status:** Completed; architecture review required before Milestone 2
**Date:** 2026-11-03 (Europe/Lisbon)
**Git branch and commit:** `main` at `d148b45b24d22cb6d0a0c8e20b71cba3b3900c8a`; all Phase 2 changes remain uncommitted in the working tree. Frozen Phase 0 commit/tag were not changed.

#### 1. Objective

Harden the existing Bronze-to-Silver Spark Batch milestone without redesigning the modular monolith. Remove driver collection from normal persistence, introduce a supported Linux runtime, strengthen raw financial and identity validation, reconcile overlapping calendar evidence, make publication failure-safe, and reprocess only the existing 390-row AAPL Bronze acquisition. Gold, Kafka, streaming, ML, databases and monitoring were out of scope.

#### 2. Changes Implemented

`labgdd.processing.silver.process_silver` now uses native Spark DataFrame Parquet writes by default. Canonical output is repartitioned by `session_date` and `feed`, capped at one million records per file, and written with Zstandard compression. No canonical, quarantine or duplicate result is collected into Python on this path. The old PyArrow writer remains explicitly named `pyarrow_compat`, bounded to Windows tests, and cannot be selected by the CLI. Its naive Spark timestamps are now explicitly attached to UTC before PyArrow conversion.

`SilverRun` publishes under `dataset=<version>/_staging/run=<UUID>`, validates output before completion, writes the terminal manifest, and then performs a same-filesystem rename into completed or `_failed` state. A failed completion rename removes the uncommitted manifest and remains eligible for controlled failed publication. Existing completed or failed run identities cannot be reused. Cached frames are unpersisted in `finally`; failed manifests record only stage and exception class, never payload or exception text.

Validation now checks exact staging inventory, Parquet presence, counts, Spark logical schemas, distributed read-back equality, consistent physical schemas, and explicit UTC microsecond timestamp types. `spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS` prevents timezone-implicit INT96 output. Artifact sizes and SHA-256 checksums are calculated before terminal publication.

`transform_observations` validates raw numeric text before canonical casts: positive finite OHLC, at most eight decimal places, at most eighteen significant digits, magnitude below `1e18`, exact/no-rounding decimal conversion, and nonnegative int64 volume without fractional or overflow casts. Identity checks now reject null/malformed IDs and quarantine same-ID conflicts across raw payload or symbol. The manifest explicitly states that Bronze v1 permits format/collision checks but not cryptographic observation-ID reconstruction because row-level hash-envelope locator fields are unavailable.

Calendar reconciliation keys sessions by label and rejects differing open, close, provider, version or schedule hash. Identical overlapping session evidence is de-duplicated before expected-minute generation. Membership gates remain versioned and unknown/ineligible symbols cannot enter canonical.

The supported runtime is `docker/spark-batch/Dockerfile`: digest-pinned Python 3.11.18 and Temurin Java 21.0.13, hash-locked PySpark 4.3.0/Py4J 0.10.9.9/PyArrow 21.0.0, non-root UID/GID 10001, no credentials, read-only Bronze, separate writable Silver and tmpfs. `scripts/smoke_silver_container.py` exercises two session partitions and replaces `DataFrame.collect` with a failing function.

#### 3. Files Created or Modified

- `.dockerignore`, `docker/spark-batch/Dockerfile`, `requirements-batch.lock`, and `scripts/smoke_silver_container.py`: reproducible minimal Linux execution and smoke validation.
- `src/labgdd/adapters/silver.py`: Bronze evidence verification and staged immutable publication.
- `src/labgdd/processing/silver.py`: native writer, physical/logical validation, metrics, cleanup and controlled failure evidence.
- `src/labgdd/processing/spark_batch.py`: strict raw numeric, identity, calendar, membership, deduplication and expected-minute logic.
- `src/labgdd/runners/silver.py`, `src/labgdd/silver_config.py`, and `config/silver.toml`: bounded CLI and validated request.
- `tests/integration/test_spark_silver.py` and `tests/unit/test_silver_config.py`: numeric parity, conflicts, overlaps, staging failures, cleanup and immutability coverage.
- `pyproject.toml`, `requirements-dev.lock`, `scripts/lock_tools.py`, and `src/labgdd/observability/logging.py`: package 0.3.0, pinned batch dependencies, CLI and allowlisted events.
- `README.md`, `docs/architecture/{ARCHITECTURE.md,DATA_CONTRACTS.md,IMPLEMENTATION_PLAN.md,PHASE2_BATCH.md}`, and `docs/decisions/007-spark-batch-and-silver-publication.md`: hardened architecture, runtime commands and limitations.
- This new handoff. Previous handoffs and immutable data artifacts were not overwritten.

#### 4. Architecture and Design Decisions

The modular-monolith boundaries remain: Spark transformation is processing logic; persistence and manifest verification are adapters; the domain remains standard-library-only. Native Spark publication is the only normal path. The compatibility collector is deliberately isolated rather than a silent fallback.

Publication provides a local visibility boundary, not a general distributed transaction. Same-filesystem rename does not establish object-store atomicity, WORM durability or crash cleanup. A crash may leave an unadvertised staging directory for operator review. Local `local[2]` execution is correctness evidence only, not multi-node scalability.

Conflict resolution remains conservative. Equal market payloads may share a canonical value with all IDs retained; different values remain quarantine because no ordered Alpaca revision namespace exists. Hash order, arrival order and price do not select corrections.

#### 5. Tests and Validation

Passed:

- `.venv/Scripts/python -m ruff format --check .` — 40 files formatted.
- `.venv/Scripts/python -m ruff check .` — passed.
- `.venv/Scripts/python -m mypy` — strict typing passed for 40 source files.
- `.venv/Scripts/python -m pytest --basetemp .venv/phase2-hardening-pytest-resumed` — **209 passed** in 297.93 seconds; no skips.
- `.venv/Scripts/python -m pip check` — no broken requirements.
- `.venv/Scripts/python scripts/check_repository.py` — 81 candidate files passed security/tracking checks in the final run.
- `.venv/Scripts/python -m labgdd.runners.silver --config config/project.toml --request config/silver.toml --check` — configuration, Bronze inventory, schema and hashes passed without Spark.
- `.venv/Scripts/python -m build --no-isolation` — 0.3.0 sdist and wheel built.
- `scripts/smoke_wheel.py ...` — clean wheel install, imports, module/console CLIs and `pip check` passed.
- `git diff --check` — passed; only line-ending notices were emitted.
- Final Docker build — passed; local image digest `sha256:a34b4c9f8e63c520986dbc8ebc5082f09c6997c48540af07b99830306b8f1fde`, user `10001:10001`.
- Final container smoke — passed: two canonical rows across two physical session partitions, zero quarantine, checksums valid, and `.collect()` disabled.
- Mount/ownership check — Bronze was non-writable; final run directory and manifest were owned by `10001:10001` inside the container.

Security checks found two blank credential placeholders in `.env.example`, zero nonempty placeholders, `.env` ignored and untracked, and zero tracked `data/`, `build/` or `dist/` files. The reference audit SHA-256 remains `553d34b93258fe35bd061956b616f30c72e3c8759419a4cfd849920d071eb069`.

Final real reprocessing used no network/API request. Run `aecb53c1-d7a8-40c7-939b-a6901f435ff5` read Bronze manifest SHA-256 `048b6815a618b2779568ec6b10d15c0d71d4462f4d1282175508283765e8b9d0`; it produced 390 canonical, zero quarantine, zero duplicate groups, and 390/390 expected coverage. All artifact sizes/hashes verified. Terminal manifest SHA-256 is `33209b03a5f5f6d39efea922376e13f08c59756aa5a7cc01e3cae367711933d6`.

#### 6. Issues and Limitations

The comparison found a material defect in the previous Windows compatibility artifact `0c822d2b-...`: it localized naive Spark UTC datetimes through Europe/Lisbon, shifting 13:30–20:00 UTC to 14:30–21:00 UTC. That immutable artifact is preserved but superseded as invalid Silver timestamp evidence. The final native run matches all prior observation IDs, OHLCV, quality, lineage and coverage, but intentionally does not reproduce the incorrect timestamps. Bronze raw `t`, Bronze `event_time` and XNYS metadata independently support the corrected interval. Intermediate native runs are preserved; `ed452342-...` used timezone-implicit INT96, while `ee5196e5-...` preceded the final test-only compatibility patch.

Historical entity/listing/sector membership remains unverified. IEX is venue-specific; source completeness and correction finality remain unproven. Bronze v1 cannot cryptographically reconstruct observation IDs per row. Scale, multi-node behavior, distributed filesystem recovery and object-store publication remain untested.

#### 7. Scientific and Data Integrity

No financial measurement formula, v2.0.0 definition, adjustment policy or missing-minute policy changed. The timestamp correction restores the approved interval-start UTC semantics rather than changing them. Missing bars are never filled, invalid raw observations remain evidence, conflicting values do not enter canonical, and the 30-symbol/six-sector `study-cohort-v1` remains a declared research classification without invented history. Prior Bronze and Silver files were preserved unchanged.

#### 8. Current Project Status

LABGDD V2 can verify immutable Bronze v1 evidence, transform bounded input through Spark SQL, and publish checksummed canonical/quarantine/duplicate Silver Parquet through a supported non-root Linux runtime. The one real AAPL session is revalidated with correct UTC physical timestamps. Gold analytics and every later service remain not started.

#### 9. Recommended Next Step

Perform architecture review of ADR 007, approve superseding the timestamp-invalid prior Silver evidence, and review the local staged-publication boundary. Before Phase 2 Milestone 2, decide whether Bronze v2 must retain complete observation-hash provenance and obtain reviewed historical membership evidence. Do not start Gold automatically.

#### 10. Review Questions

- Approve `aecb53c1-...` as the replacement Silver evidence and formally supersede `0c822d2b-...`?
- Accept local staged rename plus non-root Docker `local[2]` as sufficient for this bounded milestone, while withholding cluster/object-store claims?
- Require a Bronze schema revision for reconstructable observation identity before broader processing?

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
