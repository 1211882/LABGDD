### LABGDD V2 – Development Handoff

**Phase:** Phase 2 — Milestone 1
**Task:** Spark Batch Bronze-to-Silver canonical layer
**Status:** Completed for the bounded milestone; architecture review required before expansion
**Date:** 2026-11-03 (Europe/Lisbon)
**Git branch and commit:** `main`; based on `d148b45b24d22cb6d0a0c8e20b71cba3b3900c8a`. Phase 2 changes are present in the working tree and were not committed by this task. Frozen Phase 0 tag/commit remain unchanged.

#### 1. Objective

Implement the first modular Spark Batch pipeline from checksum-verified immutable Bronze observations to versioned canonical Silver. Preserve original observation lineage, classify invalid/duplicate/conflicting evidence without arbitrary correction selection, apply XNYS and declared research membership, publish immutable Parquet plus a terminal execution manifest, test deterministic fixtures, and process only the approved real AAPL session. Do not implement Gold analytics or later infrastructure.

#### 2. Changes Implemented

Pinned Apache Spark/PySpark 4.3.0 and Py4J 0.10.9.9 in the hash lock, added the optional `batch` extra, advanced the package to 0.3.0, and added `labgdd-silver`.

`silver_config.load_silver_request` strictly validates dataset/policy versions, V2-owned Bronze manifest paths and a UTC half-open membership-applicability interval. `adapters.silver.verify_bronze_manifest` rejects incomplete/incompatible inputs and independently verifies exact inventory, sizes, SHA-256 and physical observation schemas. `SilverRun` owns exclusive output directories, explicit canonical/quarantine/duplicate PyArrow schemas, fsync, artifact hashing and terminal-manifest-last publication.

`processing.spark_batch.transform_observations` uses Spark SQL for raw JSON normalization, decimal OHLCV validation, UTC/alignment checks, XNYS eligibility, membership joins, observation-identity integrity, replay/equivalent/conflict classification, conservative resolution and expected-minute generation. Same-ID/different-content and different-value logical conflicts quarantine; arrival time, price and hash order never choose a market value. Equivalent values retain sorted contributing IDs and use the minimum ID only as a stable representative. `processing.silver.process_silver` records source/environment/input hashes, counts, logical schemas, policies and duration. The runner binds workers to the exact project Python executable and fixes SQL timezone UTC.

Final real run: `data/silver/dataset=silver-aapl-20260914-v1/run=0c822d2b-be75-4497-a99a-06246013973a/manifest.json`. It read the existing AAPL/IEX/raw/1Min Bronze acquisition only; no Alpaca request ran.

#### 3. Files Created or Modified

- Created `config/silver.toml` for the approved one-session input and explicitly declared, nonhistorical research-cohort applicability.
- Created `src/labgdd/silver_config.py`, `src/labgdd/adapters/silver.py`, `src/labgdd/processing/spark_batch.py`, `src/labgdd/processing/silver.py`, and `src/labgdd/runners/silver.py` for configuration, input/persistence boundaries, Spark transformations, orchestration and CLI.
- Modified `pyproject.toml`, `requirements-dev.lock`, `scripts/lock_tools.py`, and `src/labgdd/observability/logging.py` for version 0.3.0, exact dependencies, entry point and allowlisted events.
- Created `tests/integration/test_spark_silver.py` and `tests/unit/test_silver_config.py`.
- Created `docs/decisions/007-spark-batch-and-silver-publication.md` and `docs/architecture/PHASE2_BATCH.md`; updated `README.md`, `docs/architecture/ARCHITECTURE.md`, `DATA_CONTRACTS.md`, and `IMPLEMENTATION_PLAN.md`.
- Created this handoff. No Bronze file, Phase 0 tag, reference audit, financial formula or previous handoff was modified.

#### 4. Architecture and Design Decisions

The modular monolith and dependency direction remain. Spark-specific code is outside `domain`; domain remains standard-library-only. Bronze access and Silver persistence are adapters; orchestration is separate from Spark transformations. Canonical output is partitioned by `session_date` and `feed`, with one bounded file per partition to avoid symbol-level small files. Partition values are encoded in Hive paths and omitted physically; the complete Spark logical schema is in the manifest.

The Windows Hadoop local committer requires an external `winutils.exe`. No unofficial executable was introduced. For this one-session milestone, Spark performs transformations/actions and the adapter collects the bounded result for pinned-PyArrow publication. This is explicitly not a scalable multi-symbol publication path. Local exclusive creation and terminal hard-link publication do not claim database transactions, WORM storage, object-store semantics or power-loss durability.

The configured 2026-09-14 applicability interval means “use the project-defined fixed research cohort for this reviewed snapshot.” It is not evidence of listing history or historical industrial classification.

#### 5. Tests and Validation

Executed on Windows, CPython 3.11.9, Spark 4.3.0 and Temurin Java 21.0.12.1. Final results:

- `.venv/Scripts/python -m ruff format --check .` — passed, 39 files.
- `.venv/Scripts/python -m ruff check .` — passed.
- `.venv/Scripts/python -m mypy` — passed strict typing, 39 source files.
- `.venv/Scripts/python -m pytest --basetemp .venv/phase2-full-pytest-final2` — **204 passed**, no skips, 242.40 seconds.
- `.venv/Scripts/python -m pip check` — passed, no broken requirements.
- `.venv/Scripts/python scripts/check_repository.py` — passed, 75 candidate files.
- `.venv/Scripts/python -m labgdd.runners.silver --check` — passed configuration, Bronze inventory, checksums and schema without starting Spark.
- `.venv/Scripts/python -m build --no-isolation` — passed; built 0.3.0 sdist and wheel.
- `.venv/Scripts/python scripts/smoke_wheel.py dist/labgdd_v2-0.3.0-py3-none-any.whl --config config/project.toml` — passed clean zero-runtime-dependency wheel install, imports, module/console CLIs and `pip check`.
- Final real Spark command with `local[2]` — passed in run `0c822d2b-be75-4497-a99a-06246013973a`.

Spark tests cover valid/invalid price and volume, equivalent duplicates, unresolved conflicts, same-ID corruption, missing minutes, unknown symbols, two companies, variable effective membership, deterministic ordering/identity, 210-minute early close, DST reference geometry, Spark-versus-pure-domain fixture checks, input tampering, output checksums, repeat semantic equality, UUID collision, and injected persistence failure.

Intermediate validation was not suppressed: one full run reported 203 passed/1 failed because PyArrow inferred the differing `run=<UUID>` path partition during a semantic-repeat comparison; the assertion was corrected to compare physical Parquet rows. A second run reported 203 passed/1 failed because tampering changed file size before checksum evaluation; the assertion now accepts either integrity gate. An initial final-format check found one test file needing formatting. No production contract was weakened. The final commands above passed.

Real counts: 390 input; 390 Bronze-valid; 390 canonical; 0 quarantined; 0 duplicate/conflict records; 390 expected/observed; 0 missing; coverage 1.0; duration 50.714461 seconds. Canonical OHLC is decimal128(26,8), volume int64 and timestamps UTC microseconds. Canonical SHA-256: `4bb8eee400bea07c6c655f3386a57b8b5ed911f105e6e6cc18ba76e20c12f709`; terminal manifest SHA-256: `53e7268db1c862ea91130045d6fd6dd95f6a5686151997d3c3d6ba188147f52d`. All four listed artifact hashes/sizes were recomputed. Bronze manifest remained `048b6815...e8b9d0`.

Six development runs are preserved as failed evidence; they exposed session-column ambiguity, host worker-Python mismatch, unsupported Windows Hadoop writing and a null empty-duplicate metric. A prior successful run predates the partition-schema fix. Only the final run above is the milestone evidence.

#### 6. Issues and Limitations

Driver collection before PyArrow publication limits this implementation to bounded data. It must not process the full cohort/long history until a supported distributed writer is reviewed. Linux/cluster execution, distributed commit semantics, executor failure recovery, scale/performance and byte-identical Parquet across platforms are unverified.

Bronze v1 supplies no documented ordered correction namespace; explicitly ordered revision selection is therefore unsupported, and conflicts quarantine. IEX is venue-specific and `source_completeness` remains `unproven`. One 100%-covered snapshot does not prove correction finality. Historical listing/entity/sector evidence remains absent. The XNYS common grid is not a listing-venue assertion. Local hard-link publication is not WORM/object-store durability.

#### 7. Scientific and Data Integrity

No financial formula or measurement version changed. Gold analytics were not implemented. Raw IEX prices, interval-start timestamps, one-minute bar ends, XNYS regular sessions, missing-as-missing policy and immutable snapshot lineage remain intact. Invalid observations are retained in quarantine, not silently dropped. Conflicting values cannot enter canonical without verifiable revision evidence. The 30-symbol/six-sector taxonomy is preserved as `study-cohort-v1`; no historical dates, listings or classifications were invented. Existing Bronze and reference evidence stayed byte-stable.

#### 8. Current Project Status

The project can now verify complete Bronze v1 acquisitions, transform bounded observations through local Spark SQL, produce canonical/quarantine/duplicate Silver Parquet, calculate schedule coverage, and publish an immutable manifest with full lineage. The one real AAPL session is verified end to end. The publication mechanism is suitable only for this milestone scale. Gold formulas, Spark Structured Streaming, Kafka, ML, PostgreSQL, Prometheus/Grafana and Power BI remain not started.

#### 9. Recommended Next Step

Perform architecture review of ADR 007, schemas, conflict policy and the bounded publisher. Before Silver-to-Gold or wider processing, select a supported Linux/cluster filesystem and distributed Spark writer, validate the Python/Java/Spark matrix there, and obtain evidence for historical entity/membership applicability. Then authorize a separate Gold milestone with independent financial fixtures; do not start it automatically.

#### 10. Review Questions

- Is driver-side PyArrow publication acceptable strictly for this one-session milestone, with distributed writing required before expansion?
- Is the declared one-day research-cohort applicability acceptable while historical membership remains explicitly unverified?
- Should every later REST snapshot remain a separate Silver dataset until a verifiable provider correction order exists?

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
