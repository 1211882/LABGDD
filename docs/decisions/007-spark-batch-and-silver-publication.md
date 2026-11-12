# ADR 007: Spark SQL normalization and staged immutable Silver publication

Date: 2026-11-03. Status: implemented and hardened for Phase 2 Milestone 1.

Use Apache Spark 4.3.0 as the bounded batch transformation engine, pinned with
Py4J 0.10.9.9, PyArrow 21.0.0, CPython 3.11.18 and Eclipse Temurin Java 21.0.13
in the supported Linux image. Spark code remains in `processing`; domain remains
standard-library-only. Bronze verification and Silver publication remain adapters.

Accept only checksum-verified, complete Bronze v1 manifests. Normalize raw JSON
with Spark expressions and validate the original numeric representation before
canonical decimal/integer conversion. Apply explicit quality, calendar and
membership eligibility. Resolve equal payloads deterministically while retaining
every observation ID. Quarantine all different-value conflicts because Bronze v1
contains no verified ordered revision namespace. Observation IDs are checked for
SHA-256 form and collisions, but are not claimed cryptographically reconstructed:
Bronze v1 does not retain all hash-envelope locator fields in each observation row.

The normal publisher uses Spark DataFrame Parquet writes and never collects result
datasets into Python. Canonical output is partitioned by session date and feed,
with a one-million-record file cap. Spark writes under
`dataset=<version>/_staging/run=<UUID>/`; validation then checks inventory, logical
and physical schemas, counts and decoded Spark equality. Physical timestamps must
be UTC `timestamp[us]`, enforced with `TIMESTAMP_MICROS`. The adapter calculates
all artifact sizes and SHA-256 values, writes the terminal manifest, and publishes
the directory by a same-filesystem rename to `run=<UUID>`. Failures publish under
`_failed/run=<UUID>` when possible. UUID reuse and overwrite fail.

The rename protocol is a local-filesystem publication boundary, not a general
object-store transaction, WORM guarantee or distributed commit protocol. A crash
can leave an unadvertised staging directory requiring operator review. The bounded
PyArrow publisher remains only for Windows tests and is never selected by the CLI;
it attaches UTC explicitly to Spark's naive Python timestamp values.

Completed directories are evidence, not authority. Downstream consumers must call
`adapters.silver.resolve_approved_silver` with an explicit dataset version and run
UUID. `config/silver_publications.toml` is the version-controlled external
authority decision and pins manifest, request and processing-source fingerprints.
It rejects superseded, unregistered, tampered or merely newest filesystem runs.

The supported execution environment is the non-root, digest-pinned Linux image in
`docker/spark-batch/Dockerfile`. Bronze is mounted read-only; Silver and Spark temp
storage are separate writable paths. This is local Spark (`local[2]`) correctness
evidence, not a multi-node scalability claim. No unofficial Windows Hadoop binary
is admitted.

The one-day `study-cohort-v1` applicability remains a research declaration, not
historical listing or sector evidence. XNYS remains a common-session benchmark,
not a listing-venue claim. Missing minutes remain missing. No Gold measurement or
approved financial formula changed.
