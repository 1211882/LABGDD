"""Bronze-to-Silver batch orchestration and immutable publication."""

import hashlib
import importlib.metadata
import platform
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast
from uuid import UUID, uuid4

from labgdd.adapters.silver import SilverRun, verify_bronze_manifest
from labgdd.config import Settings
from labgdd.processing.spark_batch import transform_observations
from labgdd.silver_config import SilverRequest

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class SilverProcessingError(RuntimeError):
    """Controlled processing failure without payload or path disclosure."""


@dataclass(frozen=True, slots=True)
class SilverResult:
    manifest: Path
    status: str
    counts: dict[str, int]


def _schema_types(frame: "DataFrame") -> dict[str, str]:
    return {field.name: field.dataType.simpleString() for field in frame.schema.fields}


def _validate_arrow_schema(name: str, schema: object) -> None:
    import pyarrow as pa

    arrow_schema = cast(pa.Schema, schema)
    expected: dict[str, pa.DataType] = {
        "event_time": pa.timestamp("us", tz="UTC"),
    }
    if name == "canonical":
        expected.update(
            {
                "bar_end": pa.timestamp("us", tz="UTC"),
                "open": pa.decimal128(26, 8),
                "high": pa.decimal128(26, 8),
                "low": pa.decimal128(26, 8),
                "close": pa.decimal128(26, 8),
                "volume": pa.int64(),
                "membership_valid_from": pa.timestamp("us", tz="UTC"),
                "membership_valid_to": pa.timestamp("us", tz="UTC"),
                "session_open": pa.timestamp("us", tz="UTC"),
                "session_close": pa.timestamp("us", tz="UTC"),
            }
        )
    elif name == "duplicates":
        expected.update(
            {
                "observation_rows": pa.int64(),
                "distinct_observation_ids": pa.int64(),
                "payload_versions": pa.int64(),
            }
        )
    for field, data_type in expected.items():
        if arrow_schema.field(field).type != data_type:
            raise ValueError("silver validation: physical schema contract mismatch")


def _write_native(frames: tuple["DataFrame", "DataFrame", "DataFrame"], path: Path) -> None:
    canonical, quarantine, duplicates = frames
    (
        canonical.repartition("session_date", "feed")
        .write.mode("errorifexists")
        .option("compression", "zstd")
        .option("maxRecordsPerFile", 1_000_000)
        .partitionBy("session_date", "feed")
        .parquet(str(path / "canonical"))
    )
    (
        quarantine.write.mode("errorifexists")
        .option("compression", "zstd")
        .option("maxRecordsPerFile", 1_000_000)
        .parquet(str(path / "quarantine"))
    )
    (
        duplicates.write.mode("errorifexists")
        .option("compression", "zstd")
        .option("maxRecordsPerFile", 1_000_000)
        .parquet(str(path / "duplicates"))
    )


def _write_compatibility(
    storage: SilverRun, frames: tuple["DataFrame", "DataFrame", "DataFrame"]
) -> None:
    """Bounded Windows/test compatibility path; never selected by the CLI."""
    from pyspark.sql import functions as F

    canonical, quarantine, duplicates = frames
    timestamp_fields = {
        "canonical": (
            "event_time",
            "bar_end",
            "membership_valid_from",
            "membership_valid_to",
            "session_open",
            "session_close",
        ),
        "quarantine": ("event_time",),
        "duplicates": ("event_time",),
    }
    epoch = datetime(1970, 1, 1, tzinfo=UTC)

    def collect_utc(frame: "DataFrame", kind: str) -> list[dict[str, object]]:
        timestamps = set(timestamp_fields[kind])
        projected = frame.select(
            *(
                F.unix_micros(F.col(field)).alias(field) if field in timestamps else F.col(field)
                for field in frame.columns
            )
        )
        rows = [row.asDict(recursive=True) for row in projected.collect()]
        for row in rows:
            for field in timestamps:
                value = row.get(field)
                if isinstance(value, int):
                    row[field] = epoch + timedelta(microseconds=value)
        return rows

    canonical_rows = collect_utc(canonical, "canonical")
    partitions: dict[tuple[str, str], list[dict[str, object]]] = {}
    for row in canonical_rows:
        key = (str(row["session_date"]), str(row["feed"]))
        partitions.setdefault(key, []).append(row)
    for index, ((session_date, feed), rows) in enumerate(sorted(partitions.items())):
        storage.write_parquet(
            f"canonical/session_date={session_date}/feed={feed}/part-{index:05d}.parquet",
            rows,
            kind="canonical",
        )
    storage.write_parquet(
        "quarantine/part-00000.parquet",
        collect_utc(quarantine, "quarantine"),
        kind="quarantine",
    )
    storage.write_parquet(
        "duplicates/part-00000.parquet",
        collect_utc(duplicates, "duplicates"),
        kind="duplicates",
    )


def _validate_outputs(
    spark: "SparkSession",
    frames: tuple["DataFrame", "DataFrame", "DataFrame"],
    path: Path,
    counts: tuple[int, int, int],
    publisher: Literal["spark", "pyarrow_compat"],
) -> dict[str, object]:
    expected = {"run.json", "canonical", "quarantine", "duplicates"}
    if {item.name for item in path.iterdir()} != expected:
        raise ValueError("silver validation: unexpected staging inventory")
    schemas: dict[str, object] = {}
    for name, source, expected_count in zip(
        ("canonical", "quarantine", "duplicates"), frames, counts, strict=True
    ):
        output = path / name
        files = sorted(output.rglob("*.parquet"))
        if not files:
            raise ValueError("silver validation: missing Parquet output")
        import pyarrow.parquet as pq

        file_schemas = [pq.read_schema(item) for item in files]
        if any(schema != file_schemas[0] for schema in file_schemas[1:]):
            raise ValueError("silver validation: inconsistent physical schemas")
        _validate_arrow_schema(name, file_schemas[0])
        if publisher == "spark":
            decoded = spark.read.parquet(str(output))
            if decoded.count() != expected_count:
                raise ValueError("silver validation: output count mismatch")
            if _schema_types(decoded) != _schema_types(source):
                raise ValueError("silver validation: output schema mismatch")
            projected = decoded.select(*source.columns)
            if (
                source.exceptAll(projected).limit(1).count()
                or projected.exceptAll(source).limit(1).count()
            ):
                raise ValueError("silver validation: output value mismatch")
            schemas[name] = decoded.schema.jsonValue()
        else:
            # The explicitly bounded Windows compatibility path cannot use Hadoop's
            # local reader without unsupported native binaries. PyArrow verifies the
            # adapter-owned explicit schemas and row-group metadata instead.
            if sum(pq.ParquetFile(item).metadata.num_rows for item in files) != expected_count:
                raise ValueError("silver validation: output count mismatch")
            schemas[name] = str(file_schemas[0])
    return schemas


def process_silver(
    settings: Settings,
    request: SilverRequest,
    spark: object,
    *,
    run_id: UUID | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    publisher: Literal["spark", "pyarrow_compat"] = "spark",
) -> SilverResult:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    if not isinstance(spark, SparkSession):
        raise TypeError("silver processing: SparkSession required")
    inputs = tuple(verify_bronze_manifest(path) for path in request.bronze_manifests)
    identity = run_id or uuid4()
    started = clock()
    package_root = Path(__file__).resolve().parents[1]
    source_digest = hashlib.sha256()
    for source_file in sorted(package_root.rglob("*.py")):
        source_digest.update(source_file.relative_to(package_root).as_posix().encode())
        source_digest.update(b"\0")
        source_digest.update(source_file.read_bytes())
    java_runtime = str(
        cast(Any, spark.sparkContext)._jvm.java.lang.System.getProperty("java.runtime.version")
    )
    metadata: dict[str, object] = {
        "manifest_version": "1.0.0",
        "silver_schema_version": request.silver_schema_version,
        "resolution_policy_version": request.resolution_policy_version,
        "run_id": str(identity),
        "dataset_version": request.dataset_version,
        "started_at": started.isoformat(),
        "source": "alpaca",
        "feed": "iex",
        "timeframe": "1Min",
        "adjustment": "raw",
        "calendar_policy": "XNYS common_regular_session_benchmark",
        "membership_version": settings.membership_version,
        "membership_valid_from": request.membership_valid_from.isoformat(),
        "membership_valid_to": request.membership_valid_to.isoformat(),
        "membership_basis": request.membership_basis,
        "input_manifests": [
            {
                "path": item.manifest.relative_to(settings.data_root).as_posix(),
                "sha256": item.manifest_sha256,
                "acquisition_id": item.acquisition_id,
                "dataset_version": item.dataset_version,
            }
            for item in inputs
        ],
        "environment_versions": {
            "labgdd-v2": importlib.metadata.version("labgdd-v2"),
            "pyspark": importlib.metadata.version("pyspark"),
            "py4j": importlib.metadata.version("py4j"),
            "python": platform.python_version(),
            "spark": spark.version,
            "java_runtime": java_runtime,
        },
        "processing_source_sha256": source_digest.hexdigest(),
        "publication": "local_staging_validate_manifest_then_atomic_directory_rename",
        "status": "started",
    }
    if publisher not in {"spark", "pyarrow_compat"}:
        raise ValueError("silver processing: unsupported publisher")
    metadata["publisher"] = publisher
    metadata["observation_identity_verification"] = (
        "format_and_collision_checks_only_provenance_locator_unavailable"
    )
    storage = SilverRun(settings.data_root, request.dataset_version, identity, metadata)
    began = time.perf_counter()
    persisted: list[DataFrame] = []
    stage = "transform"
    try:
        frames = transform_observations(spark, inputs, settings, request)
        persisted = [frames.canonical.cache(), frames.quarantine.cache(), frames.duplicates.cache()]
        input_records = frames.observations.count()
        bronze_valid_records = frames.observations.filter(F.col("valid") == F.lit(True)).count()
        canonical_records = persisted[0].count()
        quarantined_records = persisted[1].count()
        duplicate_groups = persisted[2].count()
        duplicate_row = (
            persisted[2].select(F.sum(F.col("observation_rows") - F.lit(1)).alias("count")).first()
        )
        duplicate_value = duplicate_row["count"] if duplicate_row is not None else None
        duplicate_records = int(duplicate_value) if isinstance(duplicate_value, int) else 0
        expected_records = frames.expected.count()
        observed_expected = frames.expected.join(
            persisted[0].select("symbol", "event_time"), ["symbol", "event_time"], "inner"
        ).count()
        missing_records = expected_records - observed_expected
        stage = "write_outputs"
        output_frames = (persisted[0], persisted[1], persisted[2])
        if publisher == "spark":
            _write_native(output_frames, storage.path)
        else:
            _write_compatibility(storage, output_frames)
        stage = "validate_outputs"
        physical_schemas = _validate_outputs(
            spark,
            output_frames,
            storage.path,
            (canonical_records, quarantined_records, duplicate_groups),
            publisher,
        )
        counts = {
            "input_records": input_records,
            "bronze_valid_records": bronze_valid_records,
            "canonical_records": canonical_records,
            "quarantined_records": quarantined_records,
            "duplicate_groups": duplicate_groups,
            "duplicate_records": int(duplicate_records),
            "expected_session_records": expected_records,
            "observed_expected_records": observed_expected,
            "missing_session_records": missing_records,
        }
        summary: dict[str, object] = {
            **counts,
            "coverage_ratio": observed_expected / expected_records if expected_records else None,
            "processing_duration_seconds": round(time.perf_counter() - began, 6),
            "schemas": {
                "canonical": persisted[0].schema.jsonValue(),
                "quarantine": persisted[1].schema.jsonValue(),
                "duplicates": persisted[2].schema.jsonValue(),
            },
            "partitioning": ["session_date", "feed"],
            "physical_schema_policy": "partition_columns_encoded_in_hive_path",
            "physical_schemas": physical_schemas,
            "source_completeness": "unproven",
            "correction_resolution": "equivalent_only_conflicts_quarantined",
        }
        stage = "publish_complete"
        manifest = storage.finish("complete", summary)
        return SilverResult(manifest, "complete", counts)
    except Exception as failure:
        try:
            if not storage.finished:
                storage.finish(
                    "failed",
                    {
                        "error": "processing_failed",
                        "failure_stage": stage,
                        "failure_type": type(failure).__name__,
                        "processing_duration_seconds": round(time.perf_counter() - began, 6),
                    },
                )
        except (OSError, ValueError):
            pass
        if isinstance(failure, (KeyboardInterrupt, SystemExit)):
            raise
        raise SilverProcessingError("silver processing failed") from failure
    finally:
        for frame in persisted:
            frame.unpersist()
