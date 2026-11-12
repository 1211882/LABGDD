"""Approved-Silver to immutable individual-company Gold orchestration."""

import hashlib
import importlib.metadata
import json
import platform
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID, uuid4

from labgdd.adapters.gold import GoldRun
from labgdd.adapters.silver import resolve_silver_input
from labgdd.config import Settings
from labgdd.gold_config import GoldRequest
from labgdd.processing.gold_batch import transform_individual_gold

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class GoldProcessingError(RuntimeError):
    """Controlled processing failure without data or exception text."""


@dataclass(frozen=True, slots=True)
class GoldResult:
    manifest: Path
    status: str
    counts: dict[str, int]


def _write(
    frames: tuple["DataFrame", ...],
    path: Path,
    partition_columns: tuple[str, ...] = ("session_id", "symbol"),
) -> None:
    for name, frame in zip(("returns", "rolling", "fixed"), frames, strict=True):
        (
            frame.repartition(*partition_columns)
            .write.mode("errorifexists")
            .option("compression", "zstd")
            .option("maxRecordsPerFile", 1_000_000)
            .partitionBy(*partition_columns)
            .parquet(str(path / name))
        )


def _validate(
    spark: "SparkSession",
    frames: tuple["DataFrame", ...],
    path: Path,
    counts: tuple[int, ...],
    *,
    expected_authority: bool = False,
) -> dict[str, object]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    if {item.name for item in path.iterdir()} != {"run.json", "returns", "rolling", "fixed"}:
        raise ValueError("gold validation: unexpected staging inventory")
    schemas: dict[str, object] = {}
    timestamp_fields = {
        "returns": {
            "event_time",
            "bar_end",
            "return_end",
            "session_open",
            "session_close",
            "membership_valid_from",
            "membership_valid_to",
        },
        "rolling": {
            "window_start",
            "window_end",
            "session_open",
            "session_close",
            "membership_valid_from",
            "membership_valid_to",
        },
        "fixed": {
            "window_start",
            "window_end",
            "session_open",
            "session_close",
            "membership_valid_from",
            "membership_valid_to",
        },
    }
    authority_fields = {
        "universe_version",
        "session_membership_status",
        "membership_change_times",
        "applicability_basis",
        "evidence_source",
        "evidence_status",
        "historical_listing_status",
        "entity_identity_status",
        "sector_classification_status",
        "calendar_name",
        "calendar_timezone",
        "tzdata_version",
        "calendar_applicability",
        "expected_eligible",
        "expected_ineligibility_reason",
        "eligibility_policy",
        "expectation_authority",
        "expectation_status",
        "expected_series_contract_version",
        "population_policy_version",
        "fingerprint_version",
        "expected_plan_sha256",
        "expected_identity_sha256",
    }
    for name, source, count in zip(("returns", "rolling", "fixed"), frames, counts, strict=True):
        if expected_authority and not authority_fields.issubset(source.columns):
            raise ValueError("gold validation: incomplete expected authority schema")
        files = sorted((path / name).rglob("*.parquet"))
        if not files:
            raise ValueError("gold validation: missing Parquet output")
        physical = [pq.read_schema(item) for item in files]
        if any(value != physical[0] for value in physical[1:]):
            raise ValueError("gold validation: inconsistent physical schema")
        for field in timestamp_fields[name]:
            if physical[0].field(field).type != pa.timestamp("us", tz="UTC"):
                raise ValueError("gold validation: timestamp physical contract mismatch")
        membership_timestamp = pa.list_(
            pa.field("element", pa.timestamp("us", tz="UTC"), nullable=False)
        )
        if (
            expected_authority
            and physical[0].field("membership_change_times").type != membership_timestamp
        ):
            raise ValueError("gold validation: membership timestamp physical contract mismatch")
        decoded = spark.read.parquet(str(path / name))
        if decoded.count() != count:
            raise ValueError("gold validation: output count mismatch")
        projected = decoded.select(*source.columns)
        if (
            source.exceptAll(projected).limit(1).count()
            or projected.exceptAll(source).limit(1).count()
        ):
            raise ValueError("gold validation: output value mismatch")
        schemas[name] = source.schema.jsonValue()
    return schemas


def process_gold(
    settings: Settings,
    request: GoldRequest,
    spark: object,
    *,
    run_id: UUID | None = None,
) -> GoldResult:
    from pyspark.sql import SparkSession

    if not isinstance(spark, SparkSession):
        raise TypeError("gold processing: SparkSession required")
    approved = resolve_silver_input(
        request.silver_registry,
        settings.data_root,
        request.silver_dataset_version,
        request.silver_run_id,
        request.silver_input_authority,
    )
    identity = run_id or uuid4()
    package_root = Path(__file__).resolve().parents[1]
    source_digest = hashlib.sha256()
    for source in sorted(package_root.rglob("*.py")):
        source_digest.update(source.relative_to(package_root).as_posix().encode())
        source_digest.update(b"\0")
        source_digest.update(source.read_bytes())
    request_fingerprint = hashlib.sha256(
        json.dumps(
            {
                "dataset_version": request.dataset_version,
                "silver_registry": request.silver_registry.as_posix(),
                "silver_dataset_version": request.silver_dataset_version,
                "silver_run_id": str(request.silver_run_id),
                "silver_input_authority": request.silver_input_authority,
                "measurement_version": request.measurement_version,
                "horizons": list(request.horizons),
                "gold_schema_version": request.gold_schema_version,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    metadata: dict[str, object] = {
        "manifest_version": "1.0.0",
        "gold_schema_version": request.gold_schema_version,
        "measurement_version": request.measurement_version,
        "dataset_version": request.dataset_version,
        "run_id": str(identity),
        "started_at": datetime.now(UTC).isoformat(),
        "source": "alpaca",
        "feed": "iex",
        "timeframe": "1Min",
        "adjustment": "raw",
        "horizons": list(request.horizons),
        "silver_dataset_version": approved.dataset_version,
        "silver_run_id": approved.run_id,
        "silver_manifest_sha256": approved.manifest_sha256,
        "silver_input_authority": approved.authority_status,
        "silver_registry": approved.registry.relative_to(
            approved.registry.parent.parent
        ).as_posix(),
        "processing_source_sha256": source_digest.hexdigest(),
        "configuration_sha256": request_fingerprint,
        "publication": "local_staging_validate_manifest_then_atomic_directory_rename",
        "environment_versions": {
            "labgdd-v2": importlib.metadata.version("labgdd-v2"),
            "pyspark": importlib.metadata.version("pyspark"),
            "python": platform.python_version(),
            "spark": spark.version,
            "java_runtime": str(
                cast(Any, spark.sparkContext)._jvm.java.lang.System.getProperty(
                    "java.runtime.version"
                )
            ),
        },
        "source_completeness": "unproven",
        "status": "started",
    }
    storage = GoldRun(settings.data_root, request.dataset_version, identity, metadata)
    persisted: list[DataFrame] = []
    stage = "read_input"
    began = time.perf_counter()
    try:
        canonical = spark.read.parquet(str(approved.manifest.parent / "canonical"))
        stage = "transform"
        result = transform_individual_gold(canonical, request.horizons)
        from pyspark.sql import functions as F

        lineage_columns = {
            "gold_dataset_version": request.dataset_version,
            "gold_run_id": str(identity),
            "measurement_version": request.measurement_version,
            "silver_dataset_version": approved.dataset_version,
            "silver_run_id": approved.run_id,
            "silver_manifest_sha256": approved.manifest_sha256,
            "publication_revision": "1",
            "finalization_reason": "bounded_snapshot",
            "source_completeness": "unproven",
        }

        def with_lineage(frame: "DataFrame") -> "DataFrame":
            for name, value in lineage_columns.items():
                frame = frame.withColumn(name, F.lit(value))
            return frame

        persisted = [
            with_lineage(result.returns).cache(),
            with_lineage(result.rolling).cache(),
            with_lineage(result.fixed).cache(),
        ]
        counts_tuple = tuple(frame.count() for frame in persisted)
        stage = "write_outputs"
        _write(tuple(persisted), storage.path)
        stage = "validate_outputs"
        schemas = _validate(spark, tuple(persisted), storage.path, counts_tuple)
        eligible_rolling = persisted[1].filter("eligible").count()
        eligible_fixed = persisted[2].filter("eligible").count()
        counts = {
            "return_records": counts_tuple[0],
            "rolling_records": counts_tuple[1],
            "fixed_records": counts_tuple[2],
            "eligible_rolling_records": eligible_rolling,
            "eligible_fixed_records": eligible_fixed,
        }
        stage = "publish_complete"
        manifest = storage.finish(
            "complete",
            {
                **counts,
                "schemas": schemas,
                "partitioning": ["session_id", "symbol"],
                "processing_duration_seconds": round(time.perf_counter() - began, 6),
                "fixed_window_policy": "scheduled_nonoverlapping_complete_anchors",
                "rolling_window_policy": "trailing_H_returns_each_expected_return_end",
            },
        )
        return GoldResult(manifest, "complete", counts)
    except Exception as failure:
        try:
            if not storage.finished:
                storage.finish(
                    "failed",
                    {
                        "error": "processing_failed",
                        "failure_stage": stage,
                        "failure_type": type(failure).__name__,
                    },
                )
        except (OSError, ValueError):
            pass
        if isinstance(failure, (KeyboardInterrupt, SystemExit)):
            raise
        raise GoldProcessingError("gold processing failed") from failure
    finally:
        for frame in persisted:
            frame.unpersist()
