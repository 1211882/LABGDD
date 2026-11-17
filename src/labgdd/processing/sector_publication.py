"""Synthetic-only immutable Sector Gold candidates; no publication approval API."""

import hashlib
import importlib.metadata
import json
import platform
import re
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID, uuid4

from labgdd.adapters.bronze import sha256
from labgdd.adapters.gold import GoldRun, resolve_approved_gold
from labgdd.adapters.silver import resolve_approved_silver
from labgdd.config import Settings
from labgdd.processing.approved_sector import calculate_approved_sector_gold
from labgdd.processing.expected_gold import _reason_counts, _source_sha256
from labgdd.sector_publication_config import SectorPublicationRequest

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

_BASE_TEXT = (
    "sector session_id source feed timeframe adjustment membership_version universe_version "
    "population_policy_version eligibility_policy eligibility_policy_version "
    "research_reference_sha256 "
    "input_authority expected_plan_sha256 output_identity_sha256"
).split()
_LINEAGE = (
    "input_gold_dataset_version input_gold_run_id input_gold_manifest_sha256 "
    "input_processing_source_sha256 input_configuration_sha256 input_gold_schema_version "
    "input_measurement_version input_silver_dataset_version input_silver_run_id "
    "input_silver_manifest_sha256 input_approved_expected_plan_sha256 "
    "input_research_reference_sha256 "
    "approved_input_identity_sha256 sector_dataset_version sector_run_id sector_schema_version "
    "sector_publication_status evidence_kind calendar_provider calendar_version schedule_sha256"
).split()
_COUNTS = (
    "expected_members historically_verified_members acquisition_scope_members observed_members "
    "canonical_valid_members financially_eligible_members missing_members excluded_members "
    "expected_pairs valid_pairs"
).split()
_METRICS = (
    "sector_realized_variance sector_realized_volatility portfolio_realized_variance "
    "portfolio_realized_volatility mean_log_return_realized_volatility volatility_dispersion "
    "mean_pairwise_correlation volatility_concentration_hhi "
    "volatility_effective_breadth volatility_breadth"
).split()
_REASONS = "reason correlation_reason concentration_reason breadth_reason".split()
_REASON_CODES = (
    "member_outside_acquisition_scope",
    "incomplete_sector_population",
    "unverified_historical_applicability",
    "missing_expected_member",
    "missing_observed_bar",
    "ineligible_individual_return",
    "incomplete_calculation_window",
    "insufficient_valid_pairs",
    "constant_return_series",
    "incomplete_threshold_calibration",
)


def sector_publication_schema(name: str) -> "StructType":
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DataType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    if name not in {"windows", "correlations"}:
        raise ValueError("sector publication: unknown output")
    fields: dict[str, DataType] = {key: StringType() for key in [*_BASE_TEXT, *_LINEAGE]}
    fields.update(
        {
            key: TimestampType()
            for key in ("session_open", "session_close", "window_start", "window_end")
        }
    )
    fields.update({key: LongType() for key in ("horizon", "window_index", *_COUNTS)})
    fields["expected_symbols"] = ArrayType(StringType(), True)
    fields["eligible"] = BooleanType()
    fields["reason"] = StringType()
    if name == "windows":
        fields.update({key: DoubleType() for key in _METRICS})
        fields.update({key: StringType() for key in _REASONS})
        fields["membership_applicable"] = BooleanType()
    else:
        fields = {
            key: value
            for key, value in fields.items()
            if key not in _COUNTS or key in {"expected_members", "expected_pairs"}
        }
        fields.update(
            {
                "left_symbol": StringType(),
                "right_symbol": StringType(),
                "value": DoubleType(),
                "pair_identity_sha256": StringType(),
            }
        )
    return StructType([StructField(key, value, True) for key, value in fields.items()])


def _configuration_sha256(request: SectorPublicationRequest) -> str:
    document = {key: str(value) for key, value in asdict(request).items()}
    # Location-independent fingerprint; exact source manifests bind the actual evidence.
    for key in ("gold_registry", "silver_registry", "research_reference"):
        document[key] = Path(document[key]).name
    return hashlib.sha256(json.dumps(document, sort_keys=True).encode()).hexdigest()


def _source(settings: Settings, request: SectorPublicationRequest) -> dict[str, object]:
    source = resolve_approved_gold(
        request.gold_registry,
        settings.data_root,
        request.input_dataset_version,
        request.input_run_id,
    )
    if source.manifest_sha256 != request.input_manifest_sha256:
        raise ValueError("sector publication: source authority mismatch")
    raw = source.manifest.read_bytes()
    if (
        hashlib.sha256(raw).hexdigest() != source.manifest_sha256
        or json.loads(raw) != source.metadata
    ):
        raise ValueError("sector publication: source changed during verification")
    metadata = dict(source.metadata)
    reference_rows = metadata.get("expected_plan_rows")
    if type(reference_rows) is not int or not 0 < reference_rows <= 1000:
        raise ValueError("sector publication: bounded reference population exceeded")
    if metadata.get("evidence_kind") != "synthetic_fixture":
        raise ValueError("sector publication: real or unmarked input publication is prohibited")
    if sha256(request.research_reference) != metadata.get("research_reference_sha256"):
        raise ValueError("sector publication: research configuration changed")
    silver = resolve_approved_silver(
        request.silver_registry,
        settings.data_root,
        str(metadata["silver_dataset_version"]),
        UUID(str(metadata["silver_run_id"])),
    )
    if silver.manifest_sha256 != metadata["silver_manifest_sha256"]:
        raise ValueError("sector publication: Silver lineage mismatch")
    return metadata


def _normalize(frame: "DataFrame", name: str) -> "DataFrame":
    from pyspark.sql import functions as F

    schema = sector_publication_schema(name)
    if set(frame.columns) != set(schema.fieldNames()):
        raise ValueError("sector publication: incomplete output schema")
    # Only widening integral count/index columns; financial values are never recast.
    for field in schema:
        actual = frame.schema[field.name].dataType.simpleString()
        target = field.dataType.simpleString()
        if actual != target and not (target == "bigint" and actual in {"int", "bigint"}):
            raise ValueError("sector publication: incompatible output type")
    return frame.select(
        *(F.col(field.name).cast(field.dataType).alias(field.name) for field in schema)
    ).to(schema)


def _validate_frames(windows: "DataFrame", pairs: "DataFrame") -> None:
    from pyspark.sql import functions as F

    for name, frame in (("windows", windows), ("correlations", pairs)):
        schema = sector_publication_schema(name)
        if frame.columns != schema.fieldNames() or any(
            frame.schema[f.name].dataType.simpleString() != f.dataType.simpleString()
            for f in schema
        ):
            raise ValueError("sector publication: logical schema mismatch")
        nullable = {*_METRICS, *_REASONS} if name == "windows" else {"value", "reason"}
        invalid = F.lit(False)
        for reason in _REASONS if name == "windows" else ("reason",):
            invalid = invalid | (F.col(reason).isNotNull() & ~F.col(reason).isin(*_REASON_CODES))
        for field in schema:
            column = F.col(field.name)
            if field.name not in nullable:
                invalid = invalid | column.isNull()
            if field.dataType.simpleString() == "double":
                invalid = invalid | F.isnan(column) | column.isin(float("inf"), float("-inf"))
            if field.name.endswith("sha256"):
                invalid = invalid | ~column.rlike("^[0-9a-f]{64}$")
        invalid = (
            invalid
            | (F.col("sector_publication_status") != "pending_architecture_review")
            | (F.col("evidence_kind") != "synthetic_fixture")
        )
        invalid = invalid | (~F.col("horizon").isin(5, 15, 30, 60)) | (F.col("window_index") < 0)
        invalid = invalid | (
            F.col("window_start")
            != F.col("session_open")
            + F.expr("INTERVAL 1 MINUTE")
            + F.col("window_index") * F.col("horizon") * F.expr("INTERVAL 1 MINUTE")
        )
        invalid = (
            invalid
            | (
                F.col("window_end")
                != F.col("window_start") + F.col("horizon") * F.expr("INTERVAL 1 MINUTE")
            )
            | (F.col("window_end") > F.col("session_close"))
        )
        invalid = (
            invalid
            | (F.col("eligible") != F.col("reason").isNull())
            | (F.col("reason").isNotNull() & (F.trim("reason") == ""))
        )
        invalid = (
            invalid
            | (F.col("expected_members") <= 0)
            | (F.size("expected_symbols") != F.col("expected_members"))
            | (F.size(F.array_distinct("expected_symbols")) != F.col("expected_members"))
        )
        invalid = invalid | (
            F.col("expected_pairs")
            != F.col("expected_members") * (F.col("expected_members") - 1) / 2
        )
        base_identity = F.sha2(
            F.concat_ws(
                "|",
                F.lit("sector-output-identity-v1"),
                F.col("sector"),
                F.col("session_id"),
                F.col("horizon"),
                F.col("window_index"),
                F.concat_ws(",", F.sort_array("expected_symbols")),
                F.col("population_policy_version"),
                F.col("eligibility_policy"),
                F.col("eligibility_policy_version"),
                F.col("research_reference_sha256"),
            ),
            256,
        )
        bound_identity = F.sha2(
            F.concat_ws(
                "|",
                F.lit("sector-approved-input-v1"),
                base_identity,
                F.col("input_gold_dataset_version"),
                F.col("input_gold_run_id"),
                F.col("input_gold_manifest_sha256"),
            ),
            256,
        )
        invalid = (
            invalid
            | ~F.col("output_identity_sha256").eqNullSafe(base_identity)
            | ~F.col("approved_input_identity_sha256").eqNullSafe(bound_identity)
        )
        if name == "windows":
            for count in _COUNTS:
                maximum = (
                    F.col("expected_pairs")
                    if count in {"expected_pairs", "valid_pairs"}
                    else F.col("expected_members")
                )
                invalid = invalid | (F.col(count) < 0) | (F.col(count) > maximum)
            invalid = (
                invalid
                | (
                    F.col("missing_members")
                    != F.col("expected_members") - F.col("observed_members")
                )
                | (
                    F.col("excluded_members")
                    != F.col("expected_members") - F.col("acquisition_scope_members")
                )
            )
            invalid = invalid | (
                F.col("eligible")
                & (
                    ~F.col("membership_applicable")
                    | (F.col("financially_eligible_members") != F.col("expected_members"))
                    | (F.col("observed_members") != F.col("expected_members"))
                    | (F.col("acquisition_scope_members") != F.col("expected_members"))
                )
            )
            for metric in _METRICS:
                invalid = invalid | (~F.col("eligible") & F.col(metric).isNotNull())
            for metric in _METRICS[:6]:
                invalid = (
                    invalid | (F.col("eligible") & F.col(metric).isNull()) | (F.col(metric) < 0)
                )
            invalid = invalid | (
                F.col("eligible")
                & (
                    (F.col("canonical_valid_members") != F.col("expected_members"))
                    | (
                        (F.col("eligibility_policy") == "verified_historical_applicability")
                        & (F.col("historically_verified_members") != F.col("expected_members"))
                    )
                )
            )
            invalid = (
                invalid
                | (F.col("volatility_concentration_hhi") < 0)
                | (F.col("volatility_concentration_hhi") > 1.000000000001)
            )
            invalid = (
                invalid
                | (F.col("volatility_effective_breadth") < -1e-12)
                | (F.col("volatility_effective_breadth") > 1.000000000001)
            )
            for metric, reason in (
                ("mean_pairwise_correlation", "correlation_reason"),
                ("volatility_concentration_hhi", "concentration_reason"),
                ("volatility_breadth", "breadth_reason"),
            ):
                invalid = invalid | (F.col(metric).isNull() != F.col(reason).isNotNull())
            invalid = invalid | (
                F.col("mean_pairwise_correlation").isNotNull()
                & (F.col("valid_pairs") != F.col("expected_pairs"))
            )
            key = ["approved_input_identity_sha256"]
        else:
            invalid = (
                invalid
                | (F.col("left_symbol") >= F.col("right_symbol"))
                | ~F.array_contains("expected_symbols", F.col("left_symbol"))
                | ~F.array_contains("expected_symbols", F.col("right_symbol"))
            )
            invalid = (
                invalid
                | (F.col("value").isNotNull() != F.col("eligible"))
                | (F.abs("value") > 1.000000000001)
            )
            identity = F.sha2(
                F.concat_ws(
                    "|",
                    F.lit("sector-pair-identity-v1"),
                    F.col("approved_input_identity_sha256"),
                    F.col("left_symbol"),
                    F.col("right_symbol"),
                ),
                256,
            )
            invalid = invalid | ~F.col("pair_identity_sha256").eqNullSafe(identity)
            key = ["pair_identity_sha256"]
        if (
            frame.filter(invalid).limit(1).count()
            or frame.groupBy(*key).count().filter("count > 1").limit(1).count()
        ):
            raise ValueError("sector publication: invalid identity, eligibility or population")
    summary = pairs.groupBy("approved_input_identity_sha256").agg(
        F.count("*").alias("pairs"), F.sum(F.col("eligible").cast("long")).alias("valid")
    )
    if (
        windows.join(summary, "approved_input_identity_sha256", "left")
        .filter(
            (F.coalesce(F.col("pairs"), F.lit(0)) != F.col("expected_pairs"))
            | (F.coalesce(F.col("valid"), F.lit(0)) != F.col("valid_pairs"))
        )
        .limit(1)
        .count()
    ):
        raise ValueError("sector publication: incomplete pair inventory")
    if (
        pairs.select("approved_input_identity_sha256")
        .join(
            windows.select("approved_input_identity_sha256"),
            "approved_input_identity_sha256",
            "left_anti",
        )
        .limit(1)
        .count()
    ):
        raise ValueError("sector publication: orphan pair")
    shared = [
        key for key in pairs.columns if key in windows.columns and key not in {"eligible", "reason"}
    ]
    joined = pairs.alias("pair").join(
        windows.alias("window"),
        F.col("pair.approved_input_identity_sha256")
        == F.col("window.approved_input_identity_sha256"),
        "inner",
    )
    mismatch = F.lit(False)
    for shared_field in shared:
        mismatch = mismatch | ~F.col(f"pair.{shared_field}").eqNullSafe(
            F.col(f"window.{shared_field}")
        )
    if joined.filter(mismatch).limit(1).count():
        raise ValueError("sector publication: pair/window provenance mismatch")


def _validate_expected_grid(windows: "DataFrame", population: "DataFrame") -> None:
    """Check all independent expected windows, including completely absent groups."""
    from pyspark.sql import functions as F

    expected = (
        population.groupBy("sector", "session_id", "session_open", "session_close")
        .agg(
            F.sort_array(F.collect_list("symbol")).alias("expected_symbols"),
            F.count("symbol").alias("expected_members"),
        )
        .withColumn(
            "horizon", F.explode(F.array(*(F.lit(h).cast("long") for h in (5, 15, 30, 60))))
        )
    )
    count = F.floor(
        ((F.unix_micros("session_close") - F.unix_micros("session_open")) / 60_000_000 - 1)
        / F.col("horizon")
    ).cast("long")
    expected = expected.withColumn(
        "window_index", F.explode(F.sequence(F.lit(0).cast("long"), count - 1))
    )
    keys = expected.columns
    if (
        expected.exceptAll(windows.select(*keys)).limit(1).count()
        or windows.select(*keys).exceptAll(expected).limit(1).count()
    ):
        raise ValueError("sector publication: incomplete independent window grid")


def _validate_lineage(frames: tuple["DataFrame", ...], metadata: dict[str, Any]) -> None:
    from pyspark.sql import functions as F

    expected = {key: metadata[key] for key in _LINEAGE if key in metadata}
    expected.update(
        sector_dataset_version=metadata["dataset_version"],
        sector_run_id=metadata["run_id"],
        sector_schema_version=metadata["sector_schema_version"],
        sector_publication_status="pending_architecture_review",
        population_policy_version="sector-complete-population-v1",
        eligibility_policy=metadata["eligibility_policy"],
        eligibility_policy_version="sector-eligibility-policy-v1",
        expected_plan_sha256=metadata["approved_expected_plan_sha256"],
        research_reference_sha256=metadata["research_reference_sha256"],
        input_authority="approved_expected_gold",
    )
    for frame in frames:
        invalid = F.lit(False)
        for key, value in expected.items():
            invalid = invalid | ~F.col(key).eqNullSafe(F.lit(value))
        if frame.filter(invalid).limit(1).count():
            raise ValueError("sector publication: output lineage mismatch")


def _read_outputs(spark: "SparkSession", path: Path) -> tuple["DataFrame", "DataFrame"]:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyspark.sql.pandas.types import from_arrow_type

    frames = []
    for name in ("windows", "correlations"):
        schema = sector_publication_schema(name)
        files = sorted((path / name).rglob("*.parquet"))
        if not files:
            raise ValueError("sector publication: missing output")
        for file in files:
            physical = pq.read_schema(file)
            if physical.names != schema.fieldNames():
                raise ValueError("sector publication: physical schema inventory mismatch")
            for field in physical:
                if pa.types.is_timestamp(field.type) and field.type != pa.timestamp("us", tz="UTC"):
                    raise ValueError("sector publication: UTC microseconds required")
                if (
                    from_arrow_type(field.type).simpleString()
                    != schema[field.name].dataType.simpleString()
                ):
                    raise ValueError("sector publication: physical type mismatch")
        frames.append(spark.read.schema(schema).parquet(str(path / name)))
    return frames[0], frames[1]


def publish_synthetic_sector_gold(
    spark: "SparkSession",
    settings: Settings,
    request: SectorPublicationRequest,
    *,
    run_id: UUID | None = None,
) -> Path:
    """Publish only marked synthetic upstream evidence; real publication is unavailable."""
    identity = UUID(str(run_id)) if run_id is not None else uuid4()
    from pyspark.sql import functions as F

    root = settings.data_root.resolve() / "sector_gold" / f"dataset={request.dataset_version}"
    for prefix in (root, root / "_staging", root / "_failed"):
        path = prefix / f"run={identity}"
        if path.exists() or path.is_symlink():
            raise FileExistsError("sector publication: run identity already reserved")
    source = _source(settings, request)
    projection, result = calculate_approved_sector_gold(
        spark,
        settings,
        registry=request.gold_registry,
        silver_registry=request.silver_registry,
        research_reference=request.research_reference,
        dataset_version=request.input_dataset_version,
        run_id=request.input_run_id,
        manifest_sha256=request.input_manifest_sha256,
        eligibility_policy=request.eligibility_policy,
    )
    if dict(projection.gold.metadata) != source:
        raise ValueError("sector publication: source changed during calculation")
    if projection.population.count() > 1000:
        raise ValueError("sector publication: bounded reference population exceeded")
    population = [
        json.loads(cast(str, value)) for value in projection.population.toJSON().collect()
    ]
    metadata: dict[str, object] = {
        "manifest_version": "1.0.0",
        "sector_schema_version": "1.0.0",
        "sector_calculation_contract_version": "sector-calculation-v1",
        "measurement_version": "2.0.0",
        "dataset_version": request.dataset_version,
        "run_id": str(identity),
        "evidence_kind": "synthetic_fixture",
        "publication_status": "pending_architecture_review",
        "input_authority": "synthetic_fixture_registry",
        "input_gold_dataset_version": request.input_dataset_version,
        "input_gold_run_id": str(request.input_run_id),
        "input_gold_manifest_sha256": request.input_manifest_sha256,
        "eligibility_policy": request.eligibility_policy,
        "eligibility_policy_version": "sector-eligibility-policy-v1",
        "population_policy_version": "sector-complete-population-v1",
        "output_identity_version": "sector-output-identity-v1",
        "approved_input_identity_version": "sector-approved-input-v1",
        "pair_identity_version": "sector-pair-identity-v1",
        "processing_source_sha256": _source_sha256(),
        "configuration_sha256": _configuration_sha256(request),
        "started_at": datetime.now(UTC).isoformat(),
        "population": population,
        "coverage": [
            json.loads(cast(str, value)) for value in projection.coverage.toJSON().collect()
        ],
        "environment_versions": {
            "python": platform.python_version(),
            "spark": spark.version,
            **{
                name: importlib.metadata.version(name)
                for name in ("pyarrow", "exchange_calendars", "tzdata", "pyspark")
            },
        },
        "source_completeness": "unproven",
        "historical_evidence": "see_population_rows_no_inferred_verification",
    }
    for key in (
        "source",
        "feed",
        "timeframe",
        "adjustment",
        "silver_dataset_version",
        "silver_run_id",
        "silver_manifest_sha256",
        "research_universe_version",
        "research_reference_sha256",
        "approved_expected_plan_sha256",
        "calendar_provider",
        "calendar_version",
        "calendar_name",
        "calendar_timezone",
        "tzdata_version",
        "schedule_sha256",
    ):
        metadata[key] = source[key]
    for key in (
        "processing_source_sha256",
        "configuration_sha256",
        "gold_schema_version",
        "measurement_version",
        "silver_dataset_version",
        "silver_run_id",
        "silver_manifest_sha256",
        "approved_expected_plan_sha256",
        "research_reference_sha256",
    ):
        metadata[f"input_{key}"] = source[key]
    windows = result.windows
    pair_fields = [
        "output_identity_sha256",
        *_BASE_TEXT,
        "session_open",
        "session_close",
        "window_index",
        "expected_symbols",
        "expected_members",
        "expected_pairs",
    ]
    pair_fields = list(dict.fromkeys(pair_fields))
    additions = [key for key in pair_fields if key not in result.correlations.columns]
    pairs = result.correlations.join(
        windows.select("output_identity_sha256", *additions), "output_identity_sha256"
    )
    bound = []
    for name, frame in (("windows", windows), ("correlations", pairs)):
        for key, value in {
            "sector_dataset_version": request.dataset_version,
            "sector_run_id": str(identity),
            "sector_schema_version": "1.0.0",
            "sector_publication_status": "pending_architecture_review",
            "evidence_kind": "synthetic_fixture",
            **{
                key: str(source[key])
                for key in ("calendar_provider", "calendar_version", "schedule_sha256")
            },
        }.items():
            frame = frame.withColumn(key, F.lit(value))
        if name == "correlations":
            frame = frame.withColumn(
                "pair_identity_sha256",
                F.sha2(
                    F.concat_ws(
                        "|",
                        F.lit("sector-pair-identity-v1"),
                        F.col("approved_input_identity_sha256"),
                        F.col("left_symbol"),
                        F.col("right_symbol"),
                    ),
                    256,
                ),
            )
        bound.append(_normalize(frame, name).cache())
    storage = GoldRun(
        settings.data_root, request.dataset_version, identity, metadata, sector_outputs=True
    )
    try:
        _validate_frames(*bound)
        _validate_expected_grid(bound[0], projection.population)
        _validate_lineage(tuple(bound), metadata)
        for name, frame in zip(("windows", "correlations"), bound, strict=True):
            frame.coalesce(1).write.mode("errorifexists").option("compression", "zstd").parquet(
                str(storage.path / name)
            )
        # Native Parquet only: no driver collection or PyArrow financial writer.
        decoded = _read_outputs(spark, storage.path)
        _validate_frames(*decoded)
        _validate_expected_grid(decoded[0], projection.population)
        _validate_lineage(decoded, metadata)
        for before, after in zip(bound, decoded, strict=True):
            if before.exceptAll(after).limit(1).count() or after.exceptAll(before).limit(1).count():
                raise ValueError("sector publication: readback differs from calculation")
        if (
            _source(settings, request) != source
            or _source_sha256() != metadata["processing_source_sha256"]
        ):
            raise ValueError("sector publication: authority changed before finalization")
        summary: dict[str, object] = {
            "schemas": {
                name: sector_publication_schema(name).jsonValue()
                for name, frame in zip(("windows", "correlations"), bound, strict=True)
            },
            "counts": {
                name: {"rows": frame.count(), "eligible": frame.filter("eligible").count()}
                for name, frame in zip(("windows", "correlations"), bound, strict=True)
            },
            "reason_counts": {
                name: _reason_counts(frame)
                for name, frame in zip(("windows", "correlations"), bound, strict=True)
            },
            "partitioning": [],
            "max_output_files_per_dataset": 1,
        }
        return storage.finish("complete", summary)
    except Exception:
        if not storage.finished:
            try:
                # A failed final rename can leave a terminal JSON in staging.
                # This private run is not published; preserve its artifacts as failed.
                for name in ("manifest.pending", "manifest.json"):
                    (storage.path / name).unlink(missing_ok=True)
                storage.finish("failed", {"error": "sector_publication_validation_failed"})
            except OSError:
                # Filesystem failure leaves unapproved staging, never a complete run.
                pass
        raise
    finally:
        for frame in bound:
            frame.unpersist()


def inspect_sector_candidate(
    spark: "SparkSession",
    settings: Settings,
    request: SectorPublicationRequest,
    manifest: Path,
    manifest_sha256: str,
) -> dict[str, Any]:
    """Read-only candidate inspection, never authority resolution or approval."""
    source = _source(settings, request)
    raw = manifest.read_bytes()
    if hashlib.sha256(raw).hexdigest() != manifest_sha256:
        raise ValueError("sector candidate: manifest fingerprint mismatch")
    doc = json.loads(raw)
    root = settings.data_root.resolve() / "sector_gold" / f"dataset={request.dataset_version}"
    expected = root / f"run={UUID(doc['run_id'])}" / "manifest.json"
    if (
        manifest.resolve() != expected
        or doc.get("status") != "complete"
        or doc.get("publication_status") != "pending_architecture_review"
        or doc.get("evidence_kind") != "synthetic_fixture"
        or doc.get("input_gold_manifest_sha256") != request.input_manifest_sha256
        or doc.get("configuration_sha256") != _configuration_sha256(request)
    ):
        raise ValueError("sector candidate: incompatible candidate or input authority")
    required: dict[str, object] = {
        "manifest_version": "1.0.0",
        "sector_schema_version": "1.0.0",
        "sector_calculation_contract_version": "sector-calculation-v1",
        "dataset_version": request.dataset_version,
        "measurement_version": "2.0.0",
        "input_gold_dataset_version": request.input_dataset_version,
        "input_gold_run_id": str(request.input_run_id),
        "eligibility_policy": request.eligibility_policy,
        "eligibility_policy_version": "sector-eligibility-policy-v1",
        "population_policy_version": "sector-complete-population-v1",
        "output_identity_version": "sector-output-identity-v1",
        "approved_input_identity_version": "sector-approved-input-v1",
        "pair_identity_version": "sector-pair-identity-v1",
    }
    for key in (
        "silver_dataset_version",
        "silver_run_id",
        "silver_manifest_sha256",
        "research_universe_version",
        "research_reference_sha256",
        "approved_expected_plan_sha256",
        "calendar_provider",
        "calendar_version",
        "calendar_name",
        "calendar_timezone",
        "tzdata_version",
        "schedule_sha256",
    ):
        required[key] = source[key]
    for key in (
        "processing_source_sha256",
        "configuration_sha256",
        "gold_schema_version",
        "measurement_version",
        "silver_dataset_version",
        "silver_run_id",
        "silver_manifest_sha256",
        "approved_expected_plan_sha256",
        "research_reference_sha256",
    ):
        required[f"input_{key}"] = source[key]
    if any(doc.get(key) != value for key, value in required.items()):
        raise ValueError("sector candidate: manifest contract or lineage mismatch")
    for key in ("processing_source_sha256", "configuration_sha256"):
        if not isinstance(doc.get(key), str) or re.fullmatch(r"[0-9a-f]{64}", doc[key]) is None:
            raise ValueError("sector candidate: missing processing fingerprint")
    _verify_inventory(manifest, doc)
    frames = _read_outputs(spark, manifest.parent)
    _validate_frames(*frames)
    projection, calculated = calculate_approved_sector_gold(
        spark,
        settings,
        registry=request.gold_registry,
        silver_registry=request.silver_registry,
        research_reference=request.research_reference,
        dataset_version=request.input_dataset_version,
        run_id=request.input_run_id,
        manifest_sha256=request.input_manifest_sha256,
        eligibility_policy=request.eligibility_policy,
    )
    _validate_expected_grid(frames[0], projection.population)
    _validate_lineage(frames, doc)
    # Re-evaluate through the accepted native engine, not merely the candidate's
    # self-reported flags or hashes. This also binds every inherited provenance
    # and financial column to the exact approved source. No rows reach Python.
    for expected_frame, actual_frame in zip(
        (calculated.windows, calculated.correlations), frames, strict=True
    ):
        columns = [name for name in expected_frame.columns if name != "sector_publication_status"]
        expected_frame = expected_frame.select(*columns).cache()
        try:
            actual_frame = actual_frame.select(*columns)
            if (
                expected_frame.exceptAll(actual_frame).limit(1).count()
                or actual_frame.exceptAll(expected_frame).limit(1).count()
            ):
                raise ValueError("sector candidate: calculation evidence mismatch")
        finally:
            expected_frame.unpersist()
    actual_population = [
        json.loads(cast(str, value)) for value in projection.population.toJSON().collect()
    ]

    def population_key(row: dict[str, Any]) -> tuple[str, str, str]:
        return row["sector"], row["session_id"], row["symbol"]

    if sorted(actual_population, key=population_key) != sorted(
        doc.get("population", []), key=population_key
    ):
        raise ValueError("sector candidate: population evidence mismatch")
    for name, frame in zip(("windows", "correlations"), frames, strict=True):
        if doc.get("schemas", {}).get(name) != sector_publication_schema(name).jsonValue():
            raise ValueError("sector candidate: declared schema mismatch")
        if (
            frame.count() != doc["counts"][name]["rows"]
            or frame.filter("eligible").count() != doc["counts"][name]["eligible"]
        ):
            raise ValueError("sector candidate: count mismatch")
        if _reason_counts(frame) != doc["reason_counts"][name]:
            raise ValueError("sector candidate: reason distribution mismatch")
    _verify_inventory(manifest, doc)
    if (
        hashlib.sha256(manifest.read_bytes()).hexdigest() != manifest_sha256
        or _source(settings, request) != source
    ):
        raise ValueError("sector candidate: evidence changed during inspection")
    return cast(dict[str, Any], doc)


def _verify_inventory(manifest: Path, doc: dict[str, Any]) -> None:
    if {path.name for path in manifest.parent.iterdir()} != {
        "manifest.json",
        "run.json",
        "windows",
        "correlations",
    }:
        raise ValueError("sector candidate: invalid dataset inventory")
    run = json.loads((manifest.parent / "run.json").read_bytes())
    if any(doc.get(key) != value for key, value in run.items()):
        raise ValueError("sector candidate: creation metadata mismatch")
    artifacts = doc.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise ValueError("sector candidate: missing inventory")
    inventory = {"manifest.json"}
    for item in artifacts:
        if not isinstance(item, dict) or set(item) != {"path", "bytes", "sha256"}:
            raise ValueError("sector candidate: invalid inventory entry")
        relative, size, digest = item["path"], item["bytes"], item["sha256"]
        if (
            not isinstance(relative, str)
            or relative in inventory
            or type(size) is not int
            or size < 0
            or not isinstance(digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", digest) is None
        ):
            raise ValueError("sector candidate: invalid inventory entry")
        path = (manifest.parent / relative).resolve()
        if (
            not path.is_relative_to(manifest.parent.resolve())
            or not path.is_file()
            or path.stat().st_size != size
            or sha256(path) != digest
        ):
            raise ValueError("sector candidate: artifact inventory/checksum mismatch")
        inventory.add(relative)
    if {
        p.relative_to(manifest.parent).as_posix() for p in manifest.parent.rglob("*") if p.is_file()
    } != inventory:
        raise ValueError("sector candidate: unexpected artifact")
