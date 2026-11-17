"""Read-only approved Expected-Series Gold projection into sector calculations."""

import json
from dataclasses import dataclass
from datetime import date
from hashlib import sha256 as new_sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, cast
from uuid import UUID

from labgdd.adapters.bronze import sha256
from labgdd.adapters.calendar import xnys_snapshot
from labgdd.adapters.gold import ApprovedGold, resolve_approved_gold
from labgdd.adapters.silver import resolve_approved_silver
from labgdd.analytics.sector import (
    DECLARED_RESEARCH_POLICY,
    ELIGIBILITY_POLICIES,
    SECTOR_ELIGIBILITY_POLICY_VERSION,
    SECTOR_POPULATION_POLICY_VERSION,
    VERIFIED_HISTORICAL_POLICY,
)
from labgdd.config import Settings
from labgdd.processing.expected_series import generate_xnys_expected_series
from labgdd.processing.expected_series_spark import (
    expected_plan_fingerprint,
    expected_series_frame,
    expected_series_schema,
)
from labgdd.processing.sector_gold import (
    SectorGoldFrames,
    _transform_sector_gold,
    sector_population_schema,
    sector_return_schema,
)
from labgdd.research_config import load_research_universe

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

_MAX_SNAPSHOT_BYTES = 256 * 1024 * 1024


def _verified_returns_snapshot(
    spark: "SparkSession", approved: ApprovedGold, schema: "StructType"
) -> "DataFrame":
    """Detach lazy reads from mutable paths using a bounded, byte-verified copy.

    Local checkpoint blocks belong to this Spark session. Loss of those blocks
    fails the calculation; they cannot be recomputed from the original files.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyspark import StorageLevel
    from pyspark.sql.pandas.types import from_arrow_type

    manifest_bytes = approved.manifest.read_bytes()
    if (
        new_sha256(manifest_bytes).hexdigest() != approved.manifest_sha256
        or json.loads(manifest_bytes) != approved.metadata
    ):
        raise ValueError("sector projection: snapshot manifest changed")
    artifacts = cast(list[dict[str, object]], approved.metadata["artifacts"])
    selected = [
        item
        for item in artifacts
        if Path(str(item["path"])).parts[0] == "returns"
        and Path(str(item["path"])).suffix == ".parquet"
    ]
    if not selected or sum(cast(int, item["bytes"]) for item in selected) > _MAX_SNAPSHOT_BYTES:
        raise ValueError("sector projection: missing or oversized bounded returns snapshot")
    with TemporaryDirectory(prefix="labgdd-sector-verified-") as temporary:
        root = Path(temporary)
        for item in selected:
            relative = Path(str(item["path"]))
            source = (approved.manifest.parent / relative).resolve()
            target = (root / relative).resolve()
            if not source.is_relative_to(
                approved.manifest.parent.resolve()
            ) or not target.is_relative_to(root.resolve()):
                raise ValueError("sector projection: snapshot path escapes root")
            target.parent.mkdir(parents=True, exist_ok=True)
            digest = new_sha256()
            size = 0
            with source.open("rb") as reader, target.open("xb") as writer:
                while block := reader.read(1024 * 1024):
                    size += len(block)
                    if size > cast(int, item["bytes"]):
                        raise ValueError("sector projection: snapshot artifact size mismatch")
                    digest.update(block)
                    writer.write(block)
            if size != item["bytes"] or digest.hexdigest() != item["sha256"]:
                raise ValueError("sector projection: snapshot artifact checksum mismatch")
            physical = pq.read_schema(target)
            if set(physical.names) != set(schema.fieldNames()) - {"session_id"}:
                raise ValueError("sector projection: physical schema inventory mismatch")
            for field in physical:
                if pa.types.is_timestamp(field.type) and field.type != pa.timestamp("us", tz="UTC"):
                    raise ValueError("sector projection: physical UTC timestamp mismatch")
                if from_arrow_type(field.type) != schema[field.name].dataType:
                    raise ValueError("sector projection: physical field type mismatch")
        rows = spark.read.schema(schema).parquet(str(root / "returns"))
        return rows.localCheckpoint(eager=True, storageLevel=StorageLevel.DISK_ONLY)


@dataclass(frozen=True)
class ApprovedSectorProjection:
    """Inspected input relations; this object alone does not confer authority."""

    gold: ApprovedGold
    population: "DataFrame"
    returns: "DataFrame"
    coverage: "DataFrame"


def _text(metadata: dict[str, object], key: str) -> str:
    value = metadata.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"sector projection: missing {key}")
    return value


def _strings(metadata: dict[str, object], key: str) -> tuple[str, ...]:
    value = metadata.get(key)
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(item, str) or not item for item in value)
        or len(value) != len(set(value))
    ):
        raise ValueError(f"sector projection: invalid {key}")
    return tuple(cast(list[str], value))


def project_approved_expected_gold(
    spark: "SparkSession",
    settings: Settings,
    *,
    registry: Path,
    silver_registry: Path,
    research_reference: Path,
    dataset_version: str,
    run_id: UUID,
    manifest_sha256: str,
    eligibility_policy: str = DECLARED_RESEARCH_POLICY,
) -> ApprovedSectorProjection:
    """Resolve exact files and independently validate population before projecting.

    No caller-supplied frame or preconstructed ApprovedGold is accepted. Returned
    frames read a materialized verified snapshot, never the original lazy paths.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType

    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("sector projection: Spark session must be UTC")
    if eligibility_policy not in ELIGIBILITY_POLICIES:
        raise ValueError("sector projection: unsupported eligibility policy")
    approved = resolve_approved_gold(registry, settings.data_root, dataset_version, run_id)
    if approved.manifest_sha256 != manifest_sha256:
        raise ValueError("sector projection: required manifest fingerprint mismatch")
    metadata = dict(approved.metadata)
    required = {
        "gold_schema_version": "2.0.0",
        "measurement_version": "2.0.0",
        "source": "alpaca",
        "feed": "iex",
        "timeframe": "1Min",
        "adjustment": "raw",
        "expected_series_contract_version": "1.0.0",
        "population_policy_version": "expected-series-left-join-v1",
    }
    if any(metadata.get(key) != value for key, value in required.items()):
        raise ValueError("sector projection: incompatible Gold contract")
    source_policy = _text(metadata, "eligibility_policy")
    if source_policy not in ELIGIBILITY_POLICIES or (
        source_policy == VERIFIED_HISTORICAL_POLICY
        and eligibility_policy != VERIFIED_HISTORICAL_POLICY
    ):
        raise ValueError("sector projection: unsupported policy or historical downgrade")
    upstream = resolve_approved_silver(
        silver_registry,
        settings.data_root,
        _text(metadata, "silver_dataset_version"),
        UUID(_text(metadata, "silver_run_id")),
    )
    if upstream.manifest_sha256 != metadata.get("silver_manifest_sha256"):
        raise ValueError("sector projection: upstream lineage mismatch")
    research = load_research_universe(research_reference, settings)
    if sha256(research.path) != metadata.get("research_reference_sha256"):
        raise ValueError("sector projection: research fingerprint mismatch")
    sessions = _strings(metadata, "requested_sessions")
    if sessions != tuple(sorted(sessions)):
        raise ValueError("sector projection: unordered sessions")
    dates = tuple(date.fromisoformat(value) for value in sessions)
    calendar = xnys_snapshot(min(dates), max(dates))
    calendar_fields = {
        "calendar_provider": "provider",
        "calendar_version": "version",
        "calendar_name": "calendar",
        "calendar_timezone": "timezone",
        "tzdata_version": "tzdata_version",
        "schedule_sha256": "schedule_sha256",
    }
    if any(metadata.get(key) != calendar.metadata[value] for key, value in calendar_fields.items()):
        raise ValueError("sector projection: calendar provenance mismatch")
    plan = generate_xnys_expected_series(research.universe, calendar, sessions)
    plan_hash = expected_plan_fingerprint(
        plan,
        source="alpaca",
        feed="iex",
        timeframe="1Min",
        adjustment="raw",
        strict_historical=source_policy == VERIFIED_HISTORICAL_POLICY,
    )
    if plan_hash != metadata.get("approved_expected_plan_sha256"):
        raise ValueError("sector projection: expected plan fingerprint mismatch")
    symbols = {row.symbol for row in plan.rows}
    if (
        metadata.get("research_universe_version") != research.universe.universe_version
        or metadata.get("expected_symbols") != len(symbols)
        or metadata.get("expected_plan_rows") != len(plan.rows)
        or metadata.get("horizons") != [5, 15, 30, 60]
    ):
        raise ValueError("sector projection: population metadata mismatch")
    scope = _strings(metadata, "acquisition_scope_symbols")
    if not set(scope) <= symbols:
        raise ValueError("sector projection: acquisition scope outside research population")
    expected = expected_series_frame(
        spark,
        plan,
        source="alpaca",
        feed="iex",
        timeframe="1Min",
        adjustment="raw",
        strict_historical=source_policy == VERIFIED_HISTORICAL_POLICY,
    )
    schemas = metadata.get("schemas")
    if not isinstance(schemas, dict) or not isinstance(schemas.get("returns"), dict):
        raise ValueError("sector projection: missing returns schema")
    declared_schema = StructType.fromJson(schemas["returns"])
    rows = _verified_returns_snapshot(spark, approved, declared_schema)
    expected_columns = expected_series_schema().fieldNames()
    required_columns = expected_columns + [
        "event_time",
        "return_end",
        "observed_bars",
        "eligible",
        "reason",
        "close_to_close_log_return",
        "close",
        "bar_end",
        "quality_status",
        "gold_dataset_version",
        "gold_run_id",
        "measurement_version",
        "silver_dataset_version",
        "silver_run_id",
        "silver_manifest_sha256",
    ]
    if not set(required_columns) <= set(rows.columns):
        raise ValueError("sector projection: incomplete returns schema")
    for schema_field in expected_series_schema():
        # Parquet decoding widens nested nullability. Element values are checked
        # against the regenerated authority grid below, including NULL elements.
        if (
            rows.schema[schema_field.name].dataType.simpleString()
            != schema_field.dataType.simpleString()
        ):
            raise ValueError("sector projection: incorrect authority field type")
    grid = expected.select(
        "*",
        F.explode(
            F.sequence(
                F.col("session_open"),
                F.col("session_close") - F.expr("INTERVAL 1 MINUTE"),
                F.expr("INTERVAL 1 MINUTE"),
            )
        ).alias("event_time"),
    )
    key_columns = [*expected_columns, "event_time"]
    actual = rows.select(*key_columns)
    if (
        grid.select(*key_columns).exceptAll(actual).limit(1).count()
        or actual.exceptAll(grid.select(*key_columns)).limit(1).count()
    ):
        raise ValueError("sector projection: incomplete, duplicate or contradictory expected slots")
    invalid = (
        F.col("return_end").isNull()
        | (F.col("return_end") != F.col("event_time") + F.expr("INTERVAL 1 MINUTE"))
        | F.col("eligible").isNull()
        | F.col("observed_bars").isNull()
        | ~F.col("observed_bars").isin(0, 1, 2)
        | (F.col("close").isNull() & (F.col("observed_bars") != 0))
        | (F.col("close").isNotNull() & (F.col("observed_bars") == 0))
        | (~F.col("symbol").isin(*scope) & (F.col("close").isNotNull() | F.col("eligible")))
        | (
            F.col("close").isNotNull()
            & (
                F.col("bar_end").isNull()
                | (F.col("bar_end") != F.col("return_end"))
                | ~F.coalesce(
                    F.col("quality_status").isin("canonical", "canonical_equivalent_duplicate"),
                    F.lit(False),
                )
            )
        )
        | (
            F.col("eligible")
            & (
                ~F.col("expected_eligible")
                | F.col("close").isNull()
                | F.col("reason").isNotNull()
                | F.col("close_to_close_log_return").isNull()
                | F.isnan("close_to_close_log_return")
                | F.col("close_to_close_log_return").isin(float("inf"), float("-inf"))
                | (F.col("observed_bars") != 2)
            )
        )
        | (
            ~F.col("eligible")
            & (
                F.col("close_to_close_log_return").isNotNull()
                | F.col("reason").isNull()
                | (F.trim(F.col("reason")) == "")
            )
        )
    )
    for key, value in {
        "gold_dataset_version": dataset_version,
        "gold_run_id": str(run_id),
        "measurement_version": "2.0.0",
        "silver_dataset_version": upstream.dataset_version,
        "silver_run_id": upstream.run_id,
        "silver_manifest_sha256": upstream.manifest_sha256,
    }.items():
        invalid = invalid | ~F.col(key).eqNullSafe(F.lit(value))
    for field in (
        "sector",
        "session_open",
        "session_close",
        "calendar_provider",
        "calendar_version",
        "schedule_sha256",
        "membership_version",
        "membership_valid_from",
        "membership_valid_to",
    ):
        observed = f"observed_{field}"
        if observed not in rows.columns:
            raise ValueError("sector projection: missing canonical provenance")
        invalid = invalid | (F.col("close").isNotNull() & ~F.col(observed).eqNullSafe(F.col(field)))
    if rows.filter(invalid).limit(1).count():
        raise ValueError("sector projection: invalid financial or canonical provenance")
    counts = rows.agg(
        F.count("event_time").alias("return_records"),
        F.count("close").alias("observed_bar_slots"),
        F.sum(F.col("eligible").cast("int")).alias("eligible_return_records"),
    ).first()
    if counts is None or any(
        type(metadata.get(key)) is not int or metadata[key] != counts[key]
        for key in ("return_records", "observed_bar_slots", "eligible_return_records")
    ):
        raise ValueError("sector projection: manifest coverage count mismatch")
    if metadata.get("expected_return_slots") != counts["return_records"]:
        raise ValueError("sector projection: expected slot count mismatch")
    population = (
        expected.withColumn("acquisition_scope", F.col("symbol").isin(*scope))
        .withColumn("population_policy_version", F.lit(SECTOR_POPULATION_POLICY_VERSION))
        .withColumn("eligibility_policy", F.lit(eligibility_policy))
        .withColumn("eligibility_policy_version", F.lit(SECTOR_ELIGIBILITY_POLICY_VERSION))
        .withColumn("research_reference_sha256", F.lit(sha256(research.path)))
        .withColumn("input_authority", F.lit("approved_expected_gold"))
        .select(*sector_population_schema().fieldNames())
    )
    returns = (
        rows.filter(
            F.col("symbol").isin(*scope)
            & (F.col("return_end") > F.col("session_open") + F.expr("INTERVAL 1 MINUTE"))
        )
        .withColumn(
            "canonical_valid",
            (F.col("observed_bars") == 2)
            & ~F.col("reason").eqNullSafe(F.lit("observation_provenance_mismatch")),
        )
        .withColumnRenamed("eligible", "financial_eligible")
        .withColumnRenamed("individual_reason", "unused_individual_reason")
        .withColumnRenamed("reason", "individual_reason")
        .select(*sector_return_schema().fieldNames())
    )
    coverage = (
        rows.groupBy("sector", "symbol", "session_id")
        .agg(
            F.count("event_time").alias("expected_slots"),
            F.count("close").alias("observed_slots"),
            F.sum(F.col("eligible").cast("int")).alias("upstream_eligible_returns"),
        )
        .withColumn("acquisition_scope", F.col("symbol").isin(*scope))
        .withColumn("missing_slots", F.col("expected_slots") - F.col("observed_slots"))
    )
    # Detect authority/source changes during validation as well as during copying.
    rechecked = resolve_approved_gold(registry, settings.data_root, dataset_version, run_id)
    if rechecked.manifest_sha256 != approved.manifest_sha256:
        raise ValueError("sector projection: input changed during validation")
    return ApprovedSectorProjection(approved, population, returns, coverage)


def calculate_approved_sector_gold(
    spark: "SparkSession",
    settings: Settings,
    *,
    registry: Path,
    silver_registry: Path,
    research_reference: Path,
    dataset_version: str,
    run_id: UUID,
    manifest_sha256: str,
    eligibility_policy: str = DECLARED_RESEARCH_POLICY,
) -> tuple[ApprovedSectorProjection, SectorGoldFrames]:
    """File-owning authority boundary; never accept externally supplied financial frames."""
    from pyspark.sql import functions as F

    projection = project_approved_expected_gold(
        spark,
        settings,
        registry=registry,
        silver_registry=silver_registry,
        research_reference=research_reference,
        dataset_version=dataset_version,
        run_id=run_id,
        manifest_sha256=manifest_sha256,
        eligibility_policy=eligibility_policy,
    )
    calculated = _transform_sector_gold(
        projection.population,
        projection.returns,
        (5, 15, 30, 60),
        input_authority="approved_expected_gold",
    )
    lineage = {
        "input_gold_dataset_version": dataset_version,
        "input_gold_run_id": str(run_id),
        "input_gold_manifest_sha256": projection.gold.manifest_sha256,
        "input_authority": "approved_expected_gold",
        "sector_publication_status": "calculation_only_not_published",
    }
    for name in (
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
        lineage[f"input_{name}"] = _text(dict(projection.gold.metadata), name)

    def bind(frame: "DataFrame") -> "DataFrame":
        for key, value in lineage.items():
            frame = frame.withColumn(key, F.lit(value))
        frame = frame.withColumn(
            "approved_input_identity_sha256",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.lit("sector-approved-input-v1"),
                    F.col("output_identity_sha256"),
                    F.lit(dataset_version),
                    F.lit(str(run_id)),
                    F.lit(manifest_sha256),
                ),
                256,
            ),
        )
        return frame

    return projection, SectorGoldFrames(bind(calculated.windows), bind(calculated.correlations))
