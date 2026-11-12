"""Approved expected-series plan to immutable pending-review Gold publication."""

import hashlib
import importlib.metadata
import json
import platform
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID, uuid4

from labgdd.adapters.bronze import sha256
from labgdd.adapters.calendar import CalendarSnapshot, xnys_snapshot
from labgdd.adapters.gold import GoldRun
from labgdd.adapters.silver import ApprovedSilver, resolve_silver_input
from labgdd.config import Settings
from labgdd.expected_gold_config import ExpectedGoldRequest
from labgdd.processing.expected_series import generate_xnys_expected_series
from labgdd.processing.expected_series_spark import (
    STRICT_POLICY,
    expected_plan_fingerprint,
    expected_series_frame,
)
from labgdd.processing.gold import GoldProcessingError, GoldResult, _validate, _write
from labgdd.processing.gold_batch import transform_individual_gold
from labgdd.research.universe import ExpectedSeriesPlan, ResearchUniverse
from labgdd.research_config import load_research_universe

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass(frozen=True, slots=True)
class PreparedExpectedGold:
    approved_silver: ApprovedSilver
    universe: ResearchUniverse
    plan: ExpectedSeriesPlan
    calendar: CalendarSnapshot
    research_reference_sha256: str
    approved_plan_sha256: str
    configuration_sha256: str


def _portable(path: Path, project_root: Path) -> str:
    return path.resolve().relative_to(project_root.resolve()).as_posix()


def prepare_expected_gold(settings: Settings, request: ExpectedGoldRequest) -> PreparedExpectedGold:
    """Resolve all read-only authorities without starting Spark or creating storage."""
    approved = resolve_silver_input(
        request.silver_registry,
        settings.data_root,
        request.silver_dataset_version,
        request.silver_run_id,
        request.silver_input_authority,
    )
    reference = load_research_universe(request.research_universe, settings)
    if reference.universe.universe_version != request.research_universe_version:
        raise ValueError("expected Gold: research universe version mismatch")
    labels = tuple(date.fromisoformat(value) for value in request.requested_sessions)
    calendar = xnys_snapshot(min(labels), max(labels))
    plan = generate_xnys_expected_series(reference.universe, calendar, request.requested_sessions)
    if any(row.universe_version != request.research_universe_version for row in plan.rows):
        raise ValueError("expected Gold: plan universe version mismatch")
    strict = request.eligibility_policy == STRICT_POLICY
    plan_sha256 = expected_plan_fingerprint(
        plan,
        source="alpaca",
        feed="iex",
        timeframe="1Min",
        adjustment="raw",
        strict_historical=strict,
    )
    reference_sha256 = sha256(reference.path)
    project_root = request.path.parent.parent
    configuration = {
        "dataset_version": request.dataset_version,
        "silver_registry": _portable(request.silver_registry, project_root),
        "silver_dataset_version": request.silver_dataset_version,
        "silver_run_id": str(request.silver_run_id),
        "silver_input_authority": request.silver_input_authority,
        "acquisition_scope_symbols": list(request.acquisition_scope_symbols),
        "research_universe": _portable(request.research_universe, project_root),
        "research_universe_version": request.research_universe_version,
        "research_reference_sha256": reference_sha256,
        "requested_sessions": list(request.requested_sessions),
        "expected_series_contract_version": request.expected_series_contract_version,
        "population_policy_version": request.population_policy_version,
        "eligibility_policy": request.eligibility_policy,
        "measurement_version": request.measurement_version,
        "horizons": list(request.horizons),
        "gold_schema_version": request.gold_schema_version,
    }
    configuration_sha256 = hashlib.sha256(
        json.dumps(configuration, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return PreparedExpectedGold(
        approved,
        reference.universe,
        plan,
        calendar,
        reference_sha256,
        plan_sha256,
        configuration_sha256,
    )


def _source_sha256() -> str:
    package_root = Path(__file__).resolve().parents[1]
    digest = hashlib.sha256()
    for source in sorted(package_root.rglob("*.py")):
        digest.update(source.relative_to(package_root).as_posix().encode())
        digest.update(b"\0")
        digest.update(source.read_bytes())
    return digest.hexdigest()


def _reason_counts(frame: "DataFrame") -> dict[str, int]:
    rows = frame.groupBy("reason").count().collect()
    return {
        "eligible" if row.reason is None else str(row.reason): int(row["count"]) for row in rows
    }


def _validate_financial_nulls(frames: tuple["DataFrame", ...]) -> None:
    from pyspark.sql import functions as F

    returns, rolling, fixed = frames
    invalid_returns = returns.filter(
        (~F.col("expected_eligible") | F.col("close").isNull())
        & (
            F.col("intrabar_log_return").isNotNull()
            | F.col("close_to_close_log_return").isNotNull()
        )
    )
    invalid_rolling = rolling.filter(
        (~F.col("expected_eligible") | (F.col("observed_returns") < F.col("expected_returns")))
        & F.col("rolling_return_volatility").isNotNull()
    )
    invalid_fixed = fixed.filter(
        (~F.col("expected_eligible") | (F.col("observed_returns") < F.col("expected_returns")))
        & (F.col("realized_variance").isNotNull() | F.col("realized_volatility").isNotNull())
    )
    if any(frame.limit(1).count() for frame in (invalid_returns, invalid_rolling, invalid_fixed)):
        raise ValueError("expected Gold validation: ineligible financial value")


def process_expected_gold(
    settings: Settings,
    request: ExpectedGoldRequest,
    spark: object,
    *,
    run_id: UUID | None = None,
    prepared: PreparedExpectedGold | None = None,
) -> GoldResult:
    """Execute and immutably publish the separately identified expected Gold contract."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    if not isinstance(spark, SparkSession):
        raise TypeError("expected Gold processing: SparkSession required")
    authority = prepared or prepare_expected_gold(settings, request)
    identity = run_id or uuid4()
    calendar_metadata = authority.calendar.metadata
    memberships = authority.universe.memberships
    membership_summary = {
        "membership_version": authority.universe.membership_version,
        "valid_from": min(row.valid_from for row in memberships).isoformat(),
        "valid_to": max(row.valid_to for row in memberships).isoformat(),
        "applicability_basis": sorted({row.applicability_basis for row in memberships}),
        "evidence_status": sorted({row.evidence_status for row in memberships}),
        "historical_listing_status": sorted({row.historical_listing_status for row in memberships}),
        "entity_identity_status": sorted({row.entity_identity_status for row in memberships}),
        "sector_classification_status": sorted(
            {row.sector_classification_status for row in memberships}
        ),
    }
    metadata: dict[str, object] = {
        "manifest_version": "1.0.0",
        "gold_schema_version": request.gold_schema_version,
        "measurement_version": request.measurement_version,
        "dataset_version": request.dataset_version,
        "run_id": str(identity),
        "started_at": datetime.now(UTC).isoformat(),
        "publication_status": "pending_architecture_review",
        "source": "alpaca",
        "feed": "iex",
        "timeframe": "1Min",
        "adjustment": "raw",
        "horizons": list(request.horizons),
        "requested_sessions": list(request.requested_sessions),
        "silver_dataset_version": authority.approved_silver.dataset_version,
        "silver_run_id": authority.approved_silver.run_id,
        "silver_manifest_sha256": authority.approved_silver.manifest_sha256,
        "silver_input_authority": authority.approved_silver.authority_status,
        "acquisition_scope_symbols": list(request.acquisition_scope_symbols),
        "research_universe_version": authority.universe.universe_version,
        "research_reference_sha256": authority.research_reference_sha256,
        "approved_expected_plan_sha256": authority.approved_plan_sha256,
        "expected_series_contract_version": request.expected_series_contract_version,
        "population_policy_version": request.population_policy_version,
        "eligibility_policy": request.eligibility_policy,
        "calendar_provider": calendar_metadata["provider"],
        "calendar_version": calendar_metadata["version"],
        "calendar_name": calendar_metadata["calendar"],
        "calendar_timezone": calendar_metadata["timezone"],
        "tzdata_version": calendar_metadata["tzdata_version"],
        "schedule_sha256": calendar_metadata["schedule_sha256"],
        "membership": membership_summary,
        "processing_source_sha256": _source_sha256(),
        "configuration_sha256": authority.configuration_sha256,
        "publication": "local_staging_validate_manifest_then_atomic_directory_rename",
        "environment_versions": {
            "labgdd-v2": importlib.metadata.version("labgdd-v2"),
            "pyspark": importlib.metadata.version("pyspark"),
            "exchange_calendars": importlib.metadata.version("exchange_calendars"),
            "pyarrow": importlib.metadata.version("pyarrow"),
            "tzdata": importlib.metadata.version("tzdata"),
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
        canonical = spark.read.parquet(str(authority.approved_silver.manifest.parent / "canonical"))
        canonical_count = canonical.count()
        stage = "expected_authority"
        expected = expected_series_frame(
            spark,
            authority.plan,
            source="alpaca",
            feed="iex",
            timeframe="1Min",
            adjustment="raw",
            strict_historical=request.eligibility_policy == STRICT_POLICY,
        )
        stage = "transform"
        result = transform_individual_gold(
            canonical,
            request.horizons,
            expected,
            required_eligibility_policy=request.eligibility_policy,
            approved_expected_plan_sha256=authority.approved_plan_sha256,
        )
        lineage = {
            "gold_dataset_version": request.dataset_version,
            "gold_run_id": str(identity),
            "measurement_version": request.measurement_version,
            "silver_dataset_version": authority.approved_silver.dataset_version,
            "silver_run_id": authority.approved_silver.run_id,
            "silver_manifest_sha256": authority.approved_silver.manifest_sha256,
            "publication_revision": "1",
            "publication_status": "pending_architecture_review",
            "finalization_reason": "bounded_snapshot",
            "source_completeness": "unproven",
        }

        def with_lineage(frame: "DataFrame") -> "DataFrame":
            for name, value in lineage.items():
                frame = frame.withColumn(name, F.lit(value))
            return frame

        persisted = [
            with_lineage(result.returns).cache(),
            with_lineage(result.rolling).cache(),
            with_lineage(result.fixed).cache(),
        ]
        counts_tuple = tuple(frame.count() for frame in persisted)
        _validate_financial_nulls(tuple(persisted))
        stage = "write_outputs"
        _write(tuple(persisted), storage.path, ("session_id",))
        stage = "validate_outputs"
        schemas = _validate(
            spark,
            tuple(persisted),
            storage.path,
            counts_tuple,
            expected_authority=True,
        )
        returns, rolling, fixed = persisted
        expected_minutes = sum(
            int((row.session_close - row.session_open).total_seconds() // 60)
            for row in authority.plan.rows
        )
        counts = {
            "expected_symbols": len({row.symbol for row in authority.plan.rows}),
            "expected_sessions": len(authority.plan.scheduled_sessions),
            "expected_plan_rows": len(authority.plan.rows),
            "expected_return_slots": expected_minutes,
            "return_records": counts_tuple[0],
            "rolling_records": counts_tuple[1],
            "fixed_records": counts_tuple[2],
            "canonical_observations": canonical_count,
            "observed_bar_slots": returns.filter(F.col("close").isNotNull()).count(),
            "observed_symbols": returns.filter(F.col("close").isNotNull())
            .select("symbol")
            .distinct()
            .count(),
            "eligible_return_records": returns.filter("eligible").count(),
            "eligible_rolling_records": rolling.filter("eligible").count(),
            "eligible_fixed_records": fixed.filter("eligible").count(),
        }
        stage = "publish_complete"
        manifest = storage.finish(
            "complete",
            {
                **counts,
                "quality_reason_counts": {
                    "returns": _reason_counts(returns),
                    "rolling": _reason_counts(rolling),
                    "fixed": _reason_counts(fixed),
                },
                "schemas": schemas,
                "partitioning": ["session_id"],
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
        raise GoldProcessingError("expected Gold processing failed") from failure
    finally:
        for frame in persisted:
            frame.unpersist()
