"""Read-only readiness evidence; deliberately contains no publication capability."""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID

from labgdd.adapters.bronze import sha256
from labgdd.adapters.gold import resolve_approved_gold
from labgdd.adapters.silver import resolve_approved_silver
from labgdd.analytics.sector import ELIGIBILITY_POLICIES
from labgdd.config import Settings
from labgdd.processing.approved_sector import calculate_approved_sector_gold
from labgdd.processing.expected_gold import _source_sha256
from labgdd.processing.sector_publication import _validate_expected_grid

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def _summarize(frame: "DataFrame") -> list[dict[str, Any]]:
    """Force financial evaluation despite Spark projection pruning."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType

    invalid = (
        F.col("eligible").isNull()
        | (F.col("eligible") & F.col("reason").isNotNull())
        | (~F.col("eligible") & F.col("reason").isNull())
    )
    for field in frame.schema:
        if isinstance(field.dataType, DoubleType):
            value = F.col(field.name)
            invalid = invalid | F.isnan(value) | value.isin(float("inf"), float("-inf"))
            invalid = invalid | (~F.col("eligible") & value.isNotNull())
            if field.name in {
                "sector_realized_variance",
                "sector_realized_volatility",
                "portfolio_realized_variance",
                "portfolio_realized_volatility",
                "mean_log_return_realized_volatility",
                "volatility_dispersion",
                "value",
            }:
                invalid = invalid | (F.col("eligible") & value.isNull())
    rows = [
        row.asDict()
        for row in frame.groupBy("sector", "session_id", "horizon", "eligible", "reason")
        .agg(
            F.count("*").alias("count"),
            F.sum(F.when(invalid, 1).otherwise(0)).alias("invalid_metrics"),
        )
        .collect()
    ]
    invalid_counts = [row.pop("invalid_metrics") for row in rows]
    if any(invalid_counts):
        raise ValueError("sector readiness: invalid financial values or nullability")
    return sorted(rows, key=lambda row: json.dumps(row, sort_keys=True))


def _validate_inventory(population: "DataFrame", windows: "DataFrame", pairs: "DataFrame") -> None:
    """Check the independently expected window grid and every ordered pair."""
    from pyspark.sql import functions as F

    _validate_expected_grid(windows, population)
    identity = "output_identity_sha256"
    if (
        windows.filter(F.col(identity).isNull()).limit(1).count()
        or windows.groupBy(identity).count().filter("count != 1").limit(1).count()
    ):
        raise ValueError("sector readiness: duplicate or missing window identity")
    expected = (
        windows.select(identity, "expected_symbols")
        .select(identity, "expected_symbols", F.explode("expected_symbols").alias("left_symbol"))
        .select(identity, "left_symbol", F.explode("expected_symbols").alias("right_symbol"))
    )
    expected = expected.filter("left_symbol < right_symbol")
    actual = pairs.select(*expected.columns)
    if expected.exceptAll(actual).limit(1).count() or actual.exceptAll(expected).limit(1).count():
        raise ValueError("sector readiness: incomplete or duplicate pair inventory")


@dataclass(frozen=True, slots=True)
class SectorReadinessReport:
    """Canonical JSON contains bounded reference/aggregate evidence, never prices."""

    contract_version: str
    dataset_version: str
    run_id: str
    manifest_sha256: str
    technical_gate: str
    scientific_gate: str
    governance_gate: str
    decision: str
    blockers: tuple[str, ...]
    evidence_json: str

    def evidence(self) -> dict[str, Any]:
        return dict(json.loads(self.evidence_json))


def preflight_real_sector_gold(
    spark: "SparkSession",
    settings: Settings,
    *,
    registry: Path,
    silver_registry: Path,
    research_reference: Path,
    dataset_version: str,
    run_id: UUID,
    manifest_sha256: str,
    eligibility_policy: str = "declared_research",
) -> SectorReadinessReport:
    """Evaluate the entire explicitly selected run; invalid authority raises.

    No candidate, Parquet, report file or registry is written. Spark's private
    checkpoint/spill storage is still required by the accepted projection.
    """
    from pyspark.sql import functions as F

    if eligibility_policy not in ELIGIBILITY_POLICIES:
        raise ValueError("sector readiness: unsupported eligibility policy")
    before = _source_sha256()
    approved = resolve_approved_gold(registry, settings.data_root, dataset_version, run_id)
    metadata = dict(approved.metadata)
    if approved.manifest_sha256 != manifest_sha256:
        raise ValueError("sector readiness: input fingerprint mismatch")
    # Bound every driver-side reference/aggregate collection before calculation.
    size = metadata.get("expected_plan_rows")
    if type(size) is not int or not 0 < size <= 1000:
        raise ValueError("sector readiness: bounded population exceeded")
    projection, frames = calculate_approved_sector_gold(
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
    coverage = [row.asDict() for row in projection.coverage.collect()]
    coverage.sort(key=lambda row: (row["sector"], row["session_id"], row["symbol"]))
    population = projection.population
    historical = population.groupBy("sector", "session_id").agg(
        F.count("symbol").alias("expected_members"),
        F.sum(F.col("acquisition_scope").cast("long")).alias("requested_members"),
        F.sum(
            (
                (F.col("evidence_status") == "independently_verified")
                & (F.col("historical_listing_status") == "verified")
                & (F.col("entity_identity_status") == "verified")
                & (F.col("sector_classification_status") == "independently_verified")
            ).cast("long")
        ).alias("historically_verified_members"),
        F.sum((F.col("session_membership_status") == "full_session").cast("long")).alias(
            "applicable_members"
        ),
    )
    groups = sorted(
        (r.asDict() for r in historical.collect()),
        key=lambda row: (row["sector"], row["session_id"]),
    )
    for group in groups:
        members = [
            row
            for row in coverage
            if (row["sector"], row["session_id"]) == (group["sector"], group["session_id"])
        ]
        group["observed_members"] = sum(row["observed_slots"] > 0 for row in members)
        group["fully_observed_members"] = sum(
            row["observed_slots"] == row["expected_slots"] for row in members
        )
        group["upstream_eligible_members"] = sum(
            row["upstream_eligible_returns"] > 0 for row in members
        )
    windows = frames.windows.cache()
    try:
        _validate_inventory(population, windows, frames.correlations)
        summaries = _summarize(windows)
        pair_summaries = _summarize(frames.correlations)
    finally:
        windows.unpersist()
    blockers: list[str] = []
    if any(not row["acquisition_scope"] for row in coverage):
        blockers.append("blocked_incomplete_acquisition_scope")
    if any(row["acquisition_scope"] and row["missing_slots"] for row in coverage):
        blockers.append("blocked_missing_observations")
    if eligibility_policy == "verified_historical_applicability" and any(
        row["historically_verified_members"] != row["expected_members"] for row in groups
    ):
        blockers.append("blocked_unverified_historical_applicability")
    if any(not row["eligible"] for row in summaries) or not summaries:
        blockers.append("blocked_ineligible_sector_windows")
    if any(not row["eligible"] for row in pair_summaries) or not pair_summaries:
        blockers.append("blocked_incomplete_pair_eligibility")
    scientific = "passed" if not blockers else "blocked"
    # There is no authorized real publisher/approval policy in this milestone.
    blockers.append("blocked_unsupported_publication_policy")
    session_bounds = sorted(
        (
            {
                "session_id": row.session_id,
                "session_open": row.session_open,
                "session_close": row.session_close,
            }
            for row in population.select(
                "session_id",
                F.date_format("session_open", "yyyy-MM-dd'T'HH:mm:ssXXX").alias("session_open"),
                F.date_format("session_close", "yyyy-MM-dd'T'HH:mm:ssXXX").alias("session_close"),
            )
            .distinct()
            .collect()
        ),
        key=lambda row: row["session_id"],
    )
    evidence = {
        "input_metadata": metadata,
        "processing_source_sha256": before,
        "eligibility_policy": eligibility_policy,
        "eligibility_policy_version": "sector-eligibility-policy-v1",
        "population_policy_version": "sector-complete-population-v1",
        "calculation_version": "sector-calculation-v1",
        "future_sector_schema_version": "1.0.0",
        "scope": groups,
        "session_bounds": session_bounds,
        "coverage": coverage,
        "window_reason_counts": summaries,
        "pair_reason_counts": pair_summaries,
        "expected_slots": sum(r["expected_slots"] for r in coverage),
        "observed_slots": sum(r["observed_slots"] for r in coverage),
        "outside_acquisition_slots": sum(
            r["expected_slots"] for r in coverage if not r["acquisition_scope"]
        ),
        "requested_missing_slots": sum(
            r["missing_slots"] for r in coverage if r["acquisition_scope"]
        ),
        "warnings": ["historical_research_classification_not_independently_verified"]
        if any(r["historically_verified_members"] != r["expected_members"] for r in groups)
        else [],
        "limitations": ["iex_venue_specific_not_consolidated", "provider_finality_unproven"],
        "evidence_kind": metadata.get("evidence_kind", "unspecified"),
    }
    if evidence["evidence_kind"] == "unspecified":
        evidence["warnings"].append("evidence_kind_not_attested")
    rechecked = resolve_approved_gold(registry, settings.data_root, dataset_version, run_id)
    upstream = resolve_approved_silver(
        silver_registry,
        settings.data_root,
        str(metadata["silver_dataset_version"]),
        UUID(str(metadata["silver_run_id"])),
    )
    if (
        upstream.manifest_sha256 != metadata["silver_manifest_sha256"]
        or sha256(research_reference) != metadata["research_reference_sha256"]
    ):
        raise ValueError("sector readiness: upstream evidence changed")
    if rechecked.manifest_sha256 != manifest_sha256 or before != _source_sha256():
        raise ValueError("sector readiness: evidence changed during preflight")
    return SectorReadinessReport(
        "sector-real-publication-readiness-v1",
        dataset_version,
        str(run_id),
        manifest_sha256,
        "passed",
        scientific,
        "blocked",
        blockers[0],
        tuple(blockers),
        json.dumps(evidence, sort_keys=True, allow_nan=False),
    )
