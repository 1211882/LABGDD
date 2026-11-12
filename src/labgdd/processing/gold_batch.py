"""Native Spark transformations for individual-company Gold v2.0.0 measurements."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass(frozen=True)
class GoldFrames:
    returns: "DataFrame"
    rolling: "DataFrame"
    fixed: "DataFrame"


def _validated_expected_series(
    expected_series: "DataFrame",
    required_eligibility_policy: str | None,
    approved_plan_sha256: str,
) -> "DataFrame":
    from pyspark.sql import functions as F

    from labgdd.processing.expected_series_spark import (
        DECLARED_POLICY,
        FINGERPRINT_VERSION,
        POPULATION_POLICY_VERSION,
        STRICT_POLICY,
        expected_identity_expression,
        expected_plan_expression,
        expected_series_schema,
    )

    if expected_series.schema != expected_series_schema():
        raise ValueError("gold transform: invalid expected-series schema or types")
    if len(approved_plan_sha256) != 64 or any(
        character not in "0123456789abcdef" for character in approved_plan_sha256
    ):
        raise ValueError("gold transform: invalid approved expected-plan fingerprint")
    policies = {DECLARED_POLICY, STRICT_POLICY}
    if required_eligibility_policy is not None and required_eligibility_policy not in policies:
        raise ValueError("gold transform: unsupported required eligibility policy")
    mandatory = [field.name for field in expected_series.schema.fields if not field.nullable]
    null_condition = F.col(mandatory[0]).isNull()
    for name in mandatory[1:]:
        null_condition = null_condition | F.col(name).isNull()
    text_fields = [
        field.name
        for field in expected_series.schema.fields
        if field.dataType.typeName() == "string" and field.name != "expected_ineligibility_reason"
    ]
    blank_condition = F.coalesce(F.trim(F.col(text_fields[0])) == "", F.lit(False))
    for name in text_fields[1:]:
        blank_condition = blank_condition | F.coalesce(F.trim(F.col(name)) == "", F.lit(False))
    full_membership = (
        (F.col("session_membership_status") == "full_session")
        & (F.col("membership_valid_from") <= F.col("session_open"))
        & (F.col("membership_valid_to") >= F.col("session_close"))
        & (F.size("membership_change_times") == 0)
    )
    independently_verified = (
        (F.col("evidence_status") == "independently_verified")
        & (F.col("historical_listing_status") == "verified")
        & (F.col("entity_identity_status") == "verified")
    )
    strict = F.col("eligibility_policy") == STRICT_POLICY
    derived_eligible = full_membership & (~strict | independently_verified)
    derived_reason = (
        F.when(~full_membership, F.lit("membership_not_full_session"))
        .when(strict & ~independently_verified, F.lit("unverified_historical_applicability"))
        .otherwise(F.lit(None).cast("string"))
    )
    change_times_valid = F.forall(
        F.col("membership_change_times"),
        lambda value: (value > F.col("session_open")) & (value < F.col("session_close")),
    )
    invalid = expected_series.filter(
        null_condition
        | blank_condition
        | (F.col("source") != "alpaca")
        | (F.col("feed") != "iex")
        | (F.col("timeframe") != "1Min")
        | (F.col("adjustment") != "raw")
        | ~F.col("symbol").rlike(r"^[A-Z][A-Z0-9.-]{0,15}$")
        | ~F.col("session_id").rlike(r"^\d{4}-\d{2}-\d{2}$")
        | ~F.col("session_membership_status").isin(
            "full_session", "partial_session", "membership_change_during_session"
        )
        | ~F.col("evidence_status").isin("declared_research_assumption", "independently_verified")
        | ~F.col("historical_listing_status").isin("unverified", "verified")
        | ~F.col("entity_identity_status").isin("unverified", "verified")
        | ~F.col("sector_classification_status").isin(
            "project_defined_unverified", "independently_verified"
        )
        | ~F.col("eligibility_policy").isin(*sorted(policies))
        | (F.col("expectation_authority") != "approved_research_reference")
        | (F.col("expectation_status") != "approved")
        | (F.col("expected_series_contract_version") != "1.0.0")
        | (F.col("population_policy_version") != POPULATION_POLICY_VERSION)
        | (F.col("fingerprint_version") != FINGERPRINT_VERSION)
        | (F.col("calendar_provider") != "exchange_calendars")
        | (F.col("calendar_name") != "XNYS")
        | (F.col("calendar_timezone") != "America/New_York")
        | (F.col("session_close") <= F.col("session_open"))
        | (F.col("membership_valid_to") <= F.col("membership_valid_from"))
        | ~(
            (F.col("membership_valid_from") < F.col("session_close"))
            & (F.col("session_open") < F.col("membership_valid_to"))
        )
        | ~change_times_valid
        | (
            (F.col("session_membership_status") == "membership_change_during_session")
            & (F.size("membership_change_times") == 0)
        )
        | (
            F.col("session_membership_status").isin("full_session", "partial_session")
            & (F.size("membership_change_times") != 0)
        )
        | ~F.col("expected_eligible").eqNullSafe(derived_eligible)
        | ~F.col("expected_ineligibility_reason").eqNullSafe(derived_reason)
        | ~F.col("schedule_sha256").rlike(r"^[0-9a-f]{64}$")
        | ~F.col("expected_plan_sha256").rlike(r"^[0-9a-f]{64}$")
        | ~F.col("expected_identity_sha256").rlike(r"^[0-9a-f]{64}$")
    )
    if invalid.limit(1).count():
        raise ValueError("gold transform: invalid or unapproved expected series")
    if (
        required_eligibility_policy is not None
        and expected_series.filter(F.col("eligibility_policy") != required_eligibility_policy)
        .limit(1)
        .count()
    ):
        raise ValueError("gold transform: required eligibility policy mismatch")
    computed = expected_series.withColumn("_computed_identity", expected_identity_expression())
    identity = ["symbol", "source", "feed", "timeframe", "adjustment", "session_id"]
    contradictions = (
        computed.groupBy(*identity)
        .agg(F.countDistinct("_computed_identity").alias("definitions"))
        .filter(F.col("definitions") > 1)
    )
    if contradictions.limit(1).count():
        raise ValueError("gold transform: contradictory expected-series identity")
    distinct = computed.dropDuplicates(expected_series.columns)
    if (
        distinct.filter(F.col("expected_identity_sha256") != F.col("_computed_identity"))
        .limit(1)
        .count()
    ):
        raise ValueError("gold transform: expected identity fingerprint mismatch")
    plan_row = distinct.agg(
        expected_plan_expression(F.col("_computed_identity")).alias("computed_plan")
    ).first()
    if plan_row is None or plan_row.computed_plan != approved_plan_sha256:
        raise ValueError("gold transform: expected plan fingerprint mismatch")
    if distinct.filter(F.col("expected_plan_sha256") != approved_plan_sha256).limit(1).count():
        raise ValueError("gold transform: expected plan fingerprint mismatch")
    return distinct.drop("_computed_identity")


def _validated_canonical(canonical: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F
    from pyspark.sql.types import NumericType, StringType, TimestampType

    string_fields = {
        "symbol",
        "sector",
        "source",
        "feed",
        "timeframe",
        "adjustment",
        "session_id",
        "calendar_provider",
        "calendar_version",
        "schedule_sha256",
        "membership_version",
        "selected_observation_id",
        "quality_status",
    }
    timestamp_fields = {
        "event_time",
        "bar_end",
        "session_open",
        "session_close",
        "membership_valid_from",
        "membership_valid_to",
    }
    required = string_fields | timestamp_fields | {"open", "close"}
    if not required.issubset(canonical.columns):
        raise ValueError("gold transform: incomplete canonical schema")
    fields = {field.name: field for field in canonical.schema.fields}
    if any(not isinstance(fields[name].dataType, StringType) for name in string_fields) or any(
        not isinstance(fields[name].dataType, TimestampType) for name in timestamp_fields
    ):
        raise ValueError("gold transform: invalid canonical field types")
    if any(not isinstance(fields[name].dataType, NumericType) for name in ("open", "close")):
        raise ValueError("gold transform: invalid canonical price types")
    ordered_required = sorted(required)
    null_condition = F.col(ordered_required[0]).isNull()
    for name in ordered_required[1:]:
        null_condition = null_condition | F.col(name).isNull()
    ordered_strings = sorted(string_fields)
    blank_condition = F.coalesce(F.trim(F.col(ordered_strings[0])) == "", F.lit(False))
    for name in ordered_strings[1:]:
        blank_condition = blank_condition | F.coalesce(F.trim(F.col(name)) == "", F.lit(False))
    invalid = canonical.filter(
        null_condition
        | blank_condition
        | (F.col("source") != "alpaca")
        | (F.col("feed") != "iex")
        | (F.col("timeframe") != "1Min")
        | (F.col("adjustment") != "raw")
        | (F.col("calendar_provider") != "exchange_calendars")
        | ~F.col("symbol").rlike(r"^[A-Z][A-Z0-9.-]{0,15}$")
        | ~F.col("session_id").rlike(r"^\d{4}-\d{2}-\d{2}$")
        | ~F.col("selected_observation_id").rlike(r"^[0-9a-f]{64}$")
        | ~F.col("schedule_sha256").rlike(r"^[0-9a-f]{64}$")
        | ~F.col("quality_status").isin("canonical", "canonical_equivalent_duplicate")
        | (F.col("session_close") <= F.col("session_open"))
        | (F.col("membership_valid_to") <= F.col("membership_valid_from"))
        | (F.col("event_time") < F.col("session_open"))
        | (F.col("event_time") >= F.col("session_close"))
        | (F.pmod(F.unix_micros("event_time"), F.lit(60_000_000)) != 0)
        | (F.pmod(F.unix_micros("session_open"), F.lit(60_000_000)) != 0)
        | (F.pmod(F.unix_micros("session_close"), F.lit(60_000_000)) != 0)
        | (F.col("bar_end") != F.col("event_time") + F.expr("INTERVAL 1 MINUTE"))
        | (F.col("bar_end") > F.col("session_close"))
        | (F.col("membership_valid_from") > F.col("event_time"))
        | (F.col("event_time") >= F.col("membership_valid_to"))
        | (F.col("open") <= 0)
        | (F.col("close") <= 0)
    )
    if invalid.limit(1).count():
        raise ValueError("gold transform: invalid canonical observation")
    logical_key = ["source", "feed", "symbol", "timeframe", "event_time", "adjustment"]
    duplicates = canonical.groupBy(*logical_key).count().filter(F.col("count") > 1)
    if duplicates.limit(1).count():
        raise ValueError("gold transform: duplicate canonical logical key")
    return canonical


def transform_individual_gold(
    canonical: "DataFrame",
    horizons: tuple[int, ...],
    expected_series: "DataFrame | None" = None,
    *,
    required_eligibility_policy: str | None = None,
    approved_expected_plan_sha256: str | None = None,
) -> GoldFrames:
    """Calculate Gold from observed series or an explicitly supplied expectation authority."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if not horizons or any(value not in {5, 15, 30, 60} for value in horizons):
        raise ValueError("gold transform: unsupported horizon")
    observed_series_keys = [
        "symbol",
        "sector",
        "source",
        "feed",
        "timeframe",
        "adjustment",
        "session_id",
        "session_open",
        "session_close",
        "calendar_provider",
        "calendar_version",
        "schedule_sha256",
        "membership_version",
    ]
    authoritative_expected = expected_series is not None
    if expected_series is None:
        if required_eligibility_policy is not None or approved_expected_plan_sha256 is not None:
            raise ValueError("gold transform: expected authority arguments require expected series")
        series = canonical.select(*observed_series_keys).distinct()
        series_keys = observed_series_keys
    else:
        if approved_expected_plan_sha256 is None:
            raise ValueError("gold transform: approved expected-plan fingerprint required")
        canonical = _validated_canonical(canonical)
        series = _validated_expected_series(
            expected_series,
            required_eligibility_policy,
            approved_expected_plan_sha256,
        )
        series_keys = series.columns
    expected = series.select(
        "*",
        F.explode(
            F.sequence(
                F.col("session_open"),
                F.col("session_close") - F.expr("INTERVAL 1 MINUTE"),
                F.expr("INTERVAL 1 MINUTE"),
            )
        ).alias("event_time"),
    )
    if authoritative_expected:
        observation_keys = [
            "symbol",
            "source",
            "feed",
            "timeframe",
            "adjustment",
            "session_id",
            "event_time",
        ]
        observed_provenance = [
            "sector",
            "session_open",
            "session_close",
            "calendar_provider",
            "calendar_version",
            "schedule_sha256",
            "membership_version",
            "membership_valid_from",
            "membership_valid_to",
        ]
        bar_fields = canonical.select(
            *observation_keys,
            *(F.col(name).alias(f"observed_{name}") for name in observed_provenance),
            "bar_end",
            "open",
            "close",
            "selected_observation_id",
            "quality_status",
        )
        joined = expected.join(bar_fields, observation_keys, "left")
        mismatch_terms = [
            ~F.col(f"observed_{name}").eqNullSafe(F.col(name)) for name in observed_provenance
        ]
        mismatch = F.col("close").isNotNull() & mismatch_terms[0]
        for term in mismatch_terms[1:]:
            mismatch = mismatch | (F.col("close").isNotNull() & term)
        expected_reason = F.when(
            ~F.col("expected_eligible"), F.col("expected_ineligibility_reason")
        ).when(mismatch, F.lit("observation_provenance_mismatch"))
        intrabar_allowed = F.col("close").isNotNull() & expected_reason.isNull()
        membership_cross = F.lit(False)
    else:
        bar_fields = canonical.select(
            *series_keys,
            "event_time",
            "bar_end",
            "open",
            "close",
            "selected_observation_id",
            "membership_valid_from",
            "membership_valid_to",
            "quality_status",
        )
        joined = expected.join(bar_fields, [*series_keys, "event_time"], "left")
        expected_reason = F.lit(None).cast("string")
        intrabar_allowed = F.col("close").isNotNull()
        membership_cross = (
            F.col("membership_valid_from")
            != F.lag("membership_valid_from").over(
                Window.partitionBy(*series_keys).orderBy("event_time")
            )
        ) | (
            F.col("membership_valid_to")
            != F.lag("membership_valid_to").over(
                Window.partitionBy(*series_keys).orderBy("event_time")
            )
        )
    order = Window.partitionBy(*series_keys).orderBy("event_time")
    close_return = F.log(F.col("close").cast("double") / F.lag("close").over(order).cast("double"))
    index = (
        (F.unix_micros("event_time") - F.unix_micros("session_open")) / F.lit(60_000_000)
    ).cast("int")
    reason = (
        F.when(expected_reason.isNotNull(), expected_reason)
        .when(F.col("close").isNull(), F.lit("missing_close"))
        .when(index == 0, F.lit("session_boundary"))
        .when(F.lag("close").over(order).isNull(), F.lit("nonconsecutive_bars"))
        .when(membership_cross, F.lit("membership_boundary"))
    )
    returns = (
        joined.withColumn("minute_index", index)
        .withColumn("return_end", F.col("event_time") + F.expr("INTERVAL 1 MINUTE"))
        .withColumn(
            "intrabar_log_return",
            F.when(
                intrabar_allowed,
                F.log(F.col("close").cast("double") / F.col("open").cast("double")),
            ),
        )
        .withColumn("reason", reason)
        .withColumn("close_to_close_log_return", F.when(F.col("reason").isNull(), close_return))
        .withColumn("eligible", F.col("close_to_close_log_return").isNotNull())
        .withColumn("expected_bars", F.lit(2))
        .withColumn(
            "observed_bars",
            F.when(F.col("close").isNull(), F.lit(0))
            .when((index == 0) | F.lag("close").over(order).isNull(), F.lit(1))
            .otherwise(F.lit(2)),
        )
    )
    lineage = [
        column
        for column in series_keys
        if column not in {"membership_valid_from", "membership_valid_to"}
    ]
    rolling_parts = []
    fixed_parts = []
    adjacent = returns.filter(F.col("minute_index") >= 1)
    for horizon in horizons:
        trailing = (
            Window.partitionBy(*series_keys).orderBy("minute_index").rowsBetween(-(horizon - 1), 0)
        )
        if authoritative_expected:
            rolling_reason = F.when(
                ~F.col("expected_eligible"), F.col("expected_ineligibility_reason")
            ).when(F.col("minute_index") < horizon, F.lit("insufficient_history"))
        else:
            rolling_reason = F.when(F.col("minute_index") < horizon, F.lit("insufficient_history"))
        rolling_reason = (
            rolling_reason.when(
                F.col("provenance_mismatches") > 0,
                F.lit("observation_provenance_mismatch"),
            )
            .when(F.col("membership_boundaries") > 0, F.lit("membership_boundary"))
            .when(F.col("observed_returns") < horizon, F.lit("incomplete_return_window"))
        )
        rolling_parts.append(
            adjacent.withColumn("horizon", F.lit(horizon))
            .withColumn("expected_returns", F.lit(horizon))
            .withColumn("observed_returns", F.count("close_to_close_log_return").over(trailing))
            .withColumn("membership_valid_from", F.min("membership_valid_from").over(trailing))
            .withColumn("membership_valid_to", F.max("membership_valid_to").over(trailing))
            .withColumn(
                "membership_boundaries",
                F.sum(F.when(F.col("reason") == "membership_boundary", 1).otherwise(0)).over(
                    trailing
                ),
            )
            .withColumn(
                "provenance_mismatches",
                F.sum(
                    F.when(F.col("reason") == "observation_provenance_mismatch", 1).otherwise(0)
                ).over(trailing),
            )
            .withColumn("window_start", F.col("return_end") - F.expr(f"INTERVAL {horizon} MINUTES"))
            .withColumn("window_end", F.col("return_end"))
            .withColumn("reason", rolling_reason)
            .withColumn("eligible", F.col("reason").isNull())
            .withColumn(
                "rolling_return_volatility",
                F.when(
                    F.col("eligible"), F.stddev_samp("close_to_close_log_return").over(trailing)
                ),
            )
            .select(
                *lineage,
                "horizon",
                "window_start",
                "window_end",
                "expected_returns",
                "observed_returns",
                "membership_valid_from",
                "membership_valid_to",
                "rolling_return_volatility",
                "eligible",
                "reason",
            )
        )
        window_index = F.floor((F.col("minute_index") - F.lit(1)) / F.lit(horizon)).cast("int")
        complete_count = F.floor(
            (
                (F.unix_micros("session_close") - F.unix_micros("session_open")) / F.lit(60_000_000)
                - F.lit(1)
            )
            / F.lit(horizon)
        ).cast("int")
        aggregations = [
            F.count("close_to_close_log_return").alias("observed_returns"),
            F.sum(F.pow("close_to_close_log_return", 2.0)).alias("variance_sum"),
            F.max(
                F.when(F.col("reason") == "membership_boundary", F.lit(1)).otherwise(F.lit(0))
            ).alias("membership_boundary"),
            F.max(
                F.when(F.col("reason") == "observation_provenance_mismatch", F.lit(1)).otherwise(
                    F.lit(0)
                )
            ).alias("provenance_mismatch"),
        ]
        if not authoritative_expected:
            aggregations.extend(
                [
                    F.min("membership_valid_from").alias("membership_valid_from"),
                    F.max("membership_valid_to").alias("membership_valid_to"),
                ]
            )
        grouped = (
            adjacent.withColumn("window_index", window_index)
            .withColumn("complete_count", complete_count)
            .filter(F.col("window_index") < F.col("complete_count"))
            .groupBy(*series_keys, "window_index")
            .agg(*aggregations)
        )
        if authoritative_expected:
            fixed_reason = (
                F.when(~F.col("expected_eligible"), F.col("expected_ineligibility_reason"))
                .when(
                    F.col("provenance_mismatch") == 1,
                    F.lit("observation_provenance_mismatch"),
                )
                .when(F.col("membership_boundary") == 1, F.lit("membership_boundary"))
            )
        else:
            fixed_reason = F.when(
                F.col("provenance_mismatch") == 1,
                F.lit("observation_provenance_mismatch"),
            ).when(F.col("membership_boundary") == 1, F.lit("membership_boundary"))
        fixed_reason = fixed_reason.when(
            F.col("observed_returns") < horizon, F.lit("incomplete_fixed_window")
        )
        fixed_parts.append(
            grouped.withColumn("horizon", F.lit(horizon))
            .withColumn("expected_returns", F.lit(horizon))
            .withColumn(
                "window_start",
                F.col("session_open")
                + F.expr("INTERVAL 1 MINUTE")
                + F.col("window_index") * F.expr(f"INTERVAL {horizon} MINUTES"),
            )
            .withColumn("window_end", F.col("window_start") + F.expr(f"INTERVAL {horizon} MINUTES"))
            .withColumn("reason", fixed_reason)
            .withColumn("eligible", F.col("reason").isNull())
            .withColumn("realized_variance", F.when(F.col("eligible"), F.col("variance_sum")))
            .withColumn("realized_volatility", F.when(F.col("eligible"), F.sqrt("variance_sum")))
            .select(
                *lineage,
                "horizon",
                "window_start",
                "window_end",
                "expected_returns",
                "observed_returns",
                "membership_valid_from",
                "membership_valid_to",
                "realized_variance",
                "realized_volatility",
                "eligible",
                "reason",
            )
        )
    rolling = rolling_parts[0]
    fixed = fixed_parts[0]
    for frame in rolling_parts[1:]:
        rolling = rolling.unionByName(frame)
    for frame in fixed_parts[1:]:
        fixed = fixed.unionByName(frame)
    return GoldFrames(returns.drop("minute_index"), rolling, fixed)
