"""Native Spark sector transformations for measurement specification v2.0.0."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

from labgdd.analytics.sector import (
    CONSTANT_SERIES,
    DECLARED_RESEARCH_POLICY,
    ELIGIBILITY_POLICIES,
    INCOMPLETE_CALIBRATION,
    INCOMPLETE_POPULATION,
    INELIGIBLE_INDIVIDUAL_RETURN,
    INSUFFICIENT_PAIRS,
    MISSING_EXPECTED_MEMBER,
    MISSING_OBSERVED_BAR,
    OUTSIDE_ACQUISITION_SCOPE,
    SECTOR_ELIGIBILITY_POLICY_VERSION,
    SECTOR_OUTPUT_IDENTITY_VERSION,
    SECTOR_POPULATION_POLICY_VERSION,
    UNVERIFIED_HISTORY,
    VERIFIED_HISTORICAL_POLICY,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


@dataclass(frozen=True)
class SectorGoldFrames:
    windows: "DataFrame"
    correlations: "DataFrame"


def sector_population_schema() -> "StructType":
    from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType

    text = StringType()
    timestamp = TimestampType()
    required = False
    return StructType(
        [
            StructField("symbol", text, required),
            StructField("sector", text, required),
            StructField("source", text, required),
            StructField("feed", text, required),
            StructField("timeframe", text, required),
            StructField("adjustment", text, required),
            StructField("session_id", text, required),
            StructField("session_open", timestamp, required),
            StructField("session_close", timestamp, required),
            StructField("membership_valid_from", timestamp, required),
            StructField("membership_valid_to", timestamp, required),
            StructField("membership_version", text, required),
            StructField("universe_version", text, required),
            StructField("session_membership_status", text, required),
            StructField("evidence_status", text, required),
            StructField("historical_listing_status", text, required),
            StructField("entity_identity_status", text, required),
            StructField("sector_classification_status", text, required),
            StructField("acquisition_scope", BooleanType(), required),
            StructField("population_policy_version", text, required),
            StructField("eligibility_policy", text, required),
            StructField("eligibility_policy_version", text, required),
            StructField("research_reference_sha256", text, required),
            StructField("input_authority", text, required),
            StructField("expected_plan_sha256", text, required),
        ]
    )


def sector_return_schema() -> "StructType":
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("symbol", StringType(), False),
            StructField("sector", StringType(), False),
            StructField("session_id", StringType(), False),
            StructField("return_end", TimestampType(), False),
            StructField("observed_bars", IntegerType(), False),
            StructField("canonical_valid", BooleanType(), False),
            StructField("financial_eligible", BooleanType(), False),
            StructField("close_to_close_log_return", DoubleType(), True),
            StructField("individual_reason", StringType(), True),
        ]
    )


def sector_threshold_schema() -> "StructType":
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    return StructType(
        [
            StructField("symbol", StringType(), False),
            StructField("sector", StringType(), False),
            StructField("horizon", IntegerType(), False),
            StructField("realized_volatility_threshold", DoubleType(), False),
        ]
    )


def _validate_inputs(population: "DataFrame", returns: "DataFrame") -> None:
    from pyspark.sql import functions as F

    if population.sparkSession.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("sector transform: Spark session timezone must be UTC")
    if returns.sparkSession is not population.sparkSession:
        raise ValueError("sector transform: inputs must share one Spark session")
    if population.schema != sector_population_schema():
        raise ValueError("sector transform: invalid population schema or types")
    if returns.schema != sector_return_schema():
        raise ValueError("sector transform: invalid return schema or types")
    text_fields = [
        field.name for field in population.schema.fields if field.dataType.typeName() == "string"
    ]
    blank = F.trim(F.col(text_fields[0])) == ""
    for field in text_fields[1:]:
        blank = blank | (F.trim(F.col(field)) == "")
    membership_applicable = (
        (F.col("session_membership_status") == "full_session")
        & (F.col("membership_valid_from") <= F.col("session_open"))
        & (F.col("membership_valid_to") >= F.col("session_close"))
    )
    invalid_population = population.filter(
        blank
        | (F.col("source") != "alpaca")
        | (F.col("feed") != "iex")
        | (F.col("timeframe") != "1Min")
        | (F.col("adjustment") != "raw")
        | ~F.col("symbol").rlike(r"^[A-Z][A-Z0-9.-]{0,15}$")
        | F.col("sector").contains("|")
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
        | (F.col("population_policy_version") != SECTOR_POPULATION_POLICY_VERSION)
        | ~F.col("eligibility_policy").isin(*sorted(ELIGIBILITY_POLICIES))
        | (F.col("eligibility_policy_version") != SECTOR_ELIGIBILITY_POLICY_VERSION)
        | ~F.col("research_reference_sha256").rlike(r"^[0-9a-f]{64}$")
        | ~F.col("expected_plan_sha256").rlike(r"^[0-9a-f]{64}$")
        | (F.col("input_authority") != "synthetic_fixture")
        | (F.col("session_close") <= F.col("session_open"))
        | (F.col("membership_valid_to") <= F.col("membership_valid_from"))
        | (F.pmod(F.unix_micros("session_open"), F.lit(60_000_000)) != 0)
        | (F.pmod(F.unix_micros("session_close"), F.lit(60_000_000)) != 0)
        | ((F.col("session_membership_status") == "full_session") & ~membership_applicable)
    )
    if invalid_population.limit(1).count():
        raise ValueError("sector transform: invalid population authority")
    population_key = ["symbol", "sector", "session_id"]
    if population.groupBy(*population_key).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError("sector transform: duplicate expected member")
    if (
        population.groupBy("symbol", "session_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    ):
        raise ValueError("sector transform: symbol assigned to multiple sectors")
    group_key = ["sector", "session_id"]
    shared_fields = [
        "source",
        "feed",
        "timeframe",
        "adjustment",
        "session_open",
        "session_close",
        "membership_version",
        "universe_version",
        "population_policy_version",
        "eligibility_policy",
        "eligibility_policy_version",
        "research_reference_sha256",
        "input_authority",
        "expected_plan_sha256",
    ]
    contradictions = population.groupBy(*group_key).agg(
        *(F.countDistinct(field).alias(field) for field in shared_fields)
    )
    contradiction_condition = F.col(shared_fields[0]) != 1
    for field in shared_fields[1:]:
        contradiction_condition = contradiction_condition | (F.col(field) != 1)
    if contradictions.filter(contradiction_condition).limit(1).count():
        raise ValueError("sector transform: contradictory population authority")

    finite_value = ~F.isnan("close_to_close_log_return") & ~F.col("close_to_close_log_return").isin(
        float("inf"), float("-inf")
    )
    invalid_returns = returns.filter(
        ~F.col("symbol").rlike(r"^[A-Z][A-Z0-9.-]{0,15}$")
        | ~F.col("session_id").rlike(r"^\d{4}-\d{2}-\d{2}$")
        | ~F.col("observed_bars").isin(0, 1, 2)
        | (F.col("canonical_valid") & (F.col("observed_bars") != 2))
        | (F.pmod(F.unix_micros("return_end"), F.lit(60_000_000)) != 0)
        | (
            F.col("financial_eligible")
            & (
                (F.col("observed_bars") != 2)
                | ~F.col("canonical_valid")
                | F.col("close_to_close_log_return").isNull()
                | ~finite_value
                | F.col("individual_reason").isNotNull()
            )
        )
        | (
            ~F.col("financial_eligible")
            & (
                F.col("close_to_close_log_return").isNotNull()
                | F.col("individual_reason").isNull()
                | (F.trim(F.col("individual_reason")) == "")
            )
        )
    )
    if invalid_returns.limit(1).count():
        raise ValueError("sector transform: invalid individual return")
    return_key = [*population_key, "return_end"]
    if returns.groupBy(*return_key).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError("sector transform: duplicate individual return")
    joined_authority = returns.join(
        population.select(*population_key, "session_open", "session_close", "acquisition_scope"),
        population_key,
        "left",
    )
    if (
        joined_authority.filter(
            F.col("session_open").isNull()
            | ~F.col("acquisition_scope")
            | (F.col("return_end") <= F.col("session_open") + F.expr("INTERVAL 1 MINUTE"))
            | (F.col("return_end") > F.col("session_close"))
        )
        .limit(1)
        .count()
    ):
        raise ValueError("sector transform: return outside approved population or scope")


def _validate_thresholds(thresholds: "DataFrame") -> None:
    from pyspark.sql import functions as F

    if thresholds.schema != sector_threshold_schema():
        raise ValueError("sector transform: invalid threshold schema or types")
    if (
        thresholds.filter(
            (F.trim(F.col("symbol")) == "")
            | (F.trim(F.col("sector")) == "")
            | ~F.col("horizon").isin(5, 15, 30, 60)
            | F.isnan("realized_volatility_threshold")
            | F.col("realized_volatility_threshold").isin(float("inf"), float("-inf"))
            | (F.col("realized_volatility_threshold") < 0)
        )
        .limit(1)
        .count()
    ):
        raise ValueError("sector transform: invalid threshold")
    if (
        thresholds.groupBy("symbol", "sector", "horizon")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    ):
        raise ValueError("sector transform: duplicate threshold")


def transform_sector_gold(
    population: "DataFrame",
    returns: "DataFrame",
    horizons: tuple[int, ...],
    *,
    thresholds: "DataFrame | None" = None,
) -> SectorGoldFrames:
    """Calculate fixed-window sector metrics from explicit population and return authorities."""
    from pyspark.sql import functions as F

    if (
        not horizons
        or len(horizons) != len(set(horizons))
        or any(value not in {5, 15, 30, 60} for value in horizons)
    ):
        raise ValueError("sector transform: unsupported or duplicate horizon")
    _validate_inputs(population, returns)
    if thresholds is not None:
        _validate_thresholds(thresholds)
        population_threshold_keys = population.select("symbol", "sector").distinct()
        if (
            thresholds.select("symbol", "sector")
            .distinct()
            .join(population_threshold_keys, ["symbol", "sector"], "left_anti")
            .limit(1)
            .count()
        ):
            raise ValueError("sector transform: threshold outside expected population")

    member_key = ["symbol", "sector", "session_id"]
    return_fields = [
        "return_end",
        "observed_bars",
        "canonical_valid",
        "financial_eligible",
        "close_to_close_log_return",
        "individual_reason",
    ]
    supplied_returns = returns.select(*member_key, *return_fields).withColumn(
        "return_present", F.lit(True)
    )
    slots = population.select(
        "*",
        F.explode(
            F.sequence(
                F.col("session_open") + F.expr("INTERVAL 2 MINUTES"),
                F.col("session_close"),
                F.expr("INTERVAL 1 MINUTE"),
            )
        ).alias("return_end"),
    ).join(supplied_returns, [*member_key, "return_end"], "left")
    horizon_frame = population.sparkSession.createDataFrame(
        [(value,) for value in horizons], ["horizon"]
    )
    expanded = (
        slots.crossJoin(F.broadcast(horizon_frame))
        .withColumn(
            "window_index",
            F.floor(
                (F.unix_micros("return_end") - F.unix_micros("session_open") - F.lit(120_000_000))
                / (F.col("horizon") * F.lit(60_000_000))
            ).cast("int"),
        )
        .withColumn(
            "complete_windows",
            F.floor(
                (
                    (F.unix_micros("session_close") - F.unix_micros("session_open"))
                    / F.lit(60_000_000)
                    - F.lit(1)
                )
                / F.col("horizon")
            ).cast("int"),
        )
        .filter(F.col("window_index") < F.col("complete_windows"))
    )
    member_group = [
        *member_key,
        "source",
        "feed",
        "timeframe",
        "adjustment",
        "session_open",
        "session_close",
        "membership_valid_from",
        "membership_valid_to",
        "membership_version",
        "universe_version",
        "session_membership_status",
        "evidence_status",
        "historical_listing_status",
        "entity_identity_status",
        "sector_classification_status",
        "acquisition_scope",
        "population_policy_version",
        "eligibility_policy",
        "eligibility_policy_version",
        "research_reference_sha256",
        "input_authority",
        "expected_plan_sha256",
        "horizon",
        "window_index",
    ]
    member_windows = (
        expanded.groupBy(*member_group)
        .agg(
            F.count("return_end").alias("expected_return_slots"),
            F.sum(F.coalesce(F.col("return_present").cast("int"), F.lit(0))).alias(
                "return_records"
            ),
            F.sum(F.when(F.col("observed_bars") == 2, 1).otherwise(0)).alias("observed_slots"),
            F.sum(F.when(F.col("canonical_valid"), 1).otherwise(0)).alias("canonical_slots"),
            F.sum(F.when(F.col("financial_eligible"), 1).otherwise(0)).alias("eligible_slots"),
            F.sort_array(F.collect_list(F.struct("return_end", "close_to_close_log_return"))).alias(
                "ordered_returns"
            ),
        )
        .withColumn(
            "return_vector",
            F.transform("ordered_returns", lambda item: item["close_to_close_log_return"]),
        )
        .withColumn(
            "membership_applicable",
            (F.col("session_membership_status") == "full_session")
            & (F.col("membership_valid_from") <= F.col("session_open"))
            & (F.col("membership_valid_to") >= F.col("session_close")),
        )
        .withColumn(
            "historically_verified",
            (F.col("evidence_status") == "independently_verified")
            & (F.col("historical_listing_status") == "verified")
            & (F.col("entity_identity_status") == "verified")
            & (F.col("sector_classification_status") == "independently_verified"),
        )
        .withColumn(
            "member_eligible",
            F.col("acquisition_scope")
            & F.col("membership_applicable")
            & (
                (F.col("eligibility_policy") == DECLARED_RESEARCH_POLICY)
                | F.col("historically_verified")
            )
            & (F.col("expected_return_slots") == F.col("horizon"))
            & (F.col("eligible_slots") == F.col("horizon")),
        )
        .withColumn(
            "member_variance",
            F.when(
                F.col("member_eligible"),
                F.aggregate(
                    "return_vector",
                    F.lit(0.0),
                    lambda total, value: total + value * value,
                ),
            ),
        )
        .withColumn("member_volatility", F.sqrt("member_variance"))
    )
    if thresholds is not None:
        member_windows = member_windows.join(
            thresholds,
            ["symbol", "sector", "horizon"],
            "left",
        )
    else:
        member_windows = member_windows.withColumn(
            "realized_volatility_threshold", F.lit(None).cast("double")
        )

    sector_group = [
        "sector",
        "session_id",
        "source",
        "feed",
        "timeframe",
        "adjustment",
        "session_open",
        "session_close",
        "membership_version",
        "universe_version",
        "population_policy_version",
        "eligibility_policy",
        "eligibility_policy_version",
        "research_reference_sha256",
        "input_authority",
        "expected_plan_sha256",
        "horizon",
        "window_index",
    ]
    sectors = member_windows.groupBy(*sector_group).agg(
        F.sort_array(F.collect_list("symbol")).alias("expected_symbols"),
        F.count("symbol").alias("expected_members"),
        F.sum(F.col("historically_verified").cast("int")).alias("historically_verified_members"),
        F.sum(F.col("acquisition_scope").cast("int")).alias("acquisition_scope_members"),
        F.sum((F.col("observed_slots") == F.col("horizon")).cast("int")).alias("observed_members"),
        F.sum((F.col("canonical_slots") == F.col("horizon")).cast("int")).alias(
            "canonical_valid_members"
        ),
        F.sum((F.col("eligible_slots") == F.col("horizon")).cast("int")).alias(
            "financially_eligible_members"
        ),
        F.sum((F.col("return_records") == 0).cast("int")).alias("absent_expected_members"),
        F.min(F.col("membership_applicable").cast("int"))
        .cast("boolean")
        .alias("membership_applicable"),
        F.sort_array(
            F.collect_list(
                F.struct(
                    "symbol",
                    "return_vector",
                    "member_variance",
                    "member_volatility",
                    "realized_volatility_threshold",
                )
            )
        ).alias("member_values"),
    )
    reason = (
        F.when(
            F.col("acquisition_scope_members") < F.col("expected_members"),
            F.lit(OUTSIDE_ACQUISITION_SCOPE),
        )
        .when(~F.col("membership_applicable"), F.lit(INCOMPLETE_POPULATION))
        .when(
            (F.col("eligibility_policy") == VERIFIED_HISTORICAL_POLICY)
            & (F.col("historically_verified_members") < F.col("expected_members")),
            F.lit(UNVERIFIED_HISTORY),
        )
        .when(
            F.col("absent_expected_members") > 0,
            F.lit(MISSING_EXPECTED_MEMBER),
        )
        .when(
            F.col("observed_members") < F.col("expected_members"),
            F.lit(MISSING_OBSERVED_BAR),
        )
        .when(
            F.col("canonical_valid_members") < F.col("expected_members"),
            F.lit(INELIGIBLE_INDIVIDUAL_RETURN),
        )
        .when(
            F.col("financially_eligible_members") < F.col("expected_members"),
            F.lit(INELIGIBLE_INDIVIDUAL_RETURN),
        )
    )
    sectors = (
        sectors.withColumn(
            "window_start",
            F.col("session_open")
            + F.expr("INTERVAL 1 MINUTE")
            + F.col("window_index") * F.col("horizon") * F.expr("INTERVAL 1 MINUTE"),
        )
        .withColumn(
            "window_end", F.col("window_start") + F.col("horizon") * F.expr("INTERVAL 1 MINUTE")
        )
        .withColumn("reason", reason)
        .withColumn("eligible", F.col("reason").isNull())
        .withColumn("missing_members", F.col("expected_members") - F.col("observed_members"))
        .withColumn(
            "excluded_members", F.col("expected_members") - F.col("acquisition_scope_members")
        )
        .withColumn(
            "expected_pairs", F.col("expected_members") * (F.col("expected_members") - 1) / 2
        )
        .withColumn(
            "output_identity_sha256",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.lit(SECTOR_OUTPUT_IDENTITY_VERSION),
                    F.col("sector"),
                    F.col("session_id"),
                    F.col("horizon").cast("string"),
                    F.col("window_index").cast("string"),
                    F.concat_ws(",", F.col("expected_symbols")),
                    F.col("population_policy_version"),
                    F.col("eligibility_policy"),
                    F.col("eligibility_policy_version"),
                    F.col("research_reference_sha256"),
                ),
                256,
            ),
        )
        .withColumn(
            "sector_realized_variance",
            F.when(
                F.col("eligible"),
                F.aggregate(
                    "member_values",
                    F.lit(0.0),
                    lambda total, item: total + item["member_variance"],
                )
                / F.col("expected_members"),
            ),
        )
        .withColumn("sector_realized_volatility", F.sqrt("sector_realized_variance"))
    )
    portfolio_returns = F.transform(
        F.sequence(F.lit(1), F.col("horizon")),
        lambda position: F.log1p(
            F.aggregate(
                "member_values",
                F.lit(0.0),
                lambda total, item: total
                + F.expm1(F.element_at(item["return_vector"], position.cast("int"))),
            )
            / F.col("expected_members")
        ),
    )
    proxy_returns = F.transform(
        F.sequence(F.lit(1), F.col("horizon")),
        lambda position: F.aggregate(
            "member_values",
            F.lit(0.0),
            lambda total, item: total + F.element_at(item["return_vector"], position.cast("int")),
        )
        / F.col("expected_members"),
    )
    sectors = (
        sectors.withColumn("portfolio_returns", F.when(F.col("eligible"), portfolio_returns))
        .withColumn("proxy_returns", F.when(F.col("eligible"), proxy_returns))
        .withColumn(
            "portfolio_realized_variance",
            F.when(
                F.col("eligible"),
                F.aggregate(
                    "portfolio_returns", F.lit(0.0), lambda total, value: total + value * value
                ),
            ),
        )
        .withColumn("portfolio_realized_volatility", F.sqrt("portfolio_realized_variance"))
        .withColumn(
            "mean_log_return_realized_volatility",
            F.when(
                F.col("eligible"),
                F.sqrt(
                    F.aggregate(
                        "proxy_returns", F.lit(0.0), lambda total, value: total + value * value
                    )
                ),
            ),
        )
        .withColumn(
            "mean_member_volatility",
            F.when(
                F.col("eligible"),
                F.aggregate(
                    "member_values",
                    F.lit(0.0),
                    lambda total, item: total + item["member_volatility"],
                )
                / F.col("expected_members"),
            ),
        )
        .withColumn(
            "volatility_dispersion",
            F.when(
                F.col("eligible"),
                F.sqrt(
                    F.aggregate(
                        "member_values",
                        F.lit(0.0),
                        lambda total, item: total
                        + F.pow(item["member_volatility"] - F.col("mean_member_volatility"), 2),
                    )
                    / F.col("expected_members")
                ),
            ),
        )
        .withColumn(
            "total_member_variance",
            F.when(
                F.col("eligible"),
                F.aggregate(
                    "member_values",
                    F.lit(0.0),
                    lambda total, item: total + item["member_variance"],
                ),
            ),
        )
        .withColumn(
            "volatility_concentration_hhi",
            F.when(
                F.col("eligible") & (F.col("total_member_variance") > 0),
                F.aggregate(
                    "member_values",
                    F.lit(0.0),
                    lambda total, item: total
                    + F.pow(item["member_variance"] / F.col("total_member_variance"), 2),
                ),
            ),
        )
        .withColumn(
            "concentration_reason",
            F.when(~F.col("eligible"), F.col("reason")).when(
                F.col("total_member_variance") == 0, F.lit(CONSTANT_SERIES)
            ),
        )
        .withColumn(
            "volatility_effective_breadth",
            F.when(
                F.col("volatility_concentration_hhi").isNotNull()
                & (F.col("expected_members") >= 2),
                (1.0 - F.col("volatility_concentration_hhi"))
                / (1.0 - 1.0 / F.col("expected_members")),
            ),
        )
        .withColumn(
            "thresholds_available",
            F.aggregate(
                "member_values",
                F.lit(True),
                lambda current, item: current & item["realized_volatility_threshold"].isNotNull(),
            ),
        )
        .withColumn(
            "volatility_breadth",
            F.when(
                F.col("eligible") & F.col("thresholds_available"),
                F.aggregate(
                    "member_values",
                    F.lit(0.0),
                    lambda total, item: total
                    + F.when(
                        item["member_volatility"] > item["realized_volatility_threshold"], 1.0
                    ).otherwise(0.0),
                )
                / F.col("expected_members"),
            ),
        )
        .withColumn(
            "breadth_reason",
            F.when(~F.col("eligible"), F.col("reason")).when(
                ~F.col("thresholds_available"), F.lit(INCOMPLETE_CALIBRATION)
            ),
        )
    )

    pair_condition = F.col("left.sector") == F.col("right.sector")
    for field in ["session_id", "horizon", "window_index"]:
        pair_condition = pair_condition & (F.col(f"left.{field}") == F.col(f"right.{field}"))
    pair_condition = pair_condition & (F.col("left.symbol") < F.col("right.symbol"))
    pair_base = member_windows.alias("left").join(
        member_windows.alias("right"), pair_condition, "inner"
    )
    left_vector = F.col("left.return_vector")
    right_vector = F.col("right.return_vector")
    horizon = F.col("left.horizon")
    left_min = F.array_min(left_vector)
    left_max = F.array_max(left_vector)
    right_min = F.array_min(right_vector)
    right_max = F.array_max(right_vector)
    left_offsets = F.transform(left_vector, lambda value: value - left_min)
    right_offsets = F.transform(right_vector, lambda value: value - right_min)
    left_mean_offset = (
        F.aggregate(left_offsets, F.lit(0.0), lambda total, value: total + value) / horizon
    )
    right_mean_offset = (
        F.aggregate(right_offsets, F.lit(0.0), lambda total, value: total + value) / horizon
    )
    left_scale = F.when(left_min == left_max, F.lit(1.0)).otherwise(
        F.greatest(F.abs(left_mean_offset), F.abs((left_max - left_min) - left_mean_offset))
    )
    right_scale = F.when(right_min == right_max, F.lit(1.0)).otherwise(
        F.greatest(F.abs(right_mean_offset), F.abs((right_max - right_min) - right_mean_offset))
    )
    normalized_left = F.transform(
        left_offsets, lambda value: (value - left_mean_offset) / left_scale
    )
    normalized_right = F.transform(
        right_offsets, lambda value: (value - right_mean_offset) / right_scale
    )
    products = F.zip_with(
        normalized_left,
        normalized_right,
        lambda left, right: F.struct(
            (left * right).alias("cross"),
            F.pow(left, 2).alias("left_square"),
            F.pow(right, 2).alias("right_square"),
        ),
    )
    cross = F.aggregate(products, F.lit(0.0), lambda total, item: total + item["cross"])
    left_square = F.aggregate(products, F.lit(0.0), lambda total, item: total + item["left_square"])
    right_square = F.aggregate(
        products, F.lit(0.0), lambda total, item: total + item["right_square"]
    )
    pair_eligible = F.col("left.member_eligible") & F.col("right.member_eligible")
    pair_reason = F.when(~pair_eligible, F.lit(INCOMPLETE_POPULATION)).when(
        (left_min == left_max) | (right_min == right_max), F.lit(CONSTANT_SERIES)
    )
    raw_correlations = pair_base.select(
        F.col("left.sector").alias("sector"),
        F.col("left.session_id").alias("session_id"),
        F.col("left.horizon").alias("horizon"),
        (
            F.col("left.session_open")
            + F.expr("INTERVAL 1 MINUTE")
            + F.col("left.window_index") * F.col("left.horizon") * F.expr("INTERVAL 1 MINUTE")
        ).alias("window_start"),
        (
            F.col("left.session_open")
            + F.expr("INTERVAL 1 MINUTE")
            + (F.col("left.window_index") + F.lit(1))
            * F.col("left.horizon")
            * F.expr("INTERVAL 1 MINUTE")
        ).alias("window_end"),
        F.col("left.symbol").alias("left_symbol"),
        F.col("right.symbol").alias("right_symbol"),
        F.when(pair_reason.isNull(), cross / F.sqrt(left_square * right_square)).alias("value"),
        pair_reason.isNull().alias("eligible"),
        pair_reason.alias("reason"),
    )
    sector_reasons = sectors.select(
        "sector",
        "session_id",
        "horizon",
        "window_start",
        "window_end",
        "eligibility_policy",
        "eligibility_policy_version",
        "output_identity_sha256",
        F.col("reason").alias("sector_reason"),
    )
    correlations = (
        raw_correlations.join(
            sector_reasons,
            ["sector", "session_id", "horizon", "window_start", "window_end"],
            "inner",
        )
        .withColumn("reason", F.coalesce("sector_reason", "reason"))
        .withColumn("eligible", F.col("reason").isNull())
        .withColumn("value", F.when(F.col("eligible"), F.col("value")))
        .drop("sector_reason")
    )
    pair_summary = correlations.groupBy(
        "sector", "session_id", "horizon", "window_start", "window_end"
    ).agg(
        F.count(F.when(F.col("eligible"), 1)).alias("valid_pairs"),
        F.sort_array(
            F.collect_list(
                F.when(
                    F.col("eligible"),
                    F.struct("left_symbol", "right_symbol", "value"),
                )
            )
        ).alias("ordered_valid_pairs"),
        F.max(F.when(F.col("reason") == CONSTANT_SERIES, 1).otherwise(0)).alias("constant_pair"),
    )
    pair_summary = pair_summary.withColumn(
        "candidate_mean_correlation",
        F.when(
            F.col("valid_pairs") > 0,
            F.aggregate(
                "ordered_valid_pairs",
                F.lit(0.0),
                lambda total, item: total + item["value"],
            )
            / F.col("valid_pairs"),
        ),
    ).drop("ordered_valid_pairs")
    windows = (
        sectors.join(
            pair_summary,
            ["sector", "session_id", "horizon", "window_start", "window_end"],
            "left",
        )
        .fillna({"valid_pairs": 0, "constant_pair": 0})
        .withColumn(
            "correlation_reason",
            F.when(~F.col("eligible"), F.col("reason"))
            .when(F.col("expected_pairs") == 0, F.lit(INSUFFICIENT_PAIRS))
            .when(F.col("valid_pairs") < F.col("expected_pairs"), F.lit(CONSTANT_SERIES)),
        )
        .withColumn(
            "mean_pairwise_correlation",
            F.when(F.col("correlation_reason").isNull(), F.col("candidate_mean_correlation")),
        )
        .select(
            *sector_group,
            "window_start",
            "window_end",
            "expected_symbols",
            "expected_members",
            "historically_verified_members",
            "acquisition_scope_members",
            "observed_members",
            "canonical_valid_members",
            "financially_eligible_members",
            "missing_members",
            "excluded_members",
            F.col("expected_pairs").cast("int").alias("expected_pairs"),
            F.col("valid_pairs").cast("int").alias("valid_pairs"),
            "membership_applicable",
            "output_identity_sha256",
            "sector_realized_variance",
            "sector_realized_volatility",
            "portfolio_realized_variance",
            "portfolio_realized_volatility",
            "mean_log_return_realized_volatility",
            "volatility_dispersion",
            "mean_pairwise_correlation",
            "volatility_concentration_hhi",
            "volatility_effective_breadth",
            "volatility_breadth",
            "eligible",
            "reason",
            "correlation_reason",
            "concentration_reason",
            "breadth_reason",
        )
    )
    return SectorGoldFrames(windows, correlations)
