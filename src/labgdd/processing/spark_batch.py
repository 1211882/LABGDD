"""Spark SQL Bronze-to-Silver normalization and deterministic resolution."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

from labgdd.adapters.silver import BronzeInput
from labgdd.config import Settings
from labgdd.silver_config import SilverRequest

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True, slots=True)
class Membership:
    symbol: str
    sector: str
    valid_from: datetime
    valid_to: datetime
    version: str


@dataclass(frozen=True)
class BatchFrames:
    observations: "DataFrame"
    canonical: "DataFrame"
    quarantine: "DataFrame"
    duplicates: "DataFrame"
    expected: "DataFrame"


def project_memberships(settings: Settings, request: SilverRequest) -> tuple[Membership, ...]:
    return tuple(
        Membership(
            symbol,
            sector,
            request.membership_valid_from,
            request.membership_valid_to,
            settings.membership_version,
        )
        for sector, symbols in settings.universe.items()
        for symbol in symbols
    )


def _validate_memberships(rows: tuple[Membership, ...]) -> None:
    if not rows:
        raise ValueError("silver membership: empty reference")
    for row in rows:
        if row.valid_to <= row.valid_from:
            raise ValueError("silver membership: invalid interval")
    for index, left in enumerate(rows):
        for right in rows[index + 1 :]:
            if (
                left.symbol == right.symbol
                and left.version == right.version
                and left.valid_from < right.valid_to
                and right.valid_from < left.valid_to
            ):
                raise ValueError("silver membership: overlapping interval")


def transform_observations(
    spark: "SparkSession",
    inputs: tuple[BronzeInput, ...],
    settings: Settings,
    request: SilverRequest,
    *,
    memberships: tuple[Membership, ...] | None = None,
) -> BatchFrames:
    """Return explicit canonical, quarantine, duplicate and expected-minute frames."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    if not inputs:
        raise ValueError("silver input: at least one Bronze manifest required")
    membership_rows = memberships or project_memberships(settings, request)
    _validate_memberships(membership_rows)
    schema = T.StructType(
        [
            T.StructField("observation_id", T.StringType(), True),
            T.StructField("symbol", T.StringType(), True),
            T.StructField("raw_json", T.StringType(), True),
            T.StructField("event_time", T.TimestampType(), True),
            T.StructField("valid", T.BooleanType(), True),
            T.StructField("reason", T.StringType(), True),
            T.StructField("session", T.StringType(), True),
        ]
    )
    frames = []
    input_symbols: set[str] = set()
    calendar_rows: dict[str, tuple[datetime, datetime, str, str, str]] = {}
    for source in inputs:
        metadata = source.metadata
        calendar = metadata["calendar"]
        symbols = metadata.get("symbols")
        if not isinstance(symbols, list) or not all(isinstance(value, str) for value in symbols):
            raise ValueError("silver input: invalid symbol scope")
        input_symbols.update(symbols)
        if not isinstance(calendar, dict) or not isinstance(calendar.get("sessions"), list):
            raise ValueError("silver input: invalid calendar metadata")
        calendar_version = calendar.get("version")
        schedule_hash = calendar.get("schedule_sha256")
        provider = calendar.get("provider")
        if not all(isinstance(value, str) for value in (calendar_version, schedule_hash, provider)):
            raise ValueError("silver input: incomplete calendar provenance")
        for session in calendar["sessions"]:
            if not isinstance(session, dict):
                raise ValueError("silver input: invalid calendar session")
            label = session.get("session")
            opening = session.get("open")
            closing = session.get("close")
            if not all(isinstance(value, str) for value in (label, opening, closing)):
                raise ValueError("silver input: invalid calendar session")
            label_value = cast(str, label)
            opening_value = cast(str, opening)
            closing_value = cast(str, closing)
            schedule = (
                datetime.fromisoformat(opening_value),
                datetime.fromisoformat(closing_value),
                cast(str, provider),
                cast(str, calendar_version),
                cast(str, schedule_hash),
            )
            prior = calendar_rows.setdefault(label_value, schedule)
            if prior != schedule:
                raise ValueError("silver input: incompatible calendar provenance")
        frame = spark.read.schema(schema).parquet(*[str(item) for item in source.observation_files])
        frames.append(
            frame.withColumn("input_acquisition_id", F.lit(source.acquisition_id))
            .withColumn("input_dataset_version", F.lit(source.dataset_version))
            .withColumn("input_manifest_sha256", F.lit(source.manifest_sha256))
        )
    observations = frames[0]
    for frame in frames[1:]:
        observations = observations.unionByName(frame)
    raw = {
        field: F.get_json_object("raw_json", f"$.{field}")
        for field in ("t", "o", "h", "l", "c", "v")
    }
    decimal_type = "decimal(26,8)"
    parsed = observations
    for field, expression in raw.items():
        parsed = parsed.withColumn(f"raw_{field}", expression)
    parsed = (
        parsed.withColumn("parsed_event_time", F.expr("try_to_timestamp(raw_t)"))
        .withColumn("raw_o_wide", F.expr("try_cast(raw_o as decimal(38,18))"))
        .withColumn("raw_h_wide", F.expr("try_cast(raw_h as decimal(38,18))"))
        .withColumn("raw_l_wide", F.expr("try_cast(raw_l as decimal(38,18))"))
        .withColumn("raw_c_wide", F.expr("try_cast(raw_c as decimal(38,18))"))
        .withColumn("open", F.expr(f"try_cast(raw_o as {decimal_type})"))
        .withColumn("high", F.expr(f"try_cast(raw_h as {decimal_type})"))
        .withColumn("low", F.expr(f"try_cast(raw_l as {decimal_type})"))
        .withColumn("close", F.expr(f"try_cast(raw_c as {decimal_type})"))
        .withColumn("volume", F.expr("try_cast(raw_v as bigint)"))
        .withColumn("raw_volume_decimal", F.expr("try_cast(raw_v as decimal(38,0))"))
    )
    session_schema = (
        "calendar_session string, session_open timestamp, session_close timestamp, "
        "calendar_provider string, calendar_version string, schedule_sha256 string"
    )
    sessions = spark.createDataFrame(
        [
            (label, opening, closing, provider, version, fingerprint)
            for label, (opening, closing, provider, version, fingerprint) in sorted(
                calendar_rows.items()
            )
        ],
        session_schema,
    )
    membership_schema = (
        "membership_symbol string, sector string, membership_valid_from timestamp, "
        "membership_valid_to timestamp, membership_version string"
    )
    membership = spark.createDataFrame(
        [
            (row.symbol, row.sector, row.valid_from, row.valid_to, row.version)
            for row in membership_rows
        ],
        membership_schema,
    )
    known_symbols = (
        membership.select(F.col("membership_symbol").alias("known_symbol"))
        .distinct()
        .withColumn("known", F.lit(True))
    )
    enriched = (
        parsed.join(
            F.broadcast(sessions),
            (F.col("parsed_event_time") >= F.col("session_open"))
            & (F.col("parsed_event_time") < F.col("session_close")),
            "left",
        )
        .join(
            F.broadcast(membership),
            (F.col("symbol") == F.col("membership_symbol"))
            & (F.col("parsed_event_time") >= F.col("membership_valid_from"))
            & (F.col("parsed_event_time") < F.col("membership_valid_to")),
            "left",
        )
        .drop("membership_symbol")
        .join(F.broadcast(known_symbols), F.col("symbol") == F.col("known_symbol"), "left")
        .drop("known_symbol")
    )
    timestamp_pattern = r"T\d{2}:\d{2}:00(?:\.0+)?(?:Z|[+-]\d{2}:\d{2})$"
    symbol_pattern = r"^[A-Z][A-Z0-9.-]{0,15}$"
    number_pattern = r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$"
    price_valid = F.lit(True)
    for raw_name, name in (("o", "open"), ("h", "high"), ("l", "low"), ("c", "close")):
        mantissa = F.regexp_extract(F.col(f"raw_{raw_name}"), r"^[+-]?([^eE]+)", 1)
        exponent_text = F.regexp_extract(F.col(f"raw_{raw_name}"), r"[eE]([+-]?\d+)$", 1)
        exponent = F.when(exponent_text == "", F.lit(0)).otherwise(exponent_text.cast("int"))
        fractional_digits = F.length(F.regexp_extract(mantissa, r"\.(\d*)$", 1))
        significant = F.regexp_replace(F.regexp_replace(mantissa, r"\.", ""), r"^0+", "")
        wide = F.col(f"raw_{raw_name}_wide")
        price_valid = price_valid & (
            F.col(f"raw_{raw_name}").rlike(number_pattern)
            & wide.isNotNull()
            & (wide > F.lit(0))
            & (wide < F.lit(Decimal("1e18")))
            & (F.length(significant) <= F.lit(18))
            & (F.greatest(F.lit(0), fractional_digits - exponent) <= F.lit(8))
            & F.col(name).isNotNull()
            & (wide == F.col(name).cast("decimal(38,18)"))
        )
    ohlc_valid = (F.col("low") <= F.least("open", "close")) & (
        F.greatest("open", "close") <= F.col("high")
    )
    volume_valid = (
        F.col("raw_v").rlike(r"^(?:0|[1-9]\d*)$")
        & F.col("raw_volume_decimal").isNotNull()
        & (F.col("raw_volume_decimal") <= F.lit(2**63 - 1))
        & F.col("volume").isNotNull()
        & (F.col("raw_volume_decimal") == F.col("volume").cast("decimal(38,0)"))
    )
    issues = F.filter(
        F.array(
            F.when(~F.coalesce(F.col("valid"), F.lit(False)), F.lit("bronze_invalid")),
            F.when(F.col("raw_json").isNull(), F.lit("missing_raw_payload")),
            F.when(
                ~F.coalesce(F.col("observation_id").rlike(r"^[0-9a-f]{64}$"), F.lit(False)),
                F.lit("invalid_observation_id"),
            ),
            F.when(~F.col("symbol").rlike(symbol_pattern), F.lit("invalid_symbol")),
            F.when(
                F.col("parsed_event_time").isNull()
                | ~F.col("raw_t").rlike(timestamp_pattern)
                | (F.col("parsed_event_time") != F.col("event_time")),
                F.lit("invalid_event_time"),
            ),
            F.when(~price_valid, F.lit("invalid_price")),
            F.when(price_valid & ~ohlc_valid, F.lit("inconsistent_ohlc")),
            F.when(~volume_valid, F.lit("invalid_volume")),
            F.when(F.col("session_open").isNull(), F.lit("calendar_ineligible")),
            F.when(F.col("known").isNull(), F.lit("unknown_membership")),
            F.when(
                F.col("known").isNotNull() & F.col("sector").isNull(),
                F.lit("membership_ineligible"),
            ),
        ),
        lambda item: item.isNotNull(),
    )
    enriched = enriched.withColumn("quality_reasons", issues)
    identity_stats = enriched.groupBy("observation_id").agg(
        F.countDistinct("raw_json").alias("content_versions"),
        F.countDistinct("symbol").alias("identity_symbols"),
        F.count("*").alias("id_rows"),
        F.min("symbol").alias("identity_symbol"),
        F.min("parsed_event_time").alias("identity_event_time"),
    )
    duplicate_ids = (
        identity_stats.filter((F.col("content_versions") > 1) | (F.col("identity_symbols") > 1))
        .select("observation_id")
        .withColumn("identity_corrupt", F.lit(True))
    )
    enriched = enriched.join(duplicate_ids, "observation_id", "left").withColumn(
        "quality_reasons",
        F.when(
            F.col("identity_corrupt").isNotNull(),
            F.array_union(
                F.col("quality_reasons"), F.array(F.lit("observation_identity_conflict"))
            ),
        ).otherwise(F.col("quality_reasons")),
    )
    candidates = enriched.filter(F.size("quality_reasons") == 0).withColumn(
        "payload_sha256",
        F.sha2(F.concat_ws("\u001f", "open", "high", "low", "close", "volume"), 256),
    )
    keys = ["symbol", "parsed_event_time"]
    groups = candidates.groupBy(*keys).agg(
        F.count("*").alias("observation_rows"),
        F.countDistinct("observation_id").alias("distinct_observation_ids"),
        F.countDistinct("payload_sha256").alias("payload_versions"),
        F.array_sort(F.collect_set("observation_id")).alias("contributing_observation_ids"),
        F.min("observation_id").alias("selected_observation_id"),
    )
    joined = candidates.join(groups, keys, "inner")
    conflict_rows = joined.filter(F.col("payload_versions") > 1).withColumn(
        "quality_reasons", F.array(F.lit("unresolved_conflict"))
    )
    selected = (
        joined.filter(F.col("payload_versions") == 1)
        .withColumn(
            "selection_rank",
            F.row_number().over(
                Window.partitionBy(*keys).orderBy("observation_id", "input_acquisition_id")
            ),
        )
        .filter(F.col("selection_rank") == 1)
    )
    canonical = selected.select(
        F.col("parsed_event_time").alias("event_time"),
        (F.col("parsed_event_time") + F.expr("INTERVAL 1 MINUTE")).alias("bar_end"),
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        F.lit("alpaca").alias("source"),
        F.lit("iex").alias("feed"),
        F.lit("1Min").alias("timeframe"),
        F.lit("raw").alias("adjustment"),
        "selected_observation_id",
        "contributing_observation_ids",
        "sector",
        "membership_version",
        "membership_valid_from",
        "membership_valid_to",
        F.col("calendar_session").alias("session_id"),
        "session_open",
        "session_close",
        "calendar_provider",
        "calendar_version",
        "schedule_sha256",
        F.when(F.col("observation_rows") > 1, F.lit("canonical_equivalent_duplicate"))
        .otherwise(F.lit("canonical"))
        .alias("quality_status"),
        F.lit(request.dataset_version).alias("dataset_version"),
        F.lit(request.silver_schema_version).alias("schema_version"),
        F.lit(request.resolution_policy_version).alias("resolution_policy_version"),
        "input_acquisition_id",
        "input_dataset_version",
        "input_manifest_sha256",
        F.to_date("parsed_event_time").alias("session_date"),
    )
    quarantine_columns: list[Any] = [
        "observation_id",
        "symbol",
        "raw_json",
        F.col("parsed_event_time").alias("event_time"),
        "quality_reasons",
        "input_acquisition_id",
        "input_dataset_version",
        "input_manifest_sha256",
    ]
    quarantine = (
        enriched.filter(F.size("quality_reasons") > 0)
        .select(*quarantine_columns)
        .unionByName(conflict_rows.select(*quarantine_columns))
    )
    duplicates = groups.filter(F.col("observation_rows") > 1).select(
        "symbol",
        F.col("parsed_event_time").alias("event_time"),
        "observation_rows",
        "distinct_observation_ids",
        "payload_versions",
        "contributing_observation_ids",
        F.when(F.col("payload_versions") > 1, F.lit("unresolved_conflict"))
        .when(F.col("distinct_observation_ids") == 1, F.lit("identical_observation_replay"))
        .otherwise(F.lit("equivalent_duplicate_payload"))
        .alias("duplicate_classification"),
    )
    identity_evidence = identity_stats.filter(
        (F.col("content_versions") > 1) | (F.col("identity_symbols") > 1)
    ).select(
        F.col("identity_symbol").alias("symbol"),
        F.col("identity_event_time").alias("event_time"),
        F.col("id_rows").alias("observation_rows"),
        F.lit(1).cast("long").alias("distinct_observation_ids"),
        F.col("content_versions").alias("payload_versions"),
        F.array("observation_id").alias("contributing_observation_ids"),
        F.lit("observation_identity_conflict").alias("duplicate_classification"),
    )
    duplicates = duplicates.unionByName(identity_evidence)
    expected = (
        sessions.select(
            "calendar_session",
            "session_open",
            "session_close",
            F.explode(
                F.sequence(
                    F.col("session_open"),
                    F.col("session_close") - F.expr("INTERVAL 1 MINUTE"),
                    F.expr("INTERVAL 1 MINUTE"),
                )
            ).alias("event_time"),
        )
        .crossJoin(F.broadcast(membership))
        .filter(
            F.col("membership_symbol").isin(sorted(input_symbols))
            & (F.col("event_time") >= F.col("membership_valid_from"))
            & (F.col("event_time") < F.col("membership_valid_to"))
        )
        .select(
            F.col("membership_symbol").alias("symbol"),
            "event_time",
            F.col("calendar_session").alias("session"),
        )
    )
    return BatchFrames(
        observations,
        canonical,
        quarantine,
        duplicates,
        expected,
    )
