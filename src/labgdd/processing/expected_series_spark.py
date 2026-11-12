"""Explicit Spark projection of the approved expected-series reference contract."""

import hashlib
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from labgdd.research.universe import ExpectedSeries, ExpectedSeriesPlan

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession
    from pyspark.sql.types import StructType

FINGERPRINT_VERSION = "expected-series-fingerprint-v1"
POPULATION_POLICY_VERSION = "expected-series-left-join-v1"
DECLARED_POLICY = "declared_research"
STRICT_POLICY = "verified_historical_applicability"


def expected_series_schema() -> "StructType":
    """Return the exact non-nullable authority schema accepted by Gold."""
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    required = False
    text = StringType()
    timestamp = TimestampType()
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
            StructField("membership_change_times", ArrayType(timestamp, False), required),
            StructField("applicability_basis", text, required),
            StructField("evidence_source", text, required),
            StructField("evidence_status", text, required),
            StructField("historical_listing_status", text, required),
            StructField("entity_identity_status", text, required),
            StructField("sector_classification_status", text, required),
            StructField("calendar_provider", text, required),
            StructField("calendar_version", text, required),
            StructField("calendar_name", text, required),
            StructField("calendar_timezone", text, required),
            StructField("tzdata_version", text, required),
            StructField("schedule_sha256", text, required),
            StructField("calendar_applicability", text, required),
            StructField("expected_eligible", BooleanType(), required),
            StructField("expected_ineligibility_reason", text, True),
            StructField("eligibility_policy", text, required),
            StructField("expectation_authority", text, required),
            StructField("expectation_status", text, required),
            StructField("expected_series_contract_version", text, required),
            StructField("population_policy_version", text, required),
            StructField("fingerprint_version", text, required),
            StructField("expected_plan_sha256", text, required),
            StructField("expected_identity_sha256", text, required),
        ]
    )


def _micros(value: datetime) -> str:
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError("expected Spark relation: fingerprint timestamp must be UTC")
    return str(int(value.timestamp() * 1_000_000))


def _encoded(value: str | None) -> bytes:
    if value is None:
        return b"N"
    payload = value.encode("utf-8")
    return b"V" + str(len(payload)).encode("ascii") + b":" + payload


def _identity_values(
    row: ExpectedSeries,
    observation_identity: tuple[str, str, str, str],
    eligible: bool,
    reason: str | None,
    policy: str,
) -> tuple[str | None, ...]:
    return (
        row.symbol,
        row.sector,
        *observation_identity,
        row.session_id,
        _micros(row.session_open),
        _micros(row.session_close),
        _micros(row.membership_valid_from),
        _micros(row.membership_valid_to),
        row.membership_version,
        row.universe_version,
        row.session_membership_status,
        ",".join(_micros(value) for value in row.membership_change_times),
        row.applicability_basis,
        row.evidence_source,
        row.evidence_status,
        row.historical_listing_status,
        row.entity_identity_status,
        row.sector_classification_status,
        row.calendar_provider,
        row.calendar_version,
        row.calendar_name,
        row.calendar_timezone,
        row.tzdata_version,
        row.schedule_sha256,
        row.calendar_applicability,
        str(eligible).lower(),
        reason,
        policy,
        "approved_research_reference",
        "approved",
        "1.0.0",
        POPULATION_POLICY_VERSION,
        FINGERPRINT_VERSION,
    )


def _identity_digest(values: tuple[str | None, ...]) -> str:
    return hashlib.sha256(b"".join(_encoded(value) for value in values)).hexdigest()


def _plan_digest(identity_digests: list[str]) -> str:
    payload = b"expected-series-plan-v1\0" + "".join(sorted(identity_digests)).encode("ascii")
    return hashlib.sha256(payload).hexdigest()


def expected_identity_expression() -> "Column":
    """Recompute an identity fingerprint from Spark row content."""
    from pyspark.sql import functions as F

    timestamp_fields = {
        "session_open",
        "session_close",
        "membership_valid_from",
        "membership_valid_to",
    }
    fields = [field.name for field in expected_series_schema().fields][:-2]
    encoded: list[Column] = []
    for name in fields:
        if name in timestamp_fields:
            value = F.unix_micros(F.col(name)).cast("string")
        elif name == "membership_change_times":
            value = F.concat_ws(",", F.transform(F.col(name), lambda item: F.unix_micros(item)))
        else:
            value = F.col(name).cast("string")
        encoded.append(
            F.when(value.isNull(), F.lit("N")).otherwise(
                F.concat(
                    F.lit("V"),
                    F.length(F.encode(value, "UTF-8")).cast("string"),
                    F.lit(":"),
                    value,
                )
            )
        )
    return F.sha2(F.concat(*encoded), 256)


def expected_plan_expression(identity_column: "Column") -> "Column":
    """Recompute the relation fingerprint from distinct identity fingerprints."""
    from pyspark.sql import functions as F

    return F.sha2(
        F.concat(
            F.lit("expected-series-plan-v1\0"),
            F.concat_ws("", F.sort_array(F.collect_list(identity_column))),
        ),
        256,
    )


def _eligibility(row: ExpectedSeries, strict_historical: bool) -> tuple[bool, str | None]:
    if row.session_membership_status != "full_session":
        return False, "membership_not_full_session"
    if strict_historical and (
        row.evidence_status != "independently_verified"
        or row.historical_listing_status != "verified"
        or row.entity_identity_status != "verified"
    ):
        return False, "unverified_historical_applicability"
    return True, None


def expected_plan_fingerprint(
    plan: ExpectedSeriesPlan,
    *,
    source: str,
    feed: str,
    timeframe: str,
    adjustment: str,
    strict_historical: bool = False,
) -> str:
    """Derive the trusted plan fingerprint independently of a supplied DataFrame."""
    observation_identity = (source, feed, timeframe, adjustment)
    if any(not value.strip() for value in observation_identity):
        raise ValueError("expected Spark relation: blank observation identity")
    policy = STRICT_POLICY if strict_historical else DECLARED_POLICY
    identities = []
    for row in plan.rows:
        eligible, reason = _eligibility(row, strict_historical)
        identities.append(
            _identity_digest(_identity_values(row, observation_identity, eligible, reason, policy))
        )
    return _plan_digest(identities)


def expected_series_frame(
    spark: "SparkSession",
    plan: ExpectedSeriesPlan,
    *,
    source: str,
    feed: str,
    timeframe: str,
    adjustment: str,
    strict_historical: bool = False,
) -> "DataFrame":
    """Create a bounded explicit-schema Spark authority frame without market observations."""
    observation_identity = (source, feed, timeframe, adjustment)
    if any(not value.strip() for value in observation_identity):
        raise ValueError("expected Spark relation: blank observation identity")
    if not set(plan.scheduled_sessions).issubset(plan.requested_sessions):
        raise ValueError("expected Spark relation: scheduled session was not requested")
    if any(row.session_id not in plan.scheduled_sessions for row in plan.rows):
        raise ValueError("expected Spark relation: row outside scheduled sessions")
    if plan.calendar.calendar != "XNYS" or plan.calendar.timezone != "America/New_York":
        raise ValueError("expected Spark relation: unapproved calendar authority")

    policy = STRICT_POLICY if strict_historical else DECLARED_POLICY
    prepared: list[tuple[ExpectedSeries, bool, str | None, str]] = []
    for row in plan.rows:
        eligible, reason = _eligibility(row, strict_historical)
        identity_digest = _identity_digest(
            _identity_values(row, observation_identity, eligible, reason, policy)
        )
        prepared.append((row, eligible, reason, identity_digest))
    plan_sha256 = expected_plan_fingerprint(
        plan,
        source=source,
        feed=feed,
        timeframe=timeframe,
        adjustment=adjustment,
        strict_historical=strict_historical,
    )
    values: list[tuple[object, ...]] = []
    for row, eligible, reason, identity_sha256 in prepared:
        values.append(
            (
                row.symbol,
                row.sector,
                source,
                feed,
                timeframe,
                adjustment,
                row.session_id,
                row.session_open,
                row.session_close,
                row.membership_valid_from,
                row.membership_valid_to,
                row.membership_version,
                row.universe_version,
                row.session_membership_status,
                list(row.membership_change_times),
                row.applicability_basis,
                row.evidence_source,
                row.evidence_status,
                row.historical_listing_status,
                row.entity_identity_status,
                row.sector_classification_status,
                row.calendar_provider,
                row.calendar_version,
                row.calendar_name,
                row.calendar_timezone,
                row.tzdata_version,
                row.schedule_sha256,
                row.calendar_applicability,
                eligible,
                reason,
                policy,
                "approved_research_reference",
                "approved",
                "1.0.0",
                POPULATION_POLICY_VERSION,
                FINGERPRINT_VERSION,
                plan_sha256,
                identity_sha256,
            )
        )
    return spark.createDataFrame(values, expected_series_schema())
