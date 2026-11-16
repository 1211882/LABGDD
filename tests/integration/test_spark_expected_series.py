import os
import sys
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from math import isclose
from pathlib import Path
from typing import Any
from uuid import UUID

import pyarrow.dataset as ds
import pytest

from labgdd.adapters.calendar import xnys_snapshot
from labgdd.adapters.silver import ApprovedSilver, resolve_approved_silver
from labgdd.analytics.individual import ReferenceBar, calculate_individual_measurements
from labgdd.config import load_settings
from labgdd.processing.expected_series import generate_xnys_expected_series
from labgdd.processing.expected_series_spark import (
    STRICT_POLICY,
    expected_plan_fingerprint,
    expected_series_frame,
)
from labgdd.processing.gold_batch import (
    _validated_canonical,
    _validated_expected_series,
    transform_individual_gold,
)
from labgdd.research.universe import (
    CalendarProvenance,
    ExpectedSeriesPlan,
    ResearchMembership,
    ResearchSession,
    ResearchUniverse,
    generate_expected_series,
)
from labgdd.research_config import load_research_universe

ROOT = Path(__file__).resolve().parents[2]
MEMBER_FROM = datetime(2026, 1, 1, tzinfo=UTC)
MEMBER_TO = datetime(2027, 1, 1, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark() -> Any:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("labgdd-expected-series-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def membership(
    symbol: str,
    sector: str,
    valid_from: datetime = MEMBER_FROM,
    valid_to: datetime = MEMBER_TO,
) -> ResearchMembership:
    return ResearchMembership(
        symbol,
        sector,
        "study-cohort-v1",
        valid_from,
        valid_to,
        "declared_test_applicability",
        "deterministic_test_fixture",
        "declared_research_assumption",
        "unverified",
        "unverified",
        "project_defined_unverified",
        ("extended_hours",),
        ("not_historical_evidence",),
    )


def plan(
    rows: tuple[ResearchMembership, ...],
    opening: datetime,
    minutes: int,
    *,
    schedule_sha256: str = "a" * 64,
) -> ExpectedSeriesPlan:
    session = ResearchSession(
        opening.date().isoformat(), opening, opening + timedelta(minutes=minutes)
    )
    return generate_expected_series(
        ResearchUniverse("1.0.0", "study-cohort-v1", "study-cohort-v1", rows),
        (session,),
        (session.label,),
        CalendarProvenance(
            "exchange_calendars",
            "4.11.2",
            "XNYS",
            "America/New_York",
            "2025.2",
            schedule_sha256,
            "common_regular_session_benchmark",
        ),
    )


def expected_frame(spark: Any, reference: ExpectedSeriesPlan, *, strict: bool = False) -> Any:
    return expected_series_frame(
        spark,
        reference,
        source="alpaca",
        feed="iex",
        timeframe="1Min",
        adjustment="raw",
        strict_historical=strict,
    )


def expected_hash(reference: ExpectedSeriesPlan, *, strict: bool = False) -> str:
    return expected_plan_fingerprint(
        reference,
        source="alpaca",
        feed="iex",
        timeframe="1Min",
        adjustment="raw",
        strict_historical=strict,
    )


def canonical_rows(
    symbol: str,
    sector: str,
    opening: datetime,
    minutes: int,
    *,
    missing: set[int] | None = None,
    valid_from: datetime = MEMBER_FROM,
    valid_to: datetime = MEMBER_TO,
    schedule_sha256: str = "a" * 64,
) -> tuple[list[dict[str, object]], tuple[ReferenceBar, ...]]:
    missing = missing or set()
    rows: list[dict[str, object]] = []
    reference: list[ReferenceBar] = []
    for index in range(minutes):
        if index in missing:
            continue
        event_time = opening + timedelta(minutes=index)
        close = Decimal("100") + Decimal(index) / Decimal("100")
        rows.append(
            {
                "symbol": symbol,
                "sector": sector,
                "source": "alpaca",
                "feed": "iex",
                "timeframe": "1Min",
                "adjustment": "raw",
                "session_id": opening.date().isoformat(),
                "session_open": opening,
                "session_close": opening + timedelta(minutes=minutes),
                "calendar_provider": "exchange_calendars",
                "calendar_version": "4.11.2",
                "schedule_sha256": schedule_sha256,
                "membership_version": "study-cohort-v1",
                "event_time": event_time,
                "bar_end": event_time + timedelta(minutes=1),
                "open": close,
                "close": close,
                "selected_observation_id": f"{index + 1:064x}",
                "membership_valid_from": valid_from,
                "membership_valid_to": valid_to,
                "quality_status": "canonical",
            }
        )
        reference.append(ReferenceBar(event_time, close, close, valid_from, valid_to))
    return rows, tuple(reference)


def assert_number(actual: float | None, expected: float | None) -> None:
    assert (actual is None) == (expected is None)
    if actual is not None and expected is not None:
        assert isclose(actual, expected, rel_tol=1e-10, abs_tol=1e-12)


def test_authoritative_population_retains_absent_symbols_and_sectors(spark: Any) -> None:
    opening = datetime(2026, 9, 14, 13, 30, tzinfo=UTC)
    reference = plan(
        (
            membership("AAPL", "Technology"),
            membership("MSFT", "Technology"),
            membership("JPM", "Financials"),
        ),
        opening,
        6,
    )
    authority = expected_frame(spark, reference)
    rows, _ = canonical_rows("AAPL", "Technology", opening, 6)
    canonical = spark.createDataFrame(rows)
    with pytest.raises(ValueError, match="approved expected-plan fingerprint required"):
        transform_individual_gold(canonical, (5,), authority)
    result = transform_individual_gold(
        canonical,
        (5,),
        authority,
        approved_expected_plan_sha256=expected_hash(reference),
    )

    assert result.returns.count() == 18
    assert result.fixed.count() == 3
    assert result.returns.select("symbol").distinct().count() == 3
    absent = result.returns.filter("symbol in ('MSFT', 'JPM')")
    assert absent.count() == 12
    assert absent.filter("reason != 'missing_close' or eligible").count() == 0
    assert absent.filter("close is not null or selected_observation_id is not null").count() == 0
    assert result.fixed.filter("symbol = 'AAPL' and eligible").count() == 1
    assert (
        result.fixed.filter(
            "symbol in ('MSFT', 'JPM') and reason = 'incomplete_fixed_window' and not eligible"
        ).count()
        == 2
    )
    assert authority.select("expected_plan_sha256").distinct().count() == 1
    assert authority.select("expected_identity_sha256").distinct().count() == 3
    assert (
        authority.filter("population_policy_version != 'expected-series-left-join-v1'").count() == 0
    )


def test_wholly_absent_scheduled_session_is_retained(spark: Any) -> None:
    expected_open = datetime(2026, 9, 15, 13, 30, tzinfo=UTC)
    observed_open = datetime(2026, 9, 14, 13, 30, tzinfo=UTC)
    reference = plan((membership("AAPL", "Technology"),), expected_open, 6)
    unrelated, _ = canonical_rows("AAPL", "Technology", observed_open, 6)
    result = transform_individual_gold(
        spark.createDataFrame(unrelated),
        (5,),
        expected_frame(spark, reference),
        approved_expected_plan_sha256=expected_hash(reference),
    )
    assert result.returns.count() == 6
    assert result.returns.filter("reason != 'missing_close' or close is not null").count() == 0
    absent_fixed = result.fixed.first()
    assert absent_fixed is not None and absent_fixed.reason == "incomplete_fixed_window"


def test_expected_authority_rejects_null_types_eligibility_and_forged_hashes(spark: Any) -> None:
    from pyspark.sql import functions as F

    opening = datetime(2026, 9, 14, 13, 30, tzinfo=UTC)
    reference = plan((membership("AAPL", "Technology"),), opening, 6)
    authority = expected_frame(spark, reference)
    approved_hash = expected_hash(reference)
    assert _validated_expected_series(authority, None, approved_hash).count() == 1

    mandatory_null = authority.withColumn("symbol", F.lit(None).cast("string"))
    with pytest.raises(ValueError, match="schema or types"):
        _validated_expected_series(mandatory_null, None, approved_hash)

    wrong_type = authority.withColumn(
        "expected_eligible", F.col("expected_eligible").cast("string")
    )
    with pytest.raises(ValueError, match="schema or types"):
        _validated_expected_series(wrong_type, None, approved_hash)

    inconsistent_reason = F.when(
        F.col("expected_eligible"), F.lit("membership_not_full_session")
    ).otherwise(F.lit(None).cast("string"))
    inconsistent = authority.withColumn("expected_eligible", F.lit(False)).withColumn(
        "expected_ineligibility_reason", inconsistent_reason
    )
    with pytest.raises(ValueError, match="invalid or unapproved expected series"):
        _validated_expected_series(inconsistent, None, approved_hash)

    forged_identity = authority.withColumn("expected_identity_sha256", F.lit("f" * 64))
    with pytest.raises(ValueError, match="identity fingerprint mismatch"):
        _validated_expected_series(forged_identity, None, approved_hash)
    forged_plan = authority.withColumn("expected_plan_sha256", F.lit("e" * 64))
    with pytest.raises(ValueError, match="plan fingerprint mismatch"):
        _validated_expected_series(forged_plan, None, approved_hash)

    empty = authority.limit(0)
    empty_plan = replace(reference, rows=())
    assert _validated_expected_series(empty, None, expected_hash(empty_plan)).count() == 0
    with pytest.raises(ValueError, match="plan fingerprint mismatch"):
        _validated_expected_series(empty, None, "e" * 64)


def test_canonical_keys_and_observation_contract_fail_closed(spark: Any) -> None:
    from pyspark.sql import functions as F

    opening = datetime(2026, 9, 14, 13, 30, tzinfo=UTC)
    rows, _ = canonical_rows("AAPL", "Technology", opening, 6)
    canonical = spark.createDataFrame(rows)
    assert _validated_canonical(canonical).count() == 6

    with pytest.raises(ValueError, match="duplicate canonical logical key"):
        _validated_canonical(canonical.unionByName(canonical))
    invalid_end = canonical.withColumn("bar_end", F.col("bar_end") + F.expr("INTERVAL 1 MINUTE"))
    with pytest.raises(ValueError, match="invalid canonical observation"):
        _validated_canonical(invalid_end)
    invalid_quality = canonical.withColumn("quality_status", F.lit("quarantined"))
    with pytest.raises(ValueError, match="invalid canonical observation"):
        _validated_canonical(invalid_quality)


def test_missing_slots_match_python_reference_for_every_horizon(spark: Any) -> None:
    opening = datetime(2026, 11, 2, 14, 30, tzinfo=UTC)
    reference_plan = plan((membership("AAPL", "Technology"),), opening, 61)
    rows, bars = canonical_rows("AAPL", "Technology", opening, 61, missing={0, 20})
    actual = transform_individual_gold(
        spark.createDataFrame(rows),
        (5, 15, 30, 60),
        expected_frame(spark, reference_plan),
        approved_expected_plan_sha256=expected_hash(reference_plan),
    )
    expected = calculate_individual_measurements(
        bars, opening, opening + timedelta(minutes=61), horizons=(5, 15, 30, 60)
    )

    actual_returns = actual.returns.orderBy("event_time").collect()
    assert len(actual_returns) == len(expected.returns)
    for actual_return, expected_return in zip(actual_returns, expected.returns, strict=True):
        assert actual_return.event_time.replace(tzinfo=UTC) == expected_return.event_time
        assert actual_return.return_end.replace(tzinfo=UTC) == expected_return.return_end
        assert (
            actual_return.eligible == expected_return.eligible
            and actual_return.reason == expected_return.reason
        )
        assert (actual_return.close_to_close_log_return is None) == (
            expected_return.close_to_close_log_return is None
        )
        assert_number(actual_return.intrabar_log_return, expected_return.intrabar_log_return)
        assert_number(
            actual_return.close_to_close_log_return, expected_return.close_to_close_log_return
        )

    for frame, expected_windows in (
        (actual.rolling, expected.rolling),
        (actual.fixed, expected.fixed),
    ):
        value_columns = (
            ["rolling_return_volatility"]
            if frame is actual.rolling
            else ["realized_variance", "realized_volatility"]
        )
        actual_windows = frame.select(
            "horizon",
            "window_start",
            "window_end",
            "observed_returns",
            "eligible",
            "reason",
            *value_columns,
        ).collect()
        expected_keys = {
            (
                item.horizon,
                item.window_start,
                item.window_end,
                item.observed_returns,
                item.eligible,
                item.reason,
            )
            for item in expected_windows
        }
        actual_keys = {
            (
                item.horizon,
                item.window_start.replace(tzinfo=UTC),
                item.window_end.replace(tzinfo=UTC),
                item.observed_returns,
                item.eligible,
                item.reason,
            )
            for item in actual_windows
        }
        assert actual_keys == expected_keys
        expected_by_key = {
            (item.horizon, item.window_start, item.window_end): item for item in expected_windows
        }
        for actual_window in actual_windows:
            key = (
                actual_window.horizon,
                actual_window.window_start.replace(tzinfo=UTC),
                actual_window.window_end.replace(tzinfo=UTC),
            )
            expected_window = expected_by_key[key]
            if frame is actual.rolling:
                assert_number(actual_window.rolling_return_volatility, expected_window.value)
            else:
                assert_number(actual_window.realized_variance, expected_window.realized_variance)
                assert_number(actual_window.realized_volatility, expected_window.value)


def test_membership_changes_strict_mode_and_conflicts_fail_closed(spark: Any) -> None:
    opening = datetime(2026, 9, 14, 13, 30, tzinfo=UTC)
    boundary = opening + timedelta(minutes=3)
    changed = plan(
        (
            membership("AAPL", "Technology", MEMBER_FROM, boundary),
            membership("AAPL", "Research-Other", boundary, MEMBER_TO),
        ),
        opening,
        6,
    )
    rows, _ = canonical_rows("AAPL", "Technology", opening, 6)
    for index, row in enumerate(rows):
        if index < 3:
            row["membership_valid_to"] = boundary
        else:
            row["sector"] = "Research-Other"
            row["membership_valid_from"] = boundary
    canonical = spark.createDataFrame(rows)
    changed_authority = expected_frame(spark, changed)
    changed_result = transform_individual_gold(
        canonical,
        (5,),
        changed_authority,
        approved_expected_plan_sha256=expected_hash(changed),
    )
    assert changed_result.returns.filter("reason != 'membership_not_full_session'").count() == 0
    assert changed_result.returns.filter("intrabar_log_return is not null").count() == 0
    changed_fixed = changed_result.fixed.first()
    assert changed_fixed is not None and changed_fixed.reason == "membership_not_full_session"

    full = plan((membership("AAPL", "Technology"),), opening, 6)
    strict_result = transform_individual_gold(
        canonical,
        (5,),
        expected_frame(spark, full, strict=True),
        required_eligibility_policy=STRICT_POLICY,
        approved_expected_plan_sha256=expected_hash(full, strict=True),
    )
    assert (
        strict_result.returns.filter("reason != 'unverified_historical_applicability'").count() == 0
    )
    assert strict_result.returns.filter("close is not null").count() == 6
    assert strict_result.returns.filter("intrabar_log_return is not null").count() == 0
    assert strict_result.fixed.filter("realized_volatility is not null").count() == 0
    with pytest.raises(ValueError, match="required eligibility policy mismatch"):
        transform_individual_gold(
            canonical,
            (5,),
            expected_frame(spark, full),
            required_eligibility_policy=STRICT_POLICY,
            approved_expected_plan_sha256=expected_hash(full),
        )

    authority = expected_frame(spark, full)
    identical = authority.unionByName(authority)
    assert (
        transform_individual_gold(
            canonical,
            (5,),
            identical,
            approved_expected_plan_sha256=expected_hash(full),
        ).returns.count()
        == 6
    )

    from pyspark.sql import functions as F

    contradictory = authority.unionByName(
        expected_frame(spark, plan((membership("AAPL", "Financials"),), opening, 6))
    )
    with pytest.raises(ValueError, match="contradictory expected-series identity"):
        transform_individual_gold(
            canonical,
            (5,),
            contradictory,
            approved_expected_plan_sha256=expected_hash(full),
        )
    calendar_conflict = authority.unionByName(
        expected_frame(
            spark,
            plan(
                (membership("AAPL", "Technology"),),
                opening,
                6,
                schedule_sha256="b" * 64,
            ),
        )
    )
    with pytest.raises(ValueError, match="contradictory expected-series identity"):
        transform_individual_gold(
            canonical,
            (5,),
            calendar_conflict,
            approved_expected_plan_sha256=expected_hash(full),
        )
    unapproved = authority.withColumn("expectation_status", F.lit("pending"))
    with pytest.raises(ValueError, match="invalid or unapproved expected series"):
        transform_individual_gold(
            canonical,
            (5,),
            unapproved,
            approved_expected_plan_sha256=expected_hash(full),
        )


def test_canonical_provenance_mismatch_nulls_all_financial_values(spark: Any) -> None:
    opening = datetime(2026, 9, 14, 13, 30, tzinfo=UTC)
    reference = plan((membership("AAPL", "Technology"),), opening, 6)
    rows, _ = canonical_rows("AAPL", "Technology", opening, 6, schedule_sha256="b" * 64)
    result = transform_individual_gold(
        spark.createDataFrame(rows),
        (5,),
        expected_frame(spark, reference),
        approved_expected_plan_sha256=expected_hash(reference),
    )
    assert result.returns.filter("reason != 'observation_provenance_mismatch'").count() == 0
    assert result.returns.filter("intrabar_log_return is not null").count() == 0
    assert result.returns.filter("close_to_close_log_return is not null").count() == 0
    fixed = result.fixed.first()
    assert fixed is not None
    assert fixed.reason == "observation_provenance_mismatch"
    assert fixed.realized_variance is None and fixed.realized_volatility is None


def test_calendar_holiday_dst_and_early_close_projection(spark: Any) -> None:
    research = ResearchUniverse(
        "1.0.0", "study-cohort-v1", "study-cohort-v1", (membership("AAPL", "Technology"),)
    )
    holiday = generate_xnys_expected_series(
        research, xnys_snapshot(date(2026, 9, 7), date(2026, 9, 7)), ("2026-09-07",)
    )
    assert holiday.requested_sessions == ("2026-09-07",)
    assert holiday.scheduled_sessions == ()
    assert expected_frame(spark, holiday).count() == 0

    dst = generate_xnys_expected_series(
        research,
        xnys_snapshot(date(2026, 3, 6), date(2026, 3, 9)),
        ("2026-03-06", "2026-03-09"),
    )
    assert [
        row.session_open.hour for row in expected_frame(spark, dst).orderBy("session_id").collect()
    ] == [
        14,
        13,
    ]
    early = generate_xnys_expected_series(
        research, xnys_snapshot(date(2026, 11, 27), date(2026, 11, 27)), ("2026-11-27",)
    )
    row = expected_frame(spark, early).first()
    assert row.session_open.replace(tzinfo=UTC) == datetime(2026, 11, 27, 14, 30, tzinfo=UTC)
    assert row.session_close.replace(tzinfo=UTC) == datetime(2026, 11, 27, 18, 0, tzinfo=UTC)


def _optional_approved_aapl(root: Path) -> ApprovedSilver:
    settings = load_settings(root / "config/project.toml")
    run = (
        settings.data_root
        / "silver/dataset=silver-aapl-20260914-v1"
        / "run=aecb53c1-d7a8-40c7-939b-a6901f435ff5"
    )
    # Only total absence is optional. A partial run (even an empty directory or
    # broken symlink) must reach strict authority validation and fail if invalid.
    if not run.exists() and not run.is_symlink():
        pytest.skip("optional approved AAPL Silver run is absent from this checkout")
    return resolve_approved_silver(
        root / "config/silver_publications.toml",
        settings.data_root,
        "silver-aapl-20260914-v1",
        UUID("aecb53c1-d7a8-40c7-939b-a6901f435ff5"),
    )


def test_optional_aapl_absent_evidence_skips(tmp_path: Path) -> None:
    _copy_aapl_configuration(tmp_path)
    with pytest.raises(pytest.skip.Exception, match="run is absent"):
        _optional_approved_aapl(tmp_path)


@pytest.mark.parametrize("manifest_content", (None, "{}", "corrupted manifest"))
def test_optional_aapl_present_invalid_evidence_fails(
    tmp_path: Path, manifest_content: str | None
) -> None:
    _copy_aapl_configuration(tmp_path)
    run = (
        tmp_path
        / "data/silver/dataset=silver-aapl-20260914-v1"
        / "run=aecb53c1-d7a8-40c7-939b-a6901f435ff5"
    )
    run.mkdir(parents=True)
    if manifest_content is not None:
        (run / "manifest.json").write_text(manifest_content, encoding="utf-8")
    with pytest.raises(ValueError, match="manifest fingerprint mismatch"):
        _optional_approved_aapl(tmp_path)


def _copy_aapl_configuration(root: Path) -> None:
    config = root / "config"
    config.mkdir()
    for name in ("project.toml", "silver.toml", "silver_publications.toml"):
        (config / name).write_bytes((ROOT / "config" / name).read_bytes())


def test_approved_aapl_silver_parity_without_publishing(spark: Any) -> None:
    settings = load_settings(ROOT / "config/project.toml")
    approved = _optional_approved_aapl(ROOT)
    # Native Windows Hadoop cannot reliably enumerate this immutable Linux Spark
    # publication. The bounded 390-row evidence load mirrors the independent
    # PyArrow validator; all transformations below remain native Spark.
    canonical_rows = (
        ds.dataset(approved.manifest.parent / "canonical", format="parquet", partitioning="hive")
        .to_table()
        .to_pylist()
    )
    canonical = spark.createDataFrame(canonical_rows)
    research = load_research_universe(ROOT / "config/research_universe.toml", settings).universe
    expected_plan = generate_xnys_expected_series(
        research,
        xnys_snapshot(date(2026, 9, 14), date(2026, 9, 14)),
        ("2026-09-14",),
    )
    authority = expected_frame(spark, expected_plan)
    legacy = transform_individual_gold(canonical, (5, 15, 30, 60))
    integrated = transform_individual_gold(
        canonical,
        (5, 15, 30, 60),
        authority,
        approved_expected_plan_sha256=expected_hash(expected_plan),
    )

    integrated_aapl = (
        integrated.returns.filter("symbol = 'AAPL'"),
        integrated.rolling.filter("symbol = 'AAPL'"),
        integrated.fixed.filter("symbol = 'AAPL'"),
    )
    for old, new in zip(
        (legacy.returns, legacy.rolling, legacy.fixed), integrated_aapl, strict=True
    ):
        projected = new.select(*old.columns)
        assert old.exceptAll(projected).limit(1).count() == 0
        assert projected.exceptAll(old).limit(1).count() == 0
    assert integrated_aapl[0].count() == 390
    assert integrated_aapl[0].filter("eligible").count() == 389
    assert integrated_aapl[1].count() == 1_556
    assert integrated_aapl[1].filter("eligible").count() == 1_450
    assert integrated_aapl[2].count() == 120
    assert integrated_aapl[2].filter("eligible").count() == 120
    assert integrated.returns.filter("symbol != 'AAPL'").count() == 29 * 390
    assert integrated.returns.filter("symbol != 'AAPL' and reason != 'missing_close'").count() == 0
