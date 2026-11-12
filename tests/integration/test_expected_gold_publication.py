import json
import os
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from labgdd.adapters.bronze import sha256
from labgdd.adapters.calendar import CalendarSnapshot, Session
from labgdd.adapters.silver import ApprovedSilver
from labgdd.config import load_settings
from labgdd.expected_gold_config import ExpectedGoldRequest
from labgdd.processing.expected_gold import PreparedExpectedGold, process_expected_gold
from labgdd.processing.expected_series_spark import expected_plan_fingerprint
from labgdd.processing.gold import GoldProcessingError
from labgdd.research.universe import (
    CalendarProvenance,
    ResearchMembership,
    ResearchSession,
    ResearchUniverse,
    generate_expected_series,
)

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(
    os.name == "nt", reason="authoritative Spark Parquet publication requires supported Linux"
)
OPEN = datetime(2026, 9, 14, 13, 30, tzinfo=UTC)
CLOSE = OPEN + timedelta(minutes=6)
VALID_FROM = datetime(2026, 9, 14, tzinfo=UTC)
VALID_TO = datetime(2026, 9, 15, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark() -> Any:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("labgdd-expected-publication-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _membership(symbol: str) -> ResearchMembership:
    return ResearchMembership(
        symbol,
        "Technology",
        "study-cohort-v1",
        VALID_FROM,
        VALID_TO,
        "declared_fixture",
        "fixture",
        "declared_research_assumption",
        "unverified",
        "unverified",
        "project_defined_unverified",
        ("extended_hours",),
        ("not_historical_evidence",),
    )


def _prepared(tmp_path: Path, spark: Any) -> tuple[Any, ExpectedGoldRequest, PreparedExpectedGold]:
    settings = replace(load_settings(ROOT / "config/project.toml"), data_root=tmp_path / "data")
    silver = tmp_path / "silver/run"
    canonical_path = silver / "canonical"
    rows = []
    for index in range(6):
        event = OPEN + timedelta(minutes=index)
        price = Decimal("100") + Decimal(index) / Decimal("100")
        rows.append(
            {
                "symbol": "AAPL",
                "sector": "Technology",
                "source": "alpaca",
                "feed": "iex",
                "timeframe": "1Min",
                "adjustment": "raw",
                "session_id": "2026-09-14",
                "session_open": OPEN,
                "session_close": CLOSE,
                "calendar_provider": "exchange_calendars",
                "calendar_version": "4.11.2",
                "schedule_sha256": "a" * 64,
                "membership_version": "study-cohort-v1",
                "event_time": event,
                "bar_end": event + timedelta(minutes=1),
                "open": price,
                "close": price,
                "selected_observation_id": f"{index + 1:064x}",
                "membership_valid_from": VALID_FROM,
                "membership_valid_to": VALID_TO,
                "quality_status": "canonical",
            }
        )
    spark.createDataFrame(rows).write.parquet(str(canonical_path))
    manifest = silver / "manifest.json"
    manifest.write_text("{}", encoding="utf-8")
    approved = ApprovedSilver(
        tmp_path / "config/silver_publications.toml",
        manifest,
        "b" * 64,
        "silver-fixture-v1",
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        {},
    )
    universe = ResearchUniverse(
        "1.0.0",
        "study-cohort-v1",
        "study-cohort-v1",
        (_membership("AAPL"), _membership("MSFT")),
    )
    provenance = CalendarProvenance(
        "exchange_calendars",
        "4.11.2",
        "XNYS",
        "America/New_York",
        "2025.2",
        "a" * 64,
        "common_regular_session_benchmark",
    )
    plan = generate_expected_series(
        universe,
        (ResearchSession("2026-09-14", OPEN, CLOSE),),
        ("2026-09-14",),
        provenance,
    )
    calendar = CalendarSnapshot(
        (Session("2026-09-14", OPEN, CLOSE),),
        {
            "provider": "exchange_calendars",
            "version": "4.11.2",
            "calendar": "XNYS",
            "timezone": "America/New_York",
            "tzdata_version": "2025.2",
            "schedule_sha256": "a" * 64,
        },
    )
    request = ExpectedGoldRequest(
        "gold-expected-fixture-v1",
        tmp_path / "config/silver_publications.toml",
        approved.dataset_version,
        UUID(approved.run_id),
        tmp_path / "config/research_universe.toml",
        universe.universe_version,
        ("2026-09-14",),
        "1.0.0",
        "expected-series-left-join-v1",
        "declared_research",
        "2.0.0",
        (5, 15, 30, 60),
        "2.0.0",
        tmp_path / "config/expected_gold.toml",
    )
    plan_hash = expected_plan_fingerprint(
        plan, source="alpaca", feed="iex", timeframe="1Min", adjustment="raw"
    )
    prepared = PreparedExpectedGold(
        approved, universe, plan, calendar, "c" * 64, plan_hash, "d" * 64
    )
    return settings, request, prepared


def test_expected_gold_publication_is_immutable_extended_and_checksummed(
    tmp_path: Path, spark: Any
) -> None:
    settings, request, prepared = _prepared(tmp_path, spark)
    run_id = UUID("60000000-0000-4000-8000-000000000001")
    result = process_expected_gold(settings, request, spark, run_id=run_id, prepared=prepared)
    assert result.counts == {
        "expected_symbols": 2,
        "expected_sessions": 1,
        "expected_plan_rows": 2,
        "expected_return_slots": 12,
        "return_records": 12,
        "rolling_records": 40,
        "fixed_records": 2,
        "canonical_observations": 6,
        "observed_bar_slots": 6,
        "observed_symbols": 1,
        "eligible_return_records": 5,
        "eligible_rolling_records": 1,
        "eligible_fixed_records": 1,
    }
    document = json.loads(result.manifest.read_text(encoding="utf-8"))
    assert document["status"] == "complete"
    assert document["publication_status"] == "pending_architecture_review"
    assert document["approved_expected_plan_sha256"] == prepared.approved_plan_sha256
    assert document["source_completeness"] == "unproven"
    assert document["partitioning"] == ["session_id"]
    assert {item["path"].split("/")[0] for item in document["artifacts"]} == {
        "run.json",
        "returns",
        "rolling",
        "fixed",
    }
    for artifact in document["artifacts"]:
        path = result.manifest.parent / artifact["path"]
        assert path.stat().st_size == artifact["bytes"]
        assert sha256(path) == artifact["sha256"]
        assert "symbol=" not in artifact["path"]
    absent = spark.read.parquet(str(result.manifest.parent / "returns")).filter("symbol = 'MSFT'")
    assert absent.count() == 6
    assert (
        absent.filter(
            "close is not null or intrabar_log_return is not null or "
            "close_to_close_log_return is not null"
        ).count()
        == 0
    )
    with pytest.raises(FileExistsError):
        process_expected_gold(settings, request, spark, run_id=run_id, prepared=prepared)


def test_expected_gold_failure_is_not_published_complete(
    tmp_path: Path, spark: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    import labgdd.processing.expected_gold as module

    settings, request, prepared = _prepared(tmp_path, spark)

    def fail_validation(*args: object, **kwargs: object) -> dict[str, object]:
        raise ValueError("synthetic validation failure")

    monkeypatch.setattr(module, "_validate", fail_validation)
    run_id = UUID("60000000-0000-4000-8000-000000000002")
    with pytest.raises(GoldProcessingError):
        process_expected_gold(settings, request, spark, run_id=run_id, prepared=prepared)
    failed = (
        settings.data_root
        / "gold/dataset=gold-expected-fixture-v1/_failed"
        / f"run={run_id}/manifest.json"
    )
    document = json.loads(failed.read_text(encoding="utf-8"))
    assert document["status"] == "failed"
    assert document["failure_stage"] == "validate_outputs"
