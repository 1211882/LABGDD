import hashlib
import json
import os
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast
from uuid import UUID, uuid4

import pyarrow.parquet as pq
import pytest

import labgdd.processing.silver as silver_processing
from labgdd.adapters.bronze import BronzeAcquisition, sha256
from labgdd.adapters.silver import SilverRun, verify_bronze_manifest
from labgdd.config import Settings
from labgdd.domain.market_bar import ContractError, MarketBar
from labgdd.processing.silver import SilverProcessingError, process_silver
from labgdd.processing.spark_batch import Membership, transform_observations
from labgdd.silver_config import SilverRequest

OPEN = datetime(2026, 9, 14, 13, 30, tzinfo=UTC)
CLOSE = OPEN + timedelta(minutes=6)


@pytest.fixture(scope="module")
def spark() -> Any:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("labgdd-silver-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def settings(root: Path) -> Settings:
    return Settings(
        "1.0.0",
        "study-cohort-v1",
        "study-cohort-v1",
        root / "data",
        "INFO",
        MappingProxyType({"Technology": ("AAPL", "MSFT")}),
    )


def request(root: Path, manifest: Path, dataset: str = "silver-fixture-v1") -> SilverRequest:
    return SilverRequest(
        dataset,
        (manifest,),
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2027, 1, 1, tzinfo=UTC),
        "synthetic_research_fixture",
    )


def payload(minute: int, **changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "t": (OPEN + timedelta(minutes=minute)).isoformat(),
        "o": 10,
        "h": 12,
        "l": 9,
        "c": 11,
        "v": 100,
    }
    value.update(changes)
    return value


def oid(number: int) -> str:
    return hashlib.sha256(str(number).encode()).hexdigest()


def observation(
    number: int, symbol: str, raw: dict[str, object], *, valid: bool = True
) -> dict[str, object]:
    return {
        "observation_id": oid(number),
        "symbol": symbol,
        "raw_json": json.dumps(raw, sort_keys=True),
        "event_time": datetime.fromisoformat(cast(str, raw["t"])) if valid else None,
        "valid": valid,
        "reason": None if valid else "fixture_invalid",
        "session": "regular",
    }


def unchecked_observation(number: int, raw: dict[str, object]) -> dict[str, object]:
    """Keep Bronze marked valid so Silver independently enforces the domain contract."""
    row = observation(number, "AAPL", raw)
    row["event_time"] = datetime.fromisoformat(cast(str, raw["t"]))
    row["valid"] = True
    row["reason"] = None
    return row


def bronze(
    root: Path,
    rows: list[dict[str, object]],
    *,
    opening: datetime = OPEN,
    closing: datetime = CLOSE,
    symbols: tuple[str, ...] = ("AAPL",),
    identity: UUID | None = None,
) -> Path:
    acquisition = identity or uuid4()
    sessions = [
        {"session": str(opening.date()), "open": opening.isoformat(), "close": closing.isoformat()}
    ]
    calendar = {
        "provider": "exchange_calendars",
        "version": "4.11.2",
        "calendar": "XNYS",
        "schedule_sha256": hashlib.sha256(
            json.dumps(sessions, sort_keys=True).encode()
        ).hexdigest(),
        "sessions": sessions,
    }
    metadata: dict[str, object] = {
        "manifest_version": "1.0.0",
        "schema_version": "1.0.0",
        "observation_identity_version": "bronze-observation-v1",
        "acquisition_id": str(acquisition),
        "dataset_version": "bronze-fixture-v1",
        "source": "alpaca",
        "feed": "iex",
        "timeframe": "1Min",
        "adjustment": "raw",
        "symbols": list(symbols),
        "calendar": calendar,
        "revision_resolution": "not_performed",
    }
    store = BronzeAcquisition(root / "data", "bronze-fixture-v1", acquisition, metadata)
    store.write_json("calendar.json", calendar)
    store.write_parquet(
        "response-000001.parquet",
        [
            {
                "page": 1,
                "received_at": datetime(2026, 11, 3, tzinfo=UTC),
                "request_token": None,
                "body": b'{"fixture":true}',
                "body_sha256": hashlib.sha256(b'{"fixture":true}').hexdigest(),
            }
        ],
        pages=True,
    )
    store.write_parquet("observations-000001.parquet", rows)
    return store.finish("complete", {"revision_resolution": "not_performed"})


def pure_valid(raw: dict[str, object], symbol: str, identifier: str) -> bool:
    try:
        MarketBar(
            datetime.fromisoformat(cast(str, raw["t"])),
            symbol,
            Decimal(str(raw["o"])),
            Decimal(str(raw["h"])),
            Decimal(str(raw["l"])),
            Decimal(str(raw["c"])),
            cast(int, raw["v"]),
            "alpaca",
            "iex",
            datetime(2026, 11, 3, tzinfo=UTC),
            identifier,
        )
        return True
    except (ContractError, KeyError, ValueError):
        return False


def test_quality_resolution_lineage_and_reference_parity(tmp_path: Path, spark: Any) -> None:
    rows = [
        observation(1, "AAPL", payload(0)),
        observation(2, "AAPL", payload(1)),
        observation(3, "AAPL", payload(1)),
        observation(4, "AAPL", payload(2)),
        observation(5, "AAPL", payload(2, c=10)),
        observation(6, "AAPL", payload(3, o=0), valid=False),
        observation(7, "AAPL", payload(4, v=1.5), valid=False),
        observation(8, "ZZZZ", payload(5)),
        observation(9, "AAPL", payload(4)),
        {**observation(9, "AAPL", payload(5)), "observation_id": oid(9)},
    ]
    manifest = bronze(tmp_path, rows)
    source = verify_bronze_manifest(manifest)
    frames = transform_observations(
        spark, (source,), settings(tmp_path), request(tmp_path, manifest)
    )
    canonical = sorted(frames.canonical.collect(), key=lambda row: row.event_time)
    quarantine = frames.quarantine.collect()
    duplicates = frames.duplicates.collect()
    assert len(canonical) == 2
    assert canonical[1].quality_status == "canonical_equivalent_duplicate"
    assert canonical[1].contributing_observation_ids == sorted([oid(2), oid(3)])
    assert canonical[1].selected_observation_id == min(oid(2), oid(3))
    assert len(quarantine) == 7
    reasons = [reason for row in quarantine for reason in row.quality_reasons]
    assert {
        "unresolved_conflict",
        "invalid_price",
        "invalid_volume",
        "unknown_membership",
        "observation_identity_conflict",
    } <= set(reasons)
    assert sorted(row.duplicate_classification for row in duplicates) == sorted(
        [
            "equivalent_duplicate_payload",
            "unresolved_conflict",
            "observation_identity_conflict",
        ]
    )
    assert frames.expected.count() == 6
    assert sum(pure_valid(payload(i), "AAPL", oid(20 + i)) for i in range(3)) == 3
    assert all(row.open == Decimal("10.00000000") for row in canonical)


def test_order_independence_multiple_symbols_and_variable_membership(
    tmp_path: Path, spark: Any
) -> None:
    rows = [observation(30, "AAPL", payload(0)), observation(31, "MSFT", payload(1))]
    manifest = bronze(tmp_path, list(reversed(rows)), symbols=("AAPL", "MSFT"))
    source = verify_bronze_manifest(manifest)
    req = request(tmp_path, manifest)
    variable = (
        Membership("AAPL", "Technology", OPEN, OPEN + timedelta(minutes=1), "variable-v1"),
        Membership("MSFT", "Technology", OPEN + timedelta(minutes=1), CLOSE, "variable-v1"),
    )
    first = transform_observations(spark, (source,), settings(tmp_path), req, memberships=variable)
    second = transform_observations(spark, (source,), settings(tmp_path), req, memberships=variable)

    def project(frame: Any) -> list[tuple[str, datetime, str]]:
        return [
            (row.symbol, row.event_time, row.selected_observation_id) for row in frame.collect()
        ]

    assert project(first.canonical) == project(second.canonical)
    assert [row.symbol for row in first.canonical.collect()] == ["AAPL", "MSFT"]
    assert first.expected.count() == 6


def test_early_close_and_dst_schedule_geometry(tmp_path: Path, spark: Any) -> None:
    early_open = datetime(2026, 11, 27, 14, 30, tzinfo=UTC)
    early_close = datetime(2026, 11, 27, 18, 0, tzinfo=UTC)
    row = observation(40, "AAPL", {**payload(0), "t": early_open.isoformat()})
    manifest = bronze(tmp_path, [row], opening=early_open, closing=early_close)
    frames = transform_observations(
        spark, (verify_bronze_manifest(manifest),), settings(tmp_path), request(tmp_path, manifest)
    )
    assert frames.expected.count() == 210
    assert frames.canonical.collect()[0].session_id == "2026-11-27"
    assert datetime(2026, 3, 9, 13, 30, tzinfo=UTC).hour == 13
    assert datetime(2026, 11, 2, 14, 30, tzinfo=UTC).hour == 14


def test_multi_session_utc_instants_across_dst_and_early_close(tmp_path: Path, spark: Any) -> None:
    sessions = (
        (
            datetime(2026, 9, 14, 13, 30, tzinfo=UTC),
            datetime(2026, 9, 14, 20, 0, tzinfo=UTC),
        ),
        (
            datetime(2026, 11, 2, 14, 30, tzinfo=UTC),
            datetime(2026, 11, 2, 21, 0, tzinfo=UTC),
        ),
        (
            datetime(2026, 11, 27, 14, 30, tzinfo=UTC),
            datetime(2026, 11, 27, 18, 0, tzinfo=UTC),
        ),
    )
    manifests = []
    for number, (opening, closing) in enumerate(sessions, start=500):
        raw = payload(0, t=opening.isoformat())
        manifests.append(
            bronze(tmp_path, [observation(number, "AAPL", raw)], opening=opening, closing=closing)
        )
    req = request(tmp_path, manifests[0])
    frames = transform_observations(
        spark,
        tuple(verify_bronze_manifest(manifest) for manifest in manifests),
        settings(tmp_path),
        req,
    )
    from pyspark.sql import functions as F

    timestamp_fields = (
        "event_time",
        "bar_end",
        "session_open",
        "session_close",
        "membership_valid_from",
        "membership_valid_to",
    )
    canonical = (
        frames.canonical.select(
            *(F.unix_micros(F.col(field)).alias(field) for field in timestamp_fields)
        )
        .orderBy("event_time")
        .collect()
    )

    def micros(value: datetime) -> int:
        return int(value.timestamp() * 1_000_000)

    assert [
        (row.event_time, row.bar_end, row.session_open, row.session_close) for row in canonical
    ] == [
        tuple(
            micros(value) for value in (opening, opening + timedelta(minutes=1), opening, closing)
        )
        for opening, closing in sessions
    ]
    assert all(row.membership_valid_from == micros(req.membership_valid_from) for row in canonical)
    assert all(row.membership_valid_to == micros(req.membership_valid_to) for row in canonical)
    assert frames.expected.count() == 390 + 390 + 210


def test_raw_numeric_contract_matches_market_bar(tmp_path: Path, spark: Any) -> None:
    prices: list[object] = [
        10,
        10.125,
        "0.00000001",
        "1.000000000",
        "123456789012345678.9",
        "1.123456789",
        "NaN",
        0,
        -1,
        "1000000000000000000",
    ]
    rows: list[dict[str, object]] = []
    expected: list[bool] = []
    for minute, price in enumerate(prices):
        raw = payload(minute, o=price, h=price, l=price, c=price)
        rows.append(unchecked_observation(100 + minute, raw))
        expected.append(pure_valid(raw, "AAPL", oid(100 + minute)))
    fractional = payload(len(rows), v=1.5)
    rows.append(unchecked_observation(200, fractional))
    expected.append(pure_valid(fractional, "AAPL", oid(200)))
    overflow = payload(len(rows), v=2**63)
    rows.append(unchecked_observation(201, overflow))
    expected.append(pure_valid(overflow, "AAPL", oid(201)))
    closing = OPEN + timedelta(minutes=len(rows))
    manifest = bronze(tmp_path, rows, closing=closing)
    frames = transform_observations(
        spark, (verify_bronze_manifest(manifest),), settings(tmp_path), request(tmp_path, manifest)
    )
    assert frames.canonical.count() == sum(expected)
    reasons = [reason for row in frames.quarantine.collect() for reason in row.quality_reasons]
    assert reasons.count("invalid_price") == expected.count(False) - 2
    assert reasons.count("invalid_volume") == 2


def test_identity_integrity_and_overlapping_acquisitions(tmp_path: Path, spark: Any) -> None:
    shared = payload(0)
    first_manifest = bronze(tmp_path, [observation(300, "AAPL", shared)])
    second_manifest = bronze(tmp_path, [observation(301, "AAPL", shared)])
    req = request(tmp_path, first_manifest)
    frames = transform_observations(
        spark,
        (verify_bronze_manifest(first_manifest), verify_bronze_manifest(second_manifest)),
        settings(tmp_path),
        req,
    )
    canonical = frames.canonical.collect()
    assert len(canonical) == 1
    assert canonical[0].quality_status == "canonical_equivalent_duplicate"
    assert canonical[0].contributing_observation_ids == sorted([oid(300), oid(301)])
    assert frames.expected.count() == 6

    identity_rows = [
        observation(310, "AAPL", payload(1)),
        {**observation(310, "MSFT", payload(1)), "observation_id": oid(310)},
        {**observation(312, "AAPL", payload(2)), "observation_id": None},
        {**observation(313, "AAPL", payload(3)), "observation_id": "A" * 64},
    ]
    identity_manifest = bronze(tmp_path, identity_rows, symbols=("AAPL", "MSFT"))
    identity_frames = transform_observations(
        spark,
        (verify_bronze_manifest(identity_manifest),),
        settings(tmp_path),
        request(tmp_path, identity_manifest),
    )
    reasons = [
        reason for row in identity_frames.quarantine.collect() for reason in row.quality_reasons
    ]
    assert reasons.count("observation_identity_conflict") == 2
    assert reasons.count("invalid_observation_id") == 2
    assert identity_frames.canonical.count() == 0


def test_same_session_calendar_provenance_conflict(tmp_path: Path, spark: Any) -> None:
    first_manifest = bronze(tmp_path, [observation(320, "AAPL", payload(0))])
    second_manifest = bronze(
        tmp_path,
        [observation(321, "AAPL", payload(0))],
        closing=CLOSE + timedelta(minutes=1),
    )
    with pytest.raises(ValueError, match="incompatible calendar provenance"):
        transform_observations(
            spark,
            (verify_bronze_manifest(first_manifest), verify_bronze_manifest(second_manifest)),
            settings(tmp_path),
            request(tmp_path, first_manifest),
        )


def test_manifest_immutability_repeat_and_failed_publication(
    tmp_path: Path, spark: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = bronze(tmp_path, [observation(50, "AAPL", payload(0))])
    project = settings(tmp_path)
    req = request(tmp_path, manifest)
    identity = uuid4()
    result = process_silver(project, req, spark, run_id=identity, publisher="pyarrow_compat")
    doc = json.loads(result.manifest.read_text())
    assert doc["status"] == "complete" and doc["canonical_records"] == 1
    assert doc["missing_session_records"] == 5
    before = {item["path"]: item["sha256"] for item in doc["artifacts"]}
    with pytest.raises(FileExistsError):
        process_silver(project, req, spark, run_id=identity, publisher="pyarrow_compat")
    assert {
        item["path"]: item["sha256"]
        for item in json.loads(result.manifest.read_text())["artifacts"]
    } == before
    for artifact in doc["artifacts"]:
        assert sha256(result.manifest.parent / artifact["path"]) == artifact["sha256"]
    canonical_file = next((result.manifest.parent / "canonical").rglob("*.parquet"))
    canonical_table = pq.ParquetFile(canonical_file).read()
    assert canonical_table.num_rows == 1
    assert canonical_table.column("event_time").to_pylist() == [OPEN]
    assert canonical_table.column("bar_end").to_pylist() == [OPEN + timedelta(minutes=1)]
    assert canonical_table.column("session_open").to_pylist() == [OPEN]
    assert canonical_table.column("session_close").to_pylist() == [CLOSE]
    assert canonical_table.column("membership_valid_from").to_pylist() == [
        req.membership_valid_from
    ]
    assert canonical_table.column("membership_valid_to").to_pylist() == [req.membership_valid_to]
    repeated = process_silver(project, req, spark, publisher="pyarrow_compat")
    assert repeated.manifest != result.manifest
    repeated_file = next((repeated.manifest.parent / "canonical").rglob("*.parquet"))
    assert (
        pq.ParquetFile(repeated_file).read().to_pylist()
        == pq.ParquetFile(canonical_file).read().to_pylist()
    )

    original = SilverRun.write_parquet

    def fail(self: SilverRun, relative: str, rows: Any, *, kind: str) -> None:
        raise OSError("synthetic persistence failure")

    monkeypatch.setattr(SilverRun, "write_parquet", fail)
    with pytest.raises(SilverProcessingError):
        process_silver(
            project,
            replace(req, dataset_version="silver-failed-v1"),
            spark,
            publisher="pyarrow_compat",
        )
    failed = list((project.data_root / "silver/dataset=silver-failed-v1").rglob("manifest.json"))
    assert len(failed) == 1 and json.loads(failed[0].read_text())["status"] == "failed"
    monkeypatch.setattr(SilverRun, "write_parquet", original)

    observation_file = next(manifest.parent.glob("observations-*.parquet"))
    observation_file.write_bytes(observation_file.read_bytes() + b"tamper")
    with pytest.raises(ValueError, match=r"artifact (size|checksum) mismatch"):
        verify_bronze_manifest(manifest)


def test_publication_rename_failure_is_not_complete_and_unpersists(
    tmp_path: Path, spark: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = bronze(tmp_path, [observation(400, "AAPL", payload(0))])
    project = settings(tmp_path)
    req = replace(request(tmp_path, manifest), dataset_version="silver-publish-failed-v1")
    original_transform = transform_observations
    captured: list[Any] = []

    def capture(*args: Any, **kwargs: Any) -> Any:
        frames = original_transform(*args, **kwargs)
        captured.extend([frames.canonical, frames.quarantine, frames.duplicates])
        return frames

    original_rename = os.rename
    attempts = 0

    def fail_complete_once(source: Any, target: Any) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("synthetic publication failure")
        original_rename(source, target)

    monkeypatch.setattr(silver_processing, "transform_observations", capture)
    monkeypatch.setattr(os, "rename", fail_complete_once)
    with pytest.raises(SilverProcessingError):
        process_silver(project, req, spark, publisher="pyarrow_compat")
    dataset_root = project.data_root / "silver/dataset=silver-publish-failed-v1"
    assert not list(dataset_root.glob("run=*/manifest.json"))
    failed = list(dataset_root.glob("_failed/run=*/manifest.json"))
    assert len(failed) == 1
    document = json.loads(failed[0].read_text())
    assert document["status"] == "failed"
    assert document["failure_stage"] == "publish_complete"
    assert document["failure_type"] == "OSError"
    assert all(not frame.is_cached for frame in captured)


def test_manifest_generation_failure_publishes_only_failed_evidence(
    tmp_path: Path, spark: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = bronze(tmp_path, [observation(410, "AAPL", payload(0))])
    project = settings(tmp_path)
    req = replace(request(tmp_path, manifest), dataset_version="silver-manifest-failed-v1")
    original_write_json = SilverRun.write_json
    injected = False

    def fail_complete_manifest(self: SilverRun, name: str, value: Any) -> None:
        nonlocal injected
        if name == "manifest.pending" and not injected:
            injected = True
            raise OSError("synthetic manifest failure")
        original_write_json(self, name, value)

    monkeypatch.setattr(SilverRun, "write_json", fail_complete_manifest)
    with pytest.raises(SilverProcessingError):
        process_silver(project, req, spark, publisher="pyarrow_compat")
    dataset_root = project.data_root / "silver/dataset=silver-manifest-failed-v1"
    assert not list(dataset_root.glob("run=*/manifest.json"))
    failed = list(dataset_root.glob("_failed/run=*/manifest.json"))
    assert len(failed) == 1
    document = json.loads(failed[0].read_text())
    assert document["status"] == "failed"
    assert document["failure_stage"] == "publish_complete"
    assert document["failure_type"] == "OSError"
