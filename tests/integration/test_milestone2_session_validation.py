"""September 14 session proof using synthetic responses, never live credentials."""

import hashlib
import json
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from labgdd.adapters.alpaca import AlpacaREST, Credentials, HttpResponse
from labgdd.adapters.calendar import xnys_snapshot
from labgdd.config import Settings, load_settings
from labgdd.historical_config import HistoricalRequest
from labgdd.processing.historical import (
    AcquisitionResult,
    acquire_historical,
    normalize_observation,
)

ROOT = Path(__file__).resolve().parents[2]
DAY = date(2026, 9, 14)
CAPTURED = datetime(2026, 11, 3, tzinfo=UTC)
IDENTITY = UUID("00000000-0000-4000-8000-000000000014")


def raw_bar(timestamp: str, volume: str = "10") -> str:
    # Numeric spelling and provider extras must survive in the exact page bytes.
    return (
        '{"t":'
        + json.dumps(timestamp)
        + ',"o":150.1250,"h":150.5000,"l":149.7500,"c":150.2500,"v":'
        + volume
        + ',"n":2,"vw":150.2000}'
    )


FIRST = (
    '{"bars":{"AAPL":['
    + raw_bar("2026-09-14T13:30:00Z")
    + ","
    + raw_bar("2026-09-14T09:32:00-04:00")
    + ","
    + raw_bar("2026-09-14T13:33:00Z", "1.5")
    + ']},"next_page_token":"synthetic-second-page"}'
).encode()
SECOND = (
    '{"bars":{"AAPL":['
    + raw_bar("2026-09-14T13:30:00Z")
    + ","
    + raw_bar("2026-09-14T19:59:00Z")
    + ']},"next_page_token":null}'
).encode()


class SessionTransport:
    def __init__(self, failure_status: int | None = None) -> None:
        self.parameters: list[dict[str, str]] = []
        self.failure_status = failure_status

    def __call__(
        self, parameters: Mapping[str, str], headers: Mapping[str, str], timeout: float
    ) -> HttpResponse:
        self.parameters.append(dict(parameters))
        assert parameters["symbols"] == "AAPL"
        assert parameters["feed"] == "iex" and parameters["timeframe"] == "1Min"
        assert parameters["start"] == "2026-09-14T13:30:00+00:00"
        assert parameters["end"] == "2026-09-14T19:59:59.999999+00:00"
        assert parameters["adjustment"] == "raw" and 0 < timeout <= 120
        assert set(headers) == {"APCA-API-KEY-ID", "APCA-API-SECRET-KEY"}
        if len(self.parameters) == 1:
            assert "page_token" not in parameters
            return HttpResponse(200, {}, FIRST)
        assert parameters["page_token"] == "synthetic-second-page"
        if self.failure_status is not None:
            return HttpResponse(self.failure_status, {}, b"")
        return HttpResponse(200, {}, SECOND)


@pytest.fixture
def session_config(tmp_path: Path) -> tuple[Settings, HistoricalRequest]:
    (session,) = xnys_snapshot(DAY, DAY).sessions
    assert session.label == "2026-09-14"
    assert session.open == datetime(2026, 9, 14, 13, 30, tzinfo=UTC)
    assert session.close == datetime(2026, 9, 14, 20, tzinfo=UTC)
    settings = replace(load_settings(ROOT / "config/project.toml"), data_root=tmp_path / "data")
    request = HistoricalRequest(
        "milestone2-mocked-20260914-v1",
        ("AAPL",),
        session.open,
        session.close,
        DAY,
        max_pages=2,
    )
    return settings, request


def acquire(
    settings: Settings,
    request: HistoricalRequest,
    transport: SessionTransport,
    identity: UUID = IDENTITY,
) -> AcquisitionResult:
    rest = AlpacaREST(
        Credentials("synthetic-only-key", "synthetic-only-secret"), transport, sleep=lambda _: None
    )
    return acquire_historical(
        settings, request, rest, acquisition_id=identity, clock=lambda: CAPTURED
    )


def read_manifest(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


def snapshot_files(directory: Path) -> dict[str, str]:
    return {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in directory.iterdir()
        if p.is_file()
    }


def test_requested_session_fields_identity_schemas_and_manifest(
    session_config: tuple[Settings, HistoricalRequest],
    tmp_path: Path,
) -> None:
    settings, request = session_config
    transport = SessionTransport()
    result = acquire(settings, request, transport)
    doc = read_manifest(result.manifest)
    assert result.status == "complete" and result.error is None
    assert len(transport.parameters) == doc["request_attempts"] == doc["pages"] == 2
    assert doc["observations"] == 5 and doc["invalid_observations"] == 1
    assert doc["duplicate_logical_keys"] == 1 and doc["conflicting_logical_keys"] == 0
    assert doc["expected_regular_symbol_minutes"] == 390
    assert doc["observed_unique_regular_symbol_minutes"] == 3
    assert doc["missing_regular_symbol_minutes"] == 387
    assert (
        doc["source_completeness"] == "unproven" and doc["revision_resolution"] == "not_performed"
    )
    assert doc["source"] == "alpaca" and doc["feed"] == "iex" and doc["schema_version"] == "1.0.0"
    assert doc["symbols"] == ["AAPL"] and doc["adjustment"] == "raw"
    assert doc["calendar"]["calendar"] == "XNYS"
    assert doc["calendar"]["version"] == "4.11.2"
    assert doc["calendar"]["tzdata_version"] == "2025.2"
    stored_calendar = json.loads((result.manifest.parent / "calendar.json").read_text())
    assert doc["calendar"] == stored_calendar
    assert doc["parameters"] == request.parameters()
    assert doc["project_start_inclusive"] == request.start.isoformat()
    assert doc["project_end_exclusive"] == request.end.isoformat()
    assert doc["acquisition_id"] == doc["run_id"] == str(IDENTITY)
    assert datetime.fromisoformat(doc["started_at"]) == CAPTURED
    assert datetime.fromisoformat(doc["finished_at"]).utcoffset() == UTC.utcoffset(None)

    response_schema = pa.schema(
        [
            ("page", pa.int64()),
            ("received_at", pa.timestamp("us", tz="UTC")),
            ("request_token", pa.string()),
            ("body", pa.binary()),
            ("body_sha256", pa.string()),
        ]
    )
    observation_schema = pa.schema(
        [
            ("observation_id", pa.string()),
            ("symbol", pa.string()),
            ("raw_json", pa.string()),
            ("event_time", pa.timestamp("us", tz="UTC")),
            ("valid", pa.bool_()),
            ("reason", pa.string()),
            ("session", pa.string()),
        ]
    )
    identifiers: list[str] = []
    for page, body in enumerate((FIRST, SECOND), 1):
        response_file = pq.ParquetFile(result.manifest.parent / f"response-{page:06d}.parquet")
        assert response_file.schema_arrow == response_schema
        (captured,) = response_file.read().to_pylist()
        digest = hashlib.sha256(body).hexdigest()
        assert captured["page"] == page and captured["body"] == body
        assert captured["body_sha256"] == digest and captured["received_at"] == CAPTURED
        assert captured["request_token"] == (None if page == 1 else "synthetic-second-page")
        observation_file = pq.ParquetFile(
            result.manifest.parent / f"observations-{page:06d}.parquet"
        )
        table = observation_file.read()
        assert observation_file.schema_arrow == observation_schema
        assert table.equals(observation_file.read())  # Repeat read, identical values/schema.
        rows = table.to_pylist()
        source = json.loads(body, parse_float=Decimal)["bars"]["AAPL"]
        assert len(rows) == len(source)
        for index, (stored, raw) in enumerate(zip(rows, source, strict=True)):
            fields = ("bronze-observation-v1", str(IDENTITY), str(page), "AAPL", str(index), digest)
            framed = b"".join(
                len(field.encode()).to_bytes(8, "big") + field.encode() for field in fields
            )
            assert stored["observation_id"] == hashlib.sha256(framed).hexdigest()
            identifiers.append(stored["observation_id"])
            projection = json.loads(stored["raw_json"], parse_float=Decimal)
            assert projection == json.loads(json.dumps(raw, default=str))
            assert projection["n"] == 2 and projection["vw"] == "150.2000"
            if raw["v"] == Decimal("1.5"):
                assert not stored["valid"] and stored["reason"]
                assert stored["event_time"] is None
                continue
            normalized = normalize_observation(raw, "AAPL", CAPTURED, stored["observation_id"])
            assert normalized.ohlcv == (
                Decimal("150.1250"),
                Decimal("150.5000"),
                Decimal("149.7500"),
                Decimal("150.2500"),
                10,
            )
            assert stored["valid"] and stored["session"] == "regular" and stored["reason"] is None
            assert stored["event_time"] == normalized.event_time
            assert stored["event_time"].utcoffset() == UTC.utcoffset(None)
            assert normalized.source_event_id is None
            assert normalized.source_revision is None
    assert (
        len(set(identifiers)) == 5
    )  # Equivalent bars still have different observation identities.

    before = snapshot_files(result.manifest.parent)
    for artifact in doc["artifacts"]:
        path = result.manifest.parent / artifact["path"]
        assert before[path.name] == artifact["sha256"]
        assert path.stat().st_size == artifact["bytes"]
    assert {a["path"] for a in doc["artifacts"]} == set(before) - {"manifest.json"}
    assert read_manifest(result.manifest) == doc
    assert snapshot_files(result.manifest.parent) == before
    (tmp_path / "milestone2-validation.json").write_text(
        json.dumps(
            {
                "mode": "mocked",
                "live_ran": False,
                "start": request.start.isoformat(),
                "end": request.end.isoformat(),
                "rows": 5,
                "valid": 4,
                "invalid": 1,
                "unique_regular_minutes": 3,
                "missing_minutes": 387,
                "manifest": str(result.manifest.relative_to(tmp_path)),
                "checksums_verified": True,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def test_duplicate_acquisition_id_preserves_every_completed_artifact(
    session_config: tuple[Settings, HistoricalRequest],
) -> None:
    settings, request = session_config
    completed = acquire(settings, request, SessionTransport())
    before = snapshot_files(completed.manifest.parent)
    duplicate_transport = SessionTransport()
    with pytest.raises(FileExistsError):
        acquire(settings, request, duplicate_transport)
    assert duplicate_transport.parameters == []
    assert snapshot_files(completed.manifest.parent) == before


@pytest.mark.parametrize("failure_status", [403, 503])
def test_failed_session_acquisition_never_reports_complete(
    session_config: tuple[Settings, HistoricalRequest],
    failure_status: int,
) -> None:
    settings, request = session_config
    result = acquire(settings, request, SessionTransport(failure_status))
    doc = read_manifest(result.manifest)
    assert result.status == doc["status"] == "failed" and result.error
    assert doc["pages"] == 1 and doc["observations"] == 3 and doc["invalid_observations"] == 1
    assert not (result.manifest.parent / "response-000002.parquet").exists()
    for artifact in doc["artifacts"]:
        assert snapshot_files(result.manifest.parent)[artifact["path"]] == artifact["sha256"]
