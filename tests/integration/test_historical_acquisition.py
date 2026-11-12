import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pyarrow.parquet as pq
import pytest

from labgdd.adapters.alpaca import AcquisitionError, AlpacaREST, Credentials, HttpResponse
from labgdd.adapters.bronze import BronzeAcquisition
from labgdd.config import Settings, load_settings
from labgdd.historical_config import HistoricalRequest, load_historical_request
from labgdd.processing.historical import acquire_historical, observation_id
from labgdd.runners.ingest import main

ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 11, 3, tzinfo=UTC)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return replace(load_settings(ROOT / "config/project.toml"), data_root=tmp_path / "data")


@pytest.fixture
def request_config() -> HistoricalRequest:
    return HistoricalRequest(
        "fixture-v1",
        ("AAPL",),
        datetime(2026, 11, 2, 14, 30, tzinfo=UTC),
        datetime(2026, 11, 2, 14, 36, tzinfo=UTC),
        date(2026, 11, 3),
    )


def bar(minute: int = 0, **changes: object) -> dict[str, object]:
    instant = datetime(2026, 11, 2, 14, 30, tzinfo=UTC) + timedelta(minutes=minute)
    return {"t": instant.isoformat(), "o": 10, "h": 12, "l": 9, "c": 11, "v": 100, **changes}


def response(bars: object, token: str | None = None) -> HttpResponse:
    return HttpResponse(200, {}, json.dumps({"bars": bars, "next_page_token": token}).encode())


class FakeTransport:
    def __init__(self, replies: Sequence[HttpResponse]) -> None:
        self.replies = iter(replies)
        self.calls: list[dict[str, str]] = []

    def __call__(
        self, params: Mapping[str, str], headers: Mapping[str, str], timeout: float
    ) -> HttpResponse:
        self.calls.append(dict(params))
        return next(self.replies)


def client(replies: Sequence[HttpResponse]) -> tuple[AlpacaREST, FakeTransport]:
    transport = FakeTransport(replies)
    return AlpacaREST(
        Credentials("synthetic-key", "synthetic-secret"), transport, lambda _: None
    ), transport


def manifest(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


def test_success_pagination_missing_minutes_manifest_and_checksums(
    settings: Settings, request_config: HistoricalRequest
) -> None:
    rest, transport = client(
        [response({"AAPL": [bar(), bar(1)]}, "next"), response({"AAPL": [bar(2)]})]
    )
    result = acquire_historical(settings, request_config, rest, clock=lambda: NOW)
    doc = manifest(result.manifest)
    assert result.status == "complete" and doc["pages"] == 2 and doc["observations"] == 3
    assert doc["missing_regular_symbol_minutes"] == 3
    assert doc["source_completeness"] == "unproven"
    assert doc["calendar"]["calendar"] == "XNYS"
    assert doc["parameters"]["feed"] == "iex" and doc["parameters"]["adjustment"] == "raw"
    assert len(doc["package_source_sha256"]) == 64
    assert "page_token" not in transport.calls[0] and transport.calls[1]["page_token"] == "next"
    for artifact in doc["artifacts"]:
        content = (result.manifest.parent / artifact["path"]).read_bytes()
        assert hashlib.sha256(content).hexdigest() == artifact["sha256"]
        assert len(content) == artifact["bytes"]
    text = result.manifest.read_text()
    assert "synthetic-key" not in text and "synthetic-secret" not in text


@pytest.mark.parametrize("bars", [{}, None, {"AAPL": []}])
def test_empty_responses_are_complete_snapshots_with_missing_coverage(
    settings: Settings, request_config: HistoricalRequest, bars: object
) -> None:
    rest, _ = client([response(bars)])
    result = acquire_historical(settings, request_config, rest, clock=lambda: NOW)
    assert result.status == "complete"
    assert manifest(result.manifest)["missing_regular_symbol_minutes"] == 6
    assert pq.read_table(result.manifest.parent / "observations-000001.parquet").num_rows == 0


@pytest.mark.parametrize(
    "invalid",
    [
        bar(v=1.5),
        bar(v=True),
        bar(o=0),
        bar(h=1),
        bar(t="naive"),
        bar(t="2026-11-02T14:30:00.000000001Z"),
        bar(t="2026-11-02T14:30:00"),
        None,
        {"c": 10},
    ],
)
def test_invalid_observations_and_original_bytes_preserved(
    settings: Settings, request_config: HistoricalRequest, invalid: object
) -> None:
    raw = response({"AAPL": [bar(), invalid]})
    rest, _ = client([raw])
    result = acquire_historical(settings, request_config, rest, clock=lambda: NOW)
    rows = pq.read_table(result.manifest.parent / "observations-000001.parquet").to_pylist()
    assert [row["valid"] for row in rows] == [True, False]
    assert rows[1]["raw_json"] is not None and rows[1]["reason"]
    captured = pq.read_table(result.manifest.parent / "response-000001.parquet").to_pylist()[0]
    assert captured["body"] == raw.body
    assert manifest(result.manifest)["invalid_observations"] == 1


def test_duplicate_and_conflicting_observations_not_dropped_or_ordered(
    settings: Settings, request_config: HistoricalRequest
) -> None:
    rest, _ = client([response({"AAPL": [bar(), bar()]}, "next"), response({"AAPL": [bar(c=10)]})])
    result = acquire_historical(settings, request_config, rest, clock=lambda: NOW)
    doc = manifest(result.manifest)
    assert doc["observations"] == 3 and doc["duplicate_logical_keys"] == 2
    assert doc["conflicting_logical_keys"] == 1 and doc["revision_resolution"] == "not_performed"
    rows = pq.read_table(result.manifest.parent / "observations-000001.parquet").to_pylist()
    assert rows[0]["observation_id"] != rows[1]["observation_id"]


def test_repeated_pages_fail_after_archiving_without_loop(
    settings: Settings, request_config: HistoricalRequest
) -> None:
    page = response({"AAPL": [bar()]}, "same")
    rest, transport = client([page, page])
    result = acquire_historical(settings, request_config, rest, clock=lambda: NOW)
    assert result.status == "failed" and result.error == "repeated_response_page"
    assert len(transport.calls) == 2 and manifest(result.manifest)["pages"] == 2


def test_repeated_tokens_and_page_limits_are_controlled(
    settings: Settings, request_config: HistoricalRequest
) -> None:
    rest, _ = client([response({"AAPL": [bar()]}, "loop"), response({"AAPL": [bar(1)]}, "loop")])
    result = acquire_historical(settings, request_config, rest, clock=lambda: NOW)
    assert result.error == "repeated_page_token"
    rest, _ = client([response({"AAPL": [bar()]}, "next")])
    result = acquire_historical(
        settings, replace(request_config, max_pages=1), rest, clock=lambda: NOW
    )
    assert result.error == "page_limit_exhausted"


@pytest.mark.parametrize("failure", [403, 503, 429])
def test_partial_acquisition_failure_keeps_prior_pages_and_failed_manifest(
    settings: Settings, request_config: HistoricalRequest, failure: int
) -> None:
    rest, _ = client([response({"AAPL": [bar()]}, "next"), *[HttpResponse(failure, {}, b"")] * 3])
    result = acquire_historical(settings, request_config, rest, clock=lambda: NOW)
    doc = manifest(result.manifest)
    assert result.status == "failed" and doc["status"] == "failed"
    assert doc["pages"] == 1 and doc["observations"] == 1
    assert (result.manifest.parent / "observations-000001.parquet").exists()


@pytest.mark.parametrize(
    "body",
    [
        b"not-json",
        b'{"bars":[]}',
        b'{"bars":{"AAPL":1}}',
        b'{"bars":{},"next_page_token":4}',
        b'{"bars":{"AAPL":[NaN]}}',
        b'{"bars":{},"bars":{}}',
    ],
)
def test_malformed_success_response_archived_before_failure(
    settings: Settings, request_config: HistoricalRequest, body: bytes
) -> None:
    rest, _ = client([HttpResponse(200, {}, body)])
    result = acquire_historical(settings, request_config, rest, clock=lambda: NOW)
    assert result.error == "invalid_response_envelope"
    assert (
        pq.read_table(result.manifest.parent / "response-000001.parquet").to_pylist()[0]["body"]
        == body
    )


def test_independent_acquisitions_are_immutable_and_have_distinct_identities(
    settings: Settings, request_config: HistoricalRequest
) -> None:
    identity = uuid4()
    rest, _ = client([response({"AAPL": [bar()]})])
    first = acquire_historical(
        settings, request_config, rest, acquisition_id=identity, clock=lambda: NOW
    )
    first_bytes = first.manifest.read_bytes()
    rest, transport = client([])
    with pytest.raises(FileExistsError):
        acquire_historical(
            settings, request_config, rest, acquisition_id=identity, clock=lambda: NOW
        )
    assert not transport.calls and first.manifest.read_bytes() == first_bytes
    rest, _ = client([response({"AAPL": [bar()]})])
    second = acquire_historical(settings, request_config, rest, clock=lambda: NOW)
    assert second.manifest != first.manifest

    def identifier(path: Path) -> str:
        return str(
            pq.read_table(path.parent / "observations-000001.parquet").to_pylist()[0][
                "observation_id"
            ]
        )

    assert identifier(first.manifest) != identifier(second.manifest)


def test_storage_terminal_record_cannot_be_overwritten(tmp_path: Path) -> None:
    store = BronzeAcquisition(tmp_path, "fixture", uuid4(), {"status": "started"})
    terminal = store.finish("complete", {"observations": 0})
    before = terminal.read_bytes()
    with pytest.raises(ValueError):
        store.finish("failed", {})
    with pytest.raises(ValueError):
        store.write_json("late.json", {})
    assert terminal.read_bytes() == before
    with pytest.raises(ValueError):
        BronzeAcquisition(tmp_path, "../escape", uuid4(), {})


def test_storage_failure_never_publishes_success(
    settings: Settings, request_config: HistoricalRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise OSError("synthetic-disk-failure")

    monkeypatch.setattr(BronzeAcquisition, "write_parquet", fail)
    rest, _ = client([response({"AAPL": [bar()]})])
    with pytest.raises(AcquisitionError, match="storage_failed_uncommitted"):
        acquire_historical(settings, request_config, rest, clock=lambda: NOW)
    assert not list(settings.data_root.rglob("manifest.json"))


def test_manifest_cleanup_failure_does_not_misreport_published_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = BronzeAcquisition(tmp_path, "fixture", uuid4(), {})
    unlink = Path.unlink

    def fail_cleanup(path: Path, missing_ok: bool = False) -> None:
        if path.name == "manifest.pending":
            raise PermissionError("synthetic-cleanup-failure")
        unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", fail_cleanup)
    terminal = store.finish("complete", {})
    assert manifest(terminal)["status"] == "complete" and store.finished
    with pytest.raises(ValueError):
        store.write_json("late.json", {})


def test_existing_terminal_manifest_is_not_replaced(tmp_path: Path) -> None:
    store = BronzeAcquisition(tmp_path, "fixture", uuid4(), {})
    target = store.path / "manifest.json"
    target.write_bytes(b"prior-artifact")
    with pytest.raises(FileExistsError):
        store.finish("complete", {})
    assert target.read_bytes() == b"prior-artifact"


def test_no_credentials_or_network_needed_for_cli_check(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET", raising=False)
    assert (
        main(
            [
                "--check",
                "--config",
                str(ROOT / "config/project.toml"),
                "--request",
                str(ROOT / "config/historical.toml"),
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "--config",
                str(ROOT / "config/project.toml"),
                "--request",
                str(ROOT / "config/historical.toml"),
            ]
        )
        == 2
    )


def test_request_config_unknown_fields_and_unsafe_identifiers(
    tmp_path: Path, settings: Settings, request_config: HistoricalRequest
) -> None:
    template = (ROOT / "config/historical.toml").read_text()
    for name, text in [
        ("unknown", template + "unknown = 1\n"),
        ("feed", template.replace('"iex"', '"sip"')),
        ("timezone", template.replace("T14:30:00+00:00", "T14:30:00")),
    ]:
        path = tmp_path / f"{name}.toml"
        path.write_text(text)
        with pytest.raises(ValueError):
            load_historical_request(path, settings)
    for changes in (
        {"dataset_version": "../escape"},
        {"max_attempts": 0},
        {"timeout_seconds": float("nan")},
        {"end": request_config.start},
        {"end": request_config.start + timedelta(days=32)},
    ):
        with pytest.raises(ValueError):
            replace(request_config, **changes)


def test_observation_identity_preserves_replay_locator() -> None:
    identity = uuid4()
    first = observation_id(identity, 1, "AAPL", 0, "a" * 64)
    assert observation_id(identity, 1, "AAPL", 0, "a" * 64) == first
    assert observation_id(identity, 1, "AAPL", 1, "a" * 64) != first
    assert observation_id(uuid4(), 1, "AAPL", 0, "a" * 64) != first


def test_unrequested_and_out_of_range_bars_retained_as_invalid(
    settings: Settings, request_config: HistoricalRequest
) -> None:
    rest, _ = client([response({"MSFT": [bar()], "AAPL": [bar(6)]})])
    result = acquire_historical(settings, request_config, rest, clock=lambda: NOW)
    assert manifest(result.manifest)["invalid_observations"] == 2
    assert pq.read_table(result.manifest.parent / "observations-000001.parquet").num_rows == 2


def test_extended_session_observation_is_not_silently_dropped(
    settings: Settings, request_config: HistoricalRequest
) -> None:
    premarket = replace(request_config, start=request_config.start - timedelta(minutes=1))
    rest, _ = client([response({"AAPL": [bar(-1), bar()]})])
    result = acquire_historical(settings, premarket, rest, clock=lambda: NOW)
    rows = pq.read_table(result.manifest.parent / "observations-000001.parquet").to_pylist()
    assert [row["session"] for row in rows] == ["extended", "regular"]
    assert manifest(result.manifest)["observed_unique_regular_symbol_minutes"] == 1
