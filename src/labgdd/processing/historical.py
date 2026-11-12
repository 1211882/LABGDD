"""Historical acquisition with raw-first Bronze capture and explicit failures."""

import hashlib
import importlib.metadata
import json
import platform
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import cast
from uuid import UUID, uuid4

from labgdd.adapters.alpaca import AcquisitionError, AlpacaREST
from labgdd.adapters.bronze import BronzeAcquisition
from labgdd.adapters.calendar import xnys_snapshot
from labgdd.config import ConfigurationError, Settings
from labgdd.domain.market_bar import ContractError, MarketBar
from labgdd.historical_config import HistoricalRequest


@dataclass(frozen=True)
class AcquisitionResult:
    manifest: Path
    status: str
    error: str | None


def observation_id(acquisition: UUID, page: int, symbol: str, row: int, body_hash: str) -> str:
    fields = ("bronze-observation-v1", str(acquisition), str(page), symbol, str(row), body_hash)
    encoded = b"".join(len(value.encode()).to_bytes(8, "big") + value.encode() for value in fields)
    return hashlib.sha256(encoded).hexdigest()


def parse_page(body: bytes) -> tuple[dict[str, list[object]], str | None]:
    def reject_constant(value: str) -> None:
        raise ValueError("nonstandard JSON number")

    def unique_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    try:
        document = json.loads(
            body, parse_float=Decimal, parse_constant=reject_constant, object_pairs_hook=unique_keys
        )
        if not isinstance(document, dict) or "bars" not in document:
            raise ValueError
        bars = document["bars"]
        if bars is None:
            bars = {}
        if not isinstance(bars, dict) or not all(
            isinstance(k, str) and isinstance(v, list) for k, v in bars.items()
        ):
            raise ValueError
        token = document.get("next_page_token")
        if token is not None and (
            not isinstance(token, str)
            or not token
            or len(token) > 4096
            or any(ord(c) < 32 for c in token)
        ):
            raise ValueError
        return cast(dict[str, list[object]], bars), token
    except (ValueError, TypeError, UnicodeDecodeError, RecursionError):
        raise AcquisitionError("invalid_response_envelope") from None


def normalize_observation(
    raw: object, symbol: str, received: datetime, identifier: str
) -> MarketBar:
    if not isinstance(raw, dict):
        raise ContractError("observation: object required")
    try:
        timestamp = raw["t"]
        if not isinstance(timestamp, str):
            raise ValueError
        # Python truncates sub-microsecond precision; minute labels must not hide it.
        fraction = re.search(r"\.(\d+)", timestamp)
        if fraction and any(c != "0" for c in fraction.group(1)):
            raise ValueError
        instant = datetime.fromisoformat(timestamp)
        if instant.utcoffset() is None:
            raise ValueError
        prices = []
        for field in ("o", "h", "l", "c"):
            value = raw[field]
            if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
                raise ValueError
            prices.append(Decimal(value))
        return MarketBar(
            event_time=instant.astimezone(UTC),
            symbol=symbol,
            open=prices[0],
            high=prices[1],
            low=prices[2],
            close=prices[3],
            volume=raw["v"],
            source="alpaca",
            feed="iex",
            ingested_at=received,
            observation_id=identifier,
        )
    except (KeyError, ValueError, TypeError, InvalidOperation, OverflowError):
        raise ContractError("observation: invalid OHLCV contract") from None


def acquire_historical(
    settings: Settings,
    request: HistoricalRequest,
    client: AlpacaREST,
    *,
    acquisition_id: UUID | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> AcquisitionResult:
    request.validate_universe(settings)
    started = clock()
    if request.end > started:
        raise ConfigurationError("acquisition: historical interval ends in the future")
    calendar = xnys_snapshot(request.start.date(), request.end.date())
    identity = acquisition_id or uuid4()
    versions = {
        name: importlib.metadata.version(name)
        for name in ("labgdd-v2", "exchange_calendars", "pyarrow", "numpy", "pandas", "tzdata")
    }
    package_digest = hashlib.sha256()
    package_root = Path(__file__).resolve().parents[1]
    for source_file in sorted(package_root.rglob("*.py")):
        package_digest.update(source_file.relative_to(package_root).as_posix().encode())
        package_digest.update(b"\0")
        package_digest.update(source_file.read_bytes())
    metadata: dict[str, object] = {
        "manifest_version": "1.0.0",
        "observation_identity_version": "bronze-observation-v1",
        "schema_version": "1.0.0",
        "acquisition_id": str(identity),
        "run_id": str(identity),
        "dataset_version": request.dataset_version,
        "source": "alpaca",
        "feed": "iex",
        "timeframe": "1Min",
        "adjustment": "raw",
        "parameters": request.parameters(),
        "project_start_inclusive": request.start.isoformat(),
        "project_end_exclusive": request.end.isoformat(),
        "started_at": started.isoformat(),
        "calendar": calendar.metadata,
        "membership_version": settings.membership_version,
        "symbols": list(request.symbols),
        "environment_versions": versions,
        "python_version": platform.python_version(),
        "package_source_sha256": package_digest.hexdigest(),
        "status": "started",
        "policy": {
            "max_pages": request.max_pages,
            "max_attempts": request.max_attempts,
            "timeout_seconds": request.timeout_seconds,
        },
    }
    storage = BronzeAcquisition(settings.data_root, request.dataset_version, identity, metadata)
    storage.write_json("calendar.json", calendar.metadata)
    token: str | None = None
    seen_tokens: set[str] = set()
    seen_bodies: set[str] = set()
    observed: dict[tuple[str, datetime], tuple[Decimal, Decimal, Decimal, Decimal, int]] = {}
    conflicts: set[tuple[str, datetime]] = set()
    regular_keys: set[tuple[str, datetime]] = set()
    page_count = record_count = invalid_count = duplicate_count = 0
    error: str | None = None
    initial_attempts = client.attempts
    try:
        for page in range(1, request.max_pages + 1):
            response = client.fetch(request, token)
            received = clock()
            digest = hashlib.sha256(response.body).hexdigest()
            # Capture exact response bytes before any JSON/envelope/domain checks.
            storage.write_parquet(
                f"response-{page:06d}.parquet",
                [
                    {
                        "page": page,
                        "received_at": received,
                        "request_token": token,
                        "body": response.body,
                        "body_sha256": digest,
                    }
                ],
                pages=True,
            )
            page_count += 1
            bars, next_token = parse_page(response.body)
            rows: list[dict[str, object]] = []
            for symbol, observations in bars.items():
                for index, raw in enumerate(observations):
                    identifier = observation_id(identity, page, symbol, index, digest)
                    row: dict[str, object] = {
                        "observation_id": identifier,
                        "symbol": symbol,
                        "raw_json": json.dumps(raw, default=str, sort_keys=True),
                        "event_time": None,
                        "valid": False,
                        "reason": None,
                        "session": "unclassified",
                    }
                    record_count += 1
                    try:
                        bar = normalize_observation(raw, symbol, received, identifier)
                        if symbol not in request.symbols:
                            raise ContractError("unexpected_symbol")
                        if not request.start <= bar.event_time < request.end:
                            raise ContractError("outside_requested_interval")
                        row.update(
                            event_time=bar.event_time,
                            valid=True,
                            session="regular" if calendar.regular(bar.event_time) else "extended",
                        )
                        key = (symbol, bar.event_time)
                        if key in observed:
                            duplicate_count += 1
                            if observed[key] != bar.ohlcv:
                                conflicts.add(key)
                        else:
                            observed[key] = bar.ohlcv
                        if calendar.regular(bar.event_time):
                            regular_keys.add(key)
                    except ContractError as failure:
                        row["reason"] = str(failure)  # Domain errors contain field names only.
                        invalid_count += 1
                    rows.append(row)
            storage.write_parquet(f"observations-{page:06d}.parquet", rows)
            if digest in seen_bodies:
                raise AcquisitionError("repeated_response_page")
            seen_bodies.add(digest)
            if next_token is None:
                break
            if next_token in seen_tokens:
                raise AcquisitionError("repeated_page_token")
            seen_tokens.add(next_token)
            token = next_token
        else:
            raise AcquisitionError("page_limit_exhausted")
    except AcquisitionError as failure:
        error = str(failure)
    except OSError:
        raise AcquisitionError("storage_failed_uncommitted") from None
    expected = calendar.expected_minutes(request.start, request.end) * len(request.symbols)
    summary: dict[str, object] = {
        "error": error,
        "pages": page_count,
        "observations": record_count,
        "invalid_observations": invalid_count,
        "duplicate_logical_keys": duplicate_count,
        "conflicting_logical_keys": len(conflicts),
        "expected_regular_symbol_minutes": expected,
        "observed_unique_regular_symbol_minutes": len(regular_keys),
        "missing_regular_symbol_minutes": expected - len(regular_keys),
        "source_completeness": "unproven",
        "revision_resolution": "not_performed",
        "request_attempts": client.attempts - initial_attempts,
    }
    status = "complete" if error is None else "failed"
    try:
        manifest = storage.finish(status, summary)
    except OSError:
        raise AcquisitionError("storage_failed_uncommitted") from None
    return AcquisitionResult(manifest, status, error)
