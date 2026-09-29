from __future__ import annotations

import asyncio
import json
from collections import deque
from pathlib import Path
from typing import Any

import pytest

from src.config.settings import StreamingSettings
from src.streaming.alpaca_websocket import (
    AlpacaMessageError,
    AlpacaStreamFatalError,
    AlpacaWebSocketClient,
    decode_alpaca_frame,
    market_bar_from_message,
)
from src.streaming.metrics import StreamingIngestionMetrics


def make_settings(**overrides: object) -> StreamingSettings:
    values: dict[str, object] = {
        "websocket_url": "wss://stream.example/v2/iex",
        "test_websocket_url": "wss://stream.example/v2/test",
        "feed": "iex",
        "api_key_id": "key",
        "api_secret_key": "secret",
        "symbols": ["AAPL", "MSFT"],
        "channel": "bars",
        "test_symbol": "FAKEPACA",
        "max_queue": 64,
        "open_timeout_seconds": 10.0,
        "ping_interval_seconds": 20.0,
        "ping_timeout_seconds": 20.0,
        "reconnect_initial_seconds": 1.0,
        "reconnect_max_seconds": 30.0,
        "reconnect_multiplier": 2.0,
        "reconnect_jitter_ratio": 0.0,
        "max_reconnect_attempts": 3,
        "kafka_flush_timeout_seconds": 10.0,
        "metrics_path": Path("metrics.json"),
    }
    values.update(overrides)
    return StreamingSettings(**values)  # type: ignore[arg-type]


def bar_message(symbol: str = "AAPL") -> dict[str, object]:
    return {
        "T": "b",
        "S": symbol,
        "o": 100.0,
        "h": 102.0,
        "l": 99.0,
        "c": 101.0,
        "v": 1200,
        "t": "2026-09-29T14:30:00Z",
    }


class FakeWebSocket:
    def __init__(self, responses: list[str], frames: list[str]) -> None:
        self.responses = deque(responses)
        self.frames = frames
        self.sent: list[dict[str, Any]] = []

    async def recv(self) -> str:
        return self.responses.popleft()

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))

    def __aiter__(self) -> Any:
        async def frames() -> Any:
            for frame in self.frames:
                yield frame

        return frames()


class FakeContext:
    def __init__(self, websocket: FakeWebSocket) -> None:
        self.websocket = websocket

    async def __aenter__(self) -> FakeWebSocket:
        return self.websocket

    async def __aexit__(self, *args: object) -> None:
        return None


def protocol_responses() -> list[str]:
    return [
        '[{"T":"success","msg":"connected"}]',
        '[{"T":"success","msg":"authenticated"}]',
        '[{"T":"subscription","bars":["AAPL"]}]',
    ]


def test_alpaca_bar_maps_to_existing_market_bar() -> None:
    bar = market_bar_from_message(bar_message("aapl"))

    assert bar.symbol == "AAPL"
    assert bar.timestamp.isoformat() == "2026-09-29T14:30:00+00:00"
    assert bar.open == 100.0
    assert bar.volume == 1200


def test_frame_decoder_rejects_non_array_payload() -> None:
    with pytest.raises(AlpacaMessageError, match="array"):
        decode_alpaca_frame('{"T":"b"}')


def test_client_authenticates_subscribes_and_emits_bars() -> None:
    websocket = FakeWebSocket(
        protocol_responses(),
        [json.dumps([{"T": "q", "S": "AAPL"}, bar_message()])],
    )
    connector_calls: list[dict[str, Any]] = []

    def connector(endpoint: str, **kwargs: Any) -> FakeContext:
        connector_calls.append({"endpoint": endpoint, **kwargs})
        return FakeContext(websocket)

    settings = make_settings()
    metrics = StreamingIngestionMetrics(
        endpoint=settings.websocket_url,
        feed="iex",
        channel="bars",
        symbols=["AAPL"],
        test_stream=False,
    )
    received = []
    client = AlpacaWebSocketClient(
        settings, settings.websocket_url, ["AAPL"], connector=connector
    )

    def on_bar(bar: object) -> None:
        received.append(bar)
        metrics.events_published += 1

    asyncio.run(client.run(on_bar, metrics, max_events=1))

    assert len(received) == 1
    assert websocket.sent == [
        {"action": "auth", "key": "key", "secret": "secret"},
        {"action": "subscribe", "bars": ["AAPL"]},
    ]
    assert connector_calls[0]["max_queue"] == 64
    assert metrics.events_received == 1
    assert metrics.ignored_messages == 1


def test_authentication_error_is_fatal_and_not_retried() -> None:
    websocket = FakeWebSocket(
        [
            '[{"T":"success","msg":"connected"}]',
            '[{"T":"error","code":401,"msg":"auth failed"}]',
        ],
        [],
    )
    settings = make_settings()
    metrics = StreamingIngestionMetrics(
        endpoint=settings.websocket_url,
        feed="iex",
        channel="bars",
        symbols=["AAPL"],
        test_stream=False,
    )
    client = AlpacaWebSocketClient(
        settings,
        settings.websocket_url,
        ["AAPL"],
        connector=lambda *args, **kwargs: FakeContext(websocket),
    )

    with pytest.raises(AlpacaStreamFatalError, match="401"):
        asyncio.run(client.run(lambda bar: None, metrics, max_events=1))

    assert metrics.connection_count == 1
    assert metrics.reconnect_count == 0


def test_client_reconnects_after_transient_connection_failure() -> None:
    websocket = FakeWebSocket(
        protocol_responses(), [json.dumps([bar_message()])]
    )
    calls = 0
    sleeps: list[float] = []

    class FailingContext:
        async def __aenter__(self) -> None:
            raise OSError("temporary network failure")

        async def __aexit__(self, *args: object) -> None:
            return None

    def connector(*args: object, **kwargs: object) -> object:
        nonlocal calls
        calls += 1
        return FailingContext() if calls == 1 else FakeContext(websocket)

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    settings = make_settings()
    metrics = StreamingIngestionMetrics(
        endpoint=settings.websocket_url,
        feed="iex",
        channel="bars",
        symbols=["AAPL"],
        test_stream=False,
    )
    client = AlpacaWebSocketClient(
        settings,
        settings.websocket_url,
        ["AAPL"],
        connector=connector,
        sleep=sleep,
    )

    def on_bar(bar: object) -> None:
        metrics.events_published += 1

    asyncio.run(client.run(on_bar, metrics, max_events=1))

    assert calls == 2
    assert sleeps == [1.0]
    assert metrics.reconnect_count == 1
