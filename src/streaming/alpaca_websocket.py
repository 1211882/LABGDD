from __future__ import annotations

import asyncio
import json
import logging
import random
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any, Protocol

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from src.config.settings import StreamingSettings
from src.models.market_bar import MarketBar
from src.streaming.metrics import StreamingIngestionMetrics

LOGGER = logging.getLogger(__name__)
FATAL_ERROR_CODES = {401, 402, 405, 406, 409, 410}


class AlpacaStreamError(RuntimeError):
    """Base error for the Alpaca real-time market-data stream."""


class AlpacaStreamFatalError(AlpacaStreamError):
    """An authentication, entitlement, or protocol error that should not retry."""


class AlpacaMessageError(AlpacaStreamError):
    """A malformed or invalid Alpaca stream message."""


class WebSocketConnection(Protocol):
    async def send(self, message: str) -> None: ...

    async def recv(self) -> str | bytes: ...

    def __aiter__(self) -> Any: ...


def decode_alpaca_frame(frame: str | bytes) -> list[dict[str, Any]]:
    try:
        payload = json.loads(frame)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
        raise AlpacaMessageError("Alpaca frame must be valid UTF-8 JSON.") from exc
    if not isinstance(payload, list) or not all(
        isinstance(message, dict) for message in payload
    ):
        raise AlpacaMessageError("Alpaca frame must be an array of objects.")
    return payload


def market_bar_from_message(message: Mapping[str, Any]) -> MarketBar:
    if message.get("T") != "b":
        raise AlpacaMessageError("Message is not an Alpaca minute bar.")
    required = {"t", "S", "o", "h", "l", "c", "v"}
    missing = required.difference(message)
    if missing:
        raise AlpacaMessageError(
            f"Alpaca bar is missing fields: {', '.join(sorted(missing))}."
        )
    try:
        timestamp = datetime.fromisoformat(str(message["t"]).replace("Z", "+00:00"))
        volume = int(message["v"])
        if volume != float(message["v"]):
            raise ValueError("volume must be an integer")
        return MarketBar(
            timestamp=timestamp,
            symbol=str(message["S"]).strip().upper(),
            open=float(message["o"]),
            high=float(message["h"]),
            low=float(message["l"]),
            close=float(message["c"]),
            volume=volume,
        )
    except (TypeError, ValueError) as exc:
        raise AlpacaMessageError(f"Invalid Alpaca bar field: {exc}") from exc


class AlpacaWebSocketClient:
    def __init__(
        self,
        settings: StreamingSettings,
        endpoint: str,
        symbols: list[str],
        connector: Callable[..., Any] = connect,
        sleep: Callable[[float], Any] = asyncio.sleep,
        random_value: Callable[[], float] = random.random,
    ) -> None:
        self.settings = settings
        self.endpoint = endpoint
        self.symbols = [symbol.strip().upper() for symbol in symbols]
        self._connector = connector
        self._sleep = sleep
        self._random_value = random_value

    async def run(
        self,
        on_bar: Callable[[MarketBar], None],
        metrics: StreamingIngestionMetrics,
        max_events: int | None = None,
    ) -> None:
        reconnect_attempt = 0
        while True:
            try:
                async with self._connector(
                    self.endpoint,
                    open_timeout=self.settings.open_timeout_seconds,
                    ping_interval=self.settings.ping_interval_seconds,
                    ping_timeout=self.settings.ping_timeout_seconds,
                    max_queue=self.settings.max_queue,
                ) as websocket:
                    metrics.connection_count += 1
                    await self._authenticate_and_subscribe(websocket)
                    reconnect_attempt = 0
                    LOGGER.info(
                        "Subscribed to Alpaca %s for %d symbol(s).",
                        self.settings.channel,
                        len(self.symbols),
                    )
                    async for frame in websocket:
                        reached_limit = self._process_frame(
                            frame, on_bar, metrics, max_events
                        )
                        if reached_limit:
                            return
                raise ConnectionError("Alpaca WebSocket closed normally.")
            except AlpacaStreamFatalError:
                raise
            except asyncio.CancelledError:
                raise
            except (ConnectionClosed, ConnectionError, OSError, TimeoutError) as exc:
                reconnect_attempt += 1
                if (
                    self.settings.max_reconnect_attempts
                    and reconnect_attempt > self.settings.max_reconnect_attempts
                ):
                    raise AlpacaStreamError(
                        "Alpaca reconnect limit exceeded."
                    ) from exc
                metrics.reconnect_count += 1
                delay = self._reconnect_delay(reconnect_attempt)
                LOGGER.warning(
                    "Alpaca stream disconnected (%s); reconnecting in %.2f seconds.",
                    exc,
                    delay,
                )
                await self._sleep(delay)

    async def _authenticate_and_subscribe(
        self, websocket: WebSocketConnection
    ) -> None:
        await self._expect_control(websocket, expected="connected")
        await websocket.send(
            json.dumps(
                {
                    "action": "auth",
                    "key": self.settings.api_key_id,
                    "secret": self.settings.api_secret_key,
                },
                separators=(",", ":"),
            )
        )
        await self._expect_control(websocket, expected="authenticated")
        await websocket.send(
            json.dumps(
                {
                    "action": "subscribe",
                    self.settings.channel: self.symbols,
                },
                separators=(",", ":"),
            )
        )
        messages = decode_alpaca_frame(await websocket.recv())
        self._raise_control_error(messages)
        if not any(message.get("T") == "subscription" for message in messages):
            raise AlpacaStreamFatalError(
                "Alpaca did not confirm the requested subscription."
            )

    async def _expect_control(
        self, websocket: WebSocketConnection, expected: str
    ) -> None:
        messages = decode_alpaca_frame(await websocket.recv())
        self._raise_control_error(messages)
        if not any(
            message.get("T") == "success" and message.get("msg") == expected
            for message in messages
        ):
            raise AlpacaStreamFatalError(
                f"Alpaca did not send the expected {expected!r} response."
            )

    def _process_frame(
        self,
        frame: str | bytes,
        on_bar: Callable[[MarketBar], None],
        metrics: StreamingIngestionMetrics,
        max_events: int | None,
    ) -> bool:
        try:
            messages = decode_alpaca_frame(frame)
        except AlpacaMessageError as exc:
            metrics.malformed_messages += 1
            LOGGER.warning("Ignoring malformed Alpaca frame: %s", exc)
            return False

        self._raise_control_error(messages)
        for message in messages:
            if message.get("T") != "b":
                metrics.ignored_messages += 1
                continue
            try:
                bar = market_bar_from_message(message)
                metrics.events_received += 1
                on_bar(bar)
            except (AlpacaMessageError, ValueError) as exc:
                metrics.malformed_messages += 1
                LOGGER.warning("Ignoring invalid Alpaca bar: %s", exc)
                continue
            if max_events is not None and metrics.events_published >= max_events:
                return True
        return False

    def _raise_control_error(self, messages: list[dict[str, Any]]) -> None:
        for message in messages:
            if message.get("T") != "error":
                continue
            code = int(message.get("code", 0))
            detail = str(message.get("msg", "unknown Alpaca stream error"))
            error = f"Alpaca stream error {code}: {detail}"
            if code in FATAL_ERROR_CODES:
                raise AlpacaStreamFatalError(error)
            raise ConnectionError(error)

    def _reconnect_delay(self, attempt: int) -> float:
        base = self.settings.reconnect_initial_seconds
        for _ in range(attempt - 1):
            base = min(
                base * self.settings.reconnect_multiplier,
                self.settings.reconnect_max_seconds,
            )
            if base >= self.settings.reconnect_max_seconds:
                break
        jitter = base * self.settings.reconnect_jitter_ratio * self._random_value()
        return base + jitter
