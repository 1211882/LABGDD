from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config.settings import KafkaSettings, StreamingSettings
from src.models.market_bar import MarketBar
from src.streaming.realtime_ingestion import run_realtime_ingestion


def make_streaming_settings(metrics_path: Path) -> StreamingSettings:
    return StreamingSettings(
        websocket_url="wss://stream.example/v2/iex",
        test_websocket_url="wss://stream.example/v2/test",
        feed="iex",
        api_key_id="key",
        api_secret_key="secret",
        symbols=["AAPL", "MSFT"],
        channel="bars",
        test_symbol="FAKEPACA",
        max_queue=64,
        open_timeout_seconds=10,
        ping_interval_seconds=20,
        ping_timeout_seconds=20,
        reconnect_initial_seconds=1,
        reconnect_max_seconds=30,
        reconnect_multiplier=2,
        reconnect_jitter_ratio=0.25,
        max_reconnect_attempts=0,
        kafka_flush_timeout_seconds=10,
        metrics_path=metrics_path,
    )


def make_kafka_settings() -> KafkaSettings:
    return KafkaSettings(
        bootstrap_servers="localhost:9092",
        raw_topic="market-bars-raw",
        dead_letter_topic="market-bars-dlq",
        consumer_group="test",
        client_id="test",
        partitions=6,
        replication_factor=1,
        consumer_timeout_seconds=1,
    )


class FakeProducer:
    def __init__(self) -> None:
        self.bars: list[MarketBar] = []
        self.flush_timeouts: list[float] = []

    def publish(self, bar: MarketBar) -> None:
        self.bars.append(bar)

    def flush(self, timeout: float) -> None:
        self.flush_timeouts.append(timeout)


class FakeClient:
    async def run(
        self, on_bar: Any, metrics: Any, max_events: int | None = None
    ) -> None:
        metrics.connection_count += 1
        bar = MarketBar(
            timestamp=datetime(2026, 9, 29, 14, 30, tzinfo=timezone.utc),
            symbol="FAKEPACA",
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000,
        )
        metrics.events_received += 1
        on_bar(bar)


def test_realtime_ingestion_publishes_flushes_and_saves_metrics(
    tmp_path: Path,
) -> None:
    metrics_path = tmp_path / "streaming_metrics.json"
    producer = FakeProducer()

    metrics = asyncio.run(
        run_realtime_ingestion(
            make_streaming_settings(metrics_path),
            make_kafka_settings(),
            test_stream=True,
            max_events=1,
            producer=producer,
            client=FakeClient(),
        )
    )

    saved = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert [bar.symbol for bar in producer.bars] == ["FAKEPACA"]
    assert producer.flush_timeouts == [10]
    assert metrics.status == "completed"
    assert saved["events_received"] == 1
    assert saved["events_published"] == 1
    assert saved["test_stream"] is True
