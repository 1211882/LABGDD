from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.config.settings import KafkaSettings
from src.kafka.event_codec import decode_market_bar
from src.kafka.producer import KafkaDeliveryError, MarketBarProducer
from src.models.market_bar import MarketBar


class FakeProducer:
    def __init__(self, delivery_error: object | None = None) -> None:
        self.records: list[dict[str, Any]] = []
        self.delivery_error = delivery_error

    def produce(self, **record: Any) -> None:
        self.records.append(record)
        record["on_delivery"](self.delivery_error, object())

    def poll(self, timeout: float) -> None:
        return None

    def flush(self, timeout: float) -> int:
        return 0


def test_producer_uses_symbol_as_key_and_versioned_headers() -> None:
    settings = KafkaSettings(
        bootstrap_servers="localhost:9092",
        raw_topic="market-bars-raw",
        dead_letter_topic="market-bars-dlq",
        consumer_group="test",
        client_id="test",
        partitions=6,
        replication_factor=1,
        consumer_timeout_seconds=1,
    )
    fake = FakeProducer()
    producer = MarketBarProducer(settings, producer=fake)
    bar = MarketBar(
        timestamp=datetime(2026, 9, 29, 14, 30, tzinfo=timezone.utc),
        symbol="AAPL",
        open=100.0,
        high=102.0,
        low=99.0,
        close=101.0,
        volume=1000,
    )

    assert producer.publish_many([bar]) == 1
    record = fake.records[0]
    assert record["topic"] == "market-bars-raw"
    assert record["key"] == b"AAPL"
    assert dict(record["headers"])["schema_version"] == b"1"
    assert decode_market_bar(record["value"]) == bar


def test_producer_surfaces_asynchronous_delivery_error_during_publish() -> None:
    settings = KafkaSettings(
        bootstrap_servers="localhost:9092",
        raw_topic="market-bars-raw",
        dead_letter_topic="market-bars-dlq",
        consumer_group="test",
        client_id="test",
        partitions=6,
        replication_factor=1,
        consumer_timeout_seconds=1,
    )
    producer = MarketBarProducer(
        settings, producer=FakeProducer(delivery_error="broker unavailable")
    )
    bar = MarketBar(
        timestamp=datetime(2026, 9, 29, 14, 30, tzinfo=timezone.utc),
        symbol="AAPL",
        open=100.0,
        high=102.0,
        low=99.0,
        close=101.0,
        volume=1000,
    )

    try:
        producer.publish(bar)
    except KafkaDeliveryError as exc:
        assert "broker unavailable" in str(exc)
    else:
        raise AssertionError("Expected KafkaDeliveryError")
