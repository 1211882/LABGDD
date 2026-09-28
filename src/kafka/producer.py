from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from confluent_kafka import Producer

from src.config.settings import KafkaSettings
from src.kafka.event_codec import encode_market_bar, event_headers
from src.models.market_bar import MarketBar


class KafkaDeliveryError(RuntimeError):
    """Raised when Kafka does not acknowledge every produced record."""


class MarketBarProducer:
    def __init__(
        self,
        settings: KafkaSettings,
        producer: Any | None = None,
    ) -> None:
        self.settings = settings
        self._producer = producer or Producer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "client.id": settings.client_id,
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "gzip",
            }
        )
        self._delivery_errors: list[str] = []

    def publish(self, bar: MarketBar) -> None:
        key = bar.symbol.strip().upper().encode("utf-8")
        self._producer.produce(
            topic=self.settings.raw_topic,
            key=key,
            value=encode_market_bar(bar),
            headers=event_headers(),
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def publish_many(self, bars: Iterable[MarketBar], timeout: float = 10.0) -> int:
        count = 0
        for bar in bars:
            self.publish(bar)
            count += 1
        outstanding = self._producer.flush(timeout)
        if outstanding:
            raise KafkaDeliveryError(
                f"Kafka flush timed out with {outstanding} undelivered record(s)."
            )
        if self._delivery_errors:
            details = "; ".join(self._delivery_errors)
            self._delivery_errors.clear()
            raise KafkaDeliveryError(f"Kafka delivery failed: {details}")
        return count

    def _on_delivery(self, error: Any, message: Any) -> None:
        if error is not None:
            self._delivery_errors.append(str(error))
