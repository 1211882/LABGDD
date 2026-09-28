from __future__ import annotations

import argparse
import json
import logging
import time
import uuid

from confluent_kafka import Consumer, KafkaError

from src.config.settings import load_kafka_settings
from src.kafka.event_codec import decode_market_bar

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume and validate demo market bars.")
    parser.add_argument("--max-messages", type=int, default=10)
    parser.add_argument("--timeout-seconds", type=float)
    parser.add_argument("--group-id")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    if args.max_messages <= 0:
        raise ValueError("max-messages must be greater than zero")

    settings = load_kafka_settings()
    timeout = args.timeout_seconds or settings.consumer_timeout_seconds
    if timeout <= 0:
        raise ValueError("timeout-seconds must be greater than zero")
    group_id = args.group_id or f"{settings.consumer_group}-{uuid.uuid4().hex[:8]}"
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "client.id": f"{settings.client_id}-demo-consumer",
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([settings.raw_topic])
    consumed = 0
    deadline = time.monotonic() + timeout
    try:
        while consumed < args.max_messages and time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Kafka consume failed: {message.error()}")

            bar = decode_market_bar(message.value())
            key = message.key().decode("utf-8") if message.key() else ""
            if key != bar.symbol:
                raise ValueError(
                    f"Kafka key {key!r} does not match event symbol {bar.symbol!r}."
                )
            print(
                json.dumps(
                    {
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                        "key": key,
                        "event": json.loads(message.value()),
                    },
                    separators=(",", ":"),
                )
            )
            consumer.commit(message=message, asynchronous=False)
            consumed += 1
    finally:
        consumer.close()

    if consumed < args.max_messages:
        raise TimeoutError(
            f"Consumed {consumed}/{args.max_messages} records within {timeout:g} seconds."
        )
    LOGGER.info("Consumed and validated %d market bars.", consumed)


if __name__ == "__main__":
    main()
