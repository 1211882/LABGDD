from __future__ import annotations

import logging

from confluent_kafka.admin import AdminClient, NewTopic

from src.config.settings import KafkaSettings, load_kafka_settings

LOGGER = logging.getLogger(__name__)


def ensure_topics(settings: KafkaSettings) -> None:
    admin = AdminClient(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "client.id": f"{settings.client_id}-admin",
        }
    )
    existing = admin.list_topics(timeout=10).topics
    requested = [settings.raw_topic, settings.dead_letter_topic]
    missing = [name for name in requested if name not in existing]
    if not missing:
        LOGGER.info("Kafka topics already exist: %s", ", ".join(requested))
        return

    futures = admin.create_topics(
        [
            NewTopic(
                name,
                num_partitions=settings.partitions,
                replication_factor=settings.replication_factor,
            )
            for name in missing
        ]
    )
    for name, future in futures.items():
        future.result(15)
        LOGGER.info("Created Kafka topic: %s", name)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ensure_topics(load_kafka_settings())


if __name__ == "__main__":
    main()
