from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


class ConfigurationError(RuntimeError):
    """Raised when required configuration is missing or invalid."""


@dataclass(frozen=True)
class AlpacaSettings:
    market_data_base_url: str
    paper_trading_base_url: str
    feed: str
    api_key_id: str
    api_secret_key: str


@dataclass(frozen=True)
class IngestionSettings:
    symbols: list[str]
    start_date: str
    end_date: str
    timeframe: str
    limit: int


@dataclass(frozen=True)
class StorageSettings:
    raw_data_dir: Path
    processed_data_dir: Path


@dataclass(frozen=True)
class SparkSettings:
    app_name: str
    master: str
    input_path: Path
    output_path: Path
    invalid_output_path: Path
    metrics_path: Path
    symbol_sectors: dict[str, str]


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    raw_topic: str
    dead_letter_topic: str
    consumer_group: str
    client_id: str
    partitions: int
    replication_factor: int
    consumer_timeout_seconds: float


@dataclass(frozen=True)
class AppSettings:
    alpaca: AlpacaSettings
    ingestion: IngestionSettings
    storage: StorageSettings


def load_settings(config_path: str | Path = "config/config.yaml") -> AppSettings:
    """Load YAML configuration and Alpaca credentials from the environment."""
    load_dotenv()
    raw_config = _load_yaml(config_path)

    api_key_id = os.getenv("APCA_API_KEY_ID", "").strip()
    api_secret_key = os.getenv("APCA_API_SECRET_KEY", "").strip()
    if not api_key_id or not api_secret_key:
        raise ConfigurationError(
            "Missing Alpaca credentials. Set APCA_API_KEY_ID and "
            "APCA_API_SECRET_KEY in the environment or a local .env file."
        )

    try:
        alpaca_config: dict[str, Any] = raw_config["alpaca"]
        ingestion_config: dict[str, Any] = raw_config["ingestion"]
        storage_config: dict[str, Any] = raw_config["storage"]
    except KeyError as exc:
        raise ConfigurationError(f"Missing configuration section: {exc}") from exc

    symbol_sectors = _parse_symbol_sectors(raw_config)

    limit = int(ingestion_config.get("limit", 10000))
    if limit <= 0:
        raise ConfigurationError("ingestion.limit must be greater than zero.")

    return AppSettings(
        alpaca=AlpacaSettings(
            market_data_base_url=str(alpaca_config["market_data_base_url"]).rstrip("/"),
            paper_trading_base_url=str(alpaca_config["paper_trading_base_url"]).rstrip("/"),
            feed=str(alpaca_config.get("feed", "iex")),
            api_key_id=api_key_id,
            api_secret_key=api_secret_key,
        ),
        ingestion=IngestionSettings(
            symbols=list(symbol_sectors),
            start_date=str(ingestion_config["start_date"]),
            end_date=str(ingestion_config["end_date"]),
            timeframe=str(ingestion_config.get("timeframe", "1Min")),
            limit=limit,
        ),
        storage=StorageSettings(
            raw_data_dir=Path(storage_config.get("raw_data_dir", "data/raw")),
            processed_data_dir=Path(storage_config.get("processed_data_dir", "data/processed")),
        ),
    )


def load_spark_settings(
    config_path: str | Path = "config/config.yaml",
) -> SparkSettings:
    """Load Spark batch settings without requiring Alpaca credentials."""
    raw_config = _load_yaml(config_path)
    try:
        spark_config: dict[str, Any] = raw_config["spark"]
    except KeyError as exc:
        raise ConfigurationError(f"Missing configuration section: {exc}") from exc

    return SparkSettings(
        app_name=str(spark_config.get("app_name", "financial-market-batch")),
        master=str(spark_config.get("master", "local[*]")),
        input_path=Path(spark_config.get("input_path", "data/raw")),
        output_path=Path(spark_config.get("output_path", "data/processed/batch")),
        invalid_output_path=Path(
            spark_config.get("invalid_output_path", "data/processed/invalid")
        ),
        metrics_path=Path(
            spark_config.get("metrics_path", "data/processed/batch_metrics.json")
        ),
        symbol_sectors=_parse_symbol_sectors(raw_config),
    )


def load_kafka_settings(
    config_path: str | Path = "config/config.yaml",
) -> KafkaSettings:
    """Load Kafka settings without requiring Alpaca credentials."""
    raw_config = _load_yaml(config_path)
    try:
        kafka_config: dict[str, Any] = raw_config["kafka"]
    except KeyError as exc:
        raise ConfigurationError(f"Missing configuration section: {exc}") from exc

    bootstrap_servers = str(kafka_config.get("bootstrap_servers", "")).strip()
    raw_topic = str(kafka_config.get("raw_topic", "")).strip()
    dead_letter_topic = str(kafka_config.get("dead_letter_topic", "")).strip()
    consumer_group = str(kafka_config.get("consumer_group", "")).strip()
    client_id = str(kafka_config.get("client_id", "")).strip()
    if not all(
        [bootstrap_servers, raw_topic, dead_letter_topic, consumer_group, client_id]
    ):
        raise ConfigurationError("Kafka string settings must not be empty.")

    partitions = int(kafka_config.get("partitions", 1))
    replication_factor = int(kafka_config.get("replication_factor", 1))
    consumer_timeout_seconds = float(
        kafka_config.get("consumer_timeout_seconds", 15)
    )
    if partitions <= 0 or replication_factor <= 0:
        raise ConfigurationError(
            "kafka.partitions and kafka.replication_factor must be positive."
        )
    if consumer_timeout_seconds <= 0:
        raise ConfigurationError(
            "kafka.consumer_timeout_seconds must be greater than zero."
        )

    return KafkaSettings(
        bootstrap_servers=bootstrap_servers,
        raw_topic=raw_topic,
        dead_letter_topic=dead_letter_topic,
        consumer_group=consumer_group,
        client_id=client_id,
        partitions=partitions,
        replication_factor=replication_factor,
        consumer_timeout_seconds=consumer_timeout_seconds,
    )


def _load_yaml(config_path: str | Path) -> dict[str, Any]:
    config_file = Path(config_path)
    if not config_file.exists():
        raise ConfigurationError(f"Configuration file not found: {config_file}")

    with config_file.open("r", encoding="utf-8") as file:
        raw_config = yaml.safe_load(file) or {}
    if not isinstance(raw_config, dict):
        raise ConfigurationError("Configuration root must be a mapping.")
    return raw_config


def _parse_symbol_sectors(raw_config: dict[str, Any]) -> dict[str, str]:
    symbols_config = raw_config.get("symbols", {})
    if not isinstance(symbols_config, dict) or not symbols_config:
        raise ConfigurationError("symbols must contain at least one configured symbol.")

    symbol_sectors: dict[str, str] = {}
    for symbol, metadata in symbols_config.items():
        if not isinstance(symbol, str) or not isinstance(metadata, dict):
            raise ConfigurationError("Each symbols entry must be a metadata mapping.")
        sector = metadata.get("sector")
        if not isinstance(sector, str) or not sector.strip():
            raise ConfigurationError(f"Missing sector for symbol: {symbol}")
        symbol_sectors[symbol.upper()] = sector.strip()
    return symbol_sectors
