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
class StreamingSettings:
    websocket_url: str
    test_websocket_url: str
    feed: str
    api_key_id: str
    api_secret_key: str
    symbols: list[str]
    channel: str
    test_symbol: str
    max_queue: int
    open_timeout_seconds: float
    ping_interval_seconds: float
    ping_timeout_seconds: float
    reconnect_initial_seconds: float
    reconnect_max_seconds: float
    reconnect_multiplier: float
    reconnect_jitter_ratio: float
    max_reconnect_attempts: int
    kafka_flush_timeout_seconds: float
    metrics_path: Path


@dataclass(frozen=True)
class StructuredStreamingSettings:
    app_name: str
    master: str
    kafka_connector_package: str
    bootstrap_servers: str
    topic: str
    output_path: Path
    invalid_output_path: Path
    checkpoint_path: Path
    metrics_path: Path
    watermark_delay: str
    starting_offsets: str
    fail_on_data_loss: bool
    max_offsets_per_trigger: int
    trigger_interval: str
    symbol_sectors: dict[str, str]


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


def load_streaming_settings(
    config_path: str | Path = "config/config.yaml",
) -> StreamingSettings:
    """Load Alpaca streaming settings and credentials."""
    load_dotenv()
    raw_config = _load_yaml(config_path)
    try:
        alpaca_config: dict[str, Any] = raw_config["alpaca"]
        streaming_config: dict[str, Any] = raw_config["streaming"]
    except KeyError as exc:
        raise ConfigurationError(f"Missing configuration section: {exc}") from exc

    api_key_id = os.getenv("APCA_API_KEY_ID", "").strip()
    api_secret_key = os.getenv("APCA_API_SECRET_KEY", "").strip()
    if not api_key_id or not api_secret_key:
        raise ConfigurationError(
            "Missing Alpaca credentials. Set APCA_API_KEY_ID and "
            "APCA_API_SECRET_KEY in the environment or a local .env file."
        )

    feed = str(alpaca_config.get("feed", "iex")).strip().lower()
    stream_base_url = str(alpaca_config.get("market_data_stream_url", "")).rstrip("/")
    test_websocket_url = str(alpaca_config.get("test_stream_url", "")).strip()
    channel = str(streaming_config.get("channel", "bars")).strip()
    test_symbol = str(streaming_config.get("test_symbol", "FAKEPACA")).strip().upper()
    if not all([stream_base_url, test_websocket_url, feed, channel, test_symbol]):
        raise ConfigurationError("Alpaca streaming string settings must not be empty.")
    if channel != "bars":
        raise ConfigurationError("Phase 3 supports only the Alpaca bars channel.")

    max_queue = int(streaming_config.get("max_queue", 64))
    open_timeout_seconds = float(streaming_config.get("open_timeout_seconds", 10))
    ping_interval_seconds = float(streaming_config.get("ping_interval_seconds", 20))
    ping_timeout_seconds = float(streaming_config.get("ping_timeout_seconds", 20))
    reconnect_initial_seconds = float(
        streaming_config.get("reconnect_initial_seconds", 1)
    )
    reconnect_max_seconds = float(streaming_config.get("reconnect_max_seconds", 30))
    reconnect_multiplier = float(streaming_config.get("reconnect_multiplier", 2))
    reconnect_jitter_ratio = float(
        streaming_config.get("reconnect_jitter_ratio", 0.25)
    )
    max_reconnect_attempts = int(streaming_config.get("max_reconnect_attempts", 0))
    kafka_flush_timeout_seconds = float(
        streaming_config.get("kafka_flush_timeout_seconds", 10)
    )

    positive_values = {
        "max_queue": max_queue,
        "open_timeout_seconds": open_timeout_seconds,
        "ping_interval_seconds": ping_interval_seconds,
        "ping_timeout_seconds": ping_timeout_seconds,
        "reconnect_initial_seconds": reconnect_initial_seconds,
        "reconnect_max_seconds": reconnect_max_seconds,
        "reconnect_multiplier": reconnect_multiplier,
        "kafka_flush_timeout_seconds": kafka_flush_timeout_seconds,
    }
    invalid = [name for name, value in positive_values.items() if value <= 0]
    if invalid:
        raise ConfigurationError(
            f"Streaming settings must be positive: {', '.join(sorted(invalid))}."
        )
    if reconnect_max_seconds < reconnect_initial_seconds:
        raise ConfigurationError(
            "streaming.reconnect_max_seconds must be at least the initial delay."
        )
    if not 0 <= reconnect_jitter_ratio <= 1:
        raise ConfigurationError(
            "streaming.reconnect_jitter_ratio must be between zero and one."
        )
    if max_reconnect_attempts < 0:
        raise ConfigurationError(
            "streaming.max_reconnect_attempts must be zero or greater."
        )

    return StreamingSettings(
        websocket_url=f"{stream_base_url}/{feed}",
        test_websocket_url=test_websocket_url,
        feed=feed,
        api_key_id=api_key_id,
        api_secret_key=api_secret_key,
        symbols=list(_parse_symbol_sectors(raw_config)),
        channel=channel,
        test_symbol=test_symbol,
        max_queue=max_queue,
        open_timeout_seconds=open_timeout_seconds,
        ping_interval_seconds=ping_interval_seconds,
        ping_timeout_seconds=ping_timeout_seconds,
        reconnect_initial_seconds=reconnect_initial_seconds,
        reconnect_max_seconds=reconnect_max_seconds,
        reconnect_multiplier=reconnect_multiplier,
        reconnect_jitter_ratio=reconnect_jitter_ratio,
        max_reconnect_attempts=max_reconnect_attempts,
        kafka_flush_timeout_seconds=kafka_flush_timeout_seconds,
        metrics_path=Path(
            streaming_config.get(
                "metrics_path", "data/processed/realtime_ingestion_metrics.json"
            )
        ),
    )


def load_structured_streaming_settings(
    config_path: str | Path = "config/config.yaml",
) -> StructuredStreamingSettings:
    """Load Spark Structured Streaming settings without Alpaca credentials."""
    raw_config = _load_yaml(config_path)
    try:
        stream_config: dict[str, Any] = raw_config["spark_streaming"]
        kafka_config: dict[str, Any] = raw_config["kafka"]
    except KeyError as exc:
        raise ConfigurationError(f"Missing configuration section: {exc}") from exc

    strings = {
        "app_name": str(stream_config.get("app_name", "")).strip(),
        "master": str(stream_config.get("master", "")).strip(),
        "kafka_connector_package": str(
            stream_config.get("kafka_connector_package", "")
        ).strip(),
        "bootstrap_servers": str(kafka_config.get("bootstrap_servers", "")).strip(),
        "topic": str(kafka_config.get("raw_topic", "")).strip(),
        "watermark_delay": str(stream_config.get("watermark_delay", "")).strip(),
        "starting_offsets": str(stream_config.get("starting_offsets", "")).strip(),
        "trigger_interval": str(stream_config.get("trigger_interval", "")).strip(),
    }
    missing = [name for name, value in strings.items() if not value]
    if missing:
        raise ConfigurationError(
            f"Structured Streaming settings must not be empty: {', '.join(missing)}."
        )
    if strings["starting_offsets"] not in {"earliest", "latest"}:
        raise ConfigurationError(
            "spark_streaming.starting_offsets must be 'earliest' or 'latest'."
        )
    max_offsets = int(stream_config.get("max_offsets_per_trigger", 10000))
    if max_offsets <= 0:
        raise ConfigurationError(
            "spark_streaming.max_offsets_per_trigger must be positive."
        )

    return StructuredStreamingSettings(
        app_name=strings["app_name"],
        master=strings["master"],
        kafka_connector_package=strings["kafka_connector_package"],
        bootstrap_servers=strings["bootstrap_servers"],
        topic=strings["topic"],
        output_path=Path(stream_config.get("output_path", "data/processed/streaming")),
        invalid_output_path=Path(
            stream_config.get(
                "invalid_output_path", "data/processed/streaming_invalid"
            )
        ),
        checkpoint_path=Path(
            stream_config.get("checkpoint_path", "data/checkpoints/streaming")
        ),
        metrics_path=Path(
            stream_config.get(
                "metrics_path", "data/processed/structured_streaming_metrics.json"
            )
        ),
        watermark_delay=strings["watermark_delay"],
        starting_offsets=strings["starting_offsets"],
        fail_on_data_loss=bool(stream_config.get("fail_on_data_loss", True)),
        max_offsets_per_trigger=max_offsets,
        trigger_interval=strings["trigger_interval"],
        symbol_sectors=_parse_symbol_sectors(raw_config),
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
