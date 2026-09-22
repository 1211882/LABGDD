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
class AppSettings:
    alpaca: AlpacaSettings
    ingestion: IngestionSettings
    storage: StorageSettings


def load_settings(config_path: str | Path = "config/config.yaml") -> AppSettings:
    """Load YAML configuration and Alpaca credentials from the environment."""
    load_dotenv()
    config_file = Path(config_path)
    if not config_file.exists():
        raise ConfigurationError(f"Configuration file not found: {config_file}")

    with config_file.open("r", encoding="utf-8") as file:
        raw_config = yaml.safe_load(file) or {}

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

    symbols = ingestion_config.get("symbols", [])
    if not symbols or not all(isinstance(symbol, str) for symbol in symbols):
        raise ConfigurationError("ingestion.symbols must contain at least one symbol.")

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
            symbols=[symbol.upper() for symbol in symbols],
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
