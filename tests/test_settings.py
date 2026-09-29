from __future__ import annotations

from src.config.settings import (
    load_kafka_settings,
    load_spark_settings,
    load_structured_streaming_settings,
    load_streaming_settings,
)


def test_configured_market_universe_has_six_sectors_and_thirty_symbols() -> None:
    settings = load_spark_settings()
    sectors = settings.symbol_sectors

    assert len(sectors) == 30
    assert set(sectors.values()) == {
        "Technology",
        "Healthcare",
        "Defense",
        "Aerospace",
        "Energy",
        "Financial",
    }
    assert all(list(sectors.values()).count(sector) == 5 for sector in set(sectors.values()))
    assert sectors["GE"] == "Aerospace"
    assert "SPR" not in sectors


def test_kafka_settings_define_symbol_partitioned_raw_topic() -> None:
    settings = load_kafka_settings()

    assert settings.bootstrap_servers == "localhost:9092"
    assert settings.raw_topic == "market-bars-raw"
    assert settings.dead_letter_topic == "market-bars-dlq"
    assert settings.partitions == 6
    assert settings.replication_factor == 1


def test_streaming_settings_reuse_market_universe_and_iex_feed() -> None:
    settings = load_streaming_settings()

    assert settings.websocket_url == "wss://stream.data.alpaca.markets/v2/iex"
    assert len(settings.symbols) == 30
    assert settings.channel == "bars"
    assert settings.test_symbol == "FAKEPACA"
    assert settings.max_queue == 64


def test_structured_streaming_settings_keep_outputs_and_checkpoints_separate() -> None:
    settings = load_structured_streaming_settings()

    assert settings.topic == "market-bars-raw"
    assert settings.watermark_delay == "10 minutes"
    assert settings.output_path.as_posix() == "data/processed/streaming"
    assert settings.invalid_output_path.as_posix() == "data/processed/streaming_invalid"
    assert settings.checkpoint_path.as_posix() == "data/checkpoints/streaming"
    assert settings.kafka_connector_package.endswith(":4.0.1")
