from __future__ import annotations

from src.config.settings import load_kafka_settings, load_spark_settings


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
