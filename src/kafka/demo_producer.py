from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone

from src.config.settings import load_kafka_settings, load_spark_settings
from src.kafka.producer import MarketBarProducer
from src.models.market_bar import MarketBar

LOGGER = logging.getLogger(__name__)


def create_demo_bars(symbols: list[str], count: int) -> list[MarketBar]:
    if count <= 0:
        raise ValueError("count must be greater than zero")
    if not symbols:
        raise ValueError("at least one symbol is required")

    start = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    bars: list[MarketBar] = []
    for index in range(count):
        symbol = symbols[index % len(symbols)].upper()
        minute = index // len(symbols)
        base = 100.0 + index
        bars.append(
            MarketBar(
                timestamp=start + timedelta(minutes=minute),
                symbol=symbol,
                open=base,
                high=base + 1.0,
                low=base - 0.5,
                close=base + 0.25,
                volume=1000 + index * 10,
            )
        )
    return bars


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish deterministic demo market bars.")
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--symbols", nargs="+", default=["AAPL", "MSFT"])
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    configured = load_spark_settings().symbol_sectors
    symbols = [symbol.upper() for symbol in args.symbols]
    unknown = sorted(set(symbols).difference(configured))
    if unknown:
        raise ValueError(f"Unknown configured symbols: {', '.join(unknown)}")

    settings = load_kafka_settings()
    count = MarketBarProducer(settings).publish_many(
        create_demo_bars(symbols, args.count)
    )
    LOGGER.info(
        "Published %d demo market bars to %s using symbol keys.",
        count,
        settings.raw_topic,
    )


if __name__ == "__main__":
    main()
