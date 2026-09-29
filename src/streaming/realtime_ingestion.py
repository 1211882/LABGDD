from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Any

from src.config.settings import (
    ConfigurationError,
    KafkaSettings,
    StreamingSettings,
    load_kafka_settings,
    load_streaming_settings,
)
from src.kafka.producer import KafkaDeliveryError, MarketBarProducer
from src.models.market_bar import MarketBar
from src.streaming.alpaca_websocket import AlpacaStreamError, AlpacaWebSocketClient
from src.streaming.metrics import StreamingIngestionMetrics

LOGGER = logging.getLogger(__name__)


async def run_realtime_ingestion(
    streaming_settings: StreamingSettings,
    kafka_settings: KafkaSettings,
    *,
    test_stream: bool = False,
    symbols: list[str] | None = None,
    max_events: int | None = None,
    duration_seconds: float | None = None,
    metrics_path: str | Path | None = None,
    producer: Any | None = None,
    client: Any | None = None,
) -> StreamingIngestionMetrics:
    selected_symbols = (
        [streaming_settings.test_symbol]
        if test_stream
        else _validate_symbols(symbols or streaming_settings.symbols, streaming_settings)
    )
    endpoint = (
        streaming_settings.test_websocket_url
        if test_stream
        else streaming_settings.websocket_url
    )
    metrics = StreamingIngestionMetrics(
        endpoint=endpoint,
        feed="test" if test_stream else streaming_settings.feed,
        channel=streaming_settings.channel,
        symbols=selected_symbols,
        test_stream=test_stream,
    )
    market_producer = producer or MarketBarProducer(kafka_settings)
    stream_client = client or AlpacaWebSocketClient(
        streaming_settings, endpoint, selected_symbols
    )
    failure: BaseException | None = None

    def publish(bar: MarketBar) -> None:
        market_producer.publish(bar)
        metrics.events_published += 1

    try:
        if duration_seconds is not None:
            async with asyncio.timeout(duration_seconds):
                await stream_client.run(publish, metrics, max_events=max_events)
        else:
            await stream_client.run(publish, metrics, max_events=max_events)
    except TimeoutError:
        LOGGER.info("Configured streaming duration reached.")
    except BaseException as exc:
        failure = exc

    try:
        market_producer.flush(streaming_settings.kafka_flush_timeout_seconds)
    except BaseException as exc:
        metrics.delivery_failures += 1
        if failure is None:
            failure = exc

    metrics.finish(
        status="failed" if failure else "completed",
        error=str(failure) if failure else None,
    )
    output_path = metrics_path or streaming_settings.metrics_path
    metrics.save(output_path)
    LOGGER.info(
        "Real-time ingestion finished: status=%s received=%d published=%d "
        "duration_seconds=%.3f events_per_second=%.3f metrics=%s",
        metrics.status,
        metrics.events_received,
        metrics.events_published,
        metrics.duration_seconds,
        metrics.events_per_second,
        output_path,
    )
    if failure:
        raise failure
    return metrics


def _validate_symbols(
    symbols: list[str], settings: StreamingSettings
) -> list[str]:
    normalized = list(dict.fromkeys(symbol.strip().upper() for symbol in symbols))
    unknown = sorted(set(normalized).difference(settings.symbols))
    if unknown:
        raise ConfigurationError(
            f"Unknown configured streaming symbols: {', '.join(unknown)}"
        )
    if not normalized:
        raise ConfigurationError("At least one streaming symbol is required.")
    return normalized


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish Alpaca real-time minute bars to Kafka."
    )
    parser.add_argument("--symbols", nargs="+")
    parser.add_argument("--test-stream", action="store_true")
    parser.add_argument("--max-events", type=int)
    parser.add_argument("--duration-seconds", type=float)
    parser.add_argument("--metrics-path", type=Path)
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args = parse_args()
    if args.max_events is not None and args.max_events <= 0:
        LOGGER.error("--max-events must be greater than zero.")
        return 2
    if args.duration_seconds is not None and args.duration_seconds <= 0:
        LOGGER.error("--duration-seconds must be greater than zero.")
        return 2

    try:
        asyncio.run(
            run_realtime_ingestion(
                load_streaming_settings(),
                load_kafka_settings(),
                test_stream=args.test_stream,
                symbols=args.symbols,
                max_events=args.max_events,
                duration_seconds=args.duration_seconds,
                metrics_path=args.metrics_path,
            )
        )
    except (ConfigurationError, AlpacaStreamError, KafkaDeliveryError, OSError) as exc:
        LOGGER.error("Real-time ingestion failed: %s", exc)
        return 1
    except KeyboardInterrupt:
        LOGGER.info("Real-time ingestion stopped by user.")
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
