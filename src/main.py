from __future__ import annotations

import argparse
import logging
import sys

from src.config.settings import ConfigurationError, load_settings
from src.ingestion.alpaca_client import AlpacaApiError, AlpacaClient
from src.ingestion.market_data_ingestion import MarketDataIngestionService
from src.processing.normalization import DataQualityError
from src.storage.parquet_storage import ParquetStorage


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Alpaca historical bars incrementally.")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--symbols", nargs="+")
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    args = _parse_args()
    try:
        settings = load_settings(args.config)
        configured = set(settings.ingestion.symbols)
        selected = [symbol.upper() for symbol in args.symbols] if args.symbols else None
        unknown = set(selected or []) - configured
        if unknown:
            raise ConfigurationError(
                f"Unknown configured symbols: {', '.join(sorted(unknown))}"
            )
        client = AlpacaClient(
            base_url=settings.alpaca.market_data_base_url,
            api_key_id=settings.alpaca.api_key_id,
            api_secret_key=settings.alpaca.api_secret_key,
        )
        storage = ParquetStorage(settings.storage.raw_data_dir)
        service = MarketDataIngestionService(settings, client, storage)
        report = service.run(start_date=args.start, end_date=args.end, symbols=selected)
        if report.failed:
            logger.error(
                "Ingestion completed with failures: %s",
                ", ".join(
                    f"{failure.symbol} ({failure.error})" for failure in report.failed
                ),
            )
            return 1
    except (ConfigurationError, AlpacaApiError, DataQualityError) as exc:
        logger.error("Ingestion failed: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
