from __future__ import annotations

import logging
import sys

from src.config.settings import ConfigurationError, load_settings
from src.ingestion.alpaca_client import AlpacaApiError, AlpacaClient
from src.ingestion.market_data_ingestion import MarketDataIngestionService
from src.processing.normalization import DataQualityError
from src.storage.parquet_storage import ParquetStorage


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    try:
        settings = load_settings()
        client = AlpacaClient(
            base_url=settings.alpaca.market_data_base_url,
            api_key_id=settings.alpaca.api_key_id,
            api_secret_key=settings.alpaca.api_secret_key,
        )
        storage = ParquetStorage(settings.storage.raw_data_dir)
        service = MarketDataIngestionService(settings, client, storage)
        service.run()
    except (ConfigurationError, AlpacaApiError, DataQualityError) as exc:
        logger.error("Ingestion failed: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
