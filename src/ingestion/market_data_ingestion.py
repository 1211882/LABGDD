from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path

from src.config.settings import AppSettings
from src.ingestion.alpaca_client import AlpacaApiError, AlpacaClient
from src.processing.normalization import (
    DataQualityError,
    normalize_bars,
    validate_market_bars,
)
from src.storage.parquet_storage import ParquetStorage

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestionResult:
    symbol: str
    row_count: int
    parquet_file: Path
    csv_file: Path
    elapsed_seconds: float


@dataclass(frozen=True)
class IngestionFailure:
    symbol: str
    error: str
    elapsed_seconds: float


@dataclass(frozen=True)
class IngestionReport:
    successful: list[IngestionResult]
    failed: list[IngestionFailure]


class MarketDataIngestionService:
    """Coordinates historical data ingestion without exposing HTTP details."""

    def __init__(
        self,
        settings: AppSettings,
        client: AlpacaClient,
        storage: ParquetStorage,
    ) -> None:
        self.settings = settings
        self.client = client
        self.storage = storage

    def run(self) -> IngestionReport:
        results: list[IngestionResult] = []
        failures: list[IngestionFailure] = []
        ingestion = self.settings.ingestion

        for symbol in ingestion.symbols:
            start_time = time.perf_counter()
            LOGGER.info(
                "Requesting historical bars: symbol=%s start=%s end=%s timeframe=%s",
                symbol,
                ingestion.start_date,
                ingestion.end_date,
                ingestion.timeframe,
            )

            try:
                bars = self.client.get_historical_bars(
                    symbol=symbol,
                    start=ingestion.start_date,
                    end=ingestion.end_date,
                    timeframe=ingestion.timeframe,
                    limit=ingestion.limit,
                    feed=self.settings.alpaca.feed,
                )
                dataframe = normalize_bars(symbol, bars)
                validate_market_bars(dataframe)
                output_files = self.storage.save_raw_bars(
                    dataframe=dataframe,
                    symbol=symbol,
                    start_date=ingestion.start_date,
                    end_date=ingestion.end_date,
                    timeframe=ingestion.timeframe,
                )
            except (AlpacaApiError, DataQualityError, OSError) as exc:
                elapsed_seconds = time.perf_counter() - start_time
                LOGGER.error(
                    "Ingestion failed for symbol=%s elapsed_seconds=%.2f error=%s",
                    symbol,
                    elapsed_seconds,
                    exc,
                )
                failures.append(
                    IngestionFailure(
                        symbol=symbol,
                        error=str(exc),
                        elapsed_seconds=elapsed_seconds,
                    )
                )
                continue
            elapsed_seconds = time.perf_counter() - start_time

            LOGGER.info(
                "Ingestion complete: symbol=%s rows=%s date_range=%s..%s "
                "parquet=%s csv=%s elapsed_seconds=%.2f",
                symbol,
                len(dataframe),
                ingestion.start_date,
                ingestion.end_date,
                output_files.parquet_file,
                output_files.csv_file,
                elapsed_seconds,
            )
            results.append(
                IngestionResult(
                    symbol=symbol,
                    row_count=len(dataframe),
                    parquet_file=output_files.parquet_file,
                    csv_file=output_files.csv_file,
                    elapsed_seconds=elapsed_seconds,
                )
            )

        LOGGER.info(
            "Ingestion summary: successful=%s failed=%s failed_symbols=%s",
            len(results),
            len(failures),
            ",".join(failure.symbol for failure in failures) or "none",
        )
        return IngestionReport(successful=results, failed=failures)
