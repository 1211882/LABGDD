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

    def run(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        symbols: list[str] | None = None,
    ) -> IngestionReport:
        results: list[IngestionResult] = []
        failures: list[IngestionFailure] = []
        ingestion = self.settings.ingestion

        selected_symbols = symbols or ingestion.symbols
        requested_start = start_date or ingestion.start_date
        requested_end = end_date or ingestion.end_date
        for symbol in selected_symbols:
            start_time = time.perf_counter()
            LOGGER.info(
                "Requesting historical bars: symbol=%s start=%s end=%s timeframe=%s",
                symbol,
                requested_start,
                requested_end,
                ingestion.timeframe,
            )

            try:
                bars = self.client.get_historical_bars(
                    symbol=symbol,
                    start=requested_start,
                    end=requested_end,
                    timeframe=ingestion.timeframe,
                    limit=ingestion.limit,
                    feed=self.settings.alpaca.feed,
                )
                dataframe = normalize_bars(symbol, bars)
                validate_market_bars(dataframe)
                output_files = self.storage.save_raw_bars(
                    dataframe=dataframe,
                    symbol=symbol,
                    start_date=requested_start,
                    end_date=requested_end,
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
                requested_start,
                requested_end,
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
