from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from src.ingestion.alpaca_client import AlpacaApiError
from src.ingestion.market_data_ingestion import MarketDataIngestionService
from src.storage.parquet_storage import StorageResult


class StubClient:
    def __init__(self) -> None:
        self.requested_symbols: list[str] = []

    def get_historical_bars(self, symbol: str, **kwargs):
        self.requested_symbols.append(symbol)
        if symbol == "BAD":
            raise AlpacaApiError("simulated failure")
        return [
            {
                "t": "2026-09-01T13:30:00Z",
                "o": 10.0,
                "h": 11.0,
                "l": 9.0,
                "c": 10.5,
                "v": 100,
            }
        ]


class StubStorage:
    def save_raw_bars(self, dataframe, symbol: str, **kwargs) -> StorageResult:
        return StorageResult(
            parquet_file=Path(f"{symbol}.parquet"),
            csv_file=Path(f"{symbol}.csv"),
        )


def test_ingestion_continues_after_one_symbol_fails() -> None:
    settings = SimpleNamespace(
        ingestion=SimpleNamespace(
            symbols=["AAPL", "BAD", "MSFT"],
            start_date="2026-09-01",
            end_date="2026-09-20",
            timeframe="1Min",
            limit=10000,
        ),
        alpaca=SimpleNamespace(feed="iex"),
    )
    client = StubClient()

    report = MarketDataIngestionService(settings, client, StubStorage()).run()

    assert client.requested_symbols == ["AAPL", "BAD", "MSFT"]
    assert [result.symbol for result in report.successful] == ["AAPL", "MSFT"]
    assert [failure.symbol for failure in report.failed] == ["BAD"]
