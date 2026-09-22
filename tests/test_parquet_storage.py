from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

from src.storage.parquet_storage import ParquetStorage


def test_save_raw_bars_persists_parquet_file(tmp_path) -> None:
    dataframe = pd.DataFrame(
        [
            {
                "timestamp": pd.Timestamp("2026-09-01T09:30:00Z"),
                "symbol": "AAPL",
                "open": 1.0,
                "high": 2.0,
                "low": 1.0,
                "close": 2.0,
                "volume": 10,
            }
        ]
    )

    output_path = ParquetStorage(tmp_path).save_raw_bars(
        dataframe=dataframe,
        symbol="AAPL",
        start_date="2026-09-01",
        end_date="2026-09-20",
        timeframe="1Min",
    )

    assert output_path.exists()
    assert output_path.name == "2026-09-01_2026-09-20_1Min.parquet"
    loaded = pd.read_parquet(output_path)
    assert len(loaded) == 1
    assert loaded.loc[0, "symbol"] == "AAPL"
