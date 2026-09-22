from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

from src.storage.parquet_storage import ParquetStorage


def test_save_raw_bars_persists_parquet_and_csv_files(tmp_path) -> None:
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

    output_files = ParquetStorage(tmp_path).save_raw_bars(
        dataframe=dataframe,
        symbol="AAPL",
        start_date="2026-09-01",
        end_date="2026-09-20",
        timeframe="1Min",
    )

    assert output_files.parquet_file.exists()
    assert output_files.csv_file.exists()
    assert output_files.parquet_file.name == "2026-09-01_2026-09-20_1Min.parquet"
    assert output_files.csv_file.name == "2026-09-01_2026-09-20_1Min.csv"

    parquet_data = pd.read_parquet(output_files.parquet_file)
    csv_data = pd.read_csv(output_files.csv_file)
    assert len(parquet_data) == 1
    assert len(csv_data) == 1
    assert parquet_data.loc[0, "symbol"] == "AAPL"
    assert csv_data.loc[0, "symbol"] == "AAPL"
