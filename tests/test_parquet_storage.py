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


def test_incremental_storage_deduplicates_overlapping_periods(tmp_path) -> None:
    storage = ParquetStorage(tmp_path)
    first = pd.DataFrame([
        {"timestamp": pd.Timestamp("2026-09-01T13:30:00Z"), "symbol": "AAPL", "open": 1.0, "high": 2.0, "low": 1.0, "close": 2.0, "volume": 10},
        {"timestamp": pd.Timestamp("2026-09-01T13:31:00Z"), "symbol": "AAPL", "open": 2.0, "high": 3.0, "low": 2.0, "close": 3.0, "volume": 20},
    ])
    overlap = first.iloc[[1]].copy()
    storage.save_raw_bars(first, "AAPL", "2026-09-01", "2026-09-01", "1Min")
    result = storage.save_raw_bars(overlap, "AAPL", "2026-09-01", "2026-09-02", "1Min")

    files = list((tmp_path / "AAPL").glob("*.parquet"))
    combined = pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)
    assert len(combined) == 2
    assert not combined.duplicated(["symbol", "timestamp"]).any()
    assert pd.read_parquet(result.parquet_file).empty
