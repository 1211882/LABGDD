from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class StorageResult:
    parquet_file: Path
    csv_file: Path


class ParquetStorage:
    """Persist normalized market data to local Parquet and CSV files."""

    def __init__(self, raw_data_dir: str | Path) -> None:
        self.raw_data_dir = Path(raw_data_dir)

    def save_raw_bars(
        self,
        dataframe: pd.DataFrame,
        symbol: str,
        start_date: str,
        end_date: str,
        timeframe: str,
    ) -> StorageResult:
        output_dir = self.raw_data_dir / symbol.upper()
        output_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{start_date}_{end_date}_{timeframe}"
        parquet_file = output_dir / f"{filename}.parquet"
        csv_file = output_dir / f"{filename}.csv"

        merged = dataframe.copy()
        if parquet_file.exists():
            merged = pd.concat([pd.read_parquet(parquet_file), merged], ignore_index=True)

        other_files = [path for path in output_dir.glob("*.parquet") if path != parquet_file]
        if other_files and not merged.empty:
            existing_keys = pd.concat(
                [pd.read_parquet(path, columns=["symbol", "timestamp"]) for path in other_files],
                ignore_index=True,
            ).drop_duplicates(["symbol", "timestamp"])
            keys = pd.MultiIndex.from_frame(existing_keys[["symbol", "timestamp"]])
            incoming_keys = pd.MultiIndex.from_frame(merged[["symbol", "timestamp"]])
            merged = merged.loc[~incoming_keys.isin(keys)]

        merged = (
            merged.drop_duplicates(["symbol", "timestamp"], keep="last")
            .sort_values("timestamp")
            .reset_index(drop=True)
        )
        merged.to_parquet(parquet_file, index=False)
        merged.to_csv(csv_file, index=False)

        return StorageResult(parquet_file=parquet_file, csv_file=csv_file)
