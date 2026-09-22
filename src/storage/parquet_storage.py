from __future__ import annotations

from pathlib import Path

import pandas as pd


class ParquetStorage:
    """Persist normalized market data to local Parquet files."""

    def __init__(self, raw_data_dir: str | Path) -> None:
        self.raw_data_dir = Path(raw_data_dir)

    def save_raw_bars(
        self,
        dataframe: pd.DataFrame,
        symbol: str,
        start_date: str,
        end_date: str,
        timeframe: str,
    ) -> Path:
        output_dir = self.raw_data_dir / symbol.upper()
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{start_date}_{end_date}_{timeframe}.parquet"
        dataframe.to_parquet(output_path, index=False)
        return output_path
