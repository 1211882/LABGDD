from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config.settings import load_spark_settings
from src.processing.normalization import REQUIRED_COLUMNS


def main() -> int:
    settings = load_spark_settings()
    summary: dict[str, object] = {
        "rows_per_symbol": {},
        "input_size_bytes": 0,
        "failed_symbols": [],
        "empty_symbols": [],
    }

    for symbol in settings.symbol_sectors:
        files = sorted((settings.input_path / symbol).glob("*.parquet"))
        if not files:
            summary["failed_symbols"].append(f"{symbol}: missing Parquet file")
            continue

        dataframe = pd.concat(
            [pd.read_parquet(file_path) for file_path in files], ignore_index=True
        )
        summary["input_size_bytes"] += sum(file_path.stat().st_size for file_path in files)
        summary["rows_per_symbol"][symbol] = len(dataframe)

        if dataframe.empty:
            summary["empty_symbols"].append(symbol)
            continue
        missing_columns = [
            column for column in REQUIRED_COLUMNS if column not in dataframe.columns
        ]
        if missing_columns:
            summary["failed_symbols"].append(
                f"{symbol}: missing columns {missing_columns}"
            )
            continue
        if dataframe[REQUIRED_COLUMNS].isna().any().any():
            summary["failed_symbols"].append(f"{symbol}: required values contain nulls")
        actual_symbols = set(dataframe["symbol"].unique())
        if actual_symbols != {symbol}:
            summary["failed_symbols"].append(
                f"{symbol}: unexpected symbols {sorted(actual_symbols)}"
            )

    summary["total_rows"] = sum(summary["rows_per_symbol"].values())
    summary["number_of_symbols"] = len(summary["rows_per_symbol"])
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 1 if summary["failed_symbols"] or summary["empty_symbols"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
