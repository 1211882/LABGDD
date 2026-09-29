from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config.settings import load_structured_streaming_settings


def _read_parquet_tree(path: Path) -> pd.DataFrame:
    files = sorted(path.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    frames = []
    for file_path in files:
        frame = pd.read_parquet(file_path)
        if "symbol" not in frame.columns:
            symbol_part = next(
                (
                    part.split("=", 1)[1]
                    for part in file_path.parts
                    if part.startswith("symbol=")
                ),
                None,
            )
            frame["symbol"] = symbol_part
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate processed streaming output.")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--output-path", type=Path)
    parser.add_argument("--invalid-output-path", type=Path)
    args = parser.parse_args()
    settings = load_structured_streaming_settings(args.config)
    output_path = args.output_path or settings.output_path
    invalid_path = args.invalid_output_path or settings.invalid_output_path

    valid = _read_parquet_tree(output_path)
    invalid = _read_parquet_tree(invalid_path)
    if valid.empty:
        raise SystemExit(f"No valid streaming Parquet records found under {output_path}")

    duplicate_count = int(valid.duplicated(["symbol", "timestamp"]).sum())
    expected_sectors = valid["symbol"].map(settings.symbol_sectors)
    sector_mismatches = int((valid["sector"] != expected_sectors).sum())
    invalid_valid_flags = int((~valid["is_valid"]).sum())
    summary = {
        "valid_rows": len(valid),
        "invalid_rows": len(invalid),
        "duplicate_symbol_timestamp_rows": duplicate_count,
        "sector_mismatches": sector_mismatches,
        "invalid_flags_in_valid_output": invalid_valid_flags,
        "symbols": sorted(valid["symbol"].dropna().unique().tolist()),
        "kafka_partitions": sorted(
            int(value) for value in valid["kafka_partition"].dropna().unique()
        ),
        "event_time_min": str(valid["timestamp"].min()),
        "event_time_max": str(valid["timestamp"].max()),
    }
    print(json.dumps(summary, indent=2, sort_keys=True))

    if duplicate_count or sector_mismatches or invalid_valid_flags:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
