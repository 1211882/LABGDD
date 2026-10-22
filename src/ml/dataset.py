from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.config.settings import load_settings
from src.ingestion.alpaca_client import AlpacaClient
from src.processing.normalization import normalize_bars, validate_market_bars

DATASET_VERSION = "ml_dataset_v1"
DEFAULT_START = "2025-10-01"
DEFAULT_END = "2026-10-01"
LOGICAL_KEY = ["symbol", "timestamp"]


def regular_session(dataframe: pd.DataFrame) -> pd.DataFrame:
    frame = dataframe.copy()
    local = frame["timestamp"].dt.tz_convert("America/New_York")
    minute = local.dt.hour * 60 + local.dt.minute
    frame = frame.loc[(minute >= 570) & (minute < 960)].copy()
    frame["trading_date"] = local.loc[frame.index].dt.date.astype(str)
    return frame.reset_index(drop=True)


def quality_report(dataframe: pd.DataFrame, symbol_sectors: dict[str, str]) -> dict[str, Any]:
    frame = dataframe.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["sector"] = frame["symbol"].map(symbol_sectors)
    frame["month"] = frame["timestamp"].dt.strftime("%Y-%m")
    duplicates = int(frame.duplicated(LOGICAL_KEY).sum())
    nulls = {column: int(value) for column, value in frame.isna().sum().items() if value}
    ohlc_bad = (
        (frame["high"] < frame[["open", "close", "low"]].max(axis=1))
        | (frame["low"] > frame[["open", "close", "high"]].min(axis=1))
    )
    rows_by_symbol = frame.groupby("symbol").size().sort_index()
    days_by_symbol = frame.groupby("symbol")["trading_date"].nunique().sort_index()
    expected = days_by_symbol * 390
    missing = (expected - rows_by_symbol).clip(lower=0)
    coverage = (rows_by_symbol / expected).where(expected > 0, 0.0)
    return {
        "total_rows": int(len(frame)),
        "trading_days": int(frame["trading_date"].nunique()),
        "rows_per_symbol": rows_by_symbol.astype(int).to_dict(),
        "missing_minutes_per_symbol": missing.astype(int).to_dict(),
        "missing_minutes_total": int(missing.sum()),
        "duplicate_identities": duplicates,
        "null_values": nulls,
        "ohlc_violations": int(ohlc_bad.sum()),
        "zero_volume_rows": int((frame["volume"] == 0).sum()),
        "coverage_per_symbol": {key: round(float(value), 6) for key, value in coverage.items()},
        "coverage_per_month": frame.groupby("month").size().astype(int).to_dict(),
        "coverage_per_sector": frame.groupby("sector", dropna=False).size().astype(int).to_dict(),
    }


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def build_dataset(
    *, dataset_version: str, start: str, end: str, root: Path, config: Path,
) -> Path:
    destination = root / dataset_version
    if destination.exists():
        raise FileExistsError(f"Immutable dataset already exists: {destination}")
    temporary = root / f".{dataset_version}.building"
    if temporary.exists():
        shutil.rmtree(temporary)
    raw_root = temporary / "raw"
    processed_root = temporary / "processed"
    raw_root.mkdir(parents=True)
    processed_root.mkdir(parents=True)

    settings = load_settings(config)
    client = AlpacaClient(
        settings.alpaca.market_data_base_url,
        settings.alpaca.api_key_id,
        settings.alpaca.api_secret_key,
    )
    sector_map = load_symbol_sectors(config)
    frames: list[pd.DataFrame] = []
    raw_checksums: dict[str, str] = {}
    for symbol in settings.ingestion.symbols:
        bars = client.get_historical_bars(
            symbol=symbol, start=start, end=end, timeframe="1Min",
            limit=settings.ingestion.limit, feed=settings.alpaca.feed,
        )
        frame = normalize_bars(symbol, bars)
        validate_market_bars(frame)
        raw_path = raw_root / f"{symbol}.parquet"
        frame.to_parquet(raw_path, index=False)
        raw_checksums[symbol] = sha256(raw_path)
        frames.append(regular_session(frame))

    combined = pd.concat(frames, ignore_index=True).sort_values(LOGICAL_KEY).reset_index(drop=True)
    combined["sector"] = combined["symbol"].map(sector_map)
    report = quality_report(combined, sector_map)
    if report["duplicate_identities"] or report["null_values"] or report["ohlc_violations"]:
        raise ValueError(f"ML dataset failed quality gates: {report}")
    data_path = processed_root / "market_bars.parquet"
    combined.to_parquet(data_path, index=False)
    quality_path = temporary / "quality_report.json"
    quality_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    observed = combined["timestamp"]
    manifest = {
        "dataset_version": dataset_version,
        "purpose": "machine_learning",
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": "Alpaca Market Data API v2",
        "feed": settings.alpaca.feed,
        "timeframe": "1Min",
        "requested_start_date": start,
        "requested_end_date": end,
        "observed_start_date": observed.min().isoformat(),
        "observed_end_date": observed.max().isoformat(),
        "symbols": settings.ingestion.symbols,
        "sectors": sorted(set(sector_map.values())),
        "symbol_count": len(settings.ingestion.symbols),
        "sector_count": len(set(sector_map.values())),
        "row_count": len(combined),
        "raw_bytes": sum(path.stat().st_size for path in raw_root.glob("*.parquet")),
        "processed_bytes": data_path.stat().st_size,
        "regular_session_only": True,
        "session": "09:30 <= local_time < 16:00 America/New_York",
        "missing_bar_policy": "preserve gaps; no interpolation or forward fill",
        "duplicate_policy": "reject duplicate logical identities",
        "logical_key": LOGICAL_KEY,
        "source_dataset_references": ["dataset_v1 (engineering baseline; not merged)"],
        "checksums": {"processed/market_bars.parquet": sha256(data_path), **{f"raw/{key}.parquet": value for key, value in raw_checksums.items()}},
        "quality_report": "quality_report.json",
    }
    (temporary / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    temporary.rename(destination)
    return destination


def load_symbol_sectors(config: Path) -> dict[str, str]:
    import yaml
    value = yaml.safe_load(config.read_text(encoding="utf-8"))
    return {symbol: item["sector"] for symbol, item in value["symbols"].items()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an immutable Alpaca ML dataset.")
    parser.add_argument("--dataset-version", default=DATASET_VERSION)
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=DEFAULT_END)
    parser.add_argument("--root", type=Path, default=Path("datasets/ml"))
    parser.add_argument("--config", type=Path, default=Path("config/config.yaml"))
    args = parser.parse_args()
    print(build_dataset(dataset_version=args.dataset_version, start=args.start, end=args.end, root=args.root, config=args.config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
