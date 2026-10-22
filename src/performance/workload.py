from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SYMBOLS = (
    ("AAPL", "Technology", 100.0),
    ("MSFT", "Technology", 150.0),
    ("JNJ", "Healthcare", 175.0),
    ("PFE", "Healthcare", 50.0),
    ("JPM", "Financial", 125.0),
    ("BAC", "Financial", 40.0),
)


def generate_records(size: int) -> list[dict[str, Any]]:
    if size <= 0:
        raise ValueError("Workload size must be positive.")
    start = datetime(2026, 1, 5, 14, 30, tzinfo=timezone.utc)
    records = []
    for index in range(size):
        symbol, _sector, base = SYMBOLS[index % len(SYMBOLS)]
        minute = index // len(SYMBOLS)
        timestamp = start + timedelta(minutes=minute)
        drift = (minute % 390) * 0.01
        open_price = base + drift
        close = open_price + ((minute + index) % 7 - 3) * 0.005
        records.append({
            "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
            "symbol": symbol,
            "open": round(open_price, 6),
            "high": round(max(open_price, close) + 0.02, 6),
            "low": round(min(open_price, close) - 0.02, 6),
            "close": round(close, 6),
            "volume": 1000 + (index % 5000),
        })
    return records


def write_workload(size: int, output_dir: Path) -> tuple[Path, Path]:
    records = generate_records(size)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"phase8_{size}.jsonl"
    payload = ("\n".join(json.dumps(row, separators=(",", ":"), sort_keys=True) for row in records) + "\n").encode("utf-8")
    path.write_bytes(payload)
    latest = datetime.fromisoformat(records[-1]["timestamp"].replace("Z", "+00:00"))
    sentinel = dict(records[-1])
    sentinel.update({
        "timestamp": (latest + timedelta(minutes=120)).isoformat().replace("+00:00", "Z"),
        "symbol": "AAPL", "open": 100.0, "high": 100.1,
        "low": 99.9, "close": 100.0, "volume": 1,
    })
    manifest = {
        "dataset_version": f"phase8_{size}",
        "generation_method": "deterministic timestamp-safe OHLCV expansion across six configured symbols",
        "event_count": size,
        "unique_logical_keys": size,
        "input_bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "symbols": [item[0] for item in SYMBOLS],
        "sectors": sorted({item[1] for item in SYMBOLS}),
        "watermark_closing_sentinel": sentinel,
    }
    manifest_path = output_dir / f"phase8_{size}.manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return path, manifest_path


def read_records(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
