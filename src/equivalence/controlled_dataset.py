from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.models.market_bar import MarketBar

DATASET_VERSION = "phase7_controlled_v1"
SYMBOLS = ("AAPL", "MSFT", "JNJ", "JPM")
SKIPPED_MINUTES = {
    "AAPL": set(),
    "MSFT": {2, 7, 31},
    "JNJ": set(range(1, 65, 2)),
    "JPM": set(range(35, 65)),
}


def generate_bars() -> list[MarketBar]:
    """Generate two trading days with complete and deliberately sparse windows."""
    bars: list[MarketBar] = []
    for day_index, day in enumerate((1, 2)):
        start = datetime(2026, 9, day, 13, 30, tzinfo=timezone.utc)
        day_bars: list[MarketBar] = []
        for minute in range(65):
            for symbol_index, symbol in enumerate(SYMBOLS):
                if minute in SKIPPED_MINUTES[symbol]:
                    continue
                base = 100.0 + 25.0 * symbol_index + 3.0 * day_index
                open_price = base + minute * 0.05
                close = open_price + ((minute + symbol_index) % 5 - 2) * 0.01
                day_bars.append(
                    MarketBar(
                        timestamp=start + timedelta(minutes=minute),
                        symbol=symbol,
                        open=open_price,
                        high=max(open_price, close) + 0.04,
                        low=min(open_price, close) - 0.04,
                        close=close,
                        volume=1000 + 100 * symbol_index + 10 * minute + day_index,
                    )
                )
        # Swap adjacent records to exercise bounded out-of-order arrival.
        for index in range(0, len(day_bars) - 1, 17):
            day_bars[index], day_bars[index + 1] = day_bars[index + 1], day_bars[index]
        bars.extend(day_bars)
    # Exact logical duplicates must disappear in both paths.
    bars.insert(20, bars[8])
    bars.insert(len(bars) - 10, bars[-20])
    return bars


def closing_sentinel() -> MarketBar:
    """Advance the watermark without overlapping any analytical source window."""
    return MarketBar(
        timestamp=datetime(2026, 9, 2, 16, 0, tzinfo=timezone.utc),
        symbol="AAPL",
        open=110.0,
        high=110.1,
        low=109.9,
        close=110.0,
        volume=1,
    )


def write_dataset(output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    data_path = output_dir / f"{DATASET_VERSION}.jsonl"
    bars = generate_bars()
    lines = [json.dumps(_record(bar), separators=(",", ":"), sort_keys=True) for bar in bars]
    payload = ("\n".join(lines) + "\n").encode("utf-8")
    data_path.write_bytes(payload)
    unique_keys = {(bar.symbol, bar.timestamp) for bar in bars}
    manifest = {
        "dataset_version": DATASET_VERSION,
        "description": "Deterministic Phase 7 Batch/Streaming equivalence workload.",
        "event_count": len(bars),
        "unique_event_count": len(unique_keys),
        "duplicate_event_count": len(bars) - len(unique_keys),
        "symbols": list(SYMBOLS),
        "trading_dates": ["2026-09-01", "2026-09-02"],
        "sha256": hashlib.sha256(payload).hexdigest(),
        "watermark_closing_sentinel": _record(closing_sentinel()),
        "notes": [
            "Events contain complete and incomplete windows.",
            "Adjacent events are swapped to exercise bounded out-of-order arrival.",
            "The sentinel is streaming control data and is excluded from Batch input.",
        ],
    }
    manifest_path = output_dir / f"{DATASET_VERSION}.manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return data_path, manifest_path


def read_bars(path: Path) -> list[MarketBar]:
    bars = []
    for line in path.read_text(encoding="utf-8").splitlines():
        value = json.loads(line)
        bars.append(
            MarketBar(
                timestamp=datetime.fromisoformat(value["timestamp"].replace("Z", "+00:00")),
                symbol=value["symbol"],
                open=value["open"], high=value["high"], low=value["low"],
                close=value["close"], volume=value["volume"],
            )
        )
    return bars


def _record(bar: MarketBar) -> dict[str, object]:
    value = asdict(bar)
    value["timestamp"] = bar.timestamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=Path("datasets/equivalence"))
    args = parser.parse_args()
    paths = write_dataset(args.output_dir)
    print(*(str(path) for path in paths), sep="\n")


if __name__ == "__main__":
    main()
