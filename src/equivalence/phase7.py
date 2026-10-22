from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import sys
import time
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds
from pyspark.sql import SparkSession

from src.config.settings import load_kafka_settings, load_spark_settings
from src.equivalence.controlled_dataset import (
    DATASET_VERSION,
    closing_sentinel,
    read_bars,
)
from src.kafka.producer import MarketBarProducer
from src.spark.batch_analytics import (
    _write_frame,
    build_batch_sector_analytics,
    build_batch_symbol_analytics,
    prepare_analytics_source,
)
from src.spark.transformations import add_sector, deduplicate_records, split_quality_records

WINDOWS = ("1 minute", "5 minutes", "15 minutes", "30 minutes", "60 minutes")
FLOAT_TOLERANCE = 1e-10
EXACT_FIELDS = {
    "symbol": ("sector", "total_volume", "event_count", "expected_event_count"),
    "sector": ("total_volume", "event_count", "symbol_count", "active_symbols", "expected_event_count"),
}
FLOAT_FIELDS = {
    "symbol": (
        "average_price", "minimum_price", "maximum_price", "first_price",
        "last_price", "window_return", "volatility", "coverage_ratio",
    ),
    "sector": ("average_price", "minimum_price", "maximum_price", "volatility", "coverage_ratio"),
}


def run_batch(input_path: Path, output_root: Path, config_path: Path) -> dict[str, Any]:
    spark_settings = load_spark_settings(config_path)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("phase7-controlled-batch")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    started = time.perf_counter()
    try:
        bars = read_bars(input_path)
        frame = spark.createDataFrame(
            [(b.timestamp, b.symbol, b.open, b.high, b.low, b.close, b.volume) for b in bars],
            ["timestamp", "symbol", "open", "high", "low", "close", "volume"],
        )
        valid, invalid = split_quality_records(frame)
        unique, duplicates = deduplicate_records(valid)
        source = prepare_analytics_source(
            add_sector(unique, spark_settings.symbol_sectors), False
        ).cache()
        counts: dict[str, dict[str, int]] = {}
        version_root = output_root / DATASET_VERSION / "batch"
        if version_root.exists():
            shutil.rmtree(version_root)
        for duration in WINDOWS:
            key = duration.replace(" ", "_")
            symbol = build_batch_symbol_analytics(source, (duration,), DATASET_VERSION)
            sector = build_batch_sector_analytics(source, (duration,), DATASET_VERSION)
            counts[duration] = {"symbol": symbol.count(), "sector": sector.count()}
            _write_frame(symbol, version_root / "symbol" / f"duration={key}")
            _write_frame(sector, version_root / "sector" / f"duration={key}")
        result = {
            "dataset_version": DATASET_VERSION,
            "input_events": len(bars),
            "valid_events": valid.count(),
            "invalid_events": invalid.count(),
            "duplicate_events": duplicates.count(),
            "unique_events": source.count(),
            "window_counts": counts,
            "duration_seconds": round(time.perf_counter() - started, 6),
            "output_root": str(version_root),
        }
        metrics = version_root / "batch_metrics.json"
        metrics.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        return result
    finally:
        spark.stop()


def replay(input_path: Path, config_path: Path, topic: str) -> dict[str, Any]:
    settings = replace(load_kafka_settings(config_path), raw_topic=topic)
    bars = read_bars(input_path)
    producer = MarketBarProducer(settings)
    published = producer.publish_many([*bars, closing_sentinel()], timeout=30.0)
    return {
        "dataset_version": DATASET_VERSION,
        "topic": topic,
        "dataset_events": len(bars),
        "sentinel_events": 1,
        "published_events": published,
    }


def compare(batch_root: Path, streaming_root: Path, report_path: Path) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    for level in ("symbol", "sector"):
        for duration in WINDOWS:
            key = duration.replace(" ", "_")
            batch = _rows(batch_root / DATASET_VERSION / "batch" / level / f"duration={key}")
            streaming = _rows(streaming_root / level / f"duration={key}")
            results.append(_compare_rows(level, duration, batch, streaming))
    passed = all(item["mismatched_rows"] == 0 and item["missing_rows"] == 0 for item in results)
    report = {
        "dataset_version": DATASET_VERSION,
        "status": "PASS" if passed else "FAIL",
        "input": {
            "batch": "checksummed controlled JSONL after validation and logical deduplication",
            "streaming": "the same JSONL replayed through Kafka plus one watermark-closing sentinel",
        },
        "identity": "symbol + timestamp",
        "window_policy": "fixed half-open UTC windows grouped by New York trading_date",
        "formulas": {
            "bar_return": "(close - open) / open when open > 0",
            "window_return": "(last_price - first_price) / first_price when first_price > 0",
            "volatility": "sample standard deviation of constituent bar_return values",
            "coverage_ratio_symbol": "event_count / nominal window minutes",
            "coverage_ratio_sector": "event_count / (nominal window minutes * active_symbols)",
        },
        "float_absolute_tolerance": FLOAT_TOLERANCE,
        "float_relative_tolerance": FLOAT_TOLERANCE,
        "classification": {
            "exact": "all exact fields and floating-point values are identical",
            "within_tolerance": "exact fields match and every float is within tolerance",
            "mismatch": "an exact field differs or at least one float exceeds tolerance",
            "missing": "an analytical key exists in only one path",
        },
        "comparison": results,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    return report


def _rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    table = ds.dataset(path, format="parquet").to_table()
    return [row for row in table.to_pylist() if row.get("dataset_version") == DATASET_VERSION]


def _compare_rows(level: str, duration: str, batch: list[dict[str, Any]], streaming: list[dict[str, Any]]) -> dict[str, Any]:
    key_fields = ["dataset_version", "trading_date", "window_start", "window_end", "window_duration", "sector"]
    if level == "symbol":
        key_fields.insert(5, "symbol")
    indexed_batch = {tuple(row.get(field) for field in key_fields): row for row in batch}
    indexed_streaming = {tuple(row.get(field) for field in key_fields): row for row in streaming}
    missing = set(indexed_batch) ^ set(indexed_streaming)
    exact = tolerant = mismatched = 0
    max_difference = {field: 0.0 for field in FLOAT_FIELDS[level]}
    for key in set(indexed_batch) & set(indexed_streaming):
        left, right = indexed_batch[key], indexed_streaming[key]
        bad = any(left.get(field) != right.get(field) for field in EXACT_FIELDS[level])
        has_rounding = False
        for field in FLOAT_FIELDS[level]:
            lvalue, rvalue = left.get(field), right.get(field)
            if lvalue is None or rvalue is None:
                bad |= lvalue is not rvalue
                continue
            difference = abs(float(lvalue) - float(rvalue))
            max_difference[field] = max(max_difference[field], difference)
            bad |= not math.isclose(float(lvalue), float(rvalue), rel_tol=FLOAT_TOLERANCE, abs_tol=FLOAT_TOLERANCE)
            has_rounding |= difference > 0
        if bad:
            mismatched += 1
        elif has_rounding:
            tolerant += 1
        else:
            exact += 1
    return {
        "level": level,
        "window_duration": duration,
        "batch_rows": len(batch),
        "streaming_rows": len(streaming),
        "exact_rows": exact,
        "within_tolerance_rows": tolerant,
        "mismatched_rows": mismatched,
        "missing_rows": len(missing),
        "maximum_absolute_difference": max_difference,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 7 controlled equivalence steps.")
    parser.add_argument("action", choices=("batch", "replay", "compare"))
    parser.add_argument("--config", type=Path, default=Path("config/config.yaml"))
    parser.add_argument("--input", type=Path, default=Path(f"datasets/equivalence/{DATASET_VERSION}.jsonl"))
    parser.add_argument("--output-root", type=Path, default=Path("data/analytics/equivalence"))
    parser.add_argument("--streaming-root", type=Path, default=Path(f"data/analytics/equivalence/{DATASET_VERSION}/streaming"))
    parser.add_argument("--report", type=Path, default=Path(f"benchmarks/{DATASET_VERSION}_equivalence.json"))
    parser.add_argument("--topic", default="market-bars-equivalence-v1")
    args = parser.parse_args()
    if args.action == "batch":
        result = run_batch(args.input, args.output_root, args.config)
    elif args.action == "replay":
        result = replay(args.input, args.config, args.topic)
    else:
        result = compare(args.output_root, args.streaming_root, args.report)
    print(json.dumps(result, indent=2, default=str))
    return 0 if result.get("status", "PASS") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
