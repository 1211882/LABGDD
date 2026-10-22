from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession

from src.config.settings import load_spark_settings
from src.performance.workload import read_records
from src.spark.batch_analytics import (
    _write_frame,
    build_batch_sector_analytics,
    build_batch_symbol_analytics,
    prepare_analytics_source,
)
from src.spark.transformations import add_sector, deduplicate_records, split_quality_records


def run(input_path: Path, output_root: Path, metrics_path: Path, windows: tuple[str, ...], config: Path) -> dict[str, object]:
    total_started = time.perf_counter()
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    spark_settings = load_spark_settings(config)
    spark_started = time.perf_counter()
    spark = (
        SparkSession.builder.master("local[*]").appName("phase8-batch-benchmark")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.ui.enabled", "false").getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    startup_seconds = time.perf_counter() - spark_started
    processing_started = time.perf_counter()
    try:
        records = read_records(input_path)
        frame = spark.createDataFrame(
            [(
                datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00")),
                row["symbol"], row["open"], row["high"], row["low"], row["close"], row["volume"],
            ) for row in records],
            ["timestamp", "symbol", "open", "high", "low", "close", "volume"],
        )
        valid, invalid = split_quality_records(frame)
        unique, duplicates = deduplicate_records(valid)
        source = prepare_analytics_source(add_sector(unique, spark_settings.symbol_sectors), False).cache()
        input_rows = len(records)
        output_rows = 0
        windows_generated: dict[str, dict[str, int]] = {}
        if output_root.exists():
            shutil.rmtree(output_root)
        for duration in windows:
            key = duration.replace(" ", "_")
            symbol = build_batch_symbol_analytics(source, (duration,), input_path.stem)
            sector = build_batch_sector_analytics(source, (duration,), input_path.stem)
            symbol_count, sector_count = symbol.count(), sector.count()
            windows_generated[duration] = {"symbol": symbol_count, "sector": sector_count}
            output_rows += symbol_count + sector_count
            _write_frame(symbol, output_root / "symbol" / f"duration={key}")
            _write_frame(sector, output_root / "sector" / f"duration={key}")
        processing_seconds = time.perf_counter() - processing_started
        output_bytes = sum(p.stat().st_size for p in output_root.rglob("*") if p.is_file())
        result = {
            "architecture": "batch", "status": "completed",
            "input_rows": input_rows, "input_bytes": input_path.stat().st_size,
            "valid_rows": valid.count(), "invalid_rows": invalid.count(),
            "duplicate_rows": duplicates.count(), "output_rows": output_rows,
            "output_bytes": output_bytes, "startup_time_seconds": startup_seconds,
            "processing_time_seconds": processing_seconds,
            "total_duration_seconds": time.perf_counter() - total_started,
            "throughput_rows_per_second": input_rows / processing_seconds,
            "analytical_windows": windows_generated,
        }
    finally:
        spark.stop()
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--metrics", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=Path("config/config.yaml"))
    parser.add_argument("--windows", nargs="+", default=["5 minutes", "15 minutes", "30 minutes"])
    args = parser.parse_args()
    print(json.dumps(run(args.input, args.output, args.metrics, tuple(args.windows), args.config), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
