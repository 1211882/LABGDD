from __future__ import annotations

import csv
import json

import pytest

from src.performance.benchmark import aggregate, write_csv
from src.performance.statistics import percentile, summarize
from src.performance.workload import generate_records, write_workload


def test_workload_is_deterministic_and_keys_are_unique(tmp_path) -> None:
    first = generate_records(100)
    second = generate_records(100)
    assert first == second
    assert len({(row["symbol"], row["timestamp"]) for row in first}) == 100
    data, manifest = write_workload(100, tmp_path)
    details = json.loads(manifest.read_text(encoding="utf-8"))
    assert details["event_count"] == 100
    assert details["unique_logical_keys"] == 100
    assert details["input_bytes"] == data.stat().st_size


def test_statistics_and_percentiles() -> None:
    assert percentile([1, 2, 3, 4], 0.5) == pytest.approx(2.5)
    assert percentile([1, 2, 3, 4], 0.95) == pytest.approx(3.85)
    summary = summarize([1, 2, 3])
    assert summary["mean"] == 2
    assert summary["median"] == 2
    assert summary["minimum"] == 1
    assert summary["maximum"] == 3


def test_aggregation_and_csv_generation(tmp_path) -> None:
    runs = [
        {"architecture": "batch", "status": "completed", "workload_rows": 10,
         "total_duration_seconds": 2.0, "processing_time_seconds": 1.0,
         "throughput_rows_per_second": 10.0, "cpu_mean_percent": 20.0,
         "cpu_peak_percent": 30.0, "memory_peak_mb": 100.0, "run_id": "run_1"},
        {"architecture": "batch", "status": "completed", "workload_rows": 10,
         "total_duration_seconds": 4.0, "processing_time_seconds": 2.0,
         "throughput_rows_per_second": 5.0, "cpu_mean_percent": 25.0,
         "cpu_peak_percent": 35.0, "memory_peak_mb": 110.0, "run_id": "run_2"},
    ]
    result = aggregate(runs, (10,))
    batch = next(row for row in result if row["architecture"] == "batch")
    assert batch["successful_runs"] == 2
    assert batch["statistics"]["total_duration_seconds"]["median"] == 3.0
    target = tmp_path / "summary.csv"
    write_csv(runs, target)
    with target.open(encoding="utf-8") as file:
        assert len(list(csv.DictReader(file))) == 2


def test_run_paths_are_architecture_and_run_isolated(tmp_path) -> None:
    first = tmp_path / "10000" / "run_1" / "streaming"
    second = tmp_path / "10000" / "run_2" / "streaming"
    assert first != second
    assert first / "checkpoints" != second / "checkpoints"
