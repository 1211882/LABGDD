from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import re
import statistics
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import matplotlib.pyplot as plt
import psutil
import pyarrow.dataset as ds

from src.config.settings import load_kafka_settings
from src.kafka.event_codec import decode_market_bar
from src.kafka.producer import MarketBarProducer
from src.performance.statistics import percentile, summarize
from src.performance.workload import read_records, write_workload
from src.models.market_bar import MarketBar

DEFAULT_SIZES = (10_000, 25_000, 50_000, 100_000)
WINDOWS = ("5 minutes", "15 minutes", "30 minutes")


class ResourceMonitor:
    def __init__(self, sampler: Callable[[], tuple[float | None, float | None]]) -> None:
        self.sampler = sampler
        self.cpu: list[float] = []
        self.memory_mb: list[float] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> dict[str, float | None]:
        self._stop.set()
        self._thread.join(timeout=3)
        return {
            "cpu_mean_percent": statistics.fmean(self.cpu) if self.cpu else None,
            "cpu_peak_percent": max(self.cpu) if self.cpu else None,
            "memory_peak_mb": max(self.memory_mb) if self.memory_mb else None,
        }

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                cpu, memory = self.sampler()
                if cpu is not None:
                    self.cpu.append(cpu)
                if memory is not None:
                    self.memory_mb.append(memory)
            except (OSError, psutil.Error, subprocess.SubprocessError, ValueError, json.JSONDecodeError):
                pass
            self._stop.wait(0.5)


def process_sampler(process: subprocess.Popen[str]) -> Callable[[], tuple[float | None, float | None]]:
    root = psutil.Process(process.pid)
    root.cpu_percent(None)

    def sample() -> tuple[float | None, float | None]:
        processes = [root, *root.children(recursive=True)]
        cpu = sum(item.cpu_percent(None) for item in processes if item.is_running()) / max(1, psutil.cpu_count())
        memory = sum(item.memory_info().rss for item in processes if item.is_running()) / 1024**2
        return cpu, memory
    return sample


def docker_sampler(container: str = "labgdd-spark-1") -> tuple[float | None, float | None]:
    completed = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}", container],
        capture_output=True, text=True, timeout=10, check=True,
    )
    value = json.loads(completed.stdout)
    return float(value["CPUPerc"].rstrip("%")), _memory_mb(value["MemUsage"].split("/")[0].strip())


def _memory_mb(value: str) -> float:
    match = re.fullmatch(r"([0-9.]+)([KMG]i?B)", value)
    if not match:
        raise ValueError(f"Unsupported Docker memory value: {value}")
    amount, unit = float(match.group(1)), match.group(2)
    return amount * {"KB": 1 / 1000, "KiB": 1 / 1024, "MB": 1, "MiB": 1.048576, "GB": 1000, "GiB": 1073.741824}[unit]


def run_batch(input_path: Path, size: int, run_id: str, root: Path) -> dict[str, Any]:
    run_root = root / str(size) / run_id / "batch"
    metrics_path = run_root / "metrics.json"
    command = [sys.executable, "-m", "src.performance.batch_worker", "--input", str(input_path), "--output", str(run_root / "output"), "--metrics", str(metrics_path), "--windows", *WINDOWS]
    started = time.perf_counter()
    process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)
    monitor = ResourceMonitor(process_sampler(process))
    monitor.start()
    code = process.wait()
    resources = monitor.stop()
    if code != 0 or not metrics_path.exists():
        return {"architecture": "batch", "status": "failed", "run_id": run_id, "workload_rows": size, "error": f"worker exit code {code}", **resources}
    result = json.loads(metrics_path.read_text(encoding="utf-8"))
    result.update({"run_id": run_id, "workload_rows": size, "wall_duration_seconds": time.perf_counter() - started, **resources})
    (run_root / "phase8_run_metrics.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def publish(input_path: Path, manifest_path: Path, topic: str) -> dict[str, float | int]:
    settings = replace(load_kafka_settings(), raw_topic=topic)
    records = read_records(input_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    records.append(manifest["watermark_closing_sentinel"])
    bars = [decode_market_bar(json.dumps(row)) for row in records]
    started = time.perf_counter()
    count = MarketBarProducer(settings).publish_many(bars, timeout=120.0)
    duration = time.perf_counter() - started
    return {"events_produced": count, "producer_duration_seconds": duration, "producer_throughput_events_per_second": count / duration}


def run_streaming(input_path: Path, manifest_path: Path, size: int, run_id: str, root: Path) -> dict[str, Any]:
    topic = f"phase8-{size}-{run_id}-{uuid.uuid4().hex[:8]}".lower().replace("_", "-")
    run_root = root / str(size) / run_id / "streaming"
    subprocess.run(["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:29092", "--create", "--if-not-exists", "--topic", topic, "--partitions", "6", "--replication-factor", "1"], check=True, stdout=subprocess.DEVNULL)
    overall_started = time.perf_counter()
    producer = publish(input_path, manifest_path, topic)
    posix = lambda path: path.as_posix()
    command = [
        "docker", "compose", "exec", "-T", "spark", "python3", "-m", "src.spark.structured_streaming_processor",
        "--available-now", "--bootstrap-servers", "kafka:29092", "--topic", topic,
        "--starting-offsets", "earliest", "--dataset-version", f"phase8_{size}",
        "--analytics-windows", *WINDOWS,
        "--output-path", posix(run_root / "valid"), "--invalid-output-path", posix(run_root / "invalid"),
        "--checkpoint-path", posix(run_root / "checkpoints/core"), "--metrics-path", posix(run_root / "streaming_metrics.json"),
        "--analytics-symbol-output-path", posix(run_root / "analytics/symbol"),
        "--analytics-sector-output-path", posix(run_root / "analytics/sector"),
        "--analytics-checkpoint-path", posix(run_root / "checkpoints/analytics"),
        "--analytics-metrics-path", posix(run_root / "analytics_metrics.json"),
    ]
    process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)
    monitor = ResourceMonitor(docker_sampler)
    monitor.start()
    code = process.wait()
    resources = monitor.stop()
    total = time.perf_counter() - overall_started
    metrics_path = run_root / "streaming_metrics.json"
    if code != 0 or not metrics_path.exists():
        return {"architecture": "streaming", "status": "failed", "run_id": run_id, "workload_rows": size, "error": f"worker exit code {code}", **producer, **resources}
    raw = json.loads(metrics_path.read_text(encoding="utf-8"))
    latency = _latencies(run_root / "valid")
    trigger_durations = [float(item.get("duration_ms", {}).get("triggerExecution", 0)) for item in raw["micro_batches"] if item.get("query") == "valid" and "duration_ms" in item]
    output_rows = sum(sum(level.values()) for level in raw["analytical_windows_per_duration"].values())
    result = {
        "architecture": "streaming", "status": raw["status"], "run_id": run_id,
        "workload_rows": size, "input_rows": raw["input_events"],
        "valid_rows": raw["valid_events"], "invalid_rows": raw["invalid_events"],
        "duplicate_rows": raw["duplicate_events"], "output_rows": output_rows,
        "output_bytes": raw["analytics_output_size_bytes"], "total_duration_seconds": total,
        "spark_duration_seconds": raw["duration_seconds"],
        "spark_throughput_events_per_second": raw["input_events"] / raw["duration_seconds"],
        "spark_reported_processed_rows_per_second": raw["average_processed_rows_per_second"],
        "microbatch_count": len(trigger_durations),
        "microbatch_mean_duration_ms": statistics.fmean(trigger_durations) if trigger_durations else None,
        "microbatch_max_duration_ms": max(trigger_durations) if trigger_durations else None,
        "mean_processing_latency_ms": statistics.fmean(latency) if latency else None,
        "median_processing_latency_ms": percentile(latency, 0.5),
        "p95_processing_latency_ms": percentile(latency, 0.95),
        "p99_processing_latency_ms": percentile(latency, 0.99),
        "late_events": raw["late_events"], "watermark_dropped_events": raw["dropped_watermark_events"],
        "analytical_windows": raw["analytical_windows_per_duration"],
        **producer, **resources,
    }
    result = _enrich_streaming_result(result, raw)
    (run_root / "phase8_run_metrics.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def _enrich_streaming_result(result: dict[str, Any], raw: dict[str, Any]) -> dict[str, Any]:
    progress = [item for item in raw.get("micro_batches", []) if "sources" in item]
    lags = []
    watermarks = []
    state_rows = []
    for item in progress:
        watermark = item.get("event_time", {}).get("watermark")
        if watermark:
            watermarks.append(watermark)
        for operator in item.get("state_operators", []):
            if operator.get("numRowsTotal") is not None:
                state_rows.append(int(operator["numRowsTotal"]))
        for source in item.get("sources", []):
            try:
                latest = json.loads(source.get("latestOffset") or "{}")
                end = json.loads(source.get("endOffset") or "{}")
                latest_total = sum(int(offset) for partitions in latest.values() for offset in partitions.values())
                end_total = sum(int(offset) for partitions in end.values() for offset in partitions.values())
                lags.append(max(0, latest_total - end_total))
            except (TypeError, ValueError, json.JSONDecodeError):
                pass
    result.update({
        "kafka_topic": raw.get("kafka_topic"),
        "kafka_final_lag_events": lags[-1] if lags else None,
        "watermark_progression": list(dict.fromkeys(watermarks)),
        "state_store_peak_rows": max(state_rows) if state_rows else None,
    })
    return result


def _latencies(path: Path) -> list[float]:
    table = ds.dataset(path, format="parquet", partitioning="hive").to_table(columns=["kafka_timestamp", "processing_time"])
    return [max(0.0, (row["processing_time"] - row["kafka_timestamp"]).total_seconds() * 1000) for row in table.to_pylist()]


def environment() -> dict[str, Any]:
    memory = psutil.virtual_memory().total
    return {
        "scope": "local single-machine controlled scalability experiment",
        "os": platform.platform(), "python": platform.python_version(),
        "java": _command(["java", "-version"], stderr=True),
        "spark": __import__("pyspark").__version__,
        "kafka": _command(["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--version"]),
        "docker": _command(["docker", "--version"]), "docker_compose": _command(["docker", "compose", "version"]),
        "cpu_model": platform.processor(), "logical_cpu_count": psutil.cpu_count(),
        "total_memory_bytes": memory, "spark_master": "local[*]", "spark_shuffle_partitions": 6,
        "spark_workers": 1, "kafka_brokers": 1, "kafka_partitions_per_run": 6,
        "container_memory_limit": None,
    }


def _command(command: list[str], stderr: bool = False) -> str | None:
    try:
        value = subprocess.run(command, capture_output=True, text=True, timeout=20, check=True)
        return (value.stderr if stderr else value.stdout).strip().splitlines()[0]
    except (subprocess.SubprocessError, OSError, IndexError):
        return None


def aggregate(runs: list[dict[str, Any]], sizes: tuple[int, ...]) -> list[dict[str, Any]]:
    output = []
    metrics = {
        "batch": ("total_duration_seconds", "processing_time_seconds", "throughput_rows_per_second", "cpu_mean_percent", "cpu_peak_percent", "memory_peak_mb"),
        "streaming": ("total_duration_seconds", "spark_duration_seconds", "producer_throughput_events_per_second", "spark_throughput_events_per_second", "mean_processing_latency_ms", "p95_processing_latency_ms", "p99_processing_latency_ms", "microbatch_mean_duration_ms", "cpu_mean_percent", "cpu_peak_percent", "memory_peak_mb"),
    }
    for size in sizes:
        for architecture, fields in metrics.items():
            selected = [run for run in runs if run["workload_rows"] == size and run["architecture"] == architecture and run["status"] == "completed"]
            output.append({"architecture": architecture, "workload_rows": size, "successful_runs": len(selected), "statistics": {field: summarize(run[field] for run in selected if run.get(field) is not None) for field in fields}})
    return output


def analytical_duplicate_keys(root: Path) -> int | None:
    if not root.exists():
        return None
    duplicates = 0
    found = False
    for level in ("symbol", "sector"):
        level_root = root / level
        if not level_root.exists():
            continue
        for duration in level_root.glob("duration=*"):
            table = ds.dataset(duration, format="parquet").to_table(
                columns=["dataset_version", "trading_date", "window_start", "window_end", "window_duration", *( ["symbol"] if level == "symbol" else []), "sector"]
            )
            rows = table.to_pylist()
            found = True
            keys = [tuple(row.values()) for row in rows]
            duplicates += len(keys) - len(set(keys))
    return duplicates if found else None


def write_csv(runs: list[dict[str, Any]], path: Path) -> None:
    fields = ["architecture", "workload_rows", "run_id", "status", "total_duration_seconds", "processing_time_seconds", "throughput_rows_per_second", "producer_throughput_events_per_second", "spark_throughput_events_per_second", "mean_processing_latency_ms", "p95_processing_latency_ms", "p99_processing_latency_ms", "cpu_mean_percent", "cpu_peak_percent", "memory_peak_mb"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fields)
        writer.writeheader()
        for run in runs:
            writer.writerow({field: run.get(field) for field in fields})


def write_plots(aggregated: list[dict[str, Any]], output: Path) -> list[str]:
    output.mkdir(parents=True, exist_ok=True)
    plots = []
    specs = [
        ("total_duration_seconds", "Total duration", "Seconds", "duration.png", ("batch", "streaming")),
        ("throughput", "Processing throughput", "Events or rows / second", "throughput.png", ("batch", "streaming")),
        ("p95_processing_latency_ms", "Streaming processing latency (p95)", "Milliseconds", "streaming_latency.png", ("streaming",)),
        ("memory_peak_mb", "Peak memory", "MiB", "peak_memory.png", ("batch", "streaming")),
    ]
    for metric, title, ylabel, filename, architectures in specs:
        fig, axis = plt.subplots(figsize=(7, 4.2))
        for architecture in architectures:
            rows = [row for row in aggregated if row["architecture"] == architecture]
            field = ("throughput_rows_per_second" if architecture == "batch" else "spark_throughput_events_per_second") if metric == "throughput" else metric
            x = [row["workload_rows"] for row in rows]
            y = [row["statistics"].get(field, {}).get("mean") for row in rows]
            axis.plot(x, y, marker="o", label=architecture.title())
        axis.set_title(title); axis.set_xlabel("Input events"); axis.set_ylabel(ylabel); axis.grid(alpha=0.25)
        if len(architectures) > 1: axis.legend()
        fig.tight_layout(); target = output / filename; fig.savefig(target, dpi=160); plt.close(fig); plots.append(str(target))
    return plots


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sizes", nargs="+", type=int, default=DEFAULT_SIZES)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--skip-warmup", action="store_true")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    sizes = tuple(args.sizes)
    workload_root, run_root = Path("datasets/phase8"), Path("data/benchmarks/phase8")
    files = {size: write_workload(size, workload_root) for size in sizes}
    runs: list[dict[str, Any]] = []
    if not args.skip_warmup:
        warm_input, warm_manifest = write_workload(1000, workload_root)
        for architecture, function in (
            ("batch", lambda: run_batch(warm_input, 1000, "warmup", run_root)),
            ("streaming", lambda: run_streaming(warm_input, warm_manifest, 1000, "warmup", run_root)),
        ):
            existing = run_root / "1000" / "warmup" / architecture / "phase8_run_metrics.json"
            runs.append(json.loads(existing.read_text(encoding="utf-8")) if args.resume and existing.exists() else function())
    measured = []
    for size in sizes:
        for repetition in range(1, args.repetitions + 1):
            run_id = f"run_{repetition}"
            for architecture, function in (("batch", lambda: run_batch(files[size][0], size, run_id, run_root)), ("streaming", lambda: run_streaming(files[size][0], files[size][1], size, run_id, run_root))):
                existing = run_root / str(size) / run_id / architecture / "phase8_run_metrics.json"
                if args.resume and existing.exists():
                    result = json.loads(existing.read_text(encoding="utf-8"))
                    if architecture == "streaming" and result.get("duplicate_rows", 0):
                        retry_id = f"{run_id}_retry"
                        retry_path = run_root / str(size) / retry_id / architecture / "phase8_run_metrics.json"
                        result = (
                            json.loads(retry_path.read_text(encoding="utf-8"))
                            if retry_path.exists()
                            else run_streaming(files[size][0], files[size][1], size, retry_id, run_root)
                        )
                        result["reported_run_id"] = run_id
                        result["replaces_contaminated_run"] = str(existing)
                    elif architecture == "streaming":
                        raw_path = existing.parent / "streaming_metrics.json"
                        result = _enrich_streaming_result(result, json.loads(raw_path.read_text(encoding="utf-8")))
                else:
                    result = function()
                measured.append(result)
    aggregated = aggregate(measured, sizes)
    for run in measured:
        actual_id = run["run_id"]
        architecture = run["architecture"]
        output = run_root / str(run["workload_rows"]) / actual_id / architecture
        analytics = output / ("output" if architecture == "batch" else "analytics")
        run["duplicate_analytical_keys"] = analytical_duplicate_keys(analytics)
        run["output_exists"] = analytics.exists()
        if architecture == "batch":
            run["cpu_mean_percent"] = None
            run["cpu_peak_percent"] = None
            run["cpu_measurement_note"] = "Unavailable: the host parent-process sampler did not reliably attribute JVM child CPU."
    aggregated = aggregate(measured, sizes)
    report = {
        "phase": "Phase 8 - Batch vs Streaming Performance and Scalability",
        "timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "environment": environment(),
        "configuration": {"workload_sizes": sizes, "repetitions": args.repetitions, "warmup": not args.skip_warmup, "windows": WINDOWS, "trigger_interval": "5 seconds", "watermark_delay": "10 minutes"},
        "workload_generation": "Deterministic timestamp-safe OHLCV expansion; identical logical events feed both paths; one Streaming-only watermark sentinel is excluded from analytical output.",
        "latency_definition": "Kafka record timestamp to processing_time in the committed valid sink; historical market event time is not used.",
        "runs": measured, "aggregated_results": aggregated,
        "warmup_runs": runs, "errors": [run for run in measured if run["status"] != "completed"],
        "limitations": ["Local single-machine experiment", "One Kafka broker and one local[*] Spark container", "Docker and OS scheduling overhead", "Synthetic replay rather than live exchange traffic", "Results do not prove distributed-cluster scalability"],
    }
    report_path = Path("benchmarks/phase8_batch_streaming_performance.json")
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    write_csv(measured, Path("benchmarks/phase8_performance_summary.csv"))
    figures = write_plots(aggregated, Path("reports/figures/phase8"))
    print(json.dumps({"report": str(report_path), "runs": len(measured), "errors": len(report["errors"]), "figures": figures}, indent=2))
    return 1 if report["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
