from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable

from prometheus_client import REGISTRY, start_http_server
from prometheus_client.core import GaugeMetricFamily


def discover_metric_files(roots: Iterable[Path]) -> list[Path]:
    candidates: set[Path] = set()
    patterns = ("*streaming*.json", "batch_analytics*.json", "phase8_run_metrics.json")
    excluded = {"analytics", "checkpoints", "output", "valid", "invalid", "raw"}
    for root in roots:
        if not root.exists():
            continue
        for directory, names, files in os.walk(root):
            names[:] = [name for name in names if name not in excluded]
            current = Path(directory)
            for filename in files:
                if any(Path(filename).match(pattern) for pattern in patterns):
                    candidates.add(current / filename)
    return sorted(candidates, key=lambda path: path.stat().st_mtime)


def load_latest_metrics(roots: Iterable[Path]) -> dict[str, dict[str, Any]]:
    latest: dict[str, tuple[float, dict[str, Any]]] = {}
    for path in discover_metric_files(roots):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(value, dict):
            continue
        kind = _metric_kind(value, path)
        if kind is None:
            continue
        modified = path.stat().st_mtime
        if kind not in latest or modified > latest[kind][0]:
            latest[kind] = (modified, value)
    return {kind: item[1] for kind, item in latest.items()}


def _metric_kind(value: dict[str, Any], path: Path) -> str | None:
    if "kafka_topic" in value and "micro_batches" in value:
        return "streaming"
    if value.get("architecture") == "streaming":
        return "streaming_benchmark"
    if value.get("architecture") == "batch" or "symbol_windows" in value:
        return "batch"
    if "runs" in value and "aggregated_results" in value:
        return "performance"
    return None


class PipelineCollector:
    def __init__(self, roots: Iterable[Path]) -> None:
        self.roots = tuple(roots)

    def collect(self):
        values = load_latest_metrics(self.roots)
        yield _single("financial_pipeline_exporter_up", "Pipeline exporter health", 1)
        streaming = values.get("streaming", {})
        labels = ["dataset_version", "topic"]
        label_values = [str(streaming.get("dataset_version", "unknown")), str(streaming.get("kafka_topic", "unknown"))]
        status = streaming.get("status")
        yield _labelled("financial_streaming_query_active", "Whether the latest query is running", 1 if status == "running" else 0, labels, label_values)
        yield _labelled("financial_streaming_query_failed_total", "Failed Structured Streaming queries", 1 if status == "failed" else 0, labels, label_values)
        mappings = {
            "input_events": "financial_streaming_input_events_total",
            "valid_events": "financial_pipeline_valid_events_total",
            "invalid_events": "financial_pipeline_invalid_events_total",
            "duplicate_events": "financial_pipeline_duplicate_events_total",
            "average_input_rows_per_second": "financial_streaming_input_rows_per_second",
            "average_processed_rows_per_second": "financial_streaming_processed_rows_per_second",
            "late_events": "financial_streaming_late_events_total",
            "dropped_watermark_events": "financial_streaming_watermark_dropped_rows_total",
            "duration_seconds": "financial_streaming_duration_seconds",
        }
        for field, name in mappings.items():
            yield _labelled(name, field.replace("_", " "), _number(streaming.get(field)), labels, label_values)
        analytical_rows = sum(int(value) for value in streaming.get("analytical_windows_emitted", {}).values())
        yield _labelled("financial_pipeline_analytical_rows_total", "Analytical rows emitted", analytical_rows, labels, label_values)
        progress = [item for item in streaming.get("micro_batches", []) if "duration_ms" in item]
        durations = [float(item.get("duration_ms", {}).get("triggerExecution", 0)) for item in progress]
        state_rows = [int(operator.get("numRowsTotal", 0)) for item in progress for operator in item.get("state_operators", [])]
        yield _labelled("financial_streaming_microbatch_duration_milliseconds", "Latest micro-batch trigger duration", durations[-1] if durations else 0, labels, label_values)
        yield _labelled("financial_streaming_state_store_rows", "Maximum state rows in latest progress snapshot", max(state_rows, default=0), labels, label_values)
        watermark = _latest_watermark(progress)
        yield _labelled("financial_streaming_watermark_timestamp_seconds", "Latest event-time watermark as Unix time", watermark, labels, label_values)

        benchmark = values.get("streaming_benchmark", {})
        yield _single("financial_streaming_processing_latency_mean_milliseconds", "Mean Kafka-to-valid-sink processing latency", _number(benchmark.get("mean_processing_latency_ms")))
        yield _single("financial_streaming_processing_latency_p95_milliseconds", "P95 Kafka-to-valid-sink processing latency", _number(benchmark.get("p95_processing_latency_ms")))
        yield _single("financial_kafka_final_lag_events", "Final Kafka lag recorded by latest controlled run", _number(benchmark.get("kafka_final_lag_events")))

        batch = values.get("batch", {})
        yield _single("financial_batch_input_rows", "Latest Batch input rows", _number(batch.get("input_rows")))
        yield _single("financial_batch_duration_seconds", "Latest Batch duration", _number(batch.get("total_duration_seconds", batch.get("processing_duration_seconds"))))
        yield _single("financial_batch_throughput_rows_per_second", "Latest Batch throughput", _number(batch.get("throughput_rows_per_second", batch.get("rows_per_second"))))


def _single(name: str, description: str, value: float):
    metric = GaugeMetricFamily(name, description)
    metric.add_metric([], value)
    return metric


def _labelled(name: str, description: str, value: float, labels: list[str], values: list[str]):
    metric = GaugeMetricFamily(name, description, labels=labels)
    metric.add_metric(values, value)
    return metric


def _number(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _latest_watermark(progress: list[dict[str, Any]]) -> float:
    from datetime import datetime
    values = []
    for item in progress:
        raw = item.get("event_time", {}).get("watermark")
        if raw:
            try:
                values.append(datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp())
            except ValueError:
                pass
    return max(values, default=0.0)


def main() -> None:
    roots = [Path(value) for value in os.getenv("PIPELINE_METRICS_ROOTS", "benchmarks,data").split(",")]
    REGISTRY.register(PipelineCollector(roots))
    port = int(os.getenv("PIPELINE_EXPORTER_PORT", "8000"))
    start_http_server(port)
    import time
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
