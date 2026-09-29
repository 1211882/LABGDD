from __future__ import annotations

import json
import platform
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyspark


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class StructuredStreamingMetrics:
    kafka_topic: str
    watermark_delay: str
    checkpoint_path: str
    output_path: str
    invalid_output_path: str
    connector_package: str
    started_at_utc: str = field(default_factory=_utc_now)
    finished_at_utc: str | None = None
    duration_seconds: float = 0.0
    input_events: int = 0
    valid_events: int = 0
    invalid_events: int = 0
    duplicate_events: int | None = None
    number_of_active_symbols: int = 0
    active_symbols: list[str] = field(default_factory=list)
    kafka_partitions_observed: list[int] = field(default_factory=list)
    average_input_rows_per_second: float = 0.0
    average_processed_rows_per_second: float = 0.0
    micro_batches: list[dict[str, Any]] = field(default_factory=list)
    status: str = "running"
    error: str | None = None
    _started_monotonic: float = field(default_factory=time.perf_counter, repr=False)
    _symbols: set[str] = field(default_factory=set, repr=False)
    _partitions: set[int] = field(default_factory=set, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_output(
        self,
        stream_name: str,
        batch_id: int,
        row_count: int,
        symbols: list[str],
        partitions: list[int],
    ) -> None:
        with self._lock:
            if stream_name == "valid":
                self.valid_events += row_count
            else:
                self.invalid_events += row_count
            self._symbols.update(symbols)
            self._partitions.update(partitions)
            self.micro_batches.append(
                {
                    "query": stream_name,
                    "batch_id": batch_id,
                    "output_rows": row_count,
                    "symbols": sorted(symbols),
                    "kafka_partitions": sorted(partitions),
                }
            )

    def finish(
        self,
        progress_by_query: dict[str, list[dict[str, Any]]],
        status: str,
        error: str | None = None,
    ) -> None:
        self.finished_at_utc = _utc_now()
        self.duration_seconds = round(time.perf_counter() - self._started_monotonic, 6)
        valid_progress = progress_by_query.get("valid", [])
        self.input_events = sum(int(item.get("numInputRows", 0)) for item in valid_progress)
        rates_in = [
            float(item.get("inputRowsPerSecond", 0.0)) for item in valid_progress
        ]
        rates_out = [
            float(item.get("processedRowsPerSecond", 0.0)) for item in valid_progress
        ]
        self.average_input_rows_per_second = round(
            sum(rates_in) / len(rates_in) if rates_in else 0.0, 3
        )
        self.average_processed_rows_per_second = round(
            sum(rates_out) / len(rates_out) if rates_out else 0.0, 3
        )

        duplicate_count = 0
        duplicate_metric_found = False
        for item in valid_progress:
            for operator in item.get("stateOperators", []):
                custom = operator.get("customMetrics", {})
                if "numDroppedDuplicateRows" in custom:
                    duplicate_metric_found = True
                    duplicate_count += int(custom["numDroppedDuplicateRows"])
        self.duplicate_events = duplicate_count if duplicate_metric_found else None

        progress_records = []
        for query_name, items in progress_by_query.items():
            for item in items:
                progress_records.append(
                    {
                        "query": query_name,
                        "batch_id": item.get("batchId"),
                        "timestamp": item.get("timestamp"),
                        "num_input_rows": item.get("numInputRows", 0),
                        "input_rows_per_second": item.get("inputRowsPerSecond", 0.0),
                        "processed_rows_per_second": item.get(
                            "processedRowsPerSecond", 0.0
                        ),
                        "duration_ms": item.get("durationMs", {}),
                        "event_time": item.get("eventTime", {}),
                        "state_operators": item.get("stateOperators", []),
                        "sources": item.get("sources", []),
                    }
                )
        self.micro_batches.extend(progress_records)
        self.active_symbols = sorted(self._symbols)
        self.number_of_active_symbols = len(self.active_symbols)
        self.kafka_partitions_observed = sorted(self._partitions)
        self.status = status
        self.error = error

    def to_dict(self) -> dict[str, Any]:
        return {
            "benchmark": "Spark Structured Streaming Kafka processing",
            "environment": {
                "os": platform.platform(),
                "python": platform.python_version(),
                "spark": pyspark.__version__,
            },
            "connector_package": self.connector_package,
            "kafka_topic": self.kafka_topic,
            "watermark_delay": self.watermark_delay,
            "checkpoint_path": self.checkpoint_path,
            "output_path": self.output_path,
            "invalid_output_path": self.invalid_output_path,
            "started_at_utc": self.started_at_utc,
            "finished_at_utc": self.finished_at_utc,
            "duration_seconds": self.duration_seconds,
            "input_events": self.input_events,
            "valid_events": self.valid_events,
            "invalid_events": self.invalid_events,
            "duplicate_events": self.duplicate_events,
            "average_input_rows_per_second": self.average_input_rows_per_second,
            "average_processed_rows_per_second": self.average_processed_rows_per_second,
            "number_of_active_symbols": self.number_of_active_symbols,
            "active_symbols": self.active_symbols,
            "kafka_partitions_observed": self.kafka_partitions_observed,
            "micro_batches": self.micro_batches,
            "status": self.status,
            "error": self.error,
        }

    def save(self, path: str | Path) -> Path:
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(self.to_dict(), indent=2, sort_keys=True), encoding="utf-8"
        )
        return output
