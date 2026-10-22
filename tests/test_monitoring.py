from __future__ import annotations

import json
import os
from pathlib import Path

import yaml

from src.monitoring.exporter import PipelineCollector, discover_metric_files, load_latest_metrics


def _write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_metric_discovery_and_latest_selection(tmp_path) -> None:
    old = tmp_path / "old_streaming.json"
    new = tmp_path / "new_streaming.json"
    payload = {"kafka_topic": "bars", "micro_batches": [], "dataset_version": "v1"}
    _write(old, payload)
    _write(new, dict(payload, dataset_version="v2"))
    os.utime(old, (1, 1))
    os.utime(new, (2, 2))
    files = discover_metric_files((tmp_path,))
    assert files == [old, new]
    assert load_latest_metrics((tmp_path,))["streaming"]["dataset_version"] == "v2"


def test_collector_exposes_pipeline_and_spark_metrics(tmp_path) -> None:
    _write(tmp_path / "structured_streaming_metrics.json", {
        "kafka_topic": "bars", "dataset_version": "v-test", "status": "completed",
        "micro_batches": [{"duration_ms": {"triggerExecution": 250}, "event_time": {"watermark": "2026-09-01T14:00:00Z"}, "state_operators": [{"numRowsTotal": 12}]}],
        "input_events": 10, "valid_events": 9, "invalid_events": 1,
        "duplicate_events": 0, "average_input_rows_per_second": 3,
        "average_processed_rows_per_second": 4, "late_events": 0,
        "dropped_watermark_events": 0, "duration_seconds": 2,
        "analytical_windows_emitted": {"symbol": 5, "sector": 3},
    })
    families = {metric.name: metric for metric in PipelineCollector((tmp_path,)).collect()}
    assert families["financial_pipeline_exporter_up"].samples[0].value == 1
    assert families["financial_pipeline_valid_events_total"].samples[0].value == 9
    assert families["financial_pipeline_analytical_rows_total"].samples[0].value == 8
    assert families["financial_streaming_microbatch_duration_milliseconds"].samples[0].value == 250
    assert families["financial_streaming_state_store_rows"].samples[0].value == 12


def test_monitoring_configuration_and_dashboard_are_parseable() -> None:
    prometheus = yaml.safe_load(Path("monitoring/prometheus/prometheus.yml").read_text())
    jobs = {item["job_name"] for item in prometheus["scrape_configs"]}
    assert {"prometheus", "financial-pipeline", "kafka", "containers"} <= jobs
    datasource = yaml.safe_load(Path("monitoring/grafana/provisioning/datasources/prometheus.yml").read_text())
    assert datasource["datasources"][0]["url"] == "http://prometheus:9090"
    dashboard = json.loads(Path("monitoring/grafana/dashboards/financial-pipeline-observability.json").read_text())
    assert dashboard["uid"] == "labgdd-financial-ops"
    assert len(dashboard["panels"]) >= 10


def test_compose_declares_observability_services() -> None:
    compose = yaml.safe_load(Path("docker-compose.yml").read_text())
    assert {"pipeline-exporter", "kafka-exporter", "cadvisor", "prometheus", "grafana"} <= set(compose["services"])
