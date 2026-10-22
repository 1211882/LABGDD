# Phase 9 Observability

Prometheus is the operational metrics collection and storage layer. Grafana is the
operational visualization layer; it is deliberately separate from the planned Power BI
financial and model-results layer.

## Start and inspect

```powershell
docker compose up -d --build kafka spark pipeline-exporter kafka-exporter cadvisor prometheus grafana
docker compose ps
```

- Pipeline exporter: <http://localhost:8000/metrics>
- Kafka exporter: <http://localhost:9308/metrics>
- cAdvisor: <http://localhost:8080/metrics>
- Prometheus: <http://localhost:9090>
- Grafana: <http://localhost:3000>

Grafana provisions the Prometheus datasource and the **Financial Pipeline Operations**
dashboard automatically. Local defaults are `admin` / `admin`; set
`GRAFANA_ADMIN_USER` and `GRAFANA_ADMIN_PASSWORD` before startup outside local
development. No Alpaca credentials are mounted in any monitoring container.

## Metric sources

`pipeline-exporter` reads the latest persisted Batch, Structured Streaming, and Phase 8
metric JSON files from read-only repository mounts. It exposes query state, failures,
input/processing rates, micro-batch duration, watermark, state rows, data-quality
counts, analytical rows, controlled-run latency/lag, and Batch rows/duration/throughput.

`kafka-exporter` supplies broker, topic, partition, offset, and consumer-group lag
metrics. Producer and consumer throughput are derived from Kafka offset rates when
traffic is present. `cAdvisor` supplies available container CPU, memory, and health
series. Prometheus scrapes all exporters every five seconds.

The dashboard groups panels into system health, streaming activity, and data quality.
Its JSON source is version controlled at
`monitoring/grafana/dashboards/financial-pipeline-observability.json`.

## Runtime evidence

The controlled Phase 9 validation used the immutable Phase 7 workload on an isolated
Kafka topic with fresh checkpoints and `startingOffsets=earliest`. The final Spark
metrics are in `benchmarks/phase9_observability_streaming_v2.json`; the concise endpoint
and scrape validation is in `reports/phase9_runtime_validation.json`.

Spark metrics are exported from persisted query-progress snapshots. They update when
the processor writes its initial running state and again on completion; this local
design does not expose the Spark driver's native metrics servlet. cAdvisor visibility
depends on the Docker Desktop/WSL2 integration, although CPU and memory series were
available in the verified environment.
