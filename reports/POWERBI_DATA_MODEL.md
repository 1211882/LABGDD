# Power BI Data Model

## Refresh

From the repository root:

```powershell
python -m src.powerbi.export --force
```

The command reads frozen artifacts only, validates relationships and keys, writes through a temporary directory, and atomically refreshes `data/powerbi/manifest.json` plus the files below. It performs no ingestion or model training.

## Tables

| Table | Format | Business key | Expected rows | Source |
|---|---|---|---:|---|
| dim_date | CSV | date_key | 366 | ML observed dates and full calendar span |
| dim_symbol | CSV | symbol | 30 | config/config.yaml |
| dim_sector | CSV | sector | 6 | config/config.yaml |
| dim_model | CSV | model_id | 12 | frozen experiment model lists |
| dim_horizon | CSV | horizon_minutes | 5 | 1/5/15/30/60 minutes |
| dim_dataset | CSV | dataset_version | 3 | dataset and experiment manifests |
| fact_financial_analytics | Parquet | mode, dataset, duration, symbol, timestamp | 161,840 | Batch v1 symbol windows |
| fact_sector_analytics | Parquet | mode, dataset, duration, sector, timestamp | 40,316 | Batch v1 pooled sector windows |
| fact_classification_predictions | Parquet | model, horizon, symbol, timestamp | 2,957,096 | frozen Phase 10 predictions |
| fact_classification_metrics | CSV | model, horizon, population | 36 | frozen Phase 10 full/common metrics |
| fact_classification_financial | CSV | model, horizon, evaluation type | 36 | frozen Phase 10 diagnostics |
| fact_confusion_matrix | CSV | model, horizon, population, actual, predicted | 324 | frozen Phase 10 matrices |
| fact_regression_predictions | Parquet | model, horizon, symbol, timestamp | 2,957,096 | frozen Phase 11 predictions |
| fact_regression_metrics | CSV | model, horizon, population | 36 | frozen Phase 11 full/common metrics |
| fact_regression_financial | CSV | model, horizon, evaluation type | 36 | frozen Phase 11 diagnostics |
| fact_model_comparison | CSV | task, model, horizon, population | 72 | task-specific metric projection |
| fact_equivalence | CSV | window_duration | 5 | Phase 7 frozen report |
| fact_performance | CSV | architecture, workload, run | 24 | Phase 8 summary |

Exact schemas, row counts, paths, and SHA-256 checksums are stored in `data/powerbi/manifest.json`.

## Relationships

Create single-direction one-to-many relationships from dimensions to facts:

| Dimension column | Fact columns |
|---|---|
| dim_date[date_key] | financial and prediction facts `[date_key]` |
| dim_symbol[symbol] | symbol financial and prediction facts `[symbol]` |
| dim_sector[sector] | financial, sector, and prediction facts `[sector]` |
| dim_model[model_id] | all ML prediction, metric, financial, comparison, and confusion facts `[model_id]` |
| dim_horizon[horizon_minutes] | analytical `[window_duration]` and ML `[horizon]` columns |
| dim_dataset[dataset_version] | financial and prediction facts `[dataset_version]` |

Use `model_id`, not `model_name`, because LSTM and GRU exist in both tasks. Do not relate fact tables directly or enable bidirectional filtering. `fact_equivalence` and `fact_performance` may use `dim_horizon` only where their duration field applies.

## Power BI Import

1. In Power BI Desktop, use **Get data > Parquet** for the four large Parquet facts.
2. Use **Get data > Text/CSV** for dimensions and aggregate facts.
3. Confirm UTC timestamps, whole-number keys/counts, decimal metrics, and Date columns.
4. Create the relationships above and mark `dim_date` as the date table using `date`.
5. Hide technical duplicate display fields such as fact-level `model_name` after relationships work.
6. Add the measures from `reports/POWERBI_MEASURES.md`.
7. Build pages according to `reports/POWERBI_DASHBOARD_SPEC.md`.
8. Save locally as `LABGDD_Analytics.pbix`.

## Refresh Caveat

The Parquet connector points to concrete files under `data/powerbi`. Refresh the export before Power BI refresh. Grafana remains responsible for operational monitoring; Power BI presents frozen financial, ML, correctness, and performance evidence.
