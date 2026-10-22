# Power BI Readiness Audit

## Scope

The audit covers Batch and Streaming analytics, Phase 7 equivalence, Phase 8 performance, frozen Phase 10 classification, frozen Phase 11 regression, symbol/sector configuration, dataset manifests, prediction schemas, metrics, financial diagnostics, reports, and figures. Phase 10 and Phase 11 artifacts are read-only sources.

## Findings

### Critical

None. Required scientific evidence and source artifacts are present.

### High

- **Model-name ambiguity:** `LSTM` and `GRU` occur in both tasks, so `model_name` alone cannot be a dimension key. Resolved in the export with task-scoped `model_id` values while preserving display names.
- **Prediction numeric types:** Phase 10 recurrent probabilities are float32 and classical/baseline probabilities are float64. Regression predictions have a similar model-dependent representation. Resolved by exporting prediction measures as float64 without changing values or source files.

### Medium

- **Evaluation-scope naming:** frozen files use `full_model_test_set` and `common_comparable_test_set`. Exported labels are consistently `FULL` and `COMMON`.
- **Timestamp metadata:** Batch window timestamps are timezone-naive although the Phase 7/window contract specifies UTC clock-aligned windows. Export timestamps are explicitly interpreted as UTC.
- **Different dataset roles:** Batch financial analytics use short `v1` (113,988 bars, 1-18 September 2026); prediction evidence uses one-year `ml_dataset_v1`. Separate dataset-version fields and `dim_dataset` prevent accidental blending.
- **Sector and symbol analytical grain:** pooled sector analytics cannot share the symbol-level business key. They are exported as `fact_sector_analytics`, separate from `fact_financial_analytics`.

### Low

- Batch CPU statistics are unavailable in Phase 8 and remain null.
- Classification financial evidence does not persist Sharpe ratio; the Power BI field remains null rather than being reconstructed with a different method.
- One-minute sample volatility is null because a single bar cannot produce sample standard deviation.
- Company names are not maintained by the project and remain blank.

## Validation Result

The export enforces unique dimension/business keys, valid symbol/sector/model/horizon/date references, expected prediction row counts, canonical class order, and source-preserving metric transformations. Phase 7 has zero mismatches and missing rows. No frozen scientific result is recalculated to improve presentation.
