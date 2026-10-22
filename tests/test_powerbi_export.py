import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from src.powerbi.export import (
    CLASS_ROOT,
    REG_ROOT,
    _validate,
    build_classification_metrics,
    build_dimensions,
    build_equivalence,
    build_financial_facts,
    build_model_comparison,
    build_performance,
    build_regression_metrics,
)


@pytest.fixture(scope="module")
def export_tables():
    financial, sector = build_financial_facts(Path("data/analytics/batch/v1"))
    dimensions = build_dimensions(Path("config/config.yaml"), Path("datasets/ml/ml_dataset_v1/manifest.json"), Path("datasets/manifests/dataset_v1.json"), financial.timestamp)
    class_metrics, class_financial, confusion = build_classification_metrics(CLASS_ROOT)
    reg_metrics, reg_financial = build_regression_metrics(REG_ROOT)
    comparison = build_model_comparison(class_metrics, reg_metrics, class_financial, reg_financial)
    return {**dimensions, "fact_financial_analytics": financial, "fact_sector_analytics": sector,
            "fact_classification_metrics": class_metrics, "fact_classification_financial": class_financial,
            "fact_confusion_matrix": confusion, "fact_regression_metrics": reg_metrics,
            "fact_regression_financial": reg_financial, "fact_model_comparison": comparison,
            "fact_equivalence": build_equivalence(Path("benchmarks/phase7_controlled_v1_equivalence.json")),
            "fact_performance": build_performance(Path("benchmarks/phase8_performance_summary.csv"))}


def test_dimension_generation(export_tables) -> None:
    assert len(export_tables["dim_symbol"]) == 30
    assert len(export_tables["dim_sector"]) == 6
    assert len(export_tables["dim_model"]) == 12
    assert not export_tables["dim_model"].model_id.duplicated().any()


def test_financial_fact_schema_and_business_key(export_tables) -> None:
    frame = export_tables["fact_financial_analytics"]
    assert {"timestamp", "date_key", "symbol", "sector", "window_duration", "coverage_ratio", "processing_mode"} <= set(frame)
    assert len(frame) == 161840
    assert not frame.duplicated(["processing_mode", "dataset_version", "window_duration", "symbol", "timestamp"]).any()


def test_classification_export_matches_frozen_metric(export_tables) -> None:
    exported = export_tables["fact_classification_metrics"].query("model_name == 'Random Forest' and horizon == 5 and evaluation_population == 'FULL'").iloc[0]
    source = pd.read_csv(CLASS_ROOT / "metrics" / "classification_metrics_full.csv").query("model == 'Random Forest' and horizon == 5").iloc[0]
    assert exported.macro_f1 == source.macro_f1
    assert exported.test_rows == source.test_rows


def test_regression_export_matches_frozen_metric(export_tables) -> None:
    exported = export_tables["fact_regression_metrics"].query("model_name == 'Zero Return Baseline' and horizon == 5 and evaluation_population == 'COMMON'").iloc[0]
    source = pd.read_csv(REG_ROOT / "metrics" / "regression_metrics_common.csv").query("model == 'Zero Return Baseline' and horizon == 5").iloc[0]
    assert exported.mae == source.mae
    assert exported.r2 == source.r2


def test_performance_export_preserves_unavailable_batch_cpu(export_tables) -> None:
    frame = export_tables["fact_performance"]
    assert len(frame) == 24
    assert frame.query("architecture == 'BATCH'").cpu_mean.isna().all()
    assert frame.query("architecture == 'STREAMING'").latency_ms.notna().all()


def test_equivalence_export_preserves_zero_mismatches(export_tables) -> None:
    frame = export_tables["fact_equivalence"]
    assert len(frame) == 5
    assert frame.mismatches.sum() == 0
    assert frame.max_absolute_difference.max() < frame.absolute_tolerance.min()


def test_confusion_matrix_generation(export_tables) -> None:
    frame = export_tables["fact_confusion_matrix"]
    assert len(frame) == 324
    grouped = frame.groupby(["model_id", "horizon", "evaluation_population"])["count"].sum()
    metrics = export_tables["fact_classification_metrics"].set_index(["model_id", "horizon", "evaluation_population"])["test_rows"]
    pd.testing.assert_series_equal(grouped.sort_index(), metrics.sort_index(), check_names=False)


def test_model_comparison_keeps_task_specific_metrics(export_tables) -> None:
    frame = export_tables["fact_model_comparison"]
    assert set(frame.query("task == 'Classification'").primary_metric_name) == {"Macro F1"}
    assert set(frame.query("task == 'Regression'").primary_metric_name) == {"MAE"}


def test_foreign_key_integrity(export_tables) -> None:
    result = _validate(export_tables, Path("."), {"classification": 2957096, "regression": 2957096})
    assert result["invalid_foreign_keys"] == 0


def test_duplicate_business_key_is_rejected(export_tables) -> None:
    altered = dict(export_tables)
    altered["dim_sector"] = pd.concat([export_tables["dim_sector"], export_tables["dim_sector"].iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="Duplicate business key"):
        _validate(altered, Path("."), {"classification": 2957096, "regression": 2957096})


def test_manifest_and_checksum_validation() -> None:
    root = Path("data/powerbi")
    if not root.exists(): pytest.skip("Power BI export is not generated")
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["validation"]["status"] == "PASS"
    for name, expected in manifest["checksums"].items():
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == expected
