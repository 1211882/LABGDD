from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

CLASS_ROOT = Path("models/classification/phase10_classification_final_v1")
REG_ROOT = Path("models/regression/phase11_regression_final_v1")
CLASS_NAMES = ("DOWN", "NEUTRAL", "UP")
WINDOWS = (1, 5, 15, 30, 60)
ML_HORIZONS = (5, 15, 30)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _model_id(task: str, name: str) -> str:
    return f"{task.lower()}__{name.lower().replace(' ', '_')}"


def build_dimensions(config_path: Path, ml_manifest_path: Path, dataset_manifest_path: Path, dates: pd.Series) -> dict[str, pd.DataFrame]:
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    ml = json.loads(ml_manifest_path.read_text(encoding="utf-8"))
    dataset = json.loads(dataset_manifest_path.read_text(encoding="utf-8"))
    symbols = pd.DataFrame([{"symbol": symbol, "sector": values["sector"], "company_name": pd.NA, "dataset_version": "v1;ml_dataset_v1"} for symbol, values in config["symbols"].items()])
    sectors = pd.DataFrame({"sector": list(config["symbols"][symbol]["sector"] for symbol in config["symbols"])}).drop_duplicates().sort_values("sector").reset_index(drop=True)
    model_rows = []
    for task, names in {
        "Classification": ["Majority Baseline", "Previous Direction Baseline", "Logistic Regression", "Random Forest", "LSTM", "GRU"],
        "Regression": ["Zero Return Baseline", "Train Mean Baseline", "Linear Regression", "Random Forest Regressor", "LSTM", "GRU"],
    }.items():
        for name in names:
            family = "Baseline" if "Baseline" in name else ("Deep Learning" if name in {"LSTM", "GRU"} else "Classical ML")
            model_rows.append({"model_id": _model_id(task, name), "model_name": name, "model_family": family, "task": task})
    models = pd.DataFrame(model_rows)
    horizons = pd.DataFrame({"horizon_minutes": WINDOWS, "horizon_label": [f"{value}m" for value in WINDOWS], "ml_horizon": [value in ML_HORIZONS for value in WINDOWS]})
    datasets = pd.DataFrame([
        {"dataset_version": dataset["dataset_version"], "purpose": "historical financial analytics", "start_date": dataset["start_date"], "end_date": dataset["end_date"], "feed": dataset["feed"], "session_policy": "source bars retained; no regular-session filter", "symbol_count": dataset["number_of_symbols"], "sector_count": dataset["number_of_sectors"], "row_count": dataset["row_count"]},
        {"dataset_version": ml["dataset_version"], "purpose": ml["purpose"], "start_date": ml["requested_start_date"], "end_date": ml["requested_end_date"], "feed": ml["feed"], "session_policy": ml["session"], "symbol_count": ml["symbol_count"], "sector_count": ml["sector_count"], "row_count": ml["row_count"]},
        {"dataset_version": "phase7_controlled_v1", "purpose": "Batch/Streaming numerical equivalence", "start_date": None, "end_date": None, "feed": "controlled synthetic OHLCV", "session_policy": "controlled validation", "symbol_count": 4, "sector_count": 3, "row_count": 392},
    ])
    normalized = pd.to_datetime(dates, utc=True).dt.date
    calendar = pd.date_range(min(normalized), max(normalized), freq="D")
    trading = set(normalized)
    date = pd.DataFrame({"date": calendar.date})
    date["date_key"] = calendar.strftime("%Y%m%d").astype(int)
    date["year"] = calendar.year; date["quarter"] = calendar.quarter; date["month"] = calendar.month
    date["month_name"] = calendar.strftime("%B"); date["week"] = calendar.isocalendar().week.to_numpy(dtype=int)
    date["day"] = calendar.day; date["day_of_week"] = calendar.strftime("%A"); date["is_trading_day"] = date["date"].isin(trading)
    return {"dim_date": date, "dim_symbol": symbols, "dim_sector": sectors, "dim_model": models, "dim_horizon": horizons, "dim_dataset": datasets}


def build_financial_facts(batch_root: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    symbol_frames, sector_frames = [], []
    for minutes in WINDOWS:
        label = f"{minutes}_minute" if minutes == 1 else f"{minutes}_minutes"
        symbol_frames.append(pd.read_parquet(batch_root / "symbol" / f"duration={label}"))
        sector_frames.append(pd.read_parquet(batch_root / "sector" / f"duration={label}"))
    symbol = pd.concat(symbol_frames, ignore_index=True).rename(columns={"window_start": "timestamp", "trading_date": "date"})
    sector = pd.concat(sector_frames, ignore_index=True).rename(columns={"window_start": "timestamp", "trading_date": "date"})
    for frame in (symbol, sector):
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        frame["date"] = pd.to_datetime(frame["date"]).dt.date
        frame["date_key"] = pd.to_datetime(frame["date"]).dt.strftime("%Y%m%d").astype(int)
        frame["window_duration"] = frame["window_duration"].str.extract(r"(\d+)")[0].astype(int)
        frame["processing_mode"] = "BATCH"
    symbol_columns = ["timestamp", "date", "date_key", "symbol", "sector", "window_duration", "average_price", "minimum_price", "maximum_price", "first_price", "last_price", "total_volume", "event_count", "expected_event_count", "coverage_ratio", "window_return", "volatility", "dataset_version", "processing_mode"]
    sector_columns = ["timestamp", "date", "date_key", "sector", "window_duration", "average_price", "minimum_price", "maximum_price", "total_volume", "event_count", "expected_event_count", "coverage_ratio", "volatility", "symbol_count", "active_symbols", "dataset_version", "processing_mode"]
    return symbol[symbol_columns], sector[sector_columns]


def build_classification_metrics(root: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    frames = []
    for filename, population in (("classification_metrics_full.csv", "FULL"), ("classification_metrics_common.csv", "COMMON")):
        frame = pd.read_csv(root / "metrics" / filename)
        rows = []
        for _, item in frame.iterrows():
            per_class = json.loads(item.per_class_json)
            row = {"model_name": item.model, "model_id": _model_id("Classification", item.model), "horizon": item.horizon, "evaluation_population": population,
                   "accuracy": item.accuracy, "balanced_accuracy": item.balanced_accuracy, "macro_f1": item.macro_f1, "weighted_f1": item.weighted_f1,
                   "log_loss": item.log_loss, "brier_score": item.multiclass_brier_score, "test_rows": item.test_rows}
            for name in CLASS_NAMES:
                for metric in ("precision", "recall", "f1"):
                    row[f"{metric}_{name.lower()}"] = per_class[name][metric]
            rows.append(row)
        frames.append(pd.DataFrame(rows))
    metrics = pd.concat(frames, ignore_index=True)
    financial = pd.read_csv(root / "metrics" / "financial_evaluation.csv").rename(columns={"model": "model_name", "signals": "signal_count", "transaction_cost": "transaction_cost_rate"})
    financial["model_id"] = financial.model_name.map(lambda value: _model_id("Classification", value))
    financial["evaluation_type"] = financial.evaluation_type.map({"DIAGNOSTIC_SIGNAL_EVALUATION": "DIAGNOSTIC", "NON_OVERLAPPING_SANITY_CHECK": "NON_OVERLAPPING"})
    financial["sharpe_ratio"] = pd.NA; financial["transaction_cost_bps"] = financial.transaction_cost_rate * 10000
    financial = financial[["model_id", "model_name", "horizon", "evaluation_type", "signal_count", "directional_accuracy", "cumulative_return", "sharpe_ratio", "maximum_drawdown", "turnover", "transaction_cost_bps"]]
    confusion = []
    for filename, population in (("classification_metrics_full.csv", "FULL"), ("classification_metrics_common.csv", "COMMON")):
        for _, item in pd.read_csv(root / "metrics" / filename).iterrows():
            matrix = json.loads(item.confusion_matrix_json)
            for ai, actual in enumerate(CLASS_NAMES):
                for pi, predicted in enumerate(CLASS_NAMES):
                    confusion.append({"model_id": _model_id("Classification", item.model), "model_name": item.model, "horizon": item.horizon, "evaluation_population": population, "actual_class": actual, "predicted_class": predicted, "count": matrix[ai][pi]})
    return metrics, financial, pd.DataFrame(confusion)


def build_regression_metrics(root: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    frames = []
    for filename, population in (("regression_metrics_full.csv", "FULL"), ("regression_metrics_common.csv", "COMMON")):
        frame = pd.read_csv(root / "metrics" / filename).rename(columns={"model": "model_name"})
        frame["model_id"] = frame.model_name.map(lambda value: _model_id("Regression", value)); frame["evaluation_population"] = population
        frames.append(frame)
    columns = ["model_id", "model_name", "horizon", "evaluation_population", "mae", "mse", "rmse", "r2", "median_absolute_error", "pearson_correlation", "spearman_correlation", "directional_accuracy", "test_rows"]
    metrics = pd.concat(frames, ignore_index=True)[columns]
    financial = pd.read_csv(root / "metrics" / "regression_financial_evaluation.csv").rename(columns={"model": "model_name", "signals": "signal_count", "sharpe_signal_level": "sharpe_ratio", "transaction_cost": "transaction_cost_rate"})
    financial["model_id"] = financial.model_name.map(lambda value: _model_id("Regression", value))
    financial["evaluation_type"] = financial.evaluation_type.map({"DIAGNOSTIC_SIGNAL_EVALUATION": "DIAGNOSTIC", "NON_OVERLAPPING_SANITY_CHECK": "NON_OVERLAPPING"})
    financial["transaction_cost_bps"] = financial.transaction_cost_rate * 10000
    columns = ["model_id", "model_name", "horizon", "evaluation_type", "signal_count", "directional_accuracy", "cumulative_return", "sharpe_ratio", "maximum_drawdown", "turnover", "transaction_cost_bps"]
    return metrics, financial[columns]


def build_equivalence(path: Path) -> pd.DataFrame:
    report = json.loads(path.read_text(encoding="utf-8")); grouped = {}
    for item in report["comparison"]:
        minutes = int(item["window_duration"].split()[0]); grouped.setdefault(minutes, {})[item["level"]] = item
    rows = []
    for minutes, levels in sorted(grouped.items()):
        symbol, sector = levels["symbol"], levels["sector"]
        rows.append({"window_duration": minutes, "symbol_rows_compared": symbol["batch_rows"], "sector_rows_compared": sector["batch_rows"],
                     "mismatches": symbol["mismatched_rows"] + sector["mismatched_rows"], "missing_batch": 0, "missing_streaming": 0,
                     "max_absolute_difference": max([*symbol["maximum_absolute_difference"].values(), *sector["maximum_absolute_difference"].values()]),
                     "absolute_tolerance": report["float_absolute_tolerance"], "relative_tolerance": report["float_relative_tolerance"], "status": report["status"]})
    return pd.DataFrame(rows)


def build_performance(path: Path) -> pd.DataFrame:
    source = pd.read_csv(path)
    result = pd.DataFrame({"architecture": source.architecture.str.upper(), "workload_rows": source.workload_rows, "run": source.run_id,
                           "duration_seconds": source.total_duration_seconds,
                           "throughput_rows_per_sec": source.throughput_rows_per_second.fillna(source.spark_throughput_events_per_second),
                           "latency_ms": source.mean_processing_latency_ms, "cpu_mean": source.cpu_mean_percent, "cpu_peak": source.cpu_peak_percent,
                           "memory_peak_mb": source.memory_peak_mb, "status": source.status})
    return result


def build_model_comparison(class_metrics: pd.DataFrame, regression_metrics: pd.DataFrame, class_financial: pd.DataFrame, regression_financial: pd.DataFrame) -> pd.DataFrame:
    rows = []
    class_returns = class_financial.query("evaluation_type == 'DIAGNOSTIC'").set_index(["model_id", "horizon"])["cumulative_return"]
    reg_returns = regression_financial.query("evaluation_type == 'DIAGNOSTIC'").set_index(["model_id", "horizon"])["cumulative_return"]
    for _, item in class_metrics.iterrows():
        rows.append({"task": "Classification", "model_id": item.model_id, "model_name": item.model_name, "horizon": item.horizon, "evaluation_population": item.evaluation_population,
                     "primary_metric_name": "Macro F1", "primary_metric_value": item.macro_f1, "directional_accuracy": item.accuracy,
                     "financial_return": class_returns.get((item.model_id, item.horizon)), "complexity_level": "Deep Learning" if item.model_name in {"LSTM", "GRU"} else "Baseline" if "Baseline" in item.model_name else "Classical ML",
                     "notes": "Primary directional task; compare Macro F1 only within classification."})
    for _, item in regression_metrics.iterrows():
        rows.append({"task": "Regression", "model_id": item.model_id, "model_name": item.model_name, "horizon": item.horizon, "evaluation_population": item.evaluation_population,
                     "primary_metric_name": "MAE", "primary_metric_value": item.mae, "directional_accuracy": item.directional_accuracy,
                     "financial_return": reg_returns.get((item.model_id, item.horizon)), "complexity_level": "Deep Learning" if item.model_name in {"LSTM", "GRU"} else "Baseline" if "Baseline" in item.model_name else "Classical ML",
                     "notes": "Secondary magnitude task; MAE is not comparable numerically with Macro F1."})
    return pd.DataFrame(rows)


def _write_prediction_fact(source_root: Path, destination: Path, task: str) -> int:
    writer = None; total = 0
    try:
        for source in sorted((source_root / "predictions").glob("*.parquet")):
            frame = pd.read_parquet(source)
            frame["date"] = pd.to_datetime(frame.timestamp, utc=True).dt.date
            frame["date_key"] = pd.to_datetime(frame.timestamp, utc=True).dt.strftime("%Y%m%d").astype(int)
            frame["model_id"] = frame.model_name.map(lambda value: _model_id(task, value))
            if task == "Classification":
                for column in ("probability_down", "probability_neutral", "probability_up", "max_probability", "future_return"):
                    frame[column] = frame[column].astype("float64")
                columns = ["timestamp", "date", "date_key", "symbol", "sector", "horizon", "model_id", "model_name", "actual_class", "predicted_class", "probability_down", "probability_neutral", "probability_up", "max_probability", "future_return", "split", "dataset_version"]
            else:
                for column in ("actual_return", "predicted_return", "prediction_error", "absolute_error", "squared_error"):
                    frame[column] = frame[column].astype("float64")
                columns = ["timestamp", "date", "date_key", "symbol", "sector", "horizon", "model_id", "model_name", "actual_return", "predicted_return", "prediction_error", "absolute_error", "squared_error", "actual_class_from_5bps", "predicted_class_from_5bps", "split", "dataset_version"]
            table = pa.Table.from_pandas(frame[columns], preserve_index=False)
            if writer is None: writer = pq.ParquetWriter(destination, table.schema, compression="snappy")
            writer.write_table(table); total += len(frame)
    finally:
        if writer is not None: writer.close()
    return total


def _validate(tables: dict[str, pd.DataFrame], output: Path, prediction_counts: dict[str, int]) -> dict:
    unique_keys = {"dim_date": ["date_key"], "dim_symbol": ["symbol"], "dim_sector": ["sector"], "dim_model": ["model_id"], "dim_horizon": ["horizon_minutes"], "dim_dataset": ["dataset_version"],
                   "fact_financial_analytics": ["processing_mode", "dataset_version", "window_duration", "symbol", "timestamp"], "fact_sector_analytics": ["processing_mode", "dataset_version", "window_duration", "sector", "timestamp"],
                   "fact_classification_metrics": ["model_id", "horizon", "evaluation_population"], "fact_regression_metrics": ["model_id", "horizon", "evaluation_population"],
                   "fact_classification_financial": ["model_id", "horizon", "evaluation_type"], "fact_regression_financial": ["model_id", "horizon", "evaluation_type"],
                   "fact_confusion_matrix": ["model_id", "horizon", "evaluation_population", "actual_class", "predicted_class"]}
    for name, keys in unique_keys.items():
        if tables[name].duplicated(keys).any(): raise ValueError(f"Duplicate business key in {name}: {keys}")
    symbols, sectors, models = set(tables["dim_symbol"].symbol), set(tables["dim_sector"].sector), set(tables["dim_model"].model_id)
    if not set(tables["fact_financial_analytics"].symbol) <= symbols: raise ValueError("Invalid financial symbol FK")
    for name, frame in tables.items():
        if "sector" in frame and not set(frame.sector.dropna()) <= sectors: raise ValueError(f"Invalid sector FK in {name}")
        if "model_id" in frame and not set(frame.model_id) <= models: raise ValueError(f"Invalid model FK in {name}")
    for task, expected in (("classification", 2957096), ("regression", 2957096)):
        if prediction_counts[task] != expected: raise ValueError(f"Unexpected {task} prediction rows: {prediction_counts[task]}")
    return {"status": "PASS", "duplicate_business_keys": 0, "invalid_foreign_keys": 0, "prediction_rows": prediction_counts, "table_count": len(tables) + 2}


def export(output: Path, force: bool = False) -> Path:
    if output.exists() and not force: raise FileExistsError(f"Output exists; use --force to refresh: {output}")
    temporary = output.parent / f".{output.name}.building"
    if temporary.exists(): shutil.rmtree(temporary)
    for name in ("dimensions", "financial", "classification", "regression", "performance", "metadata"): (temporary / name).mkdir(parents=True, exist_ok=True)
    financial, sector = build_financial_facts(Path("data/analytics/batch/v1"))
    ml_dates = pd.read_parquet(Path("datasets/ml/ml_dataset_v1/processed/market_bars.parquet"), columns=["timestamp"])["timestamp"]
    date_sources = pd.concat([pd.Series(financial.timestamp), ml_dates], ignore_index=True)
    dimensions = build_dimensions(Path("config/config.yaml"), Path("datasets/ml/ml_dataset_v1/manifest.json"), Path("datasets/manifests/dataset_v1.json"), date_sources)
    class_metrics, class_financial, confusion = build_classification_metrics(CLASS_ROOT)
    reg_metrics, reg_financial = build_regression_metrics(REG_ROOT)
    equivalence = build_equivalence(Path("benchmarks/phase7_controlled_v1_equivalence.json")); performance = build_performance(Path("benchmarks/phase8_performance_summary.csv"))
    comparison = build_model_comparison(class_metrics, reg_metrics, class_financial, reg_financial)
    tables = {**dimensions, "fact_financial_analytics": financial, "fact_sector_analytics": sector, "fact_classification_metrics": class_metrics,
              "fact_classification_financial": class_financial, "fact_confusion_matrix": confusion, "fact_regression_metrics": reg_metrics,
              "fact_regression_financial": reg_financial, "fact_model_comparison": comparison, "fact_equivalence": equivalence, "fact_performance": performance}
    locations = {}
    for name, frame in tables.items():
        folder = "dimensions" if name.startswith("dim_") else "financial" if name in {"fact_financial_analytics", "fact_sector_analytics"} else "classification" if name.startswith("fact_classification") or name == "fact_confusion_matrix" else "regression" if name.startswith("fact_regression") or name == "fact_model_comparison" else "performance"
        extension = "parquet" if name in {"fact_financial_analytics", "fact_sector_analytics"} else "csv"
        path = temporary / folder / f"{name}.{extension}"
        frame.to_parquet(path, index=False) if extension == "parquet" else frame.to_csv(path, index=False)
        locations[name] = path
    prediction_counts = {
        "classification": _write_prediction_fact(CLASS_ROOT, temporary / "classification" / "fact_classification_predictions.parquet", "Classification"),
        "regression": _write_prediction_fact(REG_ROOT, temporary / "regression" / "fact_regression_predictions.parquet", "Regression"),
    }
    validation = _validate(tables, temporary, prediction_counts)
    schemas, row_counts = {}, {name: len(frame) for name, frame in tables.items()}
    row_counts.update({"fact_classification_predictions": prediction_counts["classification"], "fact_regression_predictions": prediction_counts["regression"]})
    for path in temporary.rglob("*.parquet"):
        schemas[path.stem] = {field.name: str(field.type) for field in pq.read_schema(path)}
    for path in temporary.rglob("*.csv"):
        schemas[path.stem] = {column: str(dtype) for column, dtype in pd.read_csv(path, nrows=100).dtypes.items()}
    manifest = {"generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), "dataset_versions": ["v1", "ml_dataset_v1", "phase7_controlled_v1"],
                "classification_experiment": "phase10_classification_final_v1", "regression_experiment": "phase11_regression_final_v1",
                "phase7_report": "benchmarks/phase7_controlled_v1_equivalence.json", "phase8_report": "benchmarks/phase8_performance_summary.csv",
                "row_counts": row_counts, "schemas": schemas, "validation": validation}
    manifest_path = temporary / "manifest.json"; manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    manifest["checksums"] = {str(path.relative_to(temporary)).replace("\\", "/"): _sha256(path) for path in temporary.rglob("*") if path.is_file() and path != manifest_path}
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    if output.exists(): shutil.rmtree(output)
    temporary.rename(output)
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description="Export frozen project evidence for Power BI without retraining or ingestion.")
    parser.add_argument("--output", type=Path, default=Path("data/powerbi")); parser.add_argument("--force", action="store_true")
    args = parser.parse_args(); print(export(args.output, args.force)); return 0


if __name__ == "__main__": raise SystemExit(main())
