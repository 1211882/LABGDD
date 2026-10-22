from __future__ import annotations

import argparse
import hashlib
import json
import platform
import shutil
from datetime import datetime, timezone
from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sklearn

from src.ml.evaluation import classification_metrics, portfolio_financial_metrics
from src.ml.features import FEATURE_COLUMNS
from src.ml.targets import CLASS_NAMES, CLASS_TO_INDEX, HORIZONS, classify_returns

FINAL_ID = "phase10_classification_final_v1"
SOURCE_ID = "phase10_classification_v1"
CONFIDENCE_BINS = [0.0, .4, .5, .6, 1.000001]
CONFIDENCE_LABELS = ["<=0.40", "0.40-0.50", "0.50-0.60", ">0.60"]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _feature_catalog() -> pd.DataFrame:
    catalog = []
    raw = {"open", "high", "low", "close", "volume"}
    time = {"hour", "minute_of_day", "minutes_since_open"}
    sector = {"sector_return_mean", "relative_to_sector", "sector_return_dispersion"}
    for name in FEATURE_COLUMNS:
        if name in raw:
            category, formula, lookback, warmup, scaling, rationale = "raw", f"source {name}", 1, 0, True, "Market state at the bar close"
        elif name in time:
            category, formula, lookback, warmup, scaling, rationale = "time", "America/New_York bar-clock encoding", 1, 0, True, "Intraday seasonality"
        elif name in sector:
            formulas = {"sector_return_mean": "mean(return_1m) by sector,timestamp", "relative_to_sector": "return_1m-sector_return_mean", "sector_return_dispersion": "sample std(return_1m) by sector,timestamp"}
            category, formula, lookback, warmup, scaling, rationale = "sector", formulas[name], 2 if name.endswith("dispersion") else 1, 1, True, "Contemporaneous cross-sectional context available at bar close"
        elif name.startswith("return_") or name == "log_return":
            horizon = 1 if name in {"return_1m", "log_return"} else int(name.split("_")[1][:-1])
            formula = "log(close_t/close_t-1)" if name == "log_return" else f"close_t/close_t-{horizon}-1"
            category, lookback, warmup, scaling, rationale = "returns", horizon + 1, horizon, True, "Recent direction and magnitude"
        elif name.startswith(("sma_", "ema_")):
            window = int(name.split("_")[1])
            formula = f"{'rolling mean' if name.startswith('sma') else 'backward EWM'}(close,{window})"
            category, lookback, warmup, scaling, rationale = "trend", window, window - 1, True, "Local price trend"
        elif name.startswith("volatility_"):
            window = int(name.split("_")[1])
            category, formula, lookback, warmup, scaling, rationale = "volatility", f"sample std(return_1m,{window})", window + 1, window, True, "Recent realized variability"
        elif name in {"atr_14", "rsi_14"}:
            category = "technical"
            formula = "rolling mean(true range,14)" if name == "atr_14" else "Wilder-style EWM gain/loss RSI(14)"
            lookback, warmup, scaling, rationale = 15, 14, True, "Range risk" if name == "atr_14" else "Directional momentum balance"
        elif name.startswith("macd"):
            formulas = {"macd": "EMA12(close)-EMA26(close)", "macd_signal": "EMA9(MACD)", "macd_histogram": "MACD-MACD_signal"}
            category, formula, lookback, warmup, scaling, rationale = "technical", formulas[name], 34, 33, True, "Trend convergence and divergence"
        elif name.startswith("bollinger"):
            formulas = {"bollinger_upper": "SMA20+2*STD20", "bollinger_lower": "SMA20-2*STD20", "bollinger_bandwidth": "(upper-lower)/SMA20"}
            category, formula, lookback, warmup, scaling, rationale = "technical", formulas[name], 20, 19, True, "Price range relative to recent dispersion"
        elif name == "momentum_5":
            category, formula, lookback, warmup, scaling, rationale = "trend", "close_t-close_t-5", 6, 5, True, "Short-term price momentum"
        else:
            formulas = {"volume_change": "volume_t/volume_t-1-1", "volume_mean_30": "rolling mean(volume,30)", "relative_volume": "volume/volume_mean_30"}
            category, formula = "volume", formulas[name]
            lookback, warmup, scaling, rationale = (30, 29, True, "Trading-activity regime") if name != "volume_change" else (2, 1, True, "Minute activity change")
        catalog.append({"name": name, "category": category, "formula": formula, "lookback_bars": lookback, "warmup_bars": warmup, "leakage_safe": True, "scaling_required": scaling, "rationale": rationale})
    return pd.DataFrame(catalog)


def harden(features_path: Path, source_root: Path, final_root: Path) -> Path:
    if final_root.exists():
        raise FileExistsError(f"Frozen experiment already exists: {final_root}")
    temporary = final_root.parent / f".{final_root.name}.building"
    if temporary.exists():
        shutil.rmtree(temporary)
    for name in ("metrics", "predictions", "models", "scalers", "histories", "reports", "figures"):
        (temporary / name).mkdir(parents=True, exist_ok=True)

    feature_metadata = json.loads((features_path.parent / "metadata.json").read_text(encoding="utf-8"))
    source_manifest = json.loads((source_root / "experiment_manifest.json").read_text(encoding="utf-8"))
    features = pd.read_parquet(features_path)
    _validate_splits(features)
    split_summary = _split_summary(features)
    (temporary / "reports" / "split_validation.json").write_text(json.dumps(split_summary, indent=2) + "\n", encoding="utf-8")
    _feature_catalog().to_csv(temporary / "reports" / "feature_catalog.csv", index=False)
    _write_target_samples(features, temporary / "reports" / "target_validation.json")

    for path in (source_root / "models").glob("*"):
        destination = temporary / ("scalers" if path.name.startswith("scaler_") else "models") / path.name
        shutil.copy2(path, destination)
    shutil.copy2(source_root / "training_history.json", temporary / "histories" / "training_history.json")

    prediction_frames: dict[tuple[int, str], pd.DataFrame] = {}
    for source_path in sorted((source_root / "predictions").glob("*.parquet")):
        frame = pd.read_parquet(source_path)
        _validate_probabilities(frame)
        frame["max_probability"] = frame[["probability_down", "probability_neutral", "probability_up"]].max(axis=1)
        frame["prediction_confidence"] = pd.cut(frame["max_probability"], CONFIDENCE_BINS, labels=CONFIDENCE_LABELS, include_lowest=True)
        model = str(frame["model_name"].iloc[0])
        horizon = int(frame["horizon"].iloc[0])
        prediction_frames[(horizon, model)] = frame

    for horizon in HORIZONS:
        test = features.loc[(features["split"] == "test") & features[f"target_{horizon}m"].notna()].copy()
        predicted = classify_returns(test["return_1m"], .0005).to_numpy()
        probabilities = np.full((len(test), 3), .05)
        for row, label in enumerate(predicted):
            probabilities[row, CLASS_TO_INDEX[str(label)]] = .90
        previous = _prediction_frame(test, predicted, probabilities, horizon)
        prediction_frames[(horizon, "Previous Direction Baseline")] = previous

    full_metrics, confidence, financial = [], [], []
    common_metrics = []
    final_predictions = []
    for horizon in HORIZONS:
        horizon_frames = {model: frame for (value, model), frame in prediction_frames.items() if value == horizon}
        common_keys = None
        for frame in horizon_frames.values():
            keys = pd.MultiIndex.from_frame(frame[["symbol", "timestamp"]])
            common_keys = keys if common_keys is None else common_keys.intersection(keys)
        for model, frame in horizon_frames.items():
            frame = frame.sort_values(["symbol", "timestamp"]).reset_index(drop=True)
            output = temporary / "predictions" / f"{_slug(model)}_{horizon}m.parquet"
            frame.to_parquet(output, index=False)
            final_predictions.append(output)
            full_metrics.append(_metric_row(frame, model, horizon, "full_model_test_set"))
            key = pd.MultiIndex.from_frame(frame[["symbol", "timestamp"]])
            common = frame.loc[key.isin(common_keys)]
            common_metrics.append(_metric_row(common, model, horizon, "common_comparable_test_set"))
            confidence.extend(_confidence_rows(frame, model, horizon))
            for non_overlapping in (False, True):
                financial.append({"model": model, "horizon": horizon, **portfolio_financial_metrics(frame, non_overlapping=non_overlapping)})

    full = pd.DataFrame(full_metrics)
    common = pd.DataFrame(common_metrics)
    full.to_csv(temporary / "metrics" / "classification_metrics_full.csv", index=False)
    common.to_csv(temporary / "metrics" / "classification_metrics_common.csv", index=False)
    pd.DataFrame(confidence).to_csv(temporary / "metrics" / "confidence_calibration.csv", index=False)
    pd.DataFrame(financial).to_csv(temporary / "metrics" / "financial_evaluation.csv", index=False)
    rankings = full.sort_values(["horizon", "macro_f1", "balanced_accuracy", "accuracy"], ascending=[True, False, False, False]).copy()
    rankings["rank"] = rankings.groupby("horizon").cumcount() + 1
    rankings[["horizon", "rank", "model", "macro_f1", "balanced_accuracy", "accuracy", "test_rows"]].to_csv(temporary / "metrics" / "model_rankings.csv", index=False)
    _group_metrics(prediction_frames).to_csv(temporary / "metrics" / "symbol_sector_metrics.csv", index=False)
    summary = full.merge(pd.DataFrame(financial).query("evaluation_type == 'DIAGNOSTIC_SIGNAL_EVALUATION'")[["model", "horizon", "directional_accuracy"]], on=["model", "horizon"])
    summary.to_csv(temporary / "metrics" / "classification_summary.csv", index=False)
    _figures(full, feature_metadata, prediction_frames, source_root / "training_history.json", temporary / "figures")

    checksums = {str(path.relative_to(temporary)).replace("\\", "/"): _sha256(path) for path in temporary.rglob("*") if path.is_file()}
    manifest = {
        "experiment_id": FINAL_ID, "status": "frozen", "source_experiment": SOURCE_ID,
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "dataset_version": feature_metadata["dataset_version"], "features_version": feature_metadata["features_version"],
        "features_checksum_sha256": feature_metadata["checksum_sha256"], "class_order": list(CLASS_NAMES),
        "probability_order": ["probability_down", "probability_neutral", "probability_up"],
        "threshold_basis_points": 5, "horizons_minutes": list(HORIZONS), "splits": feature_metadata["splits"],
        "models": sorted({model for _, model in prediction_frames}), "random_seed": source_manifest["random_seed"],
        "hyperparameters": {"logistic": {"C": 1.0, "class_weight": "balanced", "max_iter": 150}, "random_forest": {"n_estimators": 60, "max_depth": 16, "min_samples_leaf": 10, "class_weight": "balanced_subsample"}, "deep_learning": source_manifest["deep_learning"]},
        "selection_policy": "Existing predeclared Phase 10 configurations retained; neural best weights selected by validation loss only. Test metrics were recomputed but not used for tuning.",
        "software": {"python": platform.python_version(), "pandas": pd.__version__, "numpy": np.__version__, "scikit_learn": sklearn.__version__},
        "common_subset": "Intersection of symbol+timestamp keys across every model for each horizon.",
        "financial_evaluation": "Diagnostic overlapping signal evaluation plus secondary non-overlapping clock-grid sanity check; both aggregate equal-weight symbol returns by timestamp.",
        "checksums": checksums,
    }
    (temporary / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    temporary.rename(final_root)
    return final_root


def _validate_splits(frame):
    dates = {name: set(group["trading_date"]) for name, group in frame.groupby("split")}
    if dates["train"] & dates["validation"] or dates["train"] & dates["test"] or dates["validation"] & dates["test"]:
        raise ValueError("Temporal split overlap")
    if frame.duplicated(["symbol", "timestamp"]).any():
        raise ValueError("Duplicate feature logical keys")
    if set(FEATURE_COLUMNS) & {column for column in frame if column.startswith("target_") or column.startswith("future_")}:
        raise ValueError("Target leakage in feature columns")


def _split_summary(frame):
    result = {}
    for name, group in frame.groupby("split"):
        result[name] = {"first_timestamp": group["timestamp"].min().isoformat(), "last_timestamp": group["timestamp"].max().isoformat(), "trading_days": int(group["trading_date"].nunique()), "rows": len(group)}
    result["ordered"] = result["train"]["last_timestamp"] < result["validation"]["first_timestamp"] and result["validation"]["last_timestamp"] < result["test"]["first_timestamp"]
    return result


def _write_target_samples(frame, path):
    samples = {}
    for horizon in HORIZONS:
        future = f"future_return_{horizon}m"
        normal = frame.loc[frame[future].notna()].iloc[100]
        missing = frame.loc[frame[future].isna()].iloc[0]
        samples[f"{horizon}m"] = {"normal": {"symbol": normal.symbol, "timestamp": normal.timestamp.isoformat(), "close": normal.close, "future_close": normal[f"future_close_{horizon}m"], "stored_return": normal[future], "recalculated_return": normal[f"future_close_{horizon}m"] / normal.close - 1}, "missing_future": {"symbol": missing.symbol, "timestamp": missing.timestamp.isoformat(), "label_is_null": bool(pd.isna(missing[f"target_{horizon}m"]))}}
    samples["threshold_boundaries"] = {"-5bps": str(classify_returns(pd.Series([-.0005]), .0005).iloc[0]), "+5bps": str(classify_returns(pd.Series([.0005]), .0005).iloc[0])}
    path.write_text(json.dumps(samples, indent=2) + "\n", encoding="utf-8")


def _validate_probabilities(frame):
    columns = ["probability_down", "probability_neutral", "probability_up"]
    if not np.allclose(frame[columns].sum(axis=1), 1, atol=1e-5):
        raise ValueError("Probabilities do not sum to one")
    decoded = np.asarray(CLASS_NAMES)[frame[columns].to_numpy().argmax(axis=1)]
    if not np.array_equal(decoded, frame["predicted_class"].to_numpy()):
        raise ValueError("Probability/class order mismatch")


def _prediction_frame(rows, predicted, probabilities, horizon):
    result = rows[["timestamp", "symbol", "sector", "split", f"future_return_{horizon}m"]].rename(columns={f"future_return_{horizon}m": "future_return"}).copy()
    result["horizon"] = horizon; result["actual_class"] = rows[f"target_{horizon}m"].to_numpy(); result["predicted_class"] = predicted
    for index, name in enumerate(CLASS_NAMES): result[f"probability_{name.lower()}"] = probabilities[:, index]
    result["model_name"] = "Previous Direction Baseline"; result["lookback"] = 1; result["dataset_version"] = "ml_dataset_v1"
    result["max_probability"] = probabilities.max(axis=1); result["prediction_confidence"] = pd.cut(result["max_probability"], CONFIDENCE_BINS, labels=CONFIDENCE_LABELS, include_lowest=True)
    return result


def _metric_row(frame, model, horizon, scope):
    probabilities = frame[["probability_down", "probability_neutral", "probability_up"]].to_numpy()
    metrics = classification_metrics(frame["actual_class"], frame["predicted_class"], probabilities)
    return {"model": model, "horizon": horizon, "scope": scope, "test_rows": len(frame), **{key: value for key, value in metrics.items() if key not in {"per_class", "confusion_matrix"}}, "per_class_json": json.dumps(metrics["per_class"]), "confusion_matrix_json": json.dumps(metrics["confusion_matrix"])}


def _confidence_rows(frame, model, horizon):
    rows = []
    for bucket, group in frame.groupby("prediction_confidence", observed=True):
        metrics = classification_metrics(group["actual_class"], group["predicted_class"])
        rows.append({"model": model, "horizon": horizon, "confidence_bucket": str(bucket), "rows": len(group), "mean_confidence": group["max_probability"].mean(), "accuracy": metrics["accuracy"], "balanced_accuracy": metrics["balanced_accuracy"], "macro_f1": metrics["macro_f1"]})
    return rows


def _group_metrics(frames):
    rows = []
    for (horizon, model), frame in frames.items():
        for level in ("symbol", "sector"):
            for value, group in frame.groupby(level):
                metrics = classification_metrics(group["actual_class"], group["predicted_class"])
                rows.append({"model": model, "horizon": horizon, "level": level, "value": value, "observations": len(group), "macro_f1": metrics["macro_f1"], "balanced_accuracy": metrics["balanced_accuracy"]})
    return pd.DataFrame(rows)


def _figures(metrics, metadata, frames, history_path, root):
    for measure, title in (("macro_f1", "Macro F1"), ("balanced_accuracy", "Balanced Accuracy")):
        pivot = metrics.pivot(index="model", columns="horizon", values=measure)
        ax = pivot.plot(kind="bar", figsize=(10, 5), ylim=(0, max(.5, pivot.max().max() * 1.1)))
        ax.set_ylabel(title); ax.set_title(f"{title} by model and horizon"); ax.grid(axis="y", alpha=.25); plt.tight_layout(); plt.savefig(root / f"{measure}_by_model_horizon.png", dpi=180); plt.close()
    best = frames[(5, "Random Forest")]
    matrix = json.loads(_metric_row(best, "Random Forest", 5, "full")["confusion_matrix_json"])
    fig, ax = plt.subplots(figsize=(5, 4)); image = ax.imshow(matrix, cmap="Blues");
    for i in range(3):
        for j in range(3): ax.text(j, i, matrix[i][j], ha="center", va="center")
    ax.set_xticks(range(3), CLASS_NAMES); ax.set_yticks(range(3), CLASS_NAMES); ax.set_xlabel("Predicted"); ax.set_ylabel("Actual"); ax.set_title("Random Forest 5m confusion matrix"); fig.colorbar(image); plt.tight_layout(); plt.savefig(root / "confusion_matrix_random_forest_5m.png", dpi=180); plt.close()
    distributions = []
    for horizon, split_values in metadata["class_distributions"].items():
        for split, values in split_values.items():
            total = sum(value for key, value in values.items() if key != "MISSING")
            for name in CLASS_NAMES: distributions.append({"horizon": horizon, "split": split, "class": name, "share": values.get(name, 0) / total})
    pd.DataFrame(distributions).pivot(index=["horizon", "split"], columns="class", values="share").plot(kind="bar", stacked=True, figsize=(10, 5)); plt.ylabel("Share"); plt.title("Class distribution by horizon and split"); plt.tight_layout(); plt.savefig(root / "class_distribution.png", dpi=180); plt.close()
    history = json.loads(history_path.read_text(encoding="utf-8"))
    for kind in ("LSTM", "GRU"):
        fig, axes = plt.subplots(1, 2, figsize=(10, 4))
        for key, values in history.items():
            if key.startswith(kind): axes[0].plot(values["loss"], label=key); axes[1].plot(values["val_loss"], label=key)
        axes[0].set_title(f"{kind} train loss"); axes[1].set_title(f"{kind} validation loss")
        for ax in axes: ax.set_xlabel("Epoch"); ax.set_ylabel("Cross-entropy"); ax.legend(); ax.grid(alpha=.25)
        plt.tight_layout(); plt.savefig(root / f"{kind.lower()}_learning_curves.png", dpi=180); plt.close()


def _slug(value): return value.lower().replace(" ", "_")


def main() -> int:
    parser = argparse.ArgumentParser(description="Freeze the canonical Phase 10 classification baseline.")
    parser.add_argument("--features", type=Path, default=Path("datasets/ml/ml_dataset_v1/features_v1/features_targets.parquet"))
    parser.add_argument("--source", type=Path, default=Path("models/classification/phase10_classification_v1"))
    parser.add_argument("--output", type=Path, default=Path(f"models/classification/{FINAL_ID}"))
    args = parser.parse_args(); print(harden(args.features, args.source, args.output)); return 0


if __name__ == "__main__": raise SystemExit(main())
