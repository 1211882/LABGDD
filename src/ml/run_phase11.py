from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import random
import shutil
from datetime import datetime, timezone
from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler

from src.ml.evaluation import classification_metrics
from src.ml.features import FEATURE_COLUMNS, FEATURE_VERSION
from src.ml.regression_evaluation import (
    error_distribution,
    regression_financial_metrics,
    regression_metrics,
    return_magnitude_bucket,
)
from src.ml.sequences import build_sequences
from src.ml.targets import HORIZONS, classify_returns

FINAL_ID = "phase11_regression_final_v1"
SEED = 42
LOOKBACK = 60
SEQUENCE_CAPS = {"train": 120_000, "validation": 30_000, "test": 30_000}
MODELS = ("Zero Return Baseline", "Train Mean Baseline", "Linear Regression", "Random Forest Regressor", "LSTM", "GRU")
THRESHOLD = .0005


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _slug(value: str) -> str:
    return value.lower().replace(" ", "_")


def _set_seeds() -> None:
    os.environ["TF_DETERMINISTIC_OPS"] = "1"
    random.seed(SEED)
    np.random.seed(SEED)


def _deep_model(kind: str, feature_count: int):
    import tensorflow as tf
    layer = tf.keras.layers.LSTM if kind == "LSTM" else tf.keras.layers.GRU
    model = tf.keras.Sequential([
        tf.keras.layers.Input((LOOKBACK, feature_count)), layer(24),
        tf.keras.layers.Dropout(.15), tf.keras.layers.Dense(1),
    ])
    model.compile(optimizer=tf.keras.optimizers.Adam(.001), loss="mse", metrics=["mae"])
    return model


def _prediction_frame(rows: pd.DataFrame, predicted, horizon: int, model: str, dataset_version: str) -> pd.DataFrame:
    target = f"future_return_{horizon}m"
    result = rows[["timestamp", "symbol", "sector", "split", target]].rename(columns={target: "actual_return"}).copy()
    result["horizon"] = horizon
    result["predicted_return"] = np.asarray(predicted, dtype=float)
    result["prediction_error"] = result["predicted_return"] - result["actual_return"]
    result["absolute_error"] = result["prediction_error"].abs()
    result["squared_error"] = result["prediction_error"].pow(2)
    result["model_name"] = model
    result["lookback"] = LOOKBACK if model in {"LSTM", "GRU"} else 0
    result["dataset_version"] = dataset_version
    result["actual_class_from_5bps"] = classify_returns(result["actual_return"], THRESHOLD)
    result["predicted_class_from_5bps"] = classify_returns(result["predicted_return"], THRESHOLD)
    return result


def _metric_row(frame: pd.DataFrame, scope: str) -> dict:
    return {
        "model": frame["model_name"].iloc[0], "horizon": int(frame["horizon"].iloc[0]),
        "scope": scope, "test_rows": len(frame),
        **regression_metrics(frame["actual_return"], frame["predicted_return"]),
    }


def _target_validation(frame: pd.DataFrame) -> dict:
    output = {}
    for horizon in HORIZONS:
        target, future_close = f"future_return_{horizon}m", f"future_close_{horizon}m"
        valid = frame.loc[frame[target].notna()]
        samples = {}
        selectors = {
            "normal": valid.index[100], "positive": valid[target].idxmax(), "negative": valid[target].idxmin(),
            "near_zero": valid[target].abs().idxmin(), "missing_future": frame[target].isna().idxmax(),
            "end_of_session": frame.sort_values("timestamp").groupby(["symbol", "trading_date"]).tail(1).index[0],
        }
        for name, index in selectors.items():
            row = frame.loc[index]
            stored = row[target]
            samples[name] = {
                "symbol": row["symbol"], "timestamp": row["timestamp"].isoformat(), "close": float(row["close"]),
                "future_close": None if pd.isna(row[future_close]) else float(row[future_close]),
                "stored_return": None if pd.isna(stored) else float(stored),
                "recalculated_return": None if pd.isna(row[future_close]) else float(row[future_close] / row["close"] - 1),
                "future_timestamp_exact": None if pd.isna(row[future_close]) else (row["timestamp"] + pd.Timedelta(minutes=horizon)).isoformat(),
            }
        output[f"{horizon}m"] = samples
    return output


def _distribution_rows(frame: pd.DataFrame) -> list[dict]:
    rows = []
    for horizon in HORIZONS:
        target = f"future_return_{horizon}m"
        for split, group in frame.groupby("split"):
            values = group[target].dropna()
            row = {"horizon": horizon, "split": split, "observations": len(values), "missing": int(group[target].isna().sum()),
                   "mean": values.mean(), "std": values.std(), "skewness": values.skew(),
                   "negative_share": (values < -THRESHOLD).mean(), "near_zero_share": values.between(-THRESHOLD, THRESHOLD).mean(), "positive_share": (values > THRESHOLD).mean()}
            row.update({f"q{int(q*100):02d}": values.quantile(q) for q in (.01, .05, .25, .5, .75, .95, .99)})
            rows.append(row)
    return rows


def _group_rows(predictions: dict[tuple[int, str], pd.DataFrame], level: str) -> list[dict]:
    rows = []
    for (horizon, model), frame in predictions.items():
        for value, group in frame.groupby(level):
            rows.append({"model": model, "horizon": horizon, level: value, "observations": len(group), **regression_metrics(group.actual_return, group.predicted_return)})
    return rows


def _magnitude_rows(predictions: dict[tuple[int, str], pd.DataFrame]) -> list[dict]:
    rows = []
    for (horizon, model), original in predictions.items():
        frame = original.assign(magnitude_bucket=return_magnitude_bucket(original.actual_return))
        for bucket, group in frame.groupby("magnitude_bucket", observed=True):
            metrics = regression_metrics(group.actual_return, group.predicted_return)
            rows.append({"model": model, "horizon": horizon, "magnitude_bucket": str(bucket), "observations": len(group), "mae": metrics["mae"], "directional_accuracy": metrics["directional_accuracy"]})
    return rows


def _classification_rows(predictions: dict[tuple[int, str], pd.DataFrame], classification_root: Path) -> pd.DataFrame:
    rows = []
    for (horizon, model), frame in predictions.items():
        metrics = classification_metrics(frame.actual_class_from_5bps, frame.predicted_class_from_5bps)
        rows.append({"formulation": "regression-derived", "scope": "full_model_test_set", "model": model, "horizon": horizon, "test_rows": len(frame),
                     "accuracy": metrics["accuracy"], "balanced_accuracy": metrics["balanced_accuracy"], "macro_f1": metrics["macro_f1"],
                     "comparison_note": "Regression returns converted with the same inclusive +/-5 bps neutral boundaries."})
    for horizon in HORIZONS:
        common = None
        for model in MODELS:
            keys = pd.MultiIndex.from_frame(predictions[(horizon, model)][["symbol", "timestamp"]])
            common = keys if common is None else common.intersection(keys)
        for model in MODELS:
            frame = predictions[(horizon, model)]; keys = pd.MultiIndex.from_frame(frame[["symbol", "timestamp"]]); frame = frame.loc[keys.isin(common)]
            metrics = classification_metrics(frame.actual_class_from_5bps, frame.predicted_class_from_5bps)
            rows.append({"formulation": "regression-derived", "scope": "common_comparable_test_set", "model": model, "horizon": horizon, "test_rows": len(frame),
                         "accuracy": metrics["accuracy"], "balanced_accuracy": metrics["balanced_accuracy"], "macro_f1": metrics["macro_f1"],
                         "comparison_note": "Regression returns converted with the same inclusive +/-5 bps neutral boundaries."})
    for filename in ("classification_metrics_full.csv", "classification_metrics_common.csv"):
        frozen = pd.read_csv(classification_root / "metrics" / filename)
        for _, row in frozen.iterrows():
            rows.append({"formulation": "direct-classification", "scope": row.scope, "model": row.model, "horizon": row.horizon, "test_rows": row.test_rows,
                         "accuracy": row.accuracy, "balanced_accuracy": row.balanced_accuracy, "macro_f1": row.macro_f1,
                         "comparison_note": "Frozen Phase 10 result; no retraining. Compare class metrics only, not MAE/RMSE."})
    return pd.DataFrame(rows)


def _figures(full: pd.DataFrame, predictions: dict[tuple[int, str], pd.DataFrame], comparison: pd.DataFrame, histories: dict, root: Path) -> None:
    for measure, label in (("mae", "MAE"), ("rmse", "RMSE"), ("r2", "R-squared")):
        pivot = full.pivot(index="model", columns="horizon", values=measure)
        ax = pivot.plot(kind="bar", figsize=(10, 5)); ax.set_title(f"{label} by model and horizon"); ax.set_ylabel(label); ax.grid(axis="y", alpha=.25)
        plt.tight_layout(); plt.savefig(root / f"{measure}_by_model_horizon.png", dpi=180); plt.close()
    best_name = full.loc[full.query("horizon == 5").mae.idxmin(), "model"]
    best = predictions[(5, best_name)].sample(min(10_000, len(predictions[(5, best_name)])), random_state=SEED)
    fig, ax = plt.subplots(figsize=(6, 6)); ax.scatter(best.actual_return, best.predicted_return, s=4, alpha=.18)
    bounds = np.quantile(np.r_[best.actual_return, best.predicted_return], [.005, .995]); ax.plot(bounds, bounds, color="black", linewidth=1)
    ax.set(xlim=bounds, ylim=bounds, xlabel="Actual return", ylabel="Predicted return", title=f"Actual vs predicted: {best_name}, 5m")
    plt.tight_layout(); plt.savefig(root / "actual_vs_predicted_best_5m.png", dpi=180); plt.close()
    fig, ax = plt.subplots(figsize=(8, 5)); ax.hist(best.prediction_error.clip(*np.quantile(best.prediction_error, [.005, .995])), bins=80)
    ax.set(xlabel="Prediction error", ylabel="Count", title=f"Residual distribution: {best_name}, 5m")
    plt.tight_layout(); plt.savefig(root / "residual_distribution_best_5m.png", dpi=180); plt.close()
    compare = comparison.query("scope == 'common_comparable_test_set'").groupby(["formulation", "horizon"])["balanced_accuracy"].max().unstack(0)
    ax = compare.plot(kind="bar", figsize=(8, 5)); ax.set(ylabel="Best balanced accuracy", title="Direct classification vs regression-derived classes"); ax.grid(axis="y", alpha=.25)
    plt.tight_layout(); plt.savefig(root / "classification_vs_regression.png", dpi=180); plt.close()
    for kind in ("LSTM", "GRU"):
        fig, ax = plt.subplots(figsize=(8, 5))
        for key, values in histories.items():
            if key.startswith(kind): ax.plot(values["loss"], label=f"{key} train"); ax.plot(values["val_loss"], linestyle="--", label=f"{key} validation")
        ax.set(xlabel="Epoch", ylabel="MSE", title=f"{kind} regression learning curves"); ax.legend(fontsize=8); ax.grid(alpha=.25)
        plt.tight_layout(); plt.savefig(root / f"{kind.lower()}_learning_curves.png", dpi=180); plt.close()


def run(features_path: Path, metadata_path: Path, classification_root: Path, output_root: Path, figures_root: Path) -> Path:
    if output_root.exists():
        raise FileExistsError(f"Frozen experiment already exists: {output_root}")
    temporary = output_root.parent / f".{output_root.name}.building"
    if temporary.exists(): shutil.rmtree(temporary)
    for name in ("predictions", "metrics", "models", "scalers", "histories", "reports", "figures"): (temporary / name).mkdir(parents=True, exist_ok=True)
    _set_seeds()
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    frame = pd.read_parquet(features_path)
    if frame.duplicated(["symbol", "timestamp"]).any(): raise ValueError("Duplicate feature keys")
    if set(FEATURE_COLUMNS) & {c for c in frame if c.startswith(("target_", "future_"))}: raise ValueError("Target leakage in features")
    (temporary / "reports" / "target_validation.json").write_text(json.dumps(_target_validation(frame), indent=2) + "\n", encoding="utf-8")
    pd.DataFrame(_distribution_rows(frame)).to_csv(temporary / "metrics" / "target_distribution.csv", index=False)
    predictions: dict[tuple[int, str], pd.DataFrame] = {}
    histories, means = {}, {}
    for horizon in HORIZONS:
        target = f"future_return_{horizon}m"
        usable = frame.loc[frame[target].notna()].copy()
        train = usable.loc[usable.split.eq("train")]; validation = usable.loc[usable.split.eq("validation")]; test = usable.loc[usable.split.eq("test")]
        scaler = StandardScaler().fit(train[FEATURE_COLUMNS]); joblib.dump(scaler, temporary / "scalers" / f"feature_scaler_{horizon}m.joblib")
        target_scaler = StandardScaler().fit(train[[target]])
        joblib.dump(target_scaler, temporary / "scalers" / f"target_scaler_{horizon}m.joblib")
        means[str(horizon)] = float(train[target].mean())
        model_predictions = {
            "Zero Return Baseline": np.zeros(len(test)),
            "Train Mean Baseline": np.full(len(test), means[str(horizon)]),
        }
        linear = LinearRegression(n_jobs=-1).fit(scaler.transform(train[FEATURE_COLUMNS]), train[target])
        joblib.dump(linear, temporary / "models" / f"linear_regression_{horizon}m.joblib")
        model_predictions["Linear Regression"] = linear.predict(scaler.transform(test[FEATURE_COLUMNS]))
        forest = RandomForestRegressor(n_estimators=40, max_depth=16, min_samples_leaf=10, max_samples=.25, n_jobs=-1, random_state=SEED).fit(train[FEATURE_COLUMNS], train[target])
        joblib.dump(forest, temporary / "models" / f"random_forest_regressor_{horizon}m.joblib")
        model_predictions["Random Forest Regressor"] = forest.predict(test[FEATURE_COLUMNS])
        for model, values in model_predictions.items(): predictions[(horizon, model)] = _prediction_frame(test, values, horizon, model, metadata["dataset_version"])
        scaled = usable.copy(); scaled[FEATURE_COLUMNS] = scaler.transform(usable[FEATURE_COLUMNS])
        sequences = {split: build_sequences(scaled.loc[scaled.split.eq(split)], FEATURE_COLUMNS, target, LOOKBACK, SEQUENCE_CAPS[split]) for split in ("train", "validation", "test")}
        x_train, y_train, _ = sequences["train"]; x_val, y_val, _ = sequences["validation"]; x_test, _, test_indices = sequences["test"]
        test_sequence_rows = scaled.loc[test_indices]
        for kind in ("LSTM", "GRU"):
            import tensorflow as tf
            tf.keras.backend.clear_session(); tf.random.set_seed(SEED)
            model = _deep_model(kind, len(FEATURE_COLUMNS))
            y_train_scaled = target_scaler.transform(y_train.reshape(-1, 1)).reshape(-1).astype("float32")
            y_val_scaled = target_scaler.transform(y_val.reshape(-1, 1)).reshape(-1).astype("float32")
            history = model.fit(x_train, y_train_scaled, validation_data=(x_val, y_val_scaled), epochs=4, batch_size=512,
                                callbacks=[tf.keras.callbacks.EarlyStopping(monitor="val_loss", patience=1, restore_best_weights=True)], verbose=2)
            model.save(temporary / "models" / f"{kind.lower()}_{horizon}m.keras")
            scaled_values = model.predict(x_test, batch_size=1024, verbose=0)
            values = target_scaler.inverse_transform(scaled_values).reshape(-1)
            predictions[(horizon, kind)] = _prediction_frame(test_sequence_rows, values, horizon, kind, metadata["dataset_version"])
            histories[f"{kind}_{horizon}m"] = {key: [float(v) for v in values] for key, values in history.history.items()}
    for (horizon, model), prediction in predictions.items(): prediction.to_parquet(temporary / "predictions" / f"{_slug(model)}_{horizon}m.parquet", index=False)
    full = pd.DataFrame([_metric_row(value, "full_model_test_set") for value in predictions.values()])
    common_rows = []
    for horizon in HORIZONS:
        common = None
        for model in MODELS:
            keys = pd.MultiIndex.from_frame(predictions[(horizon, model)][["symbol", "timestamp"]])
            common = keys if common is None else common.intersection(keys)
        for model in MODELS:
            value = predictions[(horizon, model)]; keys = pd.MultiIndex.from_frame(value[["symbol", "timestamp"]])
            common_rows.append(_metric_row(value.loc[keys.isin(common)], "common_comparable_test_set"))
    common = pd.DataFrame(common_rows)
    full.to_csv(temporary / "metrics" / "regression_metrics_full.csv", index=False); common.to_csv(temporary / "metrics" / "regression_metrics_common.csv", index=False)
    full.to_csv(temporary / "metrics" / "regression_metrics.csv", index=False)
    pd.DataFrame(_group_rows(predictions, "symbol")).to_csv(temporary / "metrics" / "regression_symbol_metrics.csv", index=False)
    pd.DataFrame(_group_rows(predictions, "sector")).to_csv(temporary / "metrics" / "regression_sector_metrics.csv", index=False)
    pd.DataFrame(_magnitude_rows(predictions)).to_csv(temporary / "metrics" / "return_magnitude_metrics.csv", index=False)
    pd.DataFrame([{"model": model, "horizon": horizon, **error_distribution(value.actual_return, value.predicted_return)} for (horizon, model), value in predictions.items()]).to_csv(temporary / "metrics" / "error_distribution.csv", index=False)
    finance = []
    for (horizon, model), value in predictions.items():
        for non_overlap in (False, True): finance.append({"model": model, "horizon": horizon, **regression_financial_metrics(value, non_overlapping=non_overlap)})
    pd.DataFrame(finance).to_csv(temporary / "metrics" / "regression_financial_evaluation.csv", index=False)
    ranking = pd.concat([full, common], ignore_index=True).sort_values(["scope", "horizon", "mae", "rmse", "r2", "pearson_correlation"], ascending=[True, True, True, True, False, False]).copy(); ranking["rank"] = ranking.groupby(["scope", "horizon"]).cumcount() + 1
    ranking.to_csv(temporary / "metrics" / "regression_model_rankings.csv", index=False)
    comparison = _classification_rows(predictions, classification_root); comparison.to_csv(temporary / "metrics" / "classification_vs_regression.csv", index=False)
    full.merge(pd.DataFrame(finance).query("evaluation_type == 'DIAGNOSTIC_SIGNAL_EVALUATION'")[["model", "horizon", "cumulative_return", "signals"]], on=["model", "horizon"]).to_csv(temporary / "metrics" / "regression_summary.csv", index=False)
    (temporary / "histories" / "training_history.json").write_text(json.dumps(histories, indent=2) + "\n", encoding="utf-8")
    (temporary / "reports" / "train_mean_baselines.json").write_text(json.dumps(means, indent=2) + "\n", encoding="utf-8")
    _figures(full, predictions, comparison, histories, temporary / "figures")
    _validate_artifacts(temporary)
    checksums = {str(path.relative_to(temporary)).replace("\\", "/"): _sha256(path) for path in temporary.rglob("*") if path.is_file()}
    manifest = {"experiment_id": FINAL_ID, "status": "frozen", "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "dataset_version": metadata["dataset_version"], "feature_version": FEATURE_VERSION, "features_checksum_sha256": metadata["checksum_sha256"],
                "target_definition": "(close at exact t+H - close at t) / close at t; same symbol and trading_date", "horizons_minutes": list(HORIZONS), "splits": metadata["splits"],
                "models": list(MODELS), "random_seed": SEED, "classification_baseline": "models/classification/phase10_classification_final_v1",
                "hyperparameters": {"linear_regression": {"fit_intercept": True}, "random_forest": {"n_estimators": 40, "max_depth": 16, "min_samples_leaf": 10, "max_samples": .25, "resource_policy": "All training rows form the sampling population; each deterministic bootstrap tree samples 25% to bound local CPU runtime."},
                                      "deep_learning": {"lookback": LOOKBACK, "units": 24, "dropout": .15, "loss": "mse", "learning_rate": .001, "target_scaling": "train-only StandardScaler, inverse-transformed before evaluation", "batch_size": 512, "max_epochs": 4, "early_stopping_patience": 1, "sequence_caps": SEQUENCE_CAPS}},
                "selection_policy": "Predeclared parameters; validation loss selects neural best weights; test data is evaluation-only.",
                "common_subset": "Intersection of symbol+timestamp keys across all six regression models per horizon.",
                "financial_evaluation": "Long when prediction exceeds +5 bps; otherwise flat; 5 bps position-change cost; equal-weight by timestamp; diagnostic overlapping and clock-grid non-overlapping views.",
                "software": {"python": platform.python_version(), "pandas": pd.__version__, "numpy": np.__version__, "scikit_learn": sklearn.__version__}, "artifact_reload_validated": True, "checksums": checksums}
    (temporary / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    temporary.rename(output_root)
    if figures_root.exists(): shutil.rmtree(figures_root)
    shutil.copytree(output_root / "figures", figures_root)
    return output_root


def _validate_artifacts(root: Path) -> None:
    for horizon in HORIZONS:
        scaler = joblib.load(root / "scalers" / f"feature_scaler_{horizon}m.joblib")
        if scaler.n_features_in_ != len(FEATURE_COLUMNS): raise ValueError("Scaler feature mismatch")
        target_scaler = joblib.load(root / "scalers" / f"target_scaler_{horizon}m.joblib")
        if target_scaler.n_features_in_ != 1: raise ValueError("Target scaler mismatch")
        joblib.load(root / "models" / f"linear_regression_{horizon}m.joblib")
        joblib.load(root / "models" / f"random_forest_regressor_{horizon}m.joblib")
        from tensorflow import keras
        for kind in ("lstm", "gru"): keras.models.load_model(root / "models" / f"{kind}_{horizon}m.keras")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run and freeze the secondary Phase 11 regression experiment.")
    parser.add_argument("--features", type=Path, default=Path("datasets/ml/ml_dataset_v1/features_v1/features_targets.parquet"))
    parser.add_argument("--metadata", type=Path, default=Path("datasets/ml/ml_dataset_v1/features_v1/metadata.json"))
    parser.add_argument("--classification", type=Path, default=Path("models/classification/phase10_classification_final_v1"))
    parser.add_argument("--output", type=Path, default=Path(f"models/regression/{FINAL_ID}"))
    parser.add_argument("--figures", type=Path, default=Path("reports/figures/phase11"))
    args = parser.parse_args(); print(run(args.features, args.metadata, args.classification, args.output, args.figures)); return 0


if __name__ == "__main__": raise SystemExit(main())
