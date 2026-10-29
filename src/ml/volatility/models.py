from __future__ import annotations

import json
import time
import warnings
from pathlib import Path

import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import LinearRegression, LogisticRegression, Ridge
from sklearn.preprocessing import StandardScaler

from src.ml.volatility.data import FEATURES, dump
from src.ml.volatility.metrics import primary, statistical

NEURAL = {"lstm", "gru"}
SPACES = {
    "neural": {"lookback": [15, 30, 60, 90, 120], "hidden_size": [16, 32, 64, 128],
        "layers": [1, 2, 3], "dropout": [0., .1, .2, .3, .5], "learning_rate": [.0001, .0003, .001, .003],
        "batch_size": [64, 128, 256, 512], "weight_decay": [0., .000001, .00001, .0001, .001],
        "patience": [4, 6, 8, 10], "architecture": ["recurrent", "recurrent_dense"]},
    "forest": {"n_estimators": [100, 200, 300, 500, 800], "max_depth": [5, 8, 12, 16, 24, None],
        "min_samples_split": [2, 5, 10, 20], "min_samples_leaf": [1, 2, 5, 10, 20],
        "max_features": ["sqrt", "log2", .3, .5, .75, 1.], "bootstrap": [True, False], "max_samples": [.25, .5, .75, None]},
    "logistic": {"C": [.0001, .001, .01, .1, 1., 10., 100., 1000.], "penalty": ["l1", "l2", "elasticnet"],
        "class_weight": [None, "balanced"], "l1_ratio": [.25, .5, .75]},
    "ridge": {"alpha": [float(x) for x in np.logspace(-5, 4, 10)]},
    "linear": {},
}

FEATURE_GROUPS = {
    "price_return": {"open", "high", "low", "close", "return_1m", "log_return", "return_5m", "return_15m", "return_30m", "momentum_5"},
    "volatility": {"volatility_5", "volatility_15", "volatility_30", "atr_14", "bollinger_bandwidth", *[f"past_rv_{h}" for h in (1, 5, 15, 30, 60)]},
    "volume": {"volume", "volume_change", "volume_mean_30", "relative_volume"},
    "technical": {"sma_5", "sma_15", "sma_30", "ema_5", "ema_15", "ema_30", "rsi_14", "macd", "macd_signal", "macd_histogram", "bollinger_upper", "bollinger_lower"},
    "time": {"hour", "minute_of_day", "minutes_since_open"},
    "sector": {"sector_return_mean", "relative_to_sector", "sector_return_dispersion", *[f for f in FEATURES if f.startswith("sector_")]},
}


class BudgetExpired(RuntimeError):
    pass


class ResourceInfeasible(MemoryError):
    pass


def check_forest_memory(params, rows, kind):
    """Reject oversized forest candidates explicitly, never shrink their data."""
    import psutil
    effective = rows * (params.get("max_samples") or 1.) if params["bootstrap"] else rows
    leaves = max(1, int(np.ceil(effective / params["min_samples_leaf"])))
    if params["max_depth"] is not None:
        leaves = min(leaves, 2 ** params["max_depth"])
    classes = 1 if kind == "regression" else (2 if kind == "binary" else 3)
    upper_bytes = params["n_estimators"] * (2 * leaves - 1) * (80 + 8 * classes)
    memory_budget = psutil.virtual_memory().available * .65
    if upper_bytes > memory_budget:
        raise ResourceInfeasible(f"Forest upper memory estimate {upper_bytes / 1e9:.2f} GB exceeds available fit budget {memory_budget / 1e9:.2f} GB; training population was not reduced")
    return int(upper_bytes)


def search(model, kind, horizon, task, count=8, seed=42):
    import random
    if model == "linear":
        return [{}]
    if model == "ridge":
        return [{"alpha": x} for x in SPACES["ridge"]["alpha"]]
    rng = random.Random(seed + horizon * 101 + sum((i + 1) * ord(c) for i, c in enumerate(task + model)))
    space = dict(SPACES["neural" if model in NEURAL else model])
    if model == "forest" and kind != "regression":
        space["class_weight"] = [None, "balanced", "balanced_subsample"]
    if model in NEURAL and kind != "regression":
        space["class_weight"] = [None, "balanced"]
    columns = {}
    for key, values in space.items():
        sequence = []
        while len(sequence) < count:
            cycle = list(values); rng.shuffle(cycle); sequence.extend(cycle)
        columns[key] = sequence[:count]
    rows = [dict(zip(columns, values)) for values in zip(*columns.values())]
    if model in NEURAL:
        baseline = {"lookback": 15, "hidden_size": 16, "layers": 1, "dropout": .1,
                    "learning_rate": .001, "batch_size": 128, "weight_decay": .00001,
                    "patience": 4, "architecture": "recurrent"}
    elif model == "forest":
        baseline = {"n_estimators": 100, "max_depth": 8, "min_samples_split": 5, "min_samples_leaf": 5,
                    "max_features": "sqrt", "bootstrap": True, "max_samples": .5}
        if kind != "regression": baseline["class_weight"] = "balanced"
    else:
        baseline = {"C": 1., "penalty": "l2", "class_weight": "balanced", "l1_ratio": 0.}
    rows.insert(0, baseline)
    if model in NEURAL and kind != "regression": baseline["class_weight"] = "balanced"
    for row in rows:
        if model == "forest" and not row["bootstrap"]:
            row["max_samples"] = None
        if model == "logistic":
            row["solver"] = "saga" if row["penalty"] != "l2" else "lbfgs"
            row["l1_ratio"] = 1. if row["penalty"] == "l1" else (0. if row["penalty"] == "l2" else row["l1_ratio"])
    unique = {json.dumps(row, sort_keys=True): row for row in rows}
    return list(unique.values())


def preprocess(x, indices, columns):
    indices, columns = np.asarray(indices), np.asarray(columns)
    # Exact train-only medians, one feature at a time; no full feature-matrix copy.
    median = np.empty(len(columns), dtype="float32")
    for i, column in enumerate(columns):
        observed = np.asarray(x[indices, column])
        observed = observed[np.isfinite(observed)]
        median[i] = np.median(observed) if len(observed) else 0.
    scaler = StandardScaler()
    for start in range(0, len(indices), 8192):
        values = np.asarray(x[indices[start:start + 8192, None], columns[None, :]], dtype="float32")
        values = np.where(np.isfinite(values), values, median)
        scaler.partial_fit(values)
    scale = scaler.scale_.copy()
    scale[scale < 1e-8] = 1.
    return {"median": median, "mean": scaler.mean_, "scale": scale, "columns": columns, "fit_rows": len(indices)}


def transform(values, prep):
    values = np.asarray(values, dtype="float32")
    values = np.where(np.isfinite(values), values, prep["median"])
    return ((values - prep["mean"]) / prep["scale"]).astype("float32")


def batches(panel, labels, indices, params, prep, *, train=False, seed=42):
    import tensorflow as tf
    class SequenceBatches(tf.keras.utils.PyDataset):
        def __init__(self):
            super().__init__(workers=1, use_multiprocessing=False, max_queue_size=1)
            self.order = np.arange(len(indices)); self.rng = np.random.default_rng(seed)
        def __len__(self):
            return (len(indices) + params["batch_size"] - 1) // params["batch_size"]
        def __getitem__(self, index):
            selected = indices[self.order[index * params["batch_size"]:(index + 1) * params["batch_size"]]]
            rows = selected[:, None] + np.arange(1 - params["lookback"], 1)
            values = panel.x[rows[:, :, None], prep["columns"][None, None, :]]
            values = transform(values, prep)
            return values if labels is None else (values, labels[selected].astype("float32"))
        def on_epoch_end(self):
            if train: self.rng.shuffle(self.order)
    return SequenceBatches()


def build_neural(kind, model, params, features):
    import tensorflow as tf
    recurrent = tf.keras.layers.LSTM if model == "lstm" else tf.keras.layers.GRU
    layers = [tf.keras.layers.Input((params["lookback"], features))]
    for i in range(params["layers"]):
        layers.append(recurrent(params["hidden_size"], return_sequences=i < params["layers"] - 1))
        if params["dropout"]: layers.append(tf.keras.layers.Dropout(params["dropout"]))
    if params["architecture"] == "recurrent_dense":
        layers.append(tf.keras.layers.Dense(params["hidden_size"] // 2, activation="relu"))
    classes = 2 if kind == "binary" else 3
    layers.append(tf.keras.layers.Dense(1 if kind == "regression" else classes, activation=None if kind == "regression" else "softmax"))
    model = tf.keras.Sequential(layers)
    model.compile(optimizer=tf.keras.optimizers.AdamW(learning_rate=params["learning_rate"], weight_decay=params["weight_decay"]),
                  loss="mse" if kind == "regression" else "sparse_categorical_crossentropy")
    return model


def build_tabular(kind, model, params, seed):
    if model == "linear": return LinearRegression()
    if model == "ridge": return Ridge(**params)
    if model == "forest":
        cls = RandomForestRegressor if kind == "regression" else RandomForestClassifier
        return cls(**params, n_jobs=2, random_state=seed, warm_start=True)
    import sklearn
    options = dict(params)
    if tuple(map(int, sklearn.__version__.split(".")[:2])) >= (1, 8):
        # Explicit ratio in sklearn >=1.8 implements the persisted penalty.
        options.pop("penalty")
    else:
        if options["penalty"] != "elasticnet": options.pop("l1_ratio")
    estimator = LogisticRegression(**options, max_iter=1000, random_state=seed)
    if "l1_ratio" in options and estimator.get_params()["l1_ratio"] != params["l1_ratio"]:
        raise ValueError("Regularization ratio was ignored")
    return estimator


def predict_tabular(estimator, kind, x):
    if kind == "regression": return estimator.predict(x)
    probability = estimator.predict_proba(x)
    aligned = np.zeros((len(x), 2 if kind == "binary" else 3))
    aligned[:, estimator.classes_.astype(int)] = probability
    return aligned


def predict_rows(estimator, kind, panel, indices, prep, chunk_size=8192):
    predictions = []
    for start in range(0, len(indices), chunk_size):
        rows = indices[start:start + chunk_size]
        values = transform(panel.x[rows[:, None], prep["columns"][None, :]], prep)
        predictions.append(predict_tabular(estimator, kind, values))
    return np.concatenate(predictions)


def materialize_training(panel, indices, prep, path):
    matrix = np.lib.format.open_memmap(path, mode="w+", dtype="float32", shape=(len(indices), len(prep["columns"])))
    for start in range(0, len(indices), 8192):
        rows = indices[start:start + 8192]
        matrix[start:start + len(rows)] = transform(panel.x[rows[:, None], prep["columns"][None, :]], prep)
    matrix.flush()
    return matrix


def fit(panel, labels, kind, model, params, train, validation, directory: Path, deadline=None, *, max_epochs=75, seed=42, columns=None):
    if deadline is not None and time.monotonic() >= deadline: raise BudgetExpired()
    directory.mkdir(parents=True, exist_ok=True)
    columns = np.arange(len(FEATURES)) if columns is None else np.asarray(columns)
    prep = preprocess(panel.x, train, columns)
    joblib.dump(prep, directory / "preprocessing.joblib")
    start = time.perf_counter()
    result = {"best_epoch": None, "epochs_trained": None, "best_validation_loss": None,
              "potentially_undertrained": False, "overfitting": False, "max_epochs": max_epochs if model in NEURAL else None,
              "training_rows": len(train), "validation_rows": len(validation), "seed": seed, "parameters": params,
              "feature_count": len(columns), "status": "COMPLETED", "time_cutoff_enabled": deadline is not None,
              "stopping_monitor": "validation_mae" if kind == "regression" else ("validation_event_f1" if kind == "binary" else "validation_macro_f1")}
    nonnegative = getattr(panel, "active_task", "") in {"company_rv", "sector_rv"}
    result["nonnegative_predictions"] = nonnegative
    if model not in NEURAL:
        if model == "forest": result["forest_memory_upper_bytes"] = check_forest_memory(params, len(train), kind)
        estimator = build_tabular(kind, model, params, seed)
        training_path = directory / "_fit_features.npy"
        tx = materialize_training(panel, train, prep, training_path)
        curve = []
        try:
            with warnings.catch_warnings(record=True) as captured:
                warnings.simplefilter("always", ConvergenceWarning)
                if model == "forest":
                    maximum = params["n_estimators"]
                    for count in list(range(100, maximum, 100)) + [maximum]:
                        if deadline is not None and time.monotonic() >= deadline: raise BudgetExpired()
                        estimator.set_params(n_estimators=count)
                        estimator.fit(tx, labels[train])
                        prediction = predict_rows(estimator, kind, panel, validation, prep)
                        if nonnegative: prediction = np.maximum(prediction, 0.)
                        curve.append({"n_estimators": count, "validation_primary_metric": primary(kind, labels[validation], prediction)})
                    result["forest_curve"] = curve
                    values = np.array([r["validation_primary_metric"] for r in curve])
                    result["forest_plateau"] = bool(len(values) >= 3 and abs(values[-1] - values[-3]) <= max(1e-6, abs(values[-3]) * .005))
                else:
                    estimator.fit(tx, labels[train])
                if any(issubclass(w.category, ConvergenceWarning) for w in captured):
                    raise ValueError("Non-converged solver candidate excluded")
        finally:
            # Only this fit's disposable transformed matrix is removed.
            tx._mmap.close()
            if training_path.resolve().parent != directory.resolve():
                raise RuntimeError("Unexpected training scratch path")
            training_path.unlink(missing_ok=True)
        result["training_duration_seconds"] = time.perf_counter() - start
        prediction = predict_rows(estimator, kind, panel, validation, prep)
        result["complexity"] = int(sum(tree.tree_.node_count for tree in estimator.estimators_)) if model == "forest" else int(estimator.coef_.size)
        joblib.dump(estimator, directory / "model.joblib")
    else:
        import tensorflow as tf
        tf.keras.backend.clear_session(); tf.keras.utils.set_random_seed(seed)
        target_mean = float(labels[train].mean()) if kind == "regression" else 0.
        target_scale = float(labels[train].std()) if kind == "regression" else 1.
        target_scale = target_scale or 1.
        y = (labels - target_mean) / target_scale if kind == "regression" else labels
        training = batches(panel, y, train, params, prep, train=True, seed=seed)
        val = batches(panel, y, validation, params, prep)
        val_x = batches(panel, None, validation, params, prep)
        checkpoint = directory / "best.keras"
        class Evaluate(tf.keras.callbacks.Callback):
            def __init__(self):
                super().__init__()
                self.best = float("inf") if kind == "regression" else -float("inf")
                self.best_epoch = None
            def on_train_batch_end(self, batch, logs=None):
                if deadline is not None and time.monotonic() >= deadline: self.model.stop_training = True
            def on_epoch_end(self, epoch, logs=None):
                prediction = self.model.predict(val_x, verbose=0)
                if kind == "regression": prediction = prediction.reshape(-1) * target_scale + target_mean
                if nonnegative: prediction = np.maximum(prediction, 0.)
                logs["val_primary"] = primary(kind, labels[validation], prediction)
                value = float(logs["val_primary"])
                improved = value < self.best if kind == "regression" else value > self.best
                if improved:
                    self.best, self.best_epoch = value, epoch + 1
                dump(directory / "progress.json", {"epoch": epoch + 1, "val_primary": value, "val_loss": float(logs["val_loss"]),
                    "best_validation_primary": self.best, "best_epoch": self.best_epoch,
                    "epochs_without_improvement": epoch + 1 - self.best_epoch, "patience": params["patience"]})
                print(f"EPOCH {epoch + 1}/{max_epochs} {result['stopping_monitor']}={value:.7f} best={self.best:.7f} best_epoch={self.best_epoch} wait={epoch + 1 - self.best_epoch}/{params['patience']}", flush=True)
        callbacks = [Evaluate(), tf.keras.callbacks.ModelCheckpoint(str(checkpoint), monitor="val_primary", mode="min" if kind == "regression" else "max", save_best_only=True),
                     tf.keras.callbacks.EarlyStopping(monitor="val_primary", mode="min" if kind == "regression" else "max", patience=params["patience"], restore_best_weights=True),
                     tf.keras.callbacks.TerminateOnNaN()]
        estimator = build_neural(kind, model, params, len(columns))
        start = time.perf_counter()
        class_weight = None
        if params.get("class_weight") == "balanced":
            counts = np.bincount(labels[train].astype(int), minlength=2 if kind == "binary" else 3)
            class_weight = {i: len(train) / (len(counts) * count) for i, count in enumerate(counts) if count}
        history = estimator.fit(training, validation_data=val, epochs=max_epochs, callbacks=callbacks, verbose=0, shuffle=False, class_weight=class_weight)
        result["training_duration_seconds"] = time.perf_counter() - start
        values = np.asarray(history.history["val_primary"])
        epoch = int(np.argmin(values) if kind == "regression" else np.argmax(values))
        losses = np.asarray(history.history["val_loss"])
        if not np.isfinite(values).all() or not np.isfinite(losses).all(): raise ValueError("Non-finite neural history")
        result.update({"best_epoch": epoch + 1, "epochs_trained": len(values), "best_validation_loss": float(losses.min()),
                       "validation_loss_at_best_epoch": float(losses[epoch]), "target_mean": target_mean, "target_scale": target_scale,
                       "potentially_undertrained": bool(len(values) == max_epochs and epoch >= max_epochs - 3),
                       "overfitting": bool(epoch < len(values) - 2 and history.history["loss"][-1] < history.history["loss"][epoch]),
                       "complexity": int(estimator.count_params())})
        early_best = float(np.min(values[:4]) if kind == "regression" else np.max(values[:4]))
        result["best_primary_within_four_epochs"] = early_best
        result["additional_epochs_improved_validation"] = bool(epoch >= 4 and (values[epoch] < early_best if kind == "regression" else values[epoch] > early_best))
        dump(directory / "history.json", {key: [float(v) for v in values] for key, values in history.history.items()})
        if deadline is not None and time.monotonic() >= deadline: raise BudgetExpired("Neural candidate interrupted at budget boundary")
        result["stopping_reason"] = "early_stopping_no_validation_improvement" if len(values) < max_epochs else "epoch_ceiling"
        del estimator
        tf.keras.backend.clear_session()
        estimator = tf.keras.models.load_model(checkpoint, compile=False)
        prediction = estimator.predict(val_x, verbose=0)
        if kind == "regression": prediction = prediction.reshape(-1) * target_scale + target_mean
        del estimator
        tf.keras.backend.clear_session()
    if nonnegative:
        prediction = np.maximum(prediction, 0.)
    result["best_validation_primary_metric"] = primary(kind, labels[validation], prediction)
    result["validation_metrics"] = statistical(kind, labels[validation], prediction)
    np.save(directory / "validation_predictions.npy", prediction)
    result["fit_and_validation_seconds"] = time.perf_counter() - start
    dump(directory / "result.json", result)
    return result


def inference(panel, kind, model, params, indices, directory, result):
    import psutil
    import threading
    prep = joblib.load(directory / "preprocessing.joblib")
    process = psutil.Process()
    initial_rss = process.memory_info().rss
    peak = [initial_rss]
    stop = threading.Event()
    def sample_memory():
        while not stop.wait(.02):
            peak[0] = max(peak[0], process.memory_info().rss)
    monitor = threading.Thread(target=sample_memory, daemon=True)
    monitor.start()
    start = time.perf_counter()
    if model in NEURAL:
        import tensorflow as tf
        tf.keras.backend.clear_session()
        estimator = tf.keras.models.load_model(directory / "best.keras", compile=False)
        dataset = batches(panel, None, indices, params, prep)
        prediction = estimator.predict(dataset, verbose=0)
        if kind == "regression": prediction = prediction.reshape(-1) * result["target_scale"] + result["target_mean"]
        model_bytes = (directory / "best.keras").stat().st_size
        del estimator
        tf.keras.backend.clear_session()
    else:
        estimator = joblib.load(directory / "model.joblib")
        prediction = predict_rows(estimator, kind, panel, indices, prep)
        model_bytes = (directory / "model.joblib").stat().st_size
    stop.set(); monitor.join(timeout=1)
    if result.get("nonnegative_predictions"): prediction = np.maximum(prediction, 0.)
    return prediction, {"batch_inference_seconds": time.perf_counter() - start, "observations": len(indices), "model_size_bytes": model_bytes,
        "observed_process_peak_rss_mb": peak[0] / 1024 ** 2, "incremental_peak_rss_mb": (peak[0] - initial_rss) / 1024 ** 2,
        "memory_warning": "Process RSS includes runtime and dataset caches, not an isolated deployment memory requirement"}
