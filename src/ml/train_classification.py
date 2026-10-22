from __future__ import annotations

import argparse
import json
import os
import random
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from src.ml.evaluation import CLASS_NAMES, classification_metrics, financial_metrics
from src.ml.features import FEATURE_COLUMNS, FEATURE_VERSION
from src.ml.sequences import build_sequences
from src.ml.targets import HORIZONS

SEED = 42
LOOKBACK = 60
SEQUENCE_CAPS = {"train": 120_000, "validation": 30_000, "test": 30_000}


def _set_seeds() -> None:
    os.environ["TF_DETERMINISTIC_OPS"] = "1"
    random.seed(SEED)
    np.random.seed(SEED)


def _prediction_frame(rows, actual, predicted, probabilities, horizon, model, dataset_version):
    result = rows[["timestamp", "symbol", "sector", "split", f"future_return_{horizon}m"]].copy()
    result = result.rename(columns={f"future_return_{horizon}m": "future_return"})
    result["horizon"] = horizon
    result["actual_class"] = actual
    result["predicted_class"] = predicted
    for index, name in enumerate(CLASS_NAMES):
        result[f"probability_{name.lower()}"] = probabilities[:, index]
    result["model_name"] = model
    result["lookback"] = LOOKBACK if model in {"LSTM", "GRU"} else 0
    result["dataset_version"] = dataset_version
    return result


def _evaluate(rows, actual, predicted, probabilities, horizon, model, dataset_version):
    predictions = _prediction_frame(rows, actual, predicted, probabilities, horizon, model, dataset_version)
    metrics = classification_metrics(actual, predicted, probabilities)
    finance = financial_metrics(predictions)
    return predictions, metrics, finance


def _deep_model(kind: str, feature_count: int):
    import tensorflow as tf
    layer = tf.keras.layers.LSTM if kind == "LSTM" else tf.keras.layers.GRU
    model = tf.keras.Sequential([
        tf.keras.layers.Input((LOOKBACK, feature_count)),
        layer(24),
        tf.keras.layers.Dropout(.15),
        tf.keras.layers.Dense(3, activation="softmax"),
    ])
    model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=.001), loss="sparse_categorical_crossentropy", metrics=["accuracy"])
    return model


def train(features_path: Path, metadata_path: Path, output_root: Path) -> Path:
    if output_root.exists():
        raise FileExistsError(f"Experiment output already exists: {output_root}")
    _set_seeds()
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    dataset_version = metadata["dataset_version"]
    frame = pd.read_parquet(features_path)
    output_root.mkdir(parents=True)
    models_root = output_root / "models"
    predictions_root = output_root / "predictions"
    models_root.mkdir()
    predictions_root.mkdir()
    all_metrics, all_finance, histories, prediction_files = [], [], {}, []

    for horizon in HORIZONS:
        target = f"target_{horizon}m"
        usable = frame.loc[frame[target].notna()].copy()
        train_rows = usable.loc[usable["split"] == "train"]
        validation_rows = usable.loc[usable["split"] == "validation"]
        test_rows = usable.loc[usable["split"] == "test"]
        scaler = StandardScaler().fit(train_rows[FEATURE_COLUMNS])
        scaler_path = models_root / f"scaler_{horizon}m.joblib"
        joblib.dump(scaler, scaler_path)

        majority = train_rows[target].value_counts().idxmax()
        majority_pred = np.full(len(test_rows), majority)
        majority_prob = np.zeros((len(test_rows), 3))
        majority_prob[:, CLASS_NAMES.index(majority)] = 1
        prediction, metrics, finance = _evaluate(test_rows, test_rows[target].to_numpy(), majority_pred, majority_prob, horizon, "Majority Baseline", dataset_version)
        _persist_result(prediction, metrics, finance, horizon, "majority", predictions_root, all_metrics, all_finance, prediction_files)

        x_train = scaler.transform(train_rows[FEATURE_COLUMNS])
        x_test = scaler.transform(test_rows[FEATURE_COLUMNS])
        logistic = LogisticRegression(max_iter=150, class_weight="balanced", random_state=SEED).fit(x_train, train_rows[target])
        joblib.dump(logistic, models_root / f"logistic_{horizon}m.joblib")
        probabilities = _ordered_probabilities(logistic.predict_proba(x_test), logistic.classes_)
        prediction, metrics, finance = _evaluate(test_rows, test_rows[target].to_numpy(), logistic.predict(x_test), probabilities, horizon, "Logistic Regression", dataset_version)
        _persist_result(prediction, metrics, finance, horizon, "logistic", predictions_root, all_metrics, all_finance, prediction_files)

        forest = RandomForestClassifier(n_estimators=60, max_depth=16, min_samples_leaf=10, class_weight="balanced_subsample", n_jobs=-1, random_state=SEED).fit(train_rows[FEATURE_COLUMNS], train_rows[target])
        joblib.dump(forest, models_root / f"random_forest_{horizon}m.joblib")
        probabilities = _ordered_probabilities(forest.predict_proba(test_rows[FEATURE_COLUMNS]), forest.classes_)
        prediction, metrics, finance = _evaluate(test_rows, test_rows[target].to_numpy(), forest.predict(test_rows[FEATURE_COLUMNS]), probabilities, horizon, "Random Forest", dataset_version)
        _persist_result(prediction, metrics, finance, horizon, "random_forest", predictions_root, all_metrics, all_finance, prediction_files)

        scaled = usable.copy()
        scaled[FEATURE_COLUMNS] = scaler.transform(usable[FEATURE_COLUMNS])
        sequence_sets = {}
        for split in ("train", "validation", "test"):
            sequence_sets[split] = build_sequences(scaled.loc[scaled["split"] == split], FEATURE_COLUMNS, target, LOOKBACK, SEQUENCE_CAPS[split])
        label_map = {name: index for index, name in enumerate(CLASS_NAMES)}
        x_seq_train, y_seq_train, _ = sequence_sets["train"]
        x_seq_validation, y_seq_validation, _ = sequence_sets["validation"]
        x_seq_test, y_seq_test, test_indices = sequence_sets["test"]
        y_train_encoded = np.array([label_map[value] for value in y_seq_train])
        y_validation_encoded = np.array([label_map[value] for value in y_seq_validation])
        sequence_test_rows = scaled.loc[test_indices]
        for kind in ("LSTM", "GRU"):
            import tensorflow as tf
            tf.keras.backend.clear_session()
            tf.random.set_seed(SEED)
            model = _deep_model(kind, len(FEATURE_COLUMNS))
            history = model.fit(x_seq_train, y_train_encoded, validation_data=(x_seq_validation, y_validation_encoded), epochs=4, batch_size=512, callbacks=[tf.keras.callbacks.EarlyStopping(monitor="val_loss", patience=1, restore_best_weights=True)], verbose=2)
            model_path = models_root / f"{kind.lower()}_{horizon}m.keras"
            model.save(model_path)
            probabilities = model.predict(x_seq_test, batch_size=1024, verbose=0)
            predicted = np.asarray(CLASS_NAMES)[probabilities.argmax(axis=1)]
            prediction, metrics, finance = _evaluate(sequence_test_rows, y_seq_test, predicted, probabilities, horizon, kind, dataset_version)
            _persist_result(prediction, metrics, finance, horizon, kind.lower(), predictions_root, all_metrics, all_finance, prediction_files)
            histories[f"{kind}_{horizon}m"] = {key: [float(item) for item in values] for key, values in history.history.items()}

    pd.DataFrame(all_metrics).to_csv(output_root / "classification_metrics.csv", index=False)
    pd.DataFrame(all_finance).to_csv(output_root / "financial_evaluation.csv", index=False)
    (output_root / "training_history.json").write_text(json.dumps(histories, indent=2) + "\n", encoding="utf-8")
    manifest = {
        "experiment_id": "phase10_classification_v1", "dataset_version": dataset_version,
        "features_version": FEATURE_VERSION, "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "models": ["Majority Baseline", "Logistic Regression", "Random Forest", "LSTM", "GRU"],
        "horizons_minutes": list(HORIZONS), "lookback": LOOKBACK, "random_seed": SEED,
        "deep_learning": {"units": 24, "dropout": .15, "optimizer": "Adam", "learning_rate": .001, "batch_size": 512, "max_epochs": 4, "early_stopping_patience": 1, "sequence_caps": SEQUENCE_CAPS},
        "splits": metadata["splits"], "threshold_basis_points": metadata["threshold_basis_points"],
        "prediction_files": prediction_files,
        "test_policy": "Test rows were used only for final persisted evaluation after model fitting/model selection on train/validation.",
    }
    (output_root / "experiment_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return output_root


def _ordered_probabilities(probabilities, classes):
    result = np.zeros((len(probabilities), 3))
    for source, name in enumerate(classes):
        result[:, CLASS_NAMES.index(name)] = probabilities[:, source]
    return result


def _persist_result(prediction, metrics, finance, horizon, slug, root, metrics_rows, finance_rows, files):
    path = root / f"{slug}_{horizon}m.parquet"
    prediction.to_parquet(path, index=False)
    files.append(str(path))
    metrics_rows.append({"model": prediction["model_name"].iloc[0], "horizon": horizon, **{key: value for key, value in metrics.items() if key not in {"per_class", "confusion_matrix"}}, "per_class_json": json.dumps(metrics["per_class"]), "confusion_matrix_json": json.dumps(metrics["confusion_matrix"])})
    finance_rows.append({"model": prediction["model_name"].iloc[0], "horizon": horizon, **finance})


def main() -> int:
    parser = argparse.ArgumentParser(description="Train Phase 10 classification models.")
    parser.add_argument("--features", type=Path, default=Path("datasets/ml/ml_dataset_v1/features_v1/features_targets.parquet"))
    parser.add_argument("--metadata", type=Path, default=Path("datasets/ml/ml_dataset_v1/features_v1/metadata.json"))
    parser.add_argument("--output", type=Path, default=Path("models/classification/phase10_classification_v1"))
    args = parser.parse_args()
    print(train(args.features, args.metadata, args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
