import time
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import yaml

from src.ml.features import FEATURE_COLUMNS
from src.ml.volatility.data import FEATURES, HORIZONS, WINDOWS, Panel, dump, eligible_sequences, prepare, realized_paths, representative_indices, sector_components
from src.ml.volatility.experiment import Experiment, chronological_folds, rank_candidates
from src.ml.volatility.metrics import binary_metrics, statistical
from src.ml.volatility.models import fit, inference, search


def test_realized_volatility_uses_full_future_path_and_no_session_crossing():
    close = pd.DataFrame({"A": np.exp(np.arange(12) * .01)})
    sessions = np.array(["d1"] * 6 + ["d2"] * 6)
    _, backward, forward, _ = realized_paths(close, sessions, 3)
    assert forward.iloc[0, 0] == pytest.approx(np.sqrt(3) * .01)
    assert np.isnan(forward.iloc[4, 0])
    changed = close.copy(); changed.iloc[2, 0] *= 2
    _, changed_past, changed_future, _ = realized_paths(changed, sessions, 3)
    assert changed_future.iloc[0, 0] > forward.iloc[0, 0]
    assert changed_past.iloc[1, 0] is not None
    assert np.isnan(backward.iloc[1, 0]) and np.isnan(changed_past.iloc[1, 0])
    missing = close.copy(); missing.iloc[2, 0] = np.nan
    _, _, invalid, _ = realized_paths(missing, sessions, 3)
    assert np.isnan(invalid.iloc[0, 0])


def test_sector_concentration_is_variance_share_and_one_minute_correlation_undefined():
    rv = np.array([[1., 1., 1., 1., 1.], [2., 0., 0., 0., 0.], [1., 1., np.nan, 1., 1.]])
    returns = pd.DataFrame(np.arange(15).reshape(3, 5))
    result = sector_components(rv, returns, 1)
    assert result[0, 4] == pytest.approx(.2)
    assert result[1, 4] == pytest.approx(1.)
    assert result[1, 0] == pytest.approx(np.sqrt(4 / 5))
    assert np.isnan(result[:, 7]).all()
    assert np.isnan(result[2]).all()


def test_representative_sampling_covers_dates_and_entities_without_future_rows():
    frame = pd.DataFrame([(day, entity) for day in range(100) for entity in range(30)], columns=["trading_date", "entity"])
    indices = representative_indices(frame, np.ones(len(frame), dtype=bool), 300)
    assert len(indices) == 300 and len(set(indices)) == 300
    assert frame.iloc[indices].trading_date.nunique() == 100
    assert frame.iloc[indices].entity.nunique() == 30


def test_sampler_counts_full_entity_population_once(monkeypatch):
    frame = pd.DataFrame([(day, entity) for day in range(100) for entity in range(30)], columns=["trading_date", "entity"])
    original = pd.Series.nunique
    calls = []
    def count(series, *args, **kwargs):
        if series.name == "entity" and len(series) == len(frame): calls.append(1)
        return original(series, *args, **kwargs)
    monkeypatch.setattr(pd.Series, "nunique", count)
    indices = representative_indices(frame, np.ones(len(frame), dtype=bool), 300)
    assert len(indices) == 300
    assert len(calls) == 1


def test_unlimited_population_does_not_sample_eligible_rows():
    frame = pd.DataFrame({"entity": ["A"] * 1000, "trading_date": ["d1"] * 1000})
    eligible = np.arange(1000) % 3 != 0
    np.testing.assert_array_equal(representative_indices(frame, eligible, None), np.flatnonzero(eligible))


def test_chunked_preprocessing_uses_all_training_rows_and_no_validation_values():
    from src.ml.volatility.models import preprocess
    x = np.random.default_rng(42).normal(size=(31_000, 4)).astype("float32")
    x[::3, 0] = np.nan; x[:, 1] = np.nan
    train = np.arange(30_000)
    prep = preprocess(x, train, np.arange(4))
    median = np.array([np.nanmedian(x[train, 0]), 0., np.median(x[train, 2]), np.median(x[train, 3])])
    expected = np.where(np.isfinite(x[train]), x[train], median).astype(float)
    assert prep["fit_rows"] == 30_000
    np.testing.assert_allclose(prep["median"], median)
    np.testing.assert_allclose(prep["mean"], expected.mean(axis=0), atol=1e-8)
    x[30_000:] = 1e9
    unaffected = preprocess(x, train, np.arange(4))
    np.testing.assert_array_equal(prep["mean"], unaffected["mean"])


def test_temporal_folds_expand_and_never_use_test():
    meta = pd.DataFrame({"trading_date": [f"2026-01-{d:02d}" for d in range(1, 11)], "split": ["train"] * 3 + ["validation"] * 6 + ["test"]})
    folds = chronological_folds(meta)
    assert len(folds) == 3
    assert all(f["train_end"] < f["validation_start"] <= f["validation_end"] < "2026-01-10" for f in folds)
    assert folds[1]["train_end"] == folds[0]["validation_end"]


def test_selection_rejects_one_fold_winner_and_prefers_simpler_near_tie():
    rows = [{"candidate": 0, "folds_completed": 3, "validation_mean": .5, "validation_std": .03, "complexity": 100},
            {"candidate": 1, "folds_completed": 3, "validation_mean": .49, "validation_std": .01, "complexity": 10},
            {"candidate": 2, "folds_completed": 1, "validation_mean": .9, "validation_std": 0., "complexity": 1}]
    assert rank_candidates(rows, "classification")["candidate"] == 1


@pytest.mark.parametrize("model,kind", [("logistic", "classification"), ("ridge", "regression"), ("forest", "classification"), ("lstm", "binary"), ("gru", "regression")])
def test_search_is_model_horizon_specific_and_valid(model, kind):
    first = search(model, kind, 5, "task", 8)
    assert first == search(model, kind, 5, "task", 8)
    if model != "ridge": assert first != search(model, kind, 15, "task", 8)
    for row in first:
        if model == "forest" and not row["bootstrap"]: assert row["max_samples"] is None
        if model == "logistic":
            assert row["solver"] == ("lbfgs" if row["penalty"] == "l2" else "saga")
            if row["penalty"] == "l1": assert row["l1_ratio"] == 1.


@pytest.fixture(scope="module")
def synthetic_panel(tmp_path_factory):
    base = tmp_path_factory.mktemp("volatility")
    symbols = [f"A{i}" for i in range(5)] + [f"B{i}" for i in range(5)]
    days = pd.bdate_range("2026-01-05", periods=7).strftime("%Y-%m-%d").tolist()
    rng = np.random.default_rng(42); frames = []
    for symbol in symbols:
        for day in days:
            timestamp = pd.date_range(f"{day} 09:30", periods=390, freq="min", tz="America/New_York").tz_convert("UTC")
            close = 100 * np.exp(np.cumsum(rng.normal(0, .0005, 390)))
            frame = pd.DataFrame({"timestamp": timestamp, "symbol": symbol, "close": close, "trading_date": day})
            for feature in FEATURE_COLUMNS:
                if feature not in frame: frame[feature] = rng.normal(size=390)
            frames.append(frame)
    frame = pd.concat(frames, ignore_index=True)
    source = base / "bars.parquet"; features = base / "features.parquet"
    frame[["timestamp", "symbol", "close", "trading_date"]].to_parquet(source, index=False)
    frame[["timestamp", "symbol"] + FEATURE_COLUMNS].to_parquet(features, index=False)
    metadata = base / "metadata.json"
    dump(metadata, {"splits": {"train": {"start": days[0], "end": days[1]}, "validation": {"start": days[2], "end": days[4]}, "test": {"start": days[5], "end": days[6]}}})
    config = base / "config.yaml"
    config.write_text(yaml.safe_dump({"symbols": {s: {"sector": s[0]} for s in symbols}}))
    root = base / "experiment"
    prepare(root, source, features, metadata, config)
    return root, {"processed": str(source), "features": str(features), "metadata": str(metadata), "config": str(config),
                  "budget_minutes": 30., "max_epochs": 75, "search_candidates": 8,
                  "tabular_train_cap": 120, "neural_train_cap": 60, "validation_cap": 30, "holdout_cap": 60}


def test_target_contract_thresholds_use_past_only_and_test_is_masked(synthetic_panel):
    root, settings = synthetic_panel
    experiment = Experiment(root, settings)
    experiment.panels = {level: Panel(root, level) for level in ("company", "sector")}
    experiment.sequence_masks = {level: eligible_sequences(panel, 120) for level, panel in experiment.panels.items()}
    experiment.folds = chronological_folds(experiment.panels["company"].meta)
    contract, labels, training, validation = experiment.context("sector_regime", 5, 0)
    panel = contract.panel
    assert np.isnan(labels[panel.meta.split.eq("test")]).all()
    assert not panel.meta.iloc[validation].split.eq("test").any()
    assert contract.training_end < experiment.folds[0]["validation_start"]
    for sector, entry in contract.thresholds.items():
        mask = training & panel.meta.entity.eq(sector).to_numpy()
        values = panel.rv[mask, WINDOWS.index(5)]
        assert entry["high"] == pytest.approx(np.quantile(values[np.isfinite(values)], 2 / 3))


def test_large_target_contexts_are_not_retained_without_bound(synthetic_panel):
    root, settings = synthetic_panel
    experiment = Experiment(root, settings)
    experiment.panels = {level: Panel(root, level) for level in ("company", "sector")}
    experiment.sequence_masks = {level: eligible_sequences(panel, 120) for level, panel in experiment.panels.items()}
    experiment.folds = chronological_folds(experiment.panels["company"].meta)
    for horizon in (5, 15, 30): experiment.context("company_rv", horizon, 0)
    experiment.context("sector_rv", 5, 0)
    assert len(experiment.cache) == 3
    assert ("company_rv", 5, 0) not in experiment.cache


def test_oversized_forest_is_rejected_without_silently_sampling(monkeypatch):
    import psutil
    from src.ml.volatility.models import check_forest_memory, ResourceInfeasible
    monkeypatch.setattr(psutil, "virtual_memory", lambda: SimpleNamespace(available=2 * 1024 ** 3))
    params = {"n_estimators": 800, "min_samples_leaf": 1, "bootstrap": False, "max_samples": None, "max_depth": None}
    with pytest.raises(ResourceInfeasible, match="population was not reduced"):
        check_forest_memory(params, 1_000_000, "classification")


@pytest.mark.parametrize("timed", [False, True])
def test_real_tabular_fit_preserves_predictions_and_has_no_epochs(synthetic_panel, tmp_path, timed):
    root, _ = synthetic_panel
    panel = Panel(root, "company"); panel.active_task = "company_rv"
    labels = np.asarray(panel.rv[:, WINDOWS.index(5)], dtype=float)
    mask = eligible_sequences(panel, 120) & np.isfinite(labels)
    train = representative_indices(panel.meta, mask & panel.meta.split.eq("train").to_numpy(), 100)
    val = representative_indices(panel.meta, mask & panel.meta.split.eq("validation").to_numpy(), 30)
    result = fit(panel, labels, "regression", "ridge", {"alpha": 1.}, train, val, tmp_path, time.monotonic() + 30 if timed else None)
    prediction, latency = inference(panel, "regression", "ridge", {"alpha": 1.}, val, tmp_path, result)
    assert result["best_epoch"] is None
    assert result["time_cutoff_enabled"] is timed
    assert statistical("regression", labels[val], prediction)["mae"] == pytest.approx(result["best_validation_primary_metric"])
    assert latency["model_size_bytes"] > 0
    assert not (tmp_path / "_fit_features.npy").exists()


def test_batched_tabular_prediction_bounds_materialized_rows():
    from src.ml.volatility.models import predict_rows
    class Estimator:
        def __init__(self): self.maximum = 0
        def predict(self, x):
            self.maximum = max(self.maximum, len(x))
            return x[:, 0] * 2
    estimator = Estimator()
    x = np.arange(30_000, dtype="float32").reshape(10_000, 3)
    prep = {"columns": np.arange(3), "median": np.zeros(3), "mean": np.zeros(3), "scale": np.ones(3)}
    prediction = predict_rows(estimator, "regression", SimpleNamespace(x=x), np.arange(10_000), prep, chunk_size=257)
    np.testing.assert_array_equal(prediction, x[:, 0] * 2)
    assert estimator.maximum == 257


def test_shared_target_reference_is_read_only_without_dataset_copy(synthetic_panel, tmp_path):
    from src.ml.dataset import sha256
    root, _ = synthetic_panel
    dump(tmp_path / "data_reference.json", {"experiment_root": str(root), "schema_sha256": sha256(root / "data/schema.json")})
    panel = Panel(tmp_path, "company")
    assert not (tmp_path / "data").exists()
    with pytest.raises(ValueError): panel.x[0, 0] = 123


@pytest.mark.parametrize("kind", ["regression", "binary"])
def test_neural_checkpoint_epoch_and_original_scale_metrics(tmp_path, kind):
    x = np.random.default_rng(42).normal(size=(60, len(FEATURES))).astype("float32")
    panel = SimpleNamespace(x=x, active_task="company_rv" if kind == "regression" else "emergence")
    labels = np.arange(60) % 2 if kind == "binary" else np.linspace(.001, .01, 60)
    train = np.arange(3, 40); validation = np.arange(43, 60)
    params = {"lookback": 3, "hidden_size": 4, "layers": 1, "dropout": .1, "learning_rate": .001,
              "weight_decay": .00001, "batch_size": 16, "patience": 2, "architecture": "recurrent"}
    result = fit(panel, labels, kind, "gru", params, train, validation, tmp_path, time.monotonic() + 90, max_epochs=3)
    prediction, _ = inference(panel, kind, "gru", params, validation, tmp_path, result)
    from src.ml.volatility.data import load
    history = load(tmp_path / "history.json")["val_primary"]
    expected = int(np.argmin(history) if kind == "regression" else np.argmax(history)) + 1
    assert result["best_epoch"] == expected
    from src.ml.volatility.metrics import primary
    assert primary(kind, labels[validation], prediction) == pytest.approx(result["best_validation_primary_metric"])


def test_false_alarm_and_missed_event_denominators():
    metrics = binary_metrics([0, 0, 1, 1], [0, 1, 0, 1])
    assert metrics["false_positive_rate"] == .5
    assert metrics["missed_event_rate"] == .5
    assert metrics["false_positives"] == metrics["missed_events"] == 1


def test_end_to_end_temporal_selection_holdout_gate_and_validated_report(synthetic_panel):
    from src.ml.volatility.data import load
    from src.ml.volatility.reporting import finalize
    root, settings = synthetic_panel
    settings = dict(settings, budget_minutes=None, full_final_population=True, retain_tuning_checkpoints=False)
    experiment = Experiment(root, settings)
    experiment.panels = {level: Panel(root, level) for level in ("company", "sector")}
    experiment.sequence_masks = {level: eligible_sequences(panel, 120) for level, panel in experiment.panels.items()}
    experiment.folds = chronological_folds(experiment.panels["company"].meta)
    job = ("return_reg", 5, "linear")
    experiment.jobs = [job]; experiment.configs = {job: [{}]}
    with pytest.raises(RuntimeError, match="frozen validation"):
        experiment.evaluate()
    for fold in range(3):
        result = experiment.trial(job, 0, fold, time.monotonic() + 30)
        assert result is not None
        experiment.record(job, 0, result)
        assert not (root / result["directory"] / "model.joblib").exists()
        assert (root / result["directory"] / "validation_predictions.npy").exists()
    selected = rank_candidates(experiment.aggregates(job), "regression")
    selected["test_used_for_selection"] = False
    experiment.selected = [selected]
    experiment.final_fits()
    evaluations = experiment.evaluate()
    assert len(evaluations) == 1
    record = evaluations[0]
    contract, labels, training, _ = experiment.context("return_reg", 5, 3)
    full_labels = contract.values()
    valid = np.isfinite(full_labels) & contract.panel.present
    assert record["final_fit"]["training_rows"] == int((valid & contract.panel.meta.split.eq("train").to_numpy()).sum())
    assert record["final_fit"]["validation_rows"] == int((valid & contract.panel.meta.split.eq("validation").to_numpy()).sum())
    assert record["holdout_rows"] == int((valid & contract.panel.meta.split.eq("test").to_numpy()).sum())
    assert record["holdout_rows"] > settings["holdout_cap"]
    assert record["common_holdout_rows"] < record["holdout_rows"]
    assert (root / record["final_fit"]["directory"] / "model.joblib").is_file()
    finalize(experiment, evaluations)
    assert load(root / "validation.json")["valid"]
    assert load(root / "manifest.json")["planned_combinations"] == 1
    assert load(root / "manifest.json")["time_cutoff_enabled"] is False
    assert load(root / "validation.json")["full_population_prediction_rows_checked"] == record["holdout_rows"]
    assert (root / "powerbi" / "model_comparison.csv").is_file()
    assert (root / "REPORT.md").is_file()
    # Audit must reject even a single silently omitted eligible TEST row.
    from src.ml.volatility.reporting import validate_results
    prediction_path = root / record["final_fit"]["directory"] / "holdout_predictions.parquet"
    original_bytes = prediction_path.read_bytes()
    prediction_frame = pd.read_parquet(prediction_path)
    prediction_frame.iloc[:-1].to_parquet(prediction_path, index=False)
    try:
        with pytest.raises(ValueError, match="TEST population"):
            validate_results(root, evaluations)
    finally:
        prediction_path.write_bytes(original_bytes)


def test_no_time_limit_applies_to_every_experiment_stage(tmp_path):
    experiment = Experiment(tmp_path, {"budget_minutes": None})
    assert not experiment.has_time_budget
    assert np.isinf(experiment.deadline)
    for stage in (.48, .55, .70, .76, .80, .94):
        assert np.isinf(experiment.boundary(stage))


def test_training_resume_checks_plan_and_artifacts_and_reuses_fit(tmp_path):
    experiment = Experiment(tmp_path, {"budget_minutes": None})
    job = ("return_reg", 5, "ridge")
    experiment.configs = {job: [{"alpha": 1.0}]}
    planned = [{"task": "return_reg", "horizon": 5, "model": "ridge", "candidates": [{"alpha": 1.0}]}]
    dump(tmp_path / "planned_candidates.json", planned)
    directory = tmp_path / "trials" / "return_reg" / "5m" / "ridge" / "candidate_00" / "fold_0" / "seed_42"
    directory.mkdir(parents=True)
    result = {"task": "return_reg", "horizon": 5, "model": "ridge", "candidate": 0,
              "fold": 0, "parameters": {"alpha": 1.0}, "status": "COMPLETED",
              "directory": directory.relative_to(tmp_path).as_posix(), "validation_rows": 2}
    dump(directory / "result.json", result)
    np.save(directory / "validation_indices.npy", [10, 11])
    np.save(directory / "validation_predictions.npy", [0.1, 0.2])
    assert experiment.validate_training_resume(planned) == 1
    # No panels/context exist: returning this result must skip training entirely.
    assert experiment.trial(job, 0, 0, float("inf")) == result
    with pytest.raises(ValueError, match="candidate configurations changed"):
        experiment.validate_training_resume([])
    np.save(directory / "validation_predictions.npy", [0.1])
    with pytest.raises(ValueError, match="artifacts mismatch"):
        experiment.validate_training_resume(planned)


def test_training_resume_rejects_changed_settings_and_frozen_selection(tmp_path):
    dump(tmp_path / "request.json", {"budget_minutes": None})
    with pytest.raises(ValueError, match="identical experiment settings"):
        Experiment(tmp_path, {"budget_minutes": 5}).run(resume_training=True)
    dump(tmp_path / "selection_complete.json", {})
    with pytest.raises(ValueError, match="before final selection"):
        Experiment(tmp_path, {"budget_minutes": None}).run(resume_training=True)


def test_falling_validation_f1_stops_and_restores_best_weights_without_timer(tmp_path, monkeypatch):
    import tensorflow as tf
    from src.ml.volatility import models
    real_builder = models.build_neural
    captured = {}
    scores = iter([.8, .7, .6])
    monkeypatch.setattr(models, "primary", lambda *args: next(scores, .8))

    def recording_builder(*args):
        network = real_builder(*args)
        real_fit = network.fit
        class RecordWeights(tf.keras.callbacks.Callback):
            def on_epoch_end(self, epoch, logs=None):
                captured.setdefault("epochs", []).append([weight.copy() for weight in self.model.get_weights()])
        def recording_fit(*args, **kwargs):
            kwargs["callbacks"] = list(kwargs["callbacks"]) + [RecordWeights()]
            history = real_fit(*args, **kwargs)
            captured["restored"] = network.get_weights()
            return history
        network.fit = recording_fit
        return network
    monkeypatch.setattr(models, "build_neural", recording_builder)
    panel = SimpleNamespace(x=np.random.default_rng(42).normal(size=(60, len(FEATURES))).astype("float32"), active_task="emergence")
    params = {"lookback": 3, "hidden_size": 4, "layers": 1, "dropout": 0., "learning_rate": .001,
              "weight_decay": 0., "batch_size": 16, "patience": 2, "architecture": "recurrent"}
    result = fit(panel, np.arange(60) % 2, "binary", "gru", params, np.arange(3, 40), np.arange(43, 60), tmp_path, None, max_epochs=75)
    assert result["time_cutoff_enabled"] is False
    assert result["best_epoch"] == 1
    assert result["epochs_trained"] == 3
    assert result["stopping_reason"] == "early_stopping_no_validation_improvement"
    checkpoint = tf.keras.models.load_model(tmp_path / "best.keras", compile=False)
    for first, restored, saved in zip(captured["epochs"][0], captured["restored"], checkpoint.get_weights()):
        np.testing.assert_allclose(first, restored)
        np.testing.assert_allclose(first, saved)
    assert any(not np.array_equal(a, b) for a, b in zip(captured["epochs"][0], captured["epochs"][-1]))
