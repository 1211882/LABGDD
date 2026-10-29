from __future__ import annotations

import shutil
import time
from pathlib import Path

import numpy as np
import pandas as pd

from src.ml.dataset import sha256
from src.ml.volatility.data import FEATURES, Panel, dump, load, eligible_sequences
from src.ml.volatility.metrics import statistical


def close(actual, expected):
    if isinstance(expected, dict):
        return all(key in actual and close(actual[key], value) for key, value in expected.items())
    if isinstance(expected, list):
        return len(actual) == len(expected) and all(close(a, b) for a, b in zip(actual, expected))
    if isinstance(expected, (float, int)) and not isinstance(expected, bool):
        return bool(np.isclose(actual, expected, atol=1e-7, rtol=1e-5))
    return actual == expected


def validate_results(root: Path, evaluations):
    marker = load(root / "selection_complete.json")
    if marker["test_used_for_selection"]:
        raise ValueError("TEST selection provenance invalid")
    rows = 0; epochs = 0
    panels = {}; sequence_masks = {}
    full_rows_checked = 0
    reference = root / "data_reference.json"
    if reference.is_file():
        description = load(reference)
        source = Path(description["experiment_root"]) / "data"
        for name, expected in description["checksums"].items():
            if sha256(source / name) != expected: raise ValueError(f"Referenced target cache changed: {name}")
    for record in evaluations:
        fitted = record["final_fit"]
        directory = root / fitted["directory"]
        frame = pd.read_parquet(directory / "holdout_predictions.parquet")
        if set(frame.split.astype(str)) != {"test"} or frame.duplicated(["entity", "timestamp"]).any():
            raise ValueError("Invalid holdout keys/split")
        kind = "regression" if record["task"] in {"return_reg", "company_rv", "sector_rv"} else ("binary" if record["task"] == "emergence" else "classification")
        if kind == "regression":
            prediction = frame.predicted.to_numpy()
        else:
            columns = [c for c in frame if c.startswith("probability_")]
            prediction = frame[columns].to_numpy()
            if not np.allclose(prediction.sum(axis=1), 1., atol=1e-5):
                raise ValueError("Invalid probability vectors")
        if record.get("holdout_population") == "all_eligible_selected_model":
            from src.ml.volatility.targets import Contract, TASKS
            level = TASKS[record["task"]][0]
            for name in {level, "company"}:
                if name not in panels: panels[name] = Panel(root, name)
            panel = panels[level]
            training = panel.meta.split.eq("train").to_numpy()
            contract = Contract(panel, panels["company"], record["task"], record["horizon"], training)
            values = contract.values()
            eligible = np.isfinite(values) & panel.present
            if record["model"] in {"lstm", "gru"}:
                key = (level, record["parameters"]["lookback"])
                if key not in sequence_masks: sequence_masks[key] = eligible_sequences(panel, key[1])
                eligible &= sequence_masks[key]
            expected = np.flatnonzero(eligible & panel.meta.split.eq("test").to_numpy())
            if not np.array_equal(frame.row_id.to_numpy(), expected): raise ValueError("Final TEST population was sampled or otherwise incomplete")
            if not np.allclose(frame.actual.to_numpy(), values[expected]): raise ValueError("Holdout targets do not match the frozen contract")
            if fitted["training_rows"] != int((eligible & training).sum()): raise ValueError("Final training population was capped")
            if fitted["validation_rows"] != int((eligible & panel.meta.split.eq("validation").to_numpy()).sum()): raise ValueError("Final validation population was capped")
            import joblib
            prep = joblib.load(directory / "preprocessing.joblib")
            if prep.get("fit_rows") != fitted["training_rows"]: raise ValueError("Final preprocessing did not use all eligible training endpoints")
            common_key = (level, 120)
            if common_key not in sequence_masks: sequence_masks[common_key] = eligible_sequences(panel, 120)
            common = sequence_masks[common_key][expected]
            if int(common.sum()) != record["common_holdout_rows"]: raise ValueError("Common holdout row count mismatch")
            if common.any() and not close(statistical(kind, frame.actual.to_numpy()[common], prediction[common]), record["common_holdout_metrics"]):
                raise ValueError("Common holdout metrics cannot be reproduced")
            full_rows_checked += len(expected)
        metrics = statistical(kind, frame.actual.to_numpy(), prediction)
        if record["task"] in {"company_rv", "sector_rv"}: metrics["volatility_rank_ic"] = metrics["spearman"]
        if not close(metrics, record["holdout_metrics"]):
            raise ValueError("Holdout metrics cannot be reproduced")
        if record["folds_completed"] != 3 or record["test_used_for_selection"]:
            raise ValueError("Selected model did not complete chronological validation")
        if record["model"] in {"lstm", "gru"}:
            history = load(directory / "history.json")
            values = history["val_primary"]
            best = int(np.argmin(values) if kind == "regression" else np.argmax(values)) + 1
            if best != fitted["best_epoch"] or len(values) != fitted["epochs_trained"]:
                raise ValueError("Best epoch does not match checkpoint history")
            if not close(values[best - 1], fitted["best_validation_primary_metric"]):
                raise ValueError("Checkpoint score mismatch")
            epochs += 1
        monitor = record["monitoring"]
        if monitor.get("applicable"):
            if sum(monitor[k] for k in ("true_positives", "false_positives", "missed_events", "true_negatives")) != len(frame):
                raise ValueError("Monitoring confusion counts inconsistent")
        if not (directory / ("best.keras" if record["model"] in {"lstm", "gru"} else "model.joblib")).is_file():
            raise ValueError("Selected model artifact missing")
        rows += len(frame)
    return {"valid": True, "evaluated_model_horizons": len(evaluations), "prediction_rows": rows,
            "full_population_prediction_rows_checked": full_rows_checked,
            "neural_epoch_histories_checked": epochs, "test_used_for_selection": False}


def choose(selected, tasks):
    pool = [r for r in selected if r["task"] in tasks]
    if not pool: return None
    regression = all(task in {"return_reg", "company_rv", "sector_rv"} for task in tasks)
    if regression:
        pool = [r for r in pool if r.get("validation_baseline_skill") is not None]
        if not pool: return None
        best = max(pool, key=lambda r: r["validation_baseline_skill"])
        tolerance = best.get("validation_skill_std", 0.) / np.sqrt(3)
        tied = [r for r in pool if best["validation_baseline_skill"] - r["validation_baseline_skill"] <= tolerance + 1e-12]
        chosen = min(tied, key=lambda r: (r["validation_std"], r["complexity"]))
        criterion = "mean validation MAE skill versus task-specific baseline; not raw MAE across horizons"
    else:
        best = max(pool, key=lambda r: r["validation_mean"])
        tied = [r for r in pool if best["validation_mean"] - r["validation_mean"] <= best["validation_std"] / np.sqrt(3) + 1e-12]
        chosen = min(tied, key=lambda r: ((r.get("validation_false_alarm_rate") or 0.) if tasks == {"emergence"} else 0., r["validation_std"], r["complexity"]))
        criterion = "mean chronological validation event F1" if tasks == {"emergence"} else "mean chronological validation Macro F1"
    return {"task": chosen["task"], "model": chosen["model"], "horizon": chosen["horizon"],
            "criterion": criterion, "validation_mean": chosen["validation_mean"],
            "validation_std": chosen["validation_std"], "baseline_skill": chosen.get("validation_baseline_skill"),
            "status": "provisional finite-search choice", "chosen_using_test": False}


def finalize(experiment, evaluations):
    root = experiment.root
    dump(root / "evaluations.json", evaluations)
    candidates = [r for job in experiment.jobs for r in experiment.aggregates(job)]
    dump(root / "optimization_results.json", candidates)
    flat = []
    for record in evaluations:
        fitted = record["final_fit"]
        def primary_value(metrics):
            return metrics.get("mae", metrics.get("event", {}).get("f1", metrics.get("macro_f1"))) if metrics else None
        flat.append({"task": record["task"], "model": record["model"], "horizon": record["horizon"],
            "model_id": f"phase14__{record['task']}__{record['model']}", "validation_mean": record["validation_mean"],
            "validation_std": record["validation_std"], "validation_baseline_skill": record.get("validation_baseline_skill"),
            "best_epoch": fitted["best_epoch"], "epochs_trained": fitted["epochs_trained"],
            "training_seconds": fitted["training_duration_seconds"], "complexity": fitted["complexity"],
            "final_training_rows": fitted["training_rows"], "final_validation_rows": fitted["validation_rows"],
            "holdout_rows": record["holdout_rows"], "holdout_population": record.get("holdout_population", "sampled"),
            "common_holdout_rows": record.get("common_holdout_rows"),
            "full_holdout_primary_metric": primary_value(record["holdout_metrics"]),
            "common_holdout_primary_metric": primary_value(record.get("common_holdout_metrics")),
            **{k: v for k, v in record["holdout_metrics"].items() if not isinstance(v, (dict, list))},
            **{f"common_{k}": v for k, v in (record.get("common_holdout_metrics") or {}).items() if not isinstance(v, (dict, list))},
            "model_size_bytes": record["inference"]["model_size_bytes"],
            "seconds_per_observation_amortized": record["inference"]["seconds_per_observation_amortized_including_load"]})
    pd.DataFrame(flat).to_csv(root / "model_comparison.csv", index=False)
    choices = {
        "best_return_classifier": choose(experiment.selected, {"return_class"}),
        "best_return_regressor": choose(experiment.selected, {"return_reg"}),
        "best_company_volatility_model": choose(experiment.selected, {"company_rv"}),
        "best_sector_volatility_model": choose(experiment.selected, {"sector_rv"}),
        "best_emergence_detector": choose(experiment.selected, {"emergence"}),
    }
    emergence = choices["best_emergence_detector"]
    choices["best_for_research_question"] = emergence if emergence and emergence["validation_mean"] > 0 and emergence.get("baseline_skill", -1) is not None and emergence.get("baseline_skill", -1) > 0 else None
    dump(root / "model_choices.json", choices)
    selected_keys = {(r["task"], r["horizon"], r["model"]) for r in experiment.selected}
    evaluated_keys = {(r["task"], r["horizon"], r["model"]) for r in evaluations}
    omissions = [{"task": j[0], "horizon": j[1], "model": j[2], "status": "NO_THREE_FOLD_SELECTION" if j not in selected_keys else "HOLDOUT_NOT_COMPLETED"} for j in experiment.jobs if j not in evaluated_keys]
    dump(root / "omissions.json", omissions)
    validation = validate_results(root, evaluations)
    dump(root / "validation.json", validation)
    lines = ["# Phase 14: volatility-aligned model research", "",
        "Return models are secondary. Sector realized volatility and coordinated emergence address the primary research question. Company volatility supports constituent-level interpretation.", "",
        (f"Time-bounded staged run: {experiment.settings['budget_minutes']} minutes." if experiment.settings.get('budget_minutes') is not None else "Validation-stopped staged run: no wall-clock cutoff. Neural models restore their best validation checkpoint and retain a maximum epoch ceiling."),
        f"{len(experiment.selected)}/{len(experiment.jobs)} model/horizon configurations completed three-fold selection; {len(evaluations)} received final holdout evaluation.", "",
        "All model choices below use chronological validation only. Historical TEST has already been examined in earlier phases; it is isolated during this selection but is not a new untouched dataset. Best means best tested within this finite search. Incomplete searches and omitted comparisons remain explicit.", "",
        "## Validation-selected choices", ""]
    for name, choice in choices.items():
        label = name.replace("_", " ").capitalize()
        lines.append(f"- {label}: {choice['model']} / {choice['horizon']}m ({choice['task']}); provisional." if choice else f"- {label}: not established by the available validated evidence.")
    lines += ["", "## Measured model results", "", "| Task | Model | H | CV mean | CV std | Best epoch | Holdout |", "|---|---|---:|---:|---:|---:|---:|"]
    for record in evaluations:
        metric = record["holdout_metrics"].get("mae", record["holdout_metrics"].get("event", {}).get("f1", record["holdout_metrics"].get("macro_f1")))
        lines.append(f"| {record['task']} | {record['model']} | {record['horizon']} | {record['validation_mean']:.6g} | {record['validation_std']:.6g} | {record['final_fit']['best_epoch'] or 'N/A'} | {metric:.6g} |")
    lines += ["", "## Scientific answers and limits", "",
        "1. Existing frozen return evidence favors Phase 10 Random Forest by Macro F1. Phase 11 returns generally fail to beat zero return on common keys. New provisional winners are listed above and in model_choices.json.",
        "2. Additional epochs are useful only where validation improves beyond the earlier four-epoch ceiling. Inspect each history, exact best epoch, epochs trained, and undertraining/overfitting flags; an aggregate deep-learning advantage is not assumed.",
        "3. Exact best neural epochs and every selected parameter set are in selected_configurations.json, evaluations.json, and per-trial result/history files. Forests and linear models have no epochs.",
        "4. Fair tuning compares identical validation/holdout endpoints within each task/horizon and uses expanding folds. One-fold screened candidates cannot become winners. Different tasks' MAE values are not directly comparable.",
        "5. Whether Random Forest beats deep learning must be assessed separately per task/horizon using validation means, fold variability, coverage, and available seed replications. Missing or failed fits cannot be treated as losses.",
        "6. Volatility-versus-return predictability must be compared through skill against task-specific baselines on identical keys, not through raw errors in different target distributions. Those comparisons are persisted for every evaluated model.",
        "7. High-volatility and emergence reliability is measured by precision, recall, F1, false positives, misses, observable episode detection, and sector breakdowns. A positive F1 alone does not establish operational reliability.",
        "8. Emergence is an exploratory transparent conjunction of elevated sector RV, constituent majority, positive correlation, and non-extreme concentration. It is not an externally validated economic-event taxonomy.",
        "9. Coefficients, forest importance, and limited validation permutation checks are descriptive, not causal. feature_ablation.csv records fixed-reference, identical-population, validation-only removals; unmeasured comparisons are not zero effects.",
        "10. The sector-feature ablation compares stock-only with stock-plus-sector inputs on the same keys. Its sign must be interpreted by task (lower MAE or higher F1). Feature groups are not selected on TEST.",
        "11. Horizon comparisons use task-appropriate validation metrics and baseline skill. Completion lead time equals H by construction; early economic-event onset is not observed. No causal forecasting claim is made.",
        "12. A final research-model recommendation is provisional and requires validation monitoring skill beyond persistence. Prediction, financial signals, and monitoring outcomes are kept separate; none is combined into an arbitrary score.", "",
        "## Training coverage, analytics, and streaming feasibility", "",
        ("Tuning uses large representative samples; final training, validation, and TEST use every eligible endpoint for each selected model and lookback. Tabular models use all valid rows; neural models require their own exact contiguous lookback. Common 120-bar holdout metrics are reported separately for fair cross-model comparison. Per-final-fit training_coverage.json must show 100% use of eligible endpoints. Preprocessing uses all final training endpoints, with column-wise medians, batched moments and disk-backed feature matrices. No prices or targets are interpolated." if experiment.settings.get("full_final_population") else "training_coverage.csv and training_coverage_distribution.csv audit eligible and used endpoints by lookback, symbol/entity, sector, and date. Small caps remain a pilot; common 120-bar eligibility excludes sparse symbols/windows."), "",
        "The tuning context cache is bounded. Histories, predictions, parameters, metrics, and checkpoint hashes are retained for all completed trials; temporary tuning binaries may be removed after best-checkpoint validation to conserve disk. All final models and preprocessing artifacts are retained. Resource-infeasible forest candidates are recorded as unmeasured failures rather than silently trained on smaller data. A forest's configured bootstrap/max_samples remains part of its algorithm even when all rows are eligible.", "",
        "data/analytics_*.parquet and sector_daily_analytics.parquet retain 1/5/15/30/60-minute synchronization, signed/absolute correlation, dispersion and variance-share concentration. One-minute within-window correlation is undefined. inter_sector_correlation.csv uses complete fixed-universe equal-weight minute returns. These are historical descriptive analyses, not live-runtime equivalence evidence.", "",
        "Inference timing includes checkpoint loading and batching and is amortized per observation. Process RSS includes framework/data caches. Model size and observed memory support feasibility assessment, not production readiness or a live latency guarantee. Arrival timestamps, watermark completion, and an equivalent online feature path have not been runtime verified.", "",
        "Financial outputs apply only to return tasks, using sampled non-overlapping windows, 5 bps position-change costs, day/gap liquidation, and timestamp-level equal weighting. Sharpe/Sortino are sample diagnostics. No trading policy is invented for volatility alerts.", "",
        ("Forecasts cover all model-eligible TEST endpoints." if experiment.settings.get("full_final_population") else "Forecasts are sampled.") + " Alarms/day and episode detection describe eligible observations, not uninterrupted full-day deployment. First-alert tables explicitly label nominal window-completion lead; no unmeasured ingestion delay is subtracted.", "",
        "## Artifacts", "",
        "- research audit: reports/PHASE14_RESEARCH_ALIGNMENT.md",
        "- configurations/training: optimization_results.json, selected_configurations.json, trials/",
        "- robustness: temporal_folds.json, seed_robustness.json, feature_ablation.csv",
        "- measured results: evaluations.json, model_comparison.csv, model_choices.json",
        "- incompleteness: omissions.json, progress.json and explicit trial failures",
        "- validation and preservation: validation.json, preserved_evidence.json, manifest.json",
        "- validated Power BI extension: powerbi/ (separate from the prior frozen mart)",
        "- shared immutable target cache: data_reference.json when present; source arrays are opened read-only", ""]
    (root / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")
    # Only validated metrics enter the additional mart.
    powerbi = root / "powerbi"; powerbi.mkdir(exist_ok=True)
    comparison = pd.DataFrame(flat)
    comparison.to_csv(powerbi / "model_comparison.csv", index=False)
    if len(comparison):
        comparison.loc[comparison.task.isin(["company_rv", "sector_rv", "company_regime", "sector_regime"])].to_csv(powerbi / "volatility_model_metrics.csv", index=False)
        comparison.loc[comparison.task.eq("emergence")].to_csv(powerbi / "sector_emergence_metrics.csv", index=False)
    monitor_rows = [{"task": r["task"], "model": r["model"], "horizon": r["horizon"], **{k: v for k, v in r["monitoring"].items() if not isinstance(v, (dict, list))}} for r in evaluations if r["monitoring"].get("applicable")]
    pd.DataFrame(monitor_rows).to_csv(powerbi / "event_detection_metrics.csv", index=False)
    pd.DataFrame([r for r in experiment.ablations if r["status"] == "MEASURED"]).to_csv(powerbi / "feature_ablation.csv", index=False)
    (powerbi / "README.md").write_text("Validated Phase 14 extension. Join model facts by task-scoped model_id plus horizon; event/ablation facts by task, model, horizon. Do not sum performance metrics across tasks/populations. Use Sector Volatility Monitor, Emergent Events, Synchronization, Concentration, Correlation, Monitoring Performance, and Final Selection pages. Existing Phase 12 files are preserved. Historical analytics are under ../data/. No PBIX is generated.\n", encoding="utf-8")
    code = root / "source"; code.mkdir(exist_ok=True)
    for source in Path("src/ml/volatility").glob("*.py"):
        shutil.copy2(source, code / source.name)
    shutil.copy2(Path("src/ml/run_phase14.py"), code / "run_phase14.py")
    checksums = {p.relative_to(root).as_posix(): sha256(p) for p in root.rglob("*") if p.is_file() and p.name != "manifest.json" and p.suffix != ".log"}
    dump(root / "manifest.json", {"experiment": root.name, "status": "validated_full_final_population" if experiment.settings.get("full_final_population") else ("validated_bounded_pilot" if experiment.settings.get("budget_minutes") is not None else "validated_sample_capped_experiment"), "selected_combinations": len(experiment.selected),
        "evaluated_combinations": len(evaluations), "planned_combinations": len(experiment.jobs),
        "elapsed_seconds": time.monotonic() - experiment.started, "budget_minutes": experiment.settings["budget_minutes"],
        "time_cutoff_enabled": experiment.settings.get("budget_minutes") is not None,
        "full_final_population": experiment.settings.get("full_final_population", False),
        "omitted_combinations": len(omissions), "validation": validation, "checksums": checksums,
        "selection_used_test": False, "new_untouched_holdout": False, "previous_evidence_preserved": True})
