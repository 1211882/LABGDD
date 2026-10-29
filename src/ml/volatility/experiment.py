from __future__ import annotations

import gc
import json
import os
import shutil
import time
from collections import OrderedDict
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from src.ml.dataset import sha256
from src.ml.volatility.data import FEATURES, HORIZONS, Panel, dump, eligible_sequences, load, prepare, representative_indices, resolve_data_root
from src.ml.volatility.metrics import financial, monitoring, primary, statistical
from src.ml.volatility.models import BudgetExpired, FEATURE_GROUPS, NEURAL, SPACES, fit, inference, search, transform
from src.ml.volatility.targets import Contract, TASKS


def evidence_inventory():
    result = {}
    for base in (Path("models/classification"), Path("models/regression"), Path("models/tuning")):
        for path in base.rglob("*"):
            if path.is_file():
                stat = path.stat()
                entry = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}
                if path.suffix.lower() in {".json", ".csv", ".md"}:
                    entry["sha256"] = sha256(path)
                result[path.as_posix()] = entry
    return result


def chronological_folds(meta):
    dates = meta.trading_date.astype(str).to_numpy()
    train_dates = sorted(set(dates[meta.split.eq("train")]))
    validation = sorted(set(dates[meta.split.eq("validation")]))
    if len(validation) < 3:
        raise ValueError("At least three validation dates are required")
    folds = []
    previous = list(train_dates)
    for i, window in enumerate(np.array_split(validation, 3)):
        window = list(window)
        folds.append({"fold": i, "train_end": previous[-1], "validation_start": window[0], "validation_end": window[-1]})
        previous.extend(window)
    return folds


def rank_candidates(records, kind):
    eligible = [r for r in records if r.get("folds_completed") == 3]
    if not eligible: return None
    best = (min if kind == "regression" else max)(eligible, key=lambda r: r["validation_mean"])
    tolerance = best["validation_std"] / np.sqrt(3)
    tied = [r for r in eligible if abs(r["validation_mean"] - best["validation_mean"]) <= tolerance + 1e-12]
    # One-standard-error rule; actual fitted complexity rather than tree count alone.
    return sorted(tied, key=lambda r: (r["complexity"], r["validation_std"], r["candidate"]))[0]


class Experiment:
    def __init__(self, root: Path, settings: dict):
        self.root, self.settings = root, settings
        self.started = time.monotonic()
        self.has_time_budget = settings.get("budget_minutes") is not None
        self.deadline = self.started + settings["budget_minutes"] * 60 if self.has_time_budget else float("inf")
        self.cache = OrderedDict(); self.results = {}; self.failures = []
        self.lookback_masks = {}
        self.selected = []; self.ablations = []; self.seed_results = []; self.final = []

    def boundary(self, fraction):
        return self.started + self.settings["budget_minutes"] * 60 * fraction if self.has_time_budget else float("inf")

    def context(self, task, horizon, fold):
        key = (task, horizon, fold)
        if key not in self.cache:
            panel = self.panels[TASKS[task][0]]
            spec = self.folds[fold] if fold < 3 else {"train_end": panel.schema["splits"]["train"]["end"],
                "validation_start": panel.schema["splits"]["validation"]["start"], "validation_end": panel.schema["splits"]["validation"]["end"]}
            dates = panel.meta.trading_date.astype(str).to_numpy()
            training = dates <= spec["train_end"]
            validation = (dates >= spec["validation_start"]) & (dates <= spec["validation_end"])
            if panel.meta.loc[training, "timestamp"].max() >= panel.meta.loc[validation, "timestamp"].min():
                raise ValueError("Temporal fold overlap")
            contract = Contract(panel, self.panels["company"], task, horizon, training)
            labels = contract.values()
            labels = labels.copy()
            labels[panel.meta.split.eq("test").to_numpy()] = np.nan
            valid = np.isfinite(labels)
            common = self.sequence_masks[TASKS[task][0]] & valid
            val = representative_indices(panel.meta, common & validation, self.settings["validation_cap"])
            if not len(val): raise ValueError("No complete validation endpoints at common 120-bar lookback")
            self.cache[key] = (contract, labels, training & valid & panel.present, val)
            while len(self.cache) > 3:
                self.cache.popitem(last=False)
        self.cache.move_to_end(key)
        return self.cache[key]

    def model_sequence_mask(self, panel, lookback):
        key = (panel.level, lookback)
        if key not in self.lookback_masks:
            self.lookback_masks[key] = self.sequence_masks[panel.level] if lookback == 120 else eligible_sequences(panel, lookback)
        return self.lookback_masks[key]

    def train_indices(self, panel, training, neural, *, full=False, lookback=120):
        cap = None if full else (self.settings["neural_train_cap"] if neural else self.settings["tabular_train_cap"])
        # Tuning uses shared 120-bar endpoints. Final training uses every endpoint
        # valid for the independently selected lookback, without a sample cap.
        eligible = training & self.model_sequence_mask(panel, lookback if full else 120) if neural else training
        indices = representative_indices(panel.meta, eligible, cap)
        if len(indices) < 20: raise ValueError("Fewer than 20 eligible training endpoints")
        return indices

    def trial(self, job, candidate, fold, cutoff, *, seed=42, columns=None, suffix=None):
        task, horizon, model = job
        params = self.configs[job][candidate]
        directory = self.root / "trials" / task / f"{horizon}m" / model / f"candidate_{candidate:02d}" / f"fold_{fold}" / f"seed_{seed}"
        if suffix: directory = directory / suffix
        result_path = directory / "result.json"
        if result_path.exists(): return load(result_path)
        if time.monotonic() >= cutoff: return None
        try:
            contract, labels, training, val = self.context(task, horizon, fold)
            panel = contract.panel; panel.active_task = task
            full = fold == 3 and self.settings.get("full_final_population", False)
            if full:
                eligible_val = panel.meta.split.eq("validation").to_numpy() & np.isfinite(labels) & panel.present
                if model in NEURAL: eligible_val &= self.model_sequence_mask(panel, params["lookback"])
                val = np.flatnonzero(eligible_val)
            train = self.train_indices(panel, training, model in NEURAL, full=full, lookback=params.get("lookback", 120))
            print(f"FIT {task}/{horizon}m/{model} stage={'FINAL_FULL' if full else 'TUNING'} c={candidate} fold={fold} seed={seed} n={len(train)}/{len(val)} {suffix or ''}", flush=True)
            result = fit(panel, labels, contract.kind, model, params, train, val, directory, cutoff if self.has_time_budget else None,
                         max_epochs=self.settings["max_epochs"], seed=seed, columns=columns)
            result.update({"task": task, "horizon": horizon, "model": model, "candidate": candidate, "fold": fold,
                           "directory": directory.relative_to(self.root).as_posix(), "contract": contract.describe(),
                           "population_policy": "all_eligible_selected_model" if full else "representative_tuning_sample"})
            np.save(directory / "validation_indices.npy", val)
            baseline = contract.baseline()[val]
            comparable = np.isfinite(baseline)
            prediction = np.load(directory / "validation_predictions.npy")
            if comparable.any():
                base_score = primary(contract.kind, labels[val][comparable], baseline[comparable])
                model_score = primary(contract.kind, labels[val][comparable], prediction[comparable])
                result["baseline_validation_primary"] = base_score
                result["baseline_comparison_rows"] = int(comparable.sum())
                result["validation_skill"] = 1 - model_score / base_score if contract.kind == "regression" and base_score > 0 else (model_score - base_score if contract.kind != "regression" else None)
            sector_scores = []
            for sector, positions in panel.meta.iloc[val].groupby("sector", observed=True).indices.items():
                sector_scores.append({"sector": str(sector), "rows": len(positions), "primary": primary(contract.kind, labels[val][positions], prediction[positions])})
            result["validation_sector_scores"] = sector_scores
            if fold == 3:
                eligible_training = training & self.model_sequence_mask(panel, params["lookback"] if full else 120) if model in NEURAL else training
                coverage = {"eligible_training_endpoints": int(eligible_training.sum()), "used": len(train),
                            "percentage_used": 100 * len(train) / eligible_training.sum()}
                coverage["groups"] = {}
                for group in ("entity", "sector", "trading_date"):
                    eligible_counts = panel.meta.loc[eligible_training].groupby(group, observed=False).size()
                    sampled = panel.meta.iloc[train].groupby(group, observed=False).size()
                    coverage["groups"][group] = {str(key): {"eligible": int(count), "used": int(sampled.get(key, 0))} for key, count in eligible_counts.items()}
                dump(directory / "training_coverage.json", coverage)
            dump(result_path, result)
            if fold < 3 and not self.settings.get("retain_tuning_checkpoints", True):
                archived = {}
                for name in ("model.joblib", "best.keras", "preprocessing.joblib"):
                    path = directory / name
                    if path.is_file():
                        archived[name] = {"sha256": sha256(path), "bytes": path.stat().st_size}
                        if path.resolve().parent != directory.resolve(): raise RuntimeError("Unexpected tuning artifact path")
                        path.unlink()
                result["tuning_checkpoints_removed_after_validation"] = archived
                dump(result_path, result)
            print(f"SCORE {task}/{horizon}m/{model} c={candidate} fold={fold} score={result['best_validation_primary_metric']:.7f} epoch={result['best_epoch']}", flush=True)
            return result
        except BudgetExpired:
            self.failures.append({"job": list(job), "candidate": candidate, "fold": fold, "status": "BUDGET_INTERRUPTED"})
            return None
        except Exception as exc:
            failure = {"job": list(job), "candidate": candidate, "fold": fold, "status": "FAILED", "error": f"{type(exc).__name__}: {exc}"}
            self.failures.append(failure); dump(directory / "failure.json", failure)
            print(f"FAILED {failure}", flush=True)
            return None
        finally:
            gc.collect()

    def record(self, job, candidate, result):
        if result is not None:
            self.results.setdefault((job, candidate), {})[result["fold"]] = result
        if len(self.results) % 5 == 0:
            self.persist_progress()

    def aggregates(self, job):
        rows = []
        for candidate, params in enumerate(self.configs[job]):
            folds = self.results.get((job, candidate), {})
            if not folds: continue
            metrics = [r["best_validation_primary_metric"] for r in folds.values()]
            rows.append({"task": job[0], "horizon": job[1], "model": job[2], "candidate": candidate,
                         "parameters": params, "folds_completed": len(folds), "validation_mean": float(np.mean(metrics)),
                         "validation_std": float(np.std(metrics, ddof=1)) if len(metrics) > 1 else 0.,
                         "validation_best": float(min(metrics) if TASKS[job[0]][1] == "regression" else max(metrics)),
                         "validation_worst": float(max(metrics) if TASKS[job[0]][1] == "regression" else min(metrics)),
                         "complexity": int(np.mean([r["complexity"] for r in folds.values()])),
                         "training_seconds": float(sum(r["training_duration_seconds"] for r in folds.values()))})
            skills = [r["validation_skill"] for r in folds.values() if r.get("validation_skill") is not None]
            rows[-1]["validation_baseline_skill"] = float(np.mean(skills)) if len(skills) == len(folds) else None
            rows[-1]["validation_skill_std"] = float(np.std(skills, ddof=1)) if len(skills) > 1 else 0.
            alarms = [r["validation_metrics"].get("event", {}).get("false_positive_rate") for r in folds.values()]
            alarms = [v for v in alarms if v is not None]
            rows[-1]["validation_false_alarm_rate"] = float(np.mean(alarms)) if alarms else None
        return rows

    def persist_progress(self):
        dump(self.root / "progress.json", {"elapsed_seconds": time.monotonic() - self.started,
             "completed_fits": sum(len(r) for r in self.results.values()), "failures": self.failures,
             "candidates": [row for job in self.jobs for row in self.aggregates(job)]})

    def coverage(self):
        rows = []; distributions = []
        for level, panel in self.panels.items():
            train = panel.meta.split.eq("train").to_numpy()
            for lookback in (15, 30, 60, 90, 120):
                sequences = eligible_sequences(panel, lookback)
                for column, horizon in ((1, 5), (2, 15), (3, 30)):
                    eligible = train & np.isfinite(panel.rv[:, column]) & sequences
                    selected = representative_indices(panel.meta, eligible, self.settings["neural_train_cap"])
                    print(f"COVERAGE {level} horizon={horizon}m lookback={lookback} eligible={int(eligible.sum())} sampled={len(selected)}", flush=True)
                    rows.append({"level": level, "horizon": horizon, "lookback": lookback, "eligible": int(eligible.sum()), "sampled": len(selected),
                                 "percentage_used": 100 * len(selected) / eligible.sum() if eligible.sum() else 0.,
                                 "stage": "tuning", "final_training_policy": "all_eligible_selected_model" if self.settings.get("full_final_population") else "sampled"})
                    for group in ("entity", "sector", "trading_date"):
                        eligible_counts = panel.meta.loc[eligible].groupby(group, observed=False).size()
                        used = panel.meta.iloc[selected].groupby(group, observed=False).size()
                        for key, count in eligible_counts.items():
                            distributions.append({"level": level, "horizon": horizon, "lookback": lookback, "group": group, "key": str(key),
                                                  "eligible": int(count), "sampled": int(used.get(key, 0))})
        pd.DataFrame(rows).to_csv(self.root / "training_coverage.csv", index=False)
        pd.DataFrame(distributions).to_csv(self.root / "training_coverage_distribution.csv", index=False)

    def optimize(self):
        # Every model/horizon receives a three-fold reference before additional
        # randomized screening. A time budget is optional, not the default.
        for fold in range(3):
            for job in self.jobs:
                if time.monotonic() >= self.boundary(.48): break
                self.record(job, 0, self.trial(job, 0, fold, self.boundary(.48)))
        for candidate in range(1, max(map(len, self.configs.values()))):
            for job in self.jobs:
                if time.monotonic() >= self.boundary(.55): break
                if candidate < len(self.configs[job]):
                    self.record(job, candidate, self.trial(job, candidate, 0, self.boundary(.55)))
        # Promote the two best screening configurations, then require all three
        # folds for final selection. One-fold winners never replace robust fits.
        for job in self.jobs:
            rows = self.aggregates(job)
            rows.sort(key=lambda row: row["validation_mean"], reverse=TASKS[job[0]][1] != "regression")
            for row in rows[:2]:
                for fold in range(3):
                    if time.monotonic() >= self.boundary(.70): break
                    self.record(job, row["candidate"], self.trial(job, row["candidate"], fold, self.boundary(.70)))
            winner = rank_candidates(self.aggregates(job), TASKS[job[0]][1])
            if winner:
                winner["selection_rule"] = "three-fold mean, one-standard-error near-tie rule, then lower fitted complexity and fold std"
                winner["test_used_for_selection"] = False
                self.selected.append(winner)
        self.persist_progress()

    def robustness_and_ablation(self):
        # Prioritize the actual research targets for multi-seed robustness.
        for task in ("emergence", "sector_rv", "company_rv", "sector_regime", "company_regime", "return_class", "return_reg"):
            for horizon in HORIZONS:
                neural = [r for r in self.selected if r["task"] == task and r["horizon"] == horizon and r["model"] in NEURAL]
                if not neural: continue
                kind = TASKS[task][1]
                chosen = sorted(neural, key=lambda r: r["validation_mean"], reverse=kind != "regression")[0]
                job = (task, horizon, chosen["model"])
                values = []
                for seed in (42, 43, 44):
                    scores = []
                    for fold in range(3):
                        result = self.trial(job, chosen["candidate"], fold, self.boundary(.76), seed=seed)
                        if result: scores.append(result["best_validation_primary_metric"])
                    if len(scores) == 3:
                        values.append({"seed": seed, "mean": float(np.mean(scores)), "fold_std": float(np.std(scores, ddof=1))})
                self.seed_results.append({"task": task, "horizon": horizon, "model": chosen["model"], "seeds": values,
                    "complete_three_seeds": len(values) == 3, "mean": float(np.mean([v["mean"] for v in values])) if values else None,
                    "std": float(np.std([v["mean"] for v in values], ddof=1)) if len(values) > 1 else None,
                    "best": (min if kind == "regression" else max)([v["mean"] for v in values]) if values else None,
                    "worst": (max if kind == "regression" else min)([v["mean"] for v in values]) if values else None})
        # Fixed interpretable reference per task/horizon; no TEST-based feature selection.
        for task in TASKS:
            for horizon in HORIZONS:
                candidates = [r for r in self.selected if r["task"] == task and r["horizon"] == horizon and r["model"] not in NEURAL]
                if not candidates: continue
                chosen = sorted(candidates, key=lambda r: r["validation_mean"], reverse=TASKS[task][1] != "regression")[0]
                job = (task, horizon, chosen["model"])
                reference = self.results[(job, chosen["candidate"])].get(2)
                for name, removed in FEATURE_GROUPS.items():
                    columns = np.array([i for i, feature in enumerate(FEATURES) if feature not in removed])
                    result = self.trial(job, chosen["candidate"], 2, self.boundary(.80), columns=columns, suffix=f"without_{name}")
                    self.ablations.append({"task": task, "horizon": horizon, "model": chosen["model"], "removed_group": name,
                        "status": "MEASURED" if result else ("BUDGET_OR_FIT_LIMITED" if self.has_time_budget else "FIT_FAILED"), "fold": 2,
                        "baseline": reference["best_validation_primary_metric"] if reference else None,
                        "ablated": result["best_validation_primary_metric"] if result else None,
                        "delta": result["best_validation_primary_metric"] - reference["best_validation_primary_metric"] if result and reference else None,
                        "validation_only": True, "identical_validation_keys": True})
        dump(self.root / "seed_robustness.json", self.seed_results)
        pd.DataFrame(self.ablations).to_csv(self.root / "feature_ablation.csv", index=False)

    def final_fits(self):
        # Freeze parameter choices before selecting final epochs on the declared
        # validation period. TEST is not read by any training/selection operation.
        dump(self.root / "selected_configurations.json", self.selected)
        for selected in self.selected:
            job = (selected["task"], selected["horizon"], selected["model"])
            result = self.trial(job, selected["candidate"], 3, self.boundary(.94))
            if result:
                self.final.append({"selection": selected, "fit": result})
        dump(self.root / "selection_complete.json", {"selected": self.selected, "fitted": self.final,
             "test_used_for_selection": False, "time_cutoff_enabled": self.has_time_budget,
             "selection_budget_exhausted": self.has_time_budget and time.monotonic() >= self.boundary(.94)})

    def evaluate(self):
        if not (self.root / "selection_complete.json").is_file():
            raise RuntimeError("Holdout evaluation requires frozen validation selections")
        completed = []
        for item in self.final:
            if time.monotonic() >= self.deadline - 180: break
            selected, fitted = item["selection"], item["fit"]
            task, horizon, model = selected["task"], selected["horizon"], selected["model"]
            contract, labels, _, _ = self.context(task, horizon, 3)
            labels = contract.values()  # TEST labels become available only after selection_complete.json.
            panel = contract.panel
            test = panel.meta.split.eq("test").to_numpy()
            full = self.settings.get("full_final_population", False)
            eligible = test & np.isfinite(labels) & panel.present
            if model in NEURAL:
                eligible &= self.model_sequence_mask(panel, selected["parameters"]["lookback"] if full else 120)
            elif not full:
                eligible &= self.sequence_masks[panel.level]
            ends = representative_indices(panel.meta, eligible, None if full else self.settings["holdout_cap"])
            if not len(ends): continue
            print(f"HOLDOUT {task}/{horizon}m/{model} {len(ends)}", flush=True)
            directory = self.root / fitted["directory"]
            prediction, latency = inference(panel, contract.kind, model, selected["parameters"], ends, directory, fitted)
            if task.endswith("_rv"): prediction = np.maximum(prediction, 0.)
            metric = statistical(contract.kind, labels[ends], prediction)
            common = self.sequence_masks[panel.level][ends]
            common_metric = statistical(contract.kind, labels[ends][common], prediction[common]) if common.any() else None
            baseline = contract.baseline()[ends]
            valid_baseline = np.isfinite(baseline)
            baseline_metric = statistical(contract.kind, labels[ends][valid_baseline], baseline[valid_baseline]) if valid_baseline.any() else None
            # Compare the learned model on the baseline's exact keys as well.
            comparable_metric = statistical(contract.kind, labels[ends][valid_baseline], prediction[valid_baseline]) if valid_baseline.any() else None
            monitor, sectors, alerts = monitoring(contract, labels[ends], prediction, ends, sampled=not full)
            financial_metrics = financial(contract, ends, prediction, sampled=not full)
            frame = panel.meta.iloc[ends].copy().reset_index(drop=True)
            frame["row_id"] = ends; frame["actual"] = labels[ends]
            frame["predicted"] = prediction.reshape(-1) if contract.kind == "regression" else prediction.argmax(axis=1)
            if task in {"company_rv", "sector_rv"}:
                metric["volatility_rank_ic"] = metric["spearman"]
                bucket = np.ones(len(frame), dtype=int)
                for entity, positions in frame.groupby("entity", observed=True).indices.items():
                    thresholds = contract.thresholds[str(entity)]
                    values = frame.actual.iloc[positions].to_numpy()
                    bucket[positions] = np.where(values < thresholds["low"], 0, np.where(values > thresholds["high"], 2, 1))
                calibration = frame[["actual", "predicted"]].copy(); calibration["training_quantile_regime"] = bucket
                calibration["absolute_error"] = abs(calibration.predicted - calibration.actual)
                calibration.groupby("training_quantile_regime").agg(observations=("actual", "size"), mean_actual=("actual", "mean"), mean_predicted=("predicted", "mean"), mae=("absolute_error", "mean")).reset_index().to_csv(directory / "volatility_calibration.csv", index=False)
            if contract.kind != "regression":
                for i in range(prediction.shape[1]): frame[f"probability_{i}"] = prediction[:, i]
            frame.to_parquet(directory / "holdout_predictions.parquet", index=False)
            if len(sectors): sectors.to_csv(directory / "false_alarms_by_sector.csv", index=False)
            if len(alerts): alerts.to_parquet(directory / "event_lead_times.parquet", index=False)
            subgroup = []
            for sector, indices in frame.groupby("sector", observed=True).indices.items():
                subgroup.append({"sector": str(sector), "rows": len(indices), "metrics": statistical(contract.kind, labels[ends][indices], prediction[indices])})
            latency["seconds_per_observation_amortized_including_load"] = latency["batch_inference_seconds"] / len(ends)
            latency["production_ready"] = False
            record = {**selected, "final_fit": fitted, "holdout_metrics": metric, "holdout_rows": len(ends),
                      "holdout_population": "all_eligible_selected_model" if full else "sampled_common_120bar",
                      "common_holdout_metrics": common_metric, "common_holdout_rows": int(common.sum()),
                      "eligible_holdout_rows": int(eligible.sum()), "baseline_metrics": baseline_metric,
                      "baseline_comparable_model_metrics": comparable_metric, "baseline_comparison_rows": int(valid_baseline.sum()),
                      "monitoring": monitor, "financial": financial_metrics, "inference": latency, "sector_metrics": subgroup}
            dump(directory / "evaluation.json", record)
            completed.append(record)
            dump(self.root / "evaluations.json", completed)
            self.explain(panel, fitted, selected, contract)
            gc.collect()
        return completed

    def explain(self, panel, fitted, selected, contract):
        if selected["model"] in NEURAL: return
        directory = self.root / fitted["directory"]
        model = joblib.load(directory / "model.joblib")
        if hasattr(model, "feature_importances_"):
            pd.DataFrame({"feature": FEATURES, "importance": model.feature_importances_}).to_csv(directory / "feature_importance.csv", index=False)
            # Secondary check, on validation only, limited to ten leading features
            # and one repeat under the user-authorized wall-clock budget.
            if time.monotonic() < self.deadline - 150:
                from src.ml.volatility.models import predict_tabular
                _, labels, _, validation = self.context(selected["task"], selected["horizon"], 3)
                # Permutation diagnostics are secondary and explicitly sampled;
                # predictive validation/holdout metrics remain full-population.
                available = np.zeros(len(panel.meta), dtype=bool); available[validation] = True
                validation = representative_indices(panel.meta, available, 2048)
                prep = joblib.load(directory / "preprocessing.joblib")
                values = transform(panel.x[validation[:, None], prep["columns"][None, :]], prep)
                baseline = primary(contract.kind, labels[validation], predict_tabular(model, contract.kind, values))
                rng = np.random.default_rng(42); permutations = []
                for column in np.argsort(model.feature_importances_)[-10:]:
                    if time.monotonic() >= self.deadline - 150: break
                    changed = values.copy(); rng.shuffle(changed[:, column])
                    current = primary(contract.kind, labels[validation], predict_tabular(model, contract.kind, changed))
                    permutations.append({"feature": FEATURES[column], "importance": current - baseline if contract.kind == "regression" else baseline - current, "repeats": 1, "population": "VALIDATION"})
                pd.DataFrame(permutations).to_csv(directory / "permutation_importance.csv", index=False)
        if hasattr(model, "coef_"):
            coefficient = np.atleast_2d(model.coef_)
            rows = [{"class": c, "feature": feature, "standardized_coefficient": float(coefficient[c, i]), "absolute_coefficient": float(abs(coefficient[c, i]))} for c in range(coefficient.shape[0]) for i, feature in enumerate(FEATURES)]
            pd.DataFrame(rows).to_csv(directory / "standardized_coefficients.csv", index=False)

    def validate_training_resume(self, planned):
        if load(self.root / "planned_candidates.json") != planned:
            raise ValueError("Training resume candidate configurations changed")
        reference = self.root / "data_reference.json"
        if reference.exists():
            source = resolve_data_root(self.root) / "data"
            for name, expected in load(reference)["checksums"].items():
                if sha256(source / name) != expected:
                    raise ValueError(f"Training resume prepared data changed: {name}")
        count = 0
        for path in (self.root / "trials").rglob("result.json"):
            result = load(path)
            job = (result["task"], result["horizon"], result["model"])
            if result["fold"] not in (0, 1, 2) or result["parameters"] != self.configs[job][result["candidate"]]:
                raise ValueError(f"Training resume fit configuration mismatch: {path}")
            if result["status"] != "COMPLETED" or self.root / result["directory"] != path.parent:
                raise ValueError(f"Training resume invalid completed fit: {path}")
            indices = np.load(path.parent / "validation_indices.npy", mmap_mode="r")
            prediction = np.load(path.parent / "validation_predictions.npy", mmap_mode="r")
            if len(indices) != result["validation_rows"] or len(prediction) != len(indices) or not np.isfinite(prediction).all():
                raise ValueError(f"Training resume validation artifacts mismatch: {path}")
            count += 1
        print(f"RESUME verified {count} completed fits; reuse saved results and restart incomplete fits", flush=True)
        return count

    def run(self, *, resume_prepared=False, resume_training=False):
        if resume_prepared or resume_training:
            if load(self.root / "request.json") != self.settings:
                raise ValueError("Prepared resume requires identical experiment settings and input fingerprints")
            if resume_training and any((self.root / name).exists() for name in ("selected_configurations.json", "selection_complete.json", "evaluations.json", "manifest.json")):
                raise ValueError("Training resume is allowed only before final selection and holdout evaluation")
            if resume_prepared and ((self.root / "trials").exists() or (self.root / "selection_complete.json").exists() or (self.root / "manifest.json").exists()):
                raise ValueError("Prepared resume is allowed only before model training and holdout evaluation")
            schema = load(resolve_data_root(self.root) / "data" / "schema.json")
            if schema["features"] != FEATURES:
                raise ValueError("Prepared resume feature contract mismatch")
            preserved = load(self.root / "preserved_evidence.json")
            if evidence_inventory() != preserved:
                raise ValueError("Previous experiment evidence changed before resume")
            print("RESUME existing verified prepared targets; auditing coverage before training", flush=True)
        else:
            if self.root.exists(): raise FileExistsError(f"Use a new experiment ID; output already exists: {self.root}")
            self.root.mkdir(parents=True)
            dump(self.root / "request.json", self.settings)
            preserved = evidence_inventory(); dump(self.root / "preserved_evidence.json", preserved)
        prepared = self.settings.get("prepared_data_from")
        if resume_prepared or resume_training:
            pass
        elif prepared:
            source = Path(prepared)
            previous = load(source / "request.json")
            if previous.get("input_checksums") != self.settings.get("input_checksums"):
                raise ValueError("Prepared target cache inputs do not match the new experiment")
            concrete = resolve_data_root(source)
            schema = load(concrete / "data" / "schema.json")
            if schema["features"] != FEATURES:
                raise ValueError("Prepared target feature contract does not match")
            if self.settings.get("prepared_data_mode") == "reference":
                checksums = {p.relative_to(concrete / "data").as_posix(): sha256(p) for p in (concrete / "data").rglob("*") if p.is_file()}
                dump(self.root / "data_reference.json", {"experiment_root": str(concrete.resolve()), "schema_sha256": sha256(concrete / "data" / "schema.json"), "checksums": checksums, "read_only": True})
                print(f"REUSE verified read-only targets from {concrete}; no duplicate dataset copy", flush=True)
            else:
                print(f"REUSE verified prepared targets from {source}; copying without modifying the source", flush=True)
                shutil.copytree(concrete / "data", self.root / "data")
            dump(self.root / "prepared_data_provenance.json", {"source": str(concrete.resolve()), "source_schema_sha256": sha256(concrete / "data" / "schema.json"), "source_modified": False})
        else:
            prepare(self.root, Path(self.settings["processed"]), Path(self.settings["features"]), Path(self.settings["metadata"]), Path(self.settings["config"]))
        self.panels = {level: Panel(self.root, level) for level in ("company", "sector")}
        self.sequence_masks = {level: eligible_sequences(panel, 120) for level, panel in self.panels.items()}
        self.folds = chronological_folds(self.panels["company"].meta); dump(self.root / "temporal_folds.json", self.folds)
        priority = ("emergence", "sector_rv", "sector_regime", "company_rv", "company_regime", "return_class", "return_reg")
        self.jobs = [(task, horizon, model) for neural in (False, True) for task in priority for horizon in HORIZONS
                     for model in (("linear", "ridge", "forest", "lstm", "gru") if TASKS[task][1] == "regression" else ("logistic", "forest", "lstm", "gru")) if (model in NEURAL) == neural]
        self.configs = {job: search(job[2], TASKS[job[0]][1], job[1], job[0], self.settings["search_candidates"]) for job in self.jobs}
        planned = [{"task": job[0], "horizon": job[1], "model": job[2], "candidates": values} for job, values in self.configs.items()]
        if resume_training:
            self.validate_training_resume(planned)
        dump(self.root / "search_spaces.json", SPACES)
        dump(self.root / "planned_candidates.json", planned)
        print(f"DATA READY company_rows={len(self.panels['company'].meta)} sector_rows={len(self.panels['sector'].meta)}; auditing sequence coverage", flush=True)
        print(f"POPULATION tuning: tabular<={self.settings['tabular_train_cap']} neural<={self.settings['neural_train_cap']} validation<={self.settings['validation_cap']}; final training/validation/test={'ALL_ELIGIBLE' if self.settings.get('full_final_population') else 'SAMPLED'}", flush=True)
        self.coverage()
        print("COVERAGE COMPLETE; starting chronological model training", flush=True)
        self.optimize(); self.robustness_and_ablation(); self.final_fits()
        evaluations = self.evaluate()
        after = evidence_inventory()
        if after != preserved: raise RuntimeError("Previous experiment evidence changed during this run")
        from src.ml.volatility.reporting import finalize
        finalize(self, evaluations)
        print(f"FINISHED {self.root / 'REPORT.md'}", flush=True)
