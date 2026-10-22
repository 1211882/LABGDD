from __future__ import annotations

import argparse
import json
import platform
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from src.ml.evaluation import classification_metrics, portfolio_financial_metrics
from src.ml.features import FEATURE_COLUMNS
from src.ml.targets import CLASS_NAMES, HORIZONS
from src.ml.tuning.common import TrialStore, build_sequences_fast, deterministic_tabular_subset, persist_best_config, set_seeds, sha256, split_frame
from src.ml.tuning.search_spaces import SEARCH_SPACES, deterministic_candidates

EXPERIMENT_ID = "phase13_classification_tuned_v1"
BASELINE_ROOT = Path("models/classification/phase10_classification_final_v1")
SEQUENCE_FINAL_CAPS = {"train": 120_000, "validation": 30_000, "test": 30_000}
SEQUENCE_TUNING_CAPS = {"train": 60_000, "validation": 20_000}


def _classes(probabilities, classes) -> np.ndarray:
    result = np.zeros((len(probabilities), 3))
    for source, name in enumerate(classes): result[:, CLASS_NAMES.index(str(name))] = probabilities[:, source]
    return result


def _keras_model(kind: str, params: dict):
    import tensorflow as tf
    recurrent = tf.keras.layers.LSTM if kind == "LSTM" else tf.keras.layers.GRU
    layers = [tf.keras.layers.Input((params["lookback"], len(FEATURE_COLUMNS)))]
    for layer in range(params["num_layers"]):
        layers.append(recurrent(params["hidden_size"], return_sequences=layer < params["num_layers"] - 1))
        if params["dropout"] and layer == params["num_layers"] - 1: layers.append(tf.keras.layers.Dropout(params["dropout"]))
    layers.append(tf.keras.layers.Dense(3, activation="softmax"))
    optimizer = tf.keras.optimizers.AdamW(learning_rate=params["learning_rate"], weight_decay=params["weight_decay"])
    model = tf.keras.Sequential(layers); model.compile(optimizer=optimizer, loss="sparse_categorical_crossentropy", metrics=["accuracy"]); return model


def _class_weights(values: np.ndarray, policy):
    if policy is None: return None
    counts = np.bincount(values, minlength=3); total = len(values)
    return {index: total / (3 * count) for index, count in enumerate(counts) if count}


def _trial_row(trial_id, model, horizon, params, seed, train_rows, validation_rows, metrics, duration, artifact, status="COMPLETED", error=None):
    return {"trial_id": trial_id, "task": "classification", "model": model, "horizon": horizon, "hyperparameters_json": json.dumps(params, sort_keys=True), "random_seed": seed,
            "training_rows": train_rows, "validation_rows": validation_rows, "selection_metric": "validation_macro_f1", "selection_metric_value": metrics.get("macro_f1") if metrics else None,
            "secondary_metrics_json": json.dumps({key: metrics[key] for key in ("balanced_accuracy", "weighted_f1", "accuracy", "log_loss") if key in metrics}) if metrics else None,
            "training_duration_seconds": duration, "status": status, "artifact_path": str(artifact), "error_message": error}


def _prediction(rows, actual, predicted, probabilities, model, horizon):
    result = rows[["timestamp", "symbol", "sector", "split", f"future_return_{horizon}m"]].rename(columns={f"future_return_{horizon}m": "future_return"}).copy()
    result["horizon"] = horizon; result["actual_class"] = actual; result["predicted_class"] = predicted
    for index, name in enumerate(CLASS_NAMES): result[f"probability_{name.lower()}"] = probabilities[:, index]
    result["max_probability"] = probabilities.max(axis=1); result["model_name"] = model
    result["lookback"] = 0 if model in {"Logistic Regression", "Random Forest"} else int(rows.attrs.get("lookback", 0))
    result["dataset_version"] = "ml_dataset_v1"; result["experiment_version"] = EXPERIMENT_ID; result["model_variant"] = "TUNED"
    return result


def _metric_row(frame, scope):
    metrics = classification_metrics(frame.actual_class, frame.predicted_class, frame[["probability_down", "probability_neutral", "probability_up"]].to_numpy())
    return {"model": frame.model_name.iloc[0], "horizon": int(frame.horizon.iloc[0]), "scope": scope, "test_rows": len(frame), **{key: value for key, value in metrics.items() if key not in {"per_class", "confusion_matrix"}}, "per_class_json": json.dumps(metrics["per_class"]), "confusion_matrix_json": json.dumps(metrics["confusion_matrix"])}


def tune_and_evaluate(features: Path, metadata_path: Path, output: Path, tabular_budget: int, neural_budget: int, seed: int, models: list[str], horizons: list[int]) -> Path:
    if output.exists(): raise FileExistsError(f"Immutable output already exists: {output}")
    work = output.parent / f".{output.name}.work"; work.mkdir(parents=True, exist_ok=True)
    for name in ("trials", "best_configs", "models", "scalers", "histories", "predictions", "metrics", "figures", "reports"): (work / name).mkdir(exist_ok=True)
    set_seeds(seed); metadata = json.loads(metadata_path.read_text(encoding="utf-8")); frame = pd.read_parquet(features)
    store = TrialStore(work / "tuning_trials.csv"); completed = store.completed_ids(); best_configs = {}
    for horizon in horizons:
        target = f"target_{horizon}m"; train, validation, _ = split_frame(frame, target)
        scaler = StandardScaler().fit(train[FEATURE_COLUMNS]); joblib.dump(scaler, work / "scalers" / f"feature_scaler_{horizon}m.joblib")
        train_sample = deterministic_tabular_subset(train, 240_000); validation_sample = deterministic_tabular_subset(validation, 80_000)
        x_train = scaler.transform(train_sample[FEATURE_COLUMNS]); x_validation = scaler.transform(validation_sample[FEATURE_COLUMNS])
        if "logistic" in models:
            model_name = "Logistic Regression"; candidates = deterministic_candidates("logistic", tabular_budget, seed, horizon=horizon)
            for number, params in enumerate(candidates, 1):
                trial_id = f"classification_logistic_{horizon}m_{number:03d}"
                if trial_id in completed: continue
                artifact = work / "trials" / f"{trial_id}.json"; started = time.perf_counter()
                try:
                    model = LogisticRegression(C=params["C"], class_weight=params["class_weight"], solver="lbfgs", max_iter=200, random_state=seed).fit(x_train, train_sample[target])
                    probs = _classes(model.predict_proba(x_validation), model.classes_); metrics = classification_metrics(validation_sample[target], model.predict(x_validation), probs)
                    artifact.write_text(json.dumps({"params": params, "metrics": metrics}, indent=2) + "\n"); store.append(_trial_row(trial_id, model_name, horizon, params, seed, len(train_sample), len(validation_sample), metrics, time.perf_counter()-started, artifact))
                except Exception as exc: store.append(_trial_row(trial_id, model_name, horizon, params, seed, len(train_sample), len(validation_sample), {}, time.perf_counter()-started, artifact, "FAILED", str(exc)))
            best = store.best("classification", model_name, horizon, higher_is_better=True); best_configs[(horizon, model_name)] = persist_best_config(work/"best_configs"/"logistic"/f"{horizon}m"/"best_config.json", search_space=SEARCH_SPACES["logistic"], trials=len(candidates), best=best, seed=seed)
        if "random_forest" in models:
            model_name = "Random Forest"; candidates = deterministic_candidates("random_forest_classifier", tabular_budget, seed, horizon=horizon)
            for number, params in enumerate(candidates, 1):
                trial_id = f"classification_random_forest_{horizon}m_{number:03d}"
                if trial_id in completed: continue
                artifact = work/"trials"/f"{trial_id}.json"; started=time.perf_counter()
                try:
                    model = RandomForestClassifier(**params, max_samples=.25, n_jobs=-1, random_state=seed).fit(train_sample[FEATURE_COLUMNS], train_sample[target])
                    probs = _classes(model.predict_proba(validation_sample[FEATURE_COLUMNS]), model.classes_); metrics=classification_metrics(validation_sample[target], model.predict(validation_sample[FEATURE_COLUMNS]), probs)
                    artifact.write_text(json.dumps({"params": params, "metrics": metrics}, indent=2)+"\n"); store.append(_trial_row(trial_id, model_name, horizon, params, seed, len(train_sample),len(validation_sample),metrics,time.perf_counter()-started,artifact))
                except Exception as exc: store.append(_trial_row(trial_id,model_name,horizon,params,seed,len(train_sample),len(validation_sample),{},time.perf_counter()-started,artifact,"FAILED",str(exc)))
            best=store.best("classification",model_name,horizon,higher_is_better=True); best_configs[(horizon,model_name)]=persist_best_config(work/"best_configs"/"random_forest"/f"{horizon}m"/"best_config.json",search_space=SEARCH_SPACES["random_forest_classifier"],trials=len(candidates),best=best,seed=seed)
        for offset, kind in enumerate(("LSTM", "GRU"), 10):
            if kind.lower() not in models: continue
            candidates=deterministic_candidates("deep_classification",neural_budget,seed,horizon=horizon,model_offset=offset); cache={}
            for number,params in enumerate(candidates,1):
                trial_id=f"classification_{kind.lower()}_{horizon}m_{number:03d}"; artifact=work/"trials"/f"{trial_id}.json"
                if trial_id in completed: continue
                started=time.perf_counter()
                try:
                    lookback=params["lookback"]
                    if lookback not in cache:
                        scaled_train=train.copy(); scaled_validation=validation.copy(); scaled_train[FEATURE_COLUMNS]=scaler.transform(train[FEATURE_COLUMNS]); scaled_validation[FEATURE_COLUMNS]=scaler.transform(validation[FEATURE_COLUMNS])
                        cache[lookback]=(build_sequences_fast(scaled_train,target,lookback,SEQUENCE_TUNING_CAPS["train"]),build_sequences_fast(scaled_validation,target,lookback,SEQUENCE_TUNING_CAPS["validation"]))
                    (xt,yt,_),(xv,yv,_)=cache[lookback]; labels={name:index for index,name in enumerate(CLASS_NAMES)}; yt=np.array([labels[x] for x in yt]); yv_enc=np.array([labels[x] for x in yv])
                    import tensorflow as tf
                    tf.keras.backend.clear_session(); tf.random.set_seed(seed); model=_keras_model(kind,params)
                    history=model.fit(xt,yt,validation_data=(xv,yv_enc),epochs=12,batch_size=params["batch_size"],class_weight=_class_weights(yt,params["class_weight"]),callbacks=[tf.keras.callbacks.EarlyStopping(monitor="val_loss",patience=params["patience"],restore_best_weights=True)],verbose=0)
                    probs=model.predict(xv,batch_size=1024,verbose=0); predicted=np.asarray(CLASS_NAMES)[probs.argmax(axis=1)]; metrics=classification_metrics(yv,predicted,probs)
                    artifact.write_text(json.dumps({"params":params,"metrics":metrics,"history":history.history},default=float,indent=2)+"\n"); store.append(_trial_row(trial_id,kind,horizon,params,seed,len(yt),len(yv),metrics,time.perf_counter()-started,artifact))
                except Exception as exc: store.append(_trial_row(trial_id,kind,horizon,params,seed,0,0,{},time.perf_counter()-started,artifact,"FAILED",str(exc)))
            best=store.best("classification",kind,horizon,higher_is_better=True); best_configs[(horizon,kind)]=persist_best_config(work/"best_configs"/kind.lower()/f"{horizon}m"/"best_config.json",search_space=SEARCH_SPACES["deep_classification"],trials=len(candidates),best=best,seed=seed)
    marker=work/"FINAL_TEST_EVALUATION_STARTED.json"
    if marker.exists(): raise RuntimeError("Final TEST evaluation was already started; refusing a second inspection")
    marker.write_text(json.dumps({"started_at":datetime.now(timezone.utc).isoformat(),"selection_complete":True,"selected_using":"VALIDATION only"},indent=2)+"\n")
    predictions={}; complexity=[]; histories={}
    for horizon in horizons:
        target=f"target_{horizon}m"; train,validation,test=split_frame(frame,target); scaler=joblib.load(work/"scalers"/f"feature_scaler_{horizon}m.joblib")
        if "logistic" in models:
            params=best_configs[(horizon,"Logistic Regression")]["selected_parameters"]; started=time.perf_counter(); model=LogisticRegression(C=params["C"],class_weight=params["class_weight"],solver="lbfgs",max_iter=200,random_state=seed).fit(scaler.transform(train[FEATURE_COLUMNS]),train[target]); duration=time.perf_counter()-started
            joblib.dump(model,work/"models"/f"logistic_{horizon}m.joblib"); probs=_classes(model.predict_proba(scaler.transform(test[FEATURE_COLUMNS])),model.classes_); predictions[(horizon,"Logistic Regression")]=_prediction(test,test[target].to_numpy(),model.predict(scaler.transform(test[FEATURE_COLUMNS])),probs,"Logistic Regression",horizon); complexity.append({"model":"Logistic Regression","horizon":horizon,"parameters":int(model.coef_.size+model.intercept_.size),"model_size_bytes":(work/"models"/f"logistic_{horizon}m.joblib").stat().st_size,"training_duration_seconds":duration})
        if "random_forest" in models:
            params=best_configs[(horizon,"Random Forest")]["selected_parameters"]; started=time.perf_counter(); model=RandomForestClassifier(**params,max_samples=.25,n_jobs=-1,random_state=seed).fit(train[FEATURE_COLUMNS],train[target]); duration=time.perf_counter()-started
            joblib.dump(model,work/"models"/f"random_forest_{horizon}m.joblib"); probs=_classes(model.predict_proba(test[FEATURE_COLUMNS]),model.classes_); predictions[(horizon,"Random Forest")]=_prediction(test,test[target].to_numpy(),model.predict(test[FEATURE_COLUMNS]),probs,"Random Forest",horizon); complexity.append({"model":"Random Forest","horizon":horizon,"parameters":sum(tree.tree_.node_count for tree in model.estimators_),"model_size_bytes":(work/"models"/f"random_forest_{horizon}m.joblib").stat().st_size,"training_duration_seconds":duration})
        scaled={name:rows.copy() for name,rows in (("train",train),("validation",validation),("test",test))}
        for rows in scaled.values(): rows[FEATURE_COLUMNS]=scaler.transform(rows[FEATURE_COLUMNS])
        for kind in ("LSTM","GRU"):
            if kind.lower() not in models: continue
            params=best_configs[(horizon,kind)]["selected_parameters"]
            sets={name:build_sequences_fast(rows,target,params["lookback"],SEQUENCE_FINAL_CAPS[name]) for name,rows in scaled.items()}; labels={name:index for index,name in enumerate(CLASS_NAMES)}
            xt,yt,_=sets["train"]; xv,yv,_=sets["validation"]; xs,ys,test_indices=sets["test"]; yt_enc=np.array([labels[x] for x in yt]); yv_enc=np.array([labels[x] for x in yv])
            import tensorflow as tf
            tf.keras.backend.clear_session(); tf.random.set_seed(seed); model=_keras_model(kind,params); started=time.perf_counter(); history=model.fit(xt,yt_enc,validation_data=(xv,yv_enc),epochs=12,batch_size=params["batch_size"],class_weight=_class_weights(yt_enc,params["class_weight"]),callbacks=[tf.keras.callbacks.EarlyStopping(monitor="val_loss",patience=params["patience"],restore_best_weights=True)],verbose=0); duration=time.perf_counter()-started
            path=work/"models"/f"{kind.lower()}_{horizon}m.keras"; model.save(path); probs=model.predict(xs,batch_size=1024,verbose=0); pred=np.asarray(CLASS_NAMES)[probs.argmax(axis=1)]; rows=scaled["test"].loc[test_indices].copy(); rows.attrs["lookback"]=params["lookback"]; predictions[(horizon,kind)]=_prediction(rows,ys,pred,probs,kind,horizon); histories[f"{kind}_{horizon}m"]={key:[float(x) for x in value] for key,value in history.history.items()}; complexity.append({"model":kind,"horizon":horizon,"parameters":model.count_params(),"model_size_bytes":path.stat().st_size,"training_duration_seconds":duration})
    _finalize(work,output,predictions,complexity,histories,metadata,seed,tabular_budget,neural_budget,horizons,models)
    return output


def _finalize(work,output,predictions,complexity,histories,metadata,seed,tabular_budget,neural_budget,horizons,models):
    full=[];common=[];financial=[]
    for (horizon,model),frame in predictions.items(): frame.to_parquet(work/"predictions"/f"{model.lower().replace(' ','_')}_{horizon}m.parquet",index=False); full.append(_metric_row(frame,"FULL")); financial.extend({"model":model,"horizon":horizon,**portfolio_financial_metrics(frame,non_overlapping=value)} for value in (False,True))
    for horizon in horizons:
        frames={model:frame for (h,model),frame in predictions.items() if h==horizon}; keys=None
        for frame in frames.values(): current=pd.MultiIndex.from_frame(frame[["symbol","timestamp"]]); keys=current if keys is None else keys.intersection(current)
        for model,frame in frames.items(): common.append(_metric_row(frame.loc[pd.MultiIndex.from_frame(frame[["symbol","timestamp"]]).isin(keys)],"COMMON"))
    full=pd.DataFrame(full);common=pd.DataFrame(common);full.to_csv(work/"metrics"/"classification_metrics_full.csv",index=False);common.to_csv(work/"metrics"/"classification_metrics_common.csv",index=False);pd.DataFrame(financial).to_csv(work/"metrics"/"financial_evaluation.csv",index=False);pd.DataFrame(complexity).to_csv(work/"metrics"/"model_complexity.csv",index=False);(work/"histories"/"training_history.json").write_text(json.dumps(histories,indent=2)+"\n")
    baseline=pd.read_csv(BASELINE_ROOT/"metrics"/"classification_metrics_common.csv"); tuned=common; comparison=tuned.merge(baseline,on=["model","horizon"],suffixes=("_tuned","_baseline")); comparison["macro_f1_delta"]=comparison.macro_f1_tuned-comparison.macro_f1_baseline;comparison["balanced_accuracy_delta"]=comparison.balanced_accuracy_tuned-comparison.balanced_accuracy_baseline;comparison.to_csv(work/"metrics"/"baseline_vs_tuned.csv",index=False)
    _figures(work,comparison,pd.read_csv(work/"tuning_trials.csv"),full)
    checksums={str(path.relative_to(work)).replace("\\","/"):sha256(path) for path in work.rglob("*") if path.is_file() and path.name!="manifest.json"}
    manifest={"experiment_id":EXPERIMENT_ID,"status":"frozen","created_at":datetime.now(timezone.utc).isoformat(),"dataset_version":metadata["dataset_version"],"feature_version":metadata["features_version"],"baseline_experiment":"phase10_classification_final_v1","selection_split":"validation","test_policy":"Read once only after all selected configs were persisted; no tuning followed test evaluation.","splits":metadata["splits"],"horizons":horizons,"models":models,"seed":seed,"trial_budgets":{"tabular":tabular_budget,"neural":neural_budget},"tuning_subsets":{"tabular_train":240000,"tabular_validation":80000,"neural_train":60000,"neural_validation":20000},"final_sequence_caps":SEQUENCE_FINAL_CAPS,"software":{"python":platform.python_version(),"sklearn":sklearn.__version__},"checksums":checksums}
    (work/"manifest.json").write_text(json.dumps(manifest,indent=2)+"\n"); work.rename(output)


def _figures(work,comparison,trials,full):
    for metric,label in (("macro_f1","Macro F1"),("balanced_accuracy","Balanced Accuracy")):
        data=comparison[["model","horizon",f"{metric}_baseline",f"{metric}_tuned"]].melt(["model","horizon"],var_name="variant",value_name=metric); ax=data.pivot(index=["model","horizon"],columns="variant",values=metric).plot(kind="bar",figsize=(11,5));ax.set_ylabel(label);plt.tight_layout();plt.savefig(work/"figures"/f"baseline_vs_tuned_{metric}.png",dpi=180);plt.close()
    completed=trials.query("status == 'COMPLETED'");ax=completed.boxplot(column="selection_metric_value",by="model",figsize=(9,5));ax.set_ylabel("Validation Macro F1");plt.suptitle("");plt.tight_layout();plt.savefig(work/"figures"/"validation_trial_scores.png",dpi=180);plt.close()


def main():
    parser=argparse.ArgumentParser();parser.add_argument("--features",type=Path,default=Path("datasets/ml/ml_dataset_v1/features_v1/features_targets.parquet"));parser.add_argument("--metadata",type=Path,default=Path("datasets/ml/ml_dataset_v1/features_v1/metadata.json"));parser.add_argument("--output",type=Path,default=Path(f"models/classification/{EXPERIMENT_ID}"));parser.add_argument("--tabular-trials",type=int,default=3);parser.add_argument("--neural-trials",type=int,default=2);parser.add_argument("--seed",type=int,default=42);parser.add_argument("--models",nargs="+",default=["logistic","random_forest","lstm","gru"]);parser.add_argument("--horizons",nargs="+",type=int,default=list(HORIZONS));args=parser.parse_args();print(tune_and_evaluate(args.features,args.metadata,args.output,args.tabular_trials,args.neural_trials,args.seed,args.models,args.horizons))


if __name__=="__main__":main()
