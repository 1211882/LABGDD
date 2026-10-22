from __future__ import annotations

import argparse
import json
import platform
import time
from datetime import datetime, timezone
from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.preprocessing import StandardScaler

from src.ml.features import FEATURE_COLUMNS
from src.ml.regression_evaluation import regression_financial_metrics, regression_metrics
from src.ml.targets import HORIZONS, classify_returns
from src.ml.tuning.common import TrialStore, build_sequences_fast, deterministic_tabular_subset, persist_best_config, set_seeds, sha256, split_frame
from src.ml.tuning.search_spaces import SEARCH_SPACES, deterministic_candidates

EXPERIMENT_ID = "phase13_regression_tuned_v1"
BASELINE_ROOT = Path("models/regression/phase11_regression_final_v1")
SEQUENCE_FINAL_CAPS = {"train": 120_000, "validation": 30_000, "test": 30_000}
SEQUENCE_TUNING_CAPS = {"train": 60_000, "validation": 20_000}


def _keras_model(kind: str, params: dict):
    import tensorflow as tf
    recurrent = tf.keras.layers.LSTM if kind == "LSTM" else tf.keras.layers.GRU
    layers = [tf.keras.layers.Input((params["lookback"], len(FEATURE_COLUMNS)))]
    for layer in range(params["num_layers"]):
        layers.append(recurrent(params["hidden_size"], return_sequences=layer < params["num_layers"] - 1))
        if params["dropout"] and layer == params["num_layers"] - 1: layers.append(tf.keras.layers.Dropout(params["dropout"]))
    layers.append(tf.keras.layers.Dense(1))
    optimizer = tf.keras.optimizers.AdamW(learning_rate=params["learning_rate"], weight_decay=params["weight_decay"])
    loss = tf.keras.losses.Huber() if params["loss"] == "huber" else "mse"
    model = tf.keras.Sequential(layers); model.compile(optimizer=optimizer, loss=loss, metrics=["mae"]); return model


def _trial_row(trial_id, model, horizon, params, seed, train_rows, validation_rows, metrics, duration, artifact, status="COMPLETED", error=None):
    return {"trial_id": trial_id, "task": "regression", "model": model, "horizon": horizon, "hyperparameters_json": json.dumps(params,sort_keys=True), "random_seed": seed,
            "training_rows": train_rows,"validation_rows":validation_rows,"selection_metric":"validation_mae","selection_metric_value":metrics.get("mae") if metrics else None,
            "secondary_metrics_json":json.dumps({key:metrics[key] for key in ("rmse","r2","pearson_correlation","spearman_correlation","directional_accuracy") if key in metrics}) if metrics else None,
            "training_duration_seconds":duration,"status":status,"artifact_path":str(artifact),"error_message":error}


def _prediction(rows,predicted,horizon,model):
    target=f"future_return_{horizon}m"; result=rows[["timestamp","symbol","sector","split",target]].rename(columns={target:"actual_return"}).copy(); result["horizon"]=horizon;result["predicted_return"]=np.asarray(predicted,dtype=float);result["prediction_error"]=result.predicted_return-result.actual_return;result["absolute_error"]=result.prediction_error.abs();result["squared_error"]=result.prediction_error.pow(2);result["model_name"]=model;result["lookback"]=0 if model in {"Ridge","Random Forest Regressor"} else int(rows.attrs.get("lookback",0));result["dataset_version"]="ml_dataset_v1";result["actual_class_from_5bps"]=classify_returns(result.actual_return,.0005);result["predicted_class_from_5bps"]=classify_returns(result.predicted_return,.0005);result["experiment_version"]=EXPERIMENT_ID;result["model_variant"]="TUNED";return result


def _metric_row(frame,scope): return {"model":frame.model_name.iloc[0],"horizon":int(frame.horizon.iloc[0]),"scope":scope,"test_rows":len(frame),**regression_metrics(frame.actual_return,frame.predicted_return)}


def tune_and_evaluate(features:Path,metadata_path:Path,output:Path,tabular_budget:int,neural_budget:int,seed:int,models:list[str],horizons:list[int])->Path:
    if output.exists():raise FileExistsError(f"Immutable output already exists: {output}")
    work=output.parent/f".{output.name}.work";work.mkdir(parents=True,exist_ok=True)
    for name in ("trials","best_configs","models","scalers","histories","predictions","metrics","figures","reports"):(work/name).mkdir(exist_ok=True)
    set_seeds(seed);metadata=json.loads(metadata_path.read_text());frame=pd.read_parquet(features);store=TrialStore(work/"tuning_trials.csv");completed=store.completed_ids();best_configs={}
    for horizon in horizons:
        target=f"future_return_{horizon}m";train,validation,_=split_frame(frame,target);feature_scaler=StandardScaler().fit(train[FEATURE_COLUMNS]);target_scaler=StandardScaler().fit(train[[target]]);joblib.dump(feature_scaler,work/"scalers"/f"feature_scaler_{horizon}m.joblib");joblib.dump(target_scaler,work/"scalers"/f"target_scaler_{horizon}m.joblib")
        train_sample=deterministic_tabular_subset(train,240_000);validation_sample=deterministic_tabular_subset(validation,80_000);xt=feature_scaler.transform(train_sample[FEATURE_COLUMNS]);xv=feature_scaler.transform(validation_sample[FEATURE_COLUMNS])
        if "ridge" in models:
            model_name="Ridge"; candidates=deterministic_candidates("ridge",tabular_budget,seed,horizon=horizon)
            for number,params in enumerate(candidates,1):
                trial_id=f"regression_ridge_{horizon}m_{number:03d}";artifact=work/"trials"/f"{trial_id}.json"
                if trial_id in completed:continue
                started=time.perf_counter()
                try:
                    model=Ridge(alpha=params["alpha"]).fit(xt,train_sample[target]);pred=model.predict(xv);metrics=regression_metrics(validation_sample[target],pred);artifact.write_text(json.dumps({"params":params,"metrics":metrics},indent=2)+"\n");store.append(_trial_row(trial_id,model_name,horizon,params,seed,len(train_sample),len(validation_sample),metrics,time.perf_counter()-started,artifact))
                except Exception as exc:store.append(_trial_row(trial_id,model_name,horizon,params,seed,len(train_sample),len(validation_sample),{},time.perf_counter()-started,artifact,"FAILED",str(exc)))
            best=store.best("regression",model_name,horizon,higher_is_better=False);best_configs[(horizon,model_name)]=persist_best_config(work/"best_configs"/"ridge"/f"{horizon}m"/"best_config.json",search_space=SEARCH_SPACES["ridge"],trials=len(candidates),best=best,seed=seed)
        if "random_forest" in models:
            model_name="Random Forest Regressor";candidates=deterministic_candidates("random_forest_regressor",tabular_budget,seed,horizon=horizon)
            for number,params in enumerate(candidates,1):
                trial_id=f"regression_random_forest_{horizon}m_{number:03d}";artifact=work/"trials"/f"{trial_id}.json"
                if trial_id in completed:continue
                started=time.perf_counter()
                try:
                    model=RandomForestRegressor(**params,max_samples=.25,n_jobs=-1,random_state=seed).fit(train_sample[FEATURE_COLUMNS],train_sample[target]);pred=model.predict(validation_sample[FEATURE_COLUMNS]);metrics=regression_metrics(validation_sample[target],pred);artifact.write_text(json.dumps({"params":params,"metrics":metrics},indent=2)+"\n");store.append(_trial_row(trial_id,model_name,horizon,params,seed,len(train_sample),len(validation_sample),metrics,time.perf_counter()-started,artifact))
                except Exception as exc:store.append(_trial_row(trial_id,model_name,horizon,params,seed,len(train_sample),len(validation_sample),{},time.perf_counter()-started,artifact,"FAILED",str(exc)))
            best=store.best("regression",model_name,horizon,higher_is_better=False);best_configs[(horizon,model_name)]=persist_best_config(work/"best_configs"/"random_forest"/f"{horizon}m"/"best_config.json",search_space=SEARCH_SPACES["random_forest_regressor"],trials=len(candidates),best=best,seed=seed)
        for offset,kind in enumerate(("LSTM","GRU"),20):
            if kind.lower() not in models:continue
            candidates=deterministic_candidates("deep_regression",neural_budget,seed,horizon=horizon,model_offset=offset);cache={}
            for number,params in enumerate(candidates,1):
                trial_id=f"regression_{kind.lower()}_{horizon}m_{number:03d}";artifact=work/"trials"/f"{trial_id}.json"
                if trial_id in completed:continue
                started=time.perf_counter()
                try:
                    lookback=params["lookback"]
                    if lookback not in cache:
                        st=train.copy();sv=validation.copy();st[FEATURE_COLUMNS]=feature_scaler.transform(train[FEATURE_COLUMNS]);sv[FEATURE_COLUMNS]=feature_scaler.transform(validation[FEATURE_COLUMNS]);cache[lookback]=(build_sequences_fast(st,target,lookback,SEQUENCE_TUNING_CAPS["train"]),build_sequences_fast(sv,target,lookback,SEQUENCE_TUNING_CAPS["validation"]))
                    (xtrain,ytrain,_),(xval,yval,_)=cache[lookback];ytrain_s=target_scaler.transform(ytrain.reshape(-1,1)).reshape(-1);yval_s=target_scaler.transform(yval.reshape(-1,1)).reshape(-1)
                    import tensorflow as tf
                    tf.keras.backend.clear_session();tf.random.set_seed(seed);model=_keras_model(kind,params);history=model.fit(xtrain,ytrain_s,validation_data=(xval,yval_s),epochs=12,batch_size=params["batch_size"],callbacks=[tf.keras.callbacks.EarlyStopping(monitor="val_loss",patience=params["patience"],restore_best_weights=True)],verbose=0);pred=target_scaler.inverse_transform(model.predict(xval,batch_size=1024,verbose=0)).reshape(-1);metrics=regression_metrics(yval,pred);artifact.write_text(json.dumps({"params":params,"metrics":metrics,"history":history.history},default=float,indent=2)+"\n");store.append(_trial_row(trial_id,kind,horizon,params,seed,len(ytrain),len(yval),metrics,time.perf_counter()-started,artifact))
                except Exception as exc:store.append(_trial_row(trial_id,kind,horizon,params,seed,0,0,{},time.perf_counter()-started,artifact,"FAILED",str(exc)))
            best=store.best("regression",kind,horizon,higher_is_better=False);best_configs[(horizon,kind)]=persist_best_config(work/"best_configs"/kind.lower()/f"{horizon}m"/"best_config.json",search_space=SEARCH_SPACES["deep_regression"],trials=len(candidates),best=best,seed=seed)
    marker=work/"FINAL_TEST_EVALUATION_STARTED.json"
    if marker.exists():raise RuntimeError("Final TEST evaluation was already started; refusing a second inspection")
    marker.write_text(json.dumps({"started_at":datetime.now(timezone.utc).isoformat(),"selection_complete":True,"selected_using":"VALIDATION only"},indent=2)+"\n")
    predictions={};complexity=[];histories={}
    for horizon in horizons:
        target=f"future_return_{horizon}m";train,validation,test=split_frame(frame,target);feature_scaler=joblib.load(work/"scalers"/f"feature_scaler_{horizon}m.joblib");target_scaler=joblib.load(work/"scalers"/f"target_scaler_{horizon}m.joblib")
        if "ridge" in models:
            params=best_configs[(horizon,"Ridge")]["selected_parameters"];started=time.perf_counter();model=Ridge(alpha=params["alpha"]).fit(feature_scaler.transform(train[FEATURE_COLUMNS]),train[target]);duration=time.perf_counter()-started;path=work/"models"/f"ridge_{horizon}m.joblib";joblib.dump(model,path);pred=model.predict(feature_scaler.transform(test[FEATURE_COLUMNS]));predictions[(horizon,"Ridge")]=_prediction(test,pred,horizon,"Ridge");complexity.append({"model":"Ridge","horizon":horizon,"parameters":int(model.coef_.size+1),"model_size_bytes":path.stat().st_size,"training_duration_seconds":duration})
        if "random_forest" in models:
            params=best_configs[(horizon,"Random Forest Regressor")]["selected_parameters"];started=time.perf_counter();model=RandomForestRegressor(**params,max_samples=.25,n_jobs=-1,random_state=seed).fit(train[FEATURE_COLUMNS],train[target]);duration=time.perf_counter()-started;path=work/"models"/f"random_forest_regressor_{horizon}m.joblib";joblib.dump(model,path);predictions[(horizon,"Random Forest Regressor")]=_prediction(test,model.predict(test[FEATURE_COLUMNS]),horizon,"Random Forest Regressor");complexity.append({"model":"Random Forest Regressor","horizon":horizon,"parameters":sum(tree.tree_.node_count for tree in model.estimators_),"model_size_bytes":path.stat().st_size,"training_duration_seconds":duration})
        scaled={name:rows.copy() for name,rows in (("train",train),("validation",validation),("test",test))}
        for rows in scaled.values():rows[FEATURE_COLUMNS]=feature_scaler.transform(rows[FEATURE_COLUMNS])
        for kind in ("LSTM","GRU"):
            if kind.lower() not in models:continue
            params=best_configs[(horizon,kind)]["selected_parameters"];sets={name:build_sequences_fast(rows,target,params["lookback"],SEQUENCE_FINAL_CAPS[name]) for name,rows in scaled.items()};xt,yt,_=sets["train"];xv,yv,_=sets["validation"];xs,ys,test_indices=sets["test"];yt_s=target_scaler.transform(yt.reshape(-1,1)).reshape(-1);yv_s=target_scaler.transform(yv.reshape(-1,1)).reshape(-1)
            import tensorflow as tf
            tf.keras.backend.clear_session();tf.random.set_seed(seed);model=_keras_model(kind,params);started=time.perf_counter();history=model.fit(xt,yt_s,validation_data=(xv,yv_s),epochs=12,batch_size=params["batch_size"],callbacks=[tf.keras.callbacks.EarlyStopping(monitor="val_loss",patience=params["patience"],restore_best_weights=True)],verbose=0);duration=time.perf_counter()-started;path=work/"models"/f"{kind.lower()}_{horizon}m.keras";model.save(path);pred=target_scaler.inverse_transform(model.predict(xs,batch_size=1024,verbose=0)).reshape(-1);rows=scaled["test"].loc[test_indices].copy();rows.attrs["lookback"]=params["lookback"];predictions[(horizon,kind)]=_prediction(rows,pred,horizon,kind);histories[f"{kind}_{horizon}m"]={key:[float(x) for x in value] for key,value in history.history.items()};complexity.append({"model":kind,"horizon":horizon,"parameters":model.count_params(),"model_size_bytes":path.stat().st_size,"training_duration_seconds":duration})
    _finalize(work,output,predictions,complexity,histories,metadata,seed,tabular_budget,neural_budget,horizons,models);return output


def _finalize(work,output,predictions,complexity,histories,metadata,seed,tabular_budget,neural_budget,horizons,models):
    full=[];common=[];financial=[]
    for (horizon,model),frame in predictions.items():frame.to_parquet(work/"predictions"/f"{model.lower().replace(' ','_')}_{horizon}m.parquet",index=False);full.append(_metric_row(frame,"FULL"));financial.extend({"model":model,"horizon":horizon,**regression_financial_metrics(frame,non_overlapping=value)} for value in (False,True))
    for horizon in horizons:
        frames={model:frame for (h,model),frame in predictions.items() if h==horizon};keys=None
        for frame in frames.values():current=pd.MultiIndex.from_frame(frame[["symbol","timestamp"]]);keys=current if keys is None else keys.intersection(current)
        for model,frame in frames.items():common.append(_metric_row(frame.loc[pd.MultiIndex.from_frame(frame[["symbol","timestamp"]]).isin(keys)],"COMMON"))
    full=pd.DataFrame(full);common=pd.DataFrame(common);full.to_csv(work/"metrics"/"regression_metrics_full.csv",index=False);common.to_csv(work/"metrics"/"regression_metrics_common.csv",index=False);pd.DataFrame(financial).to_csv(work/"metrics"/"financial_evaluation.csv",index=False);pd.DataFrame(complexity).to_csv(work/"metrics"/"model_complexity.csv",index=False);(work/"histories"/"training_history.json").write_text(json.dumps(histories,indent=2)+"\n")
    baseline=pd.read_csv(BASELINE_ROOT/"metrics"/"regression_metrics_common.csv");baseline["model"]=baseline.model.replace({"Linear Regression":"Ridge"});comparison=common.merge(baseline,on=["model","horizon"],suffixes=("_tuned","_baseline"));comparison["mae_delta"]=comparison.mae_tuned-comparison.mae_baseline;comparison["rmse_delta"]=comparison.rmse_tuned-comparison.rmse_baseline;comparison["r2_delta"]=comparison.r2_tuned-comparison.r2_baseline;comparison.to_csv(work/"metrics"/"baseline_vs_tuned.csv",index=False)
    _figures(work,comparison,pd.read_csv(work/"tuning_trials.csv"));checksums={str(path.relative_to(work)).replace("\\","/"):sha256(path) for path in work.rglob("*") if path.is_file() and path.name!="manifest.json"};manifest={"experiment_id":EXPERIMENT_ID,"status":"frozen","created_at":datetime.now(timezone.utc).isoformat(),"dataset_version":metadata["dataset_version"],"feature_version":metadata["features_version"],"baseline_experiment":"phase11_regression_final_v1","selection_split":"validation","test_policy":"Read once only after all selected configs were persisted; no tuning followed test evaluation.","splits":metadata["splits"],"horizons":horizons,"models":models,"seed":seed,"trial_budgets":{"tabular":tabular_budget,"neural":neural_budget},"tuning_subsets":{"tabular_train":240000,"tabular_validation":80000,"neural_train":60000,"neural_validation":20000},"final_sequence_caps":SEQUENCE_FINAL_CAPS,"software":{"python":platform.python_version(),"sklearn":sklearn.__version__},"checksums":checksums};(work/"manifest.json").write_text(json.dumps(manifest,indent=2)+"\n");work.rename(output)


def _figures(work,comparison,trials):
    for metric,label in (("mae","MAE"),("rmse","RMSE")):
        data=comparison[["model","horizon",f"{metric}_baseline",f"{metric}_tuned"]].melt(["model","horizon"],var_name="variant",value_name=metric);ax=data.pivot(index=["model","horizon"],columns="variant",values=metric).plot(kind="bar",figsize=(11,5));ax.set_ylabel(label);plt.tight_layout();plt.savefig(work/"figures"/f"baseline_vs_tuned_{metric}.png",dpi=180);plt.close()
    completed=trials.query("status == 'COMPLETED'");ax=completed.boxplot(column="selection_metric_value",by="model",figsize=(9,5));ax.set_ylabel("Validation MAE");plt.suptitle("");plt.tight_layout();plt.savefig(work/"figures"/"validation_trial_scores.png",dpi=180);plt.close()


def main():
    parser=argparse.ArgumentParser();parser.add_argument("--features",type=Path,default=Path("datasets/ml/ml_dataset_v1/features_v1/features_targets.parquet"));parser.add_argument("--metadata",type=Path,default=Path("datasets/ml/ml_dataset_v1/features_v1/metadata.json"));parser.add_argument("--output",type=Path,default=Path(f"models/regression/{EXPERIMENT_ID}"));parser.add_argument("--tabular-trials",type=int,default=3);parser.add_argument("--neural-trials",type=int,default=2);parser.add_argument("--seed",type=int,default=42);parser.add_argument("--models",nargs="+",default=["ridge","random_forest","lstm","gru"]);parser.add_argument("--horizons",nargs="+",type=int,default=list(HORIZONS));args=parser.parse_args();print(tune_and_evaluate(args.features,args.metadata,args.output,args.tabular_trials,args.neural_trials,args.seed,args.models,args.horizons))


if __name__=="__main__":main()
