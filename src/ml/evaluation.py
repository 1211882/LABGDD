from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, balanced_accuracy_score, confusion_matrix, f1_score, log_loss, precision_recall_fscore_support, roc_auc_score

from src.ml.targets import CLASS_NAMES


def classification_metrics(actual, predicted, probabilities=None) -> dict:
    precision, recall, per_f1, support = precision_recall_fscore_support(actual, predicted, labels=CLASS_NAMES, zero_division=0)
    result = {
        "accuracy": float(accuracy_score(actual, predicted)),
        "balanced_accuracy": float(balanced_accuracy_score(actual, predicted)),
        "macro_f1": float(f1_score(actual, predicted, average="macro")),
        "weighted_f1": float(f1_score(actual, predicted, average="weighted")),
        "per_class": {name: {"precision": float(precision[i]), "recall": float(recall[i]), "f1": float(per_f1[i]), "support": int(support[i])} for i, name in enumerate(CLASS_NAMES)},
        "confusion_matrix": confusion_matrix(actual, predicted, labels=CLASS_NAMES).astype(int).tolist(),
    }
    if probabilities is not None and len(set(actual)) == 3:
        encoded = pd.get_dummies(pd.Categorical(actual, categories=CLASS_NAMES)).to_numpy()
        result["roc_auc_ovr_macro"] = float(roc_auc_score(encoded, probabilities, multi_class="ovr", average="macro"))
        result["log_loss"] = float(log_loss(actual, probabilities, labels=CLASS_NAMES))
        result["multiclass_brier_score"] = float(np.mean(np.sum((probabilities - encoded) ** 2, axis=1)))
    return result


def financial_metrics(frame: pd.DataFrame, transaction_cost: float = .0005) -> dict:
    ordered = frame.sort_values(["symbol", "timestamp"]).copy()
    ordered["position"] = (ordered["predicted_class"] == "UP").astype(int)
    ordered["strategy_return_gross"] = ordered["position"] * ordered["future_return"]
    ordered["trade"] = ordered.groupby("symbol")["position"].diff().abs().fillna(ordered["position"])
    ordered["strategy_return_net"] = ordered["strategy_return_gross"] - ordered["trade"] * transaction_cost
    returns = ordered.loc[ordered["position"] == 1, "strategy_return_net"]
    equity = (1 + ordered["strategy_return_net"]).cumprod()
    drawdown = equity / equity.cummax() - 1
    directional = ordered.loc[ordered["position"] == 1, "future_return"] > 0
    return {
        "signal_policy": "UP=long; NEUTRAL/DOWN=flat",
        "transaction_cost": transaction_cost,
        "signals": int(ordered["position"].sum()),
        "directional_accuracy": float(directional.mean()) if len(directional) else 0.0,
        "cumulative_return": float(equity.iloc[-1] - 1) if len(equity) else 0.0,
        "average_return_per_signal": float(returns.mean()) if len(returns) else 0.0,
        "sharpe_signal_level": float(returns.mean() / returns.std(ddof=1) * np.sqrt(252)) if len(returns) > 1 and returns.std(ddof=1) else 0.0,
        "maximum_drawdown": float(drawdown.min()) if len(drawdown) else 0.0,
        "turnover": float(ordered["trade"].sum()),
        "transaction_cost_adjusted_return": float(equity.iloc[-1] - 1) if len(equity) else 0.0,
        "warning": "Overlapping horizon signals are a research simulation, not executable trading profits.",
    }


def portfolio_financial_metrics(frame: pd.DataFrame, transaction_cost: float = .0005, non_overlapping: bool = False) -> dict:
    ordered = frame.sort_values(["symbol", "timestamp"]).copy()
    if non_overlapping:
        horizon = int(ordered["horizon"].iloc[0])
        local = pd.to_datetime(ordered["timestamp"], utc=True).dt.tz_convert("America/New_York")
        minute = local.dt.hour * 60 + local.dt.minute - 570
        ordered = ordered.loc[minute.mod(horizon).eq(0)].copy()
    ordered["position"] = ordered["predicted_class"].eq("UP").astype(int)
    ordered["trade"] = ordered.groupby("symbol")["position"].diff().abs().fillna(ordered["position"])
    ordered["net_return"] = ordered["position"] * ordered["future_return"] - ordered["trade"] * transaction_cost
    portfolio = ordered.groupby("timestamp")["net_return"].mean().sort_index()
    equity = (1 + portfolio).cumprod()
    drawdown = equity / equity.cummax() - 1
    signals = ordered.loc[ordered["position"].eq(1)]
    return {
        "evaluation_type": "NON_OVERLAPPING_SANITY_CHECK" if non_overlapping else "DIAGNOSTIC_SIGNAL_EVALUATION",
        "signals": int(len(signals)),
        "timestamps": int(len(portfolio)),
        "directional_accuracy": float((signals["future_return"] > 0).mean()) if len(signals) else 0.0,
        "cumulative_return": float(equity.iloc[-1] - 1) if len(equity) else 0.0,
        "maximum_drawdown": float(drawdown.min()) if len(drawdown) else 0.0,
        "turnover": float(ordered["trade"].sum()),
        "transaction_cost": transaction_cost,
        "overlapping_horizons": not non_overlapping,
    }
