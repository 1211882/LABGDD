from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import pearsonr, spearmanr
from sklearn.metrics import mean_absolute_error, mean_squared_error, median_absolute_error, r2_score


def regression_metrics(actual, predicted, *, zero_epsilon: float = .0005) -> dict:
    actual = np.asarray(actual, dtype=float)
    predicted = np.asarray(predicted, dtype=float)
    mse = mean_squared_error(actual, predicted)
    actual_direction = np.where(actual > zero_epsilon, 1, np.where(actual < -zero_epsilon, -1, 0))
    predicted_direction = np.where(predicted > zero_epsilon, 1, np.where(predicted < -zero_epsilon, -1, 0))
    has_variation = np.ptp(predicted) > 0 and np.ptp(actual) > 0
    pearson = pearsonr(actual, predicted).statistic if has_variation else 0.0
    spearman = spearmanr(actual, predicted).statistic if has_variation else 0.0
    return {
        "mae": float(mean_absolute_error(actual, predicted)),
        "mse": float(mse),
        "rmse": float(np.sqrt(mse)),
        "r2": float(r2_score(actual, predicted)),
        "median_absolute_error": float(median_absolute_error(actual, predicted)),
        "pearson_correlation": float(pearson),
        "spearman_correlation": float(spearman),
        "directional_accuracy": float(np.mean(actual_direction == predicted_direction)),
        "zero_epsilon": zero_epsilon,
    }


def error_distribution(actual, predicted) -> dict:
    error = np.asarray(predicted, dtype=float) - np.asarray(actual, dtype=float)
    return {
        "mean_error": float(np.mean(error)), "median_error": float(np.median(error)),
        "std_error": float(np.std(error, ddof=1)),
        **{f"p{int(q * 100)}": float(np.quantile(error, q)) for q in (.05, .25, .5, .75, .95)},
    }


def return_magnitude_bucket(values: pd.Series) -> pd.Series:
    absolute = values.abs()
    return pd.cut(absolute, [-np.inf, .0005, .001, .0025, .005, np.inf], labels=["<5bps", "5-10bps", "10-25bps", "25-50bps", ">50bps"], right=False)


def regression_financial_metrics(frame: pd.DataFrame, *, threshold: float = .0005, transaction_cost: float = .0005, non_overlapping: bool = False) -> dict:
    ordered = frame.sort_values(["symbol", "timestamp"]).copy()
    if non_overlapping:
        horizon = int(ordered["horizon"].iloc[0])
        local = pd.to_datetime(ordered["timestamp"], utc=True).dt.tz_convert("America/New_York")
        minute = local.dt.hour * 60 + local.dt.minute - 570
        ordered = ordered.loc[minute.mod(horizon).eq(0)].copy()
    ordered["position"] = ordered["predicted_return"].gt(threshold).astype(int)
    ordered["trade"] = ordered.groupby("symbol")["position"].diff().abs().fillna(ordered["position"])
    ordered["net_return"] = ordered["position"] * ordered["actual_return"] - ordered["trade"] * transaction_cost
    portfolio = ordered.groupby("timestamp")["net_return"].mean().sort_index()
    equity = (1 + portfolio).cumprod()
    drawdown = equity / equity.cummax() - 1
    signals = ordered.loc[ordered["position"].eq(1)]
    signal_returns = signals["actual_return"] - transaction_cost * signals["trade"]
    return {
        "evaluation_type": "NON_OVERLAPPING_SANITY_CHECK" if non_overlapping else "DIAGNOSTIC_SIGNAL_EVALUATION",
        "signals": int(len(signals)), "timestamps": int(len(portfolio)),
        "directional_accuracy": float((signals["actual_return"] > 0).mean()) if len(signals) else 0.0,
        "average_return_per_signal": float(signal_returns.mean()) if len(signals) else 0.0,
        "sharpe_signal_level": float(signal_returns.mean() / signal_returns.std(ddof=1) * np.sqrt(252)) if len(signal_returns) > 1 and signal_returns.std(ddof=1) else 0.0,
        "cumulative_return": float(equity.iloc[-1] - 1) if len(equity) else 0.0,
        "maximum_drawdown": float(drawdown.min()) if len(drawdown) else 0.0,
        "turnover": float(ordered["trade"].sum()), "transaction_cost": transaction_cost,
        "signal_threshold": threshold, "overlapping_horizons": not non_overlapping,
    }
