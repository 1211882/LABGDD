from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import pearsonr, spearmanr
from sklearn.metrics import balanced_accuracy_score, confusion_matrix, f1_score, mean_absolute_error, mean_squared_error, precision_recall_fscore_support, r2_score


def binary_metrics(actual, predicted):
    a, p = np.asarray(actual, dtype=bool), np.asarray(predicted, dtype=bool)
    tp = int((a & p).sum()); fp = int((~a & p).sum()); fn = int((a & ~p).sum()); tn = int((~a & ~p).sum())
    precision = tp / (tp + fp) if tp + fp else 0.
    recall = tp / (tp + fn) if tp + fn else 0.
    return {"precision": precision, "recall": recall, "f1": 2 * precision * recall / (precision + recall) if precision + recall else 0.,
            "true_positives": tp, "false_positives": fp, "missed_events": fn, "true_negatives": tn,
            "false_positive_rate": fp / (fp + tn) if fp + tn else None,
            "missed_event_rate": fn / (fn + tp) if fn + tp else None}


def statistical(kind, actual, prediction):
    actual = np.asarray(actual)
    if kind == "regression":
        prediction = np.asarray(prediction).reshape(-1)
        varied = np.ptp(actual) > 0 and np.ptp(prediction) > 0
        return {"mae": float(mean_absolute_error(actual, prediction)), "rmse": float(np.sqrt(mean_squared_error(actual, prediction))),
                "r2": float(r2_score(actual, prediction)) if len(actual) > 1 else None,
                "pearson": float(pearsonr(actual, prediction).statistic) if varied else None,
                "spearman": float(spearmanr(actual, prediction).statistic) if varied else None}
    prediction = np.asarray(prediction)
    predicted = prediction.argmax(axis=1) if prediction.ndim == 2 else prediction.astype(int)
    classes = [0, 1] if kind == "binary" else [0, 1, 2]
    precision, recall, f1, support = precision_recall_fscore_support(actual.astype(int), predicted, labels=classes, zero_division=0)
    return {"macro_f1": float(f1_score(actual, predicted, labels=classes, average="macro", zero_division=0)),
            "balanced_accuracy": float(balanced_accuracy_score(actual, predicted)),
            "accuracy": float(np.mean(actual == predicted)),
            "per_class": {str(c): {"precision": float(precision[i]), "recall": float(recall[i]), "f1": float(f1[i]), "support": int(support[i])} for i, c in enumerate(classes)},
            "confusion_matrix": confusion_matrix(actual, predicted, labels=classes).tolist(),
            "high_volatility" if kind != "binary" else "event": binary_metrics(actual == classes[-1], predicted == classes[-1])}


def primary(kind, actual, prediction):
    if kind == "regression":
        return float(mean_absolute_error(actual, np.asarray(prediction).reshape(-1)))
    predicted = prediction.argmax(axis=1) if np.asarray(prediction).ndim == 2 else prediction
    if kind == "binary":
        return binary_metrics(actual, predicted)["f1"]
    return float(f1_score(actual, predicted, labels=[0, 1, 2], average="macro", zero_division=0))


def monitoring(contract, actual, prediction, indices, *, sampled=True):
    panel = contract.panel
    meta = panel.meta.iloc[indices]
    if contract.task.startswith("return"):
        return {"applicable": False, "reason": "Direction/magnitude forecasts are secondary, not volatility alerts"}, pd.DataFrame(), pd.DataFrame()
    if contract.kind == "regression":
        threshold = np.array([contract.thresholds.get(str(entity), {}).get("high", np.nan) if contract.thresholds.get(str(entity)) else np.nan for entity in meta.entity])
        truth = actual > threshold; alert = np.asarray(prediction).reshape(-1) > threshold
    else:
        high = 1 if contract.kind == "binary" else 2
        truth = actual == high
        p = np.asarray(prediction)
        alert = (p.argmax(axis=1) if p.ndim == 2 else p) == high
    metrics = binary_metrics(truth, alert)
    days = meta.trading_date.nunique()
    metrics.update({"applicable": True, "false_alarms_per_observed_day": metrics["false_positives"] / days if days else None,
                    "observed_days": int(days), "evaluated_rows": len(indices), "alarm_rate_is_sampled": sampled,
                    "nominal_completion_lead_minutes": contract.horizon,
                    "lead_time_warning": "Completion lead is target geometry, not measured early economic-event onset; ingestion delay is unavailable"})
    detail = meta[["timestamp", "trading_date", "entity", "sector"]].copy().reset_index(drop=True)
    detail["actual_event"] = truth; detail["alert"] = alert
    detail["false_alarm"] = ~truth & alert; detail["missed_event"] = truth & ~alert
    sector = detail.groupby("sector", observed=True).agg(false_alarms=("false_alarm", "sum"), missed_events=("missed_event", "sum"), observations=("alert", "size"), days=("trading_date", "nunique")).reset_index()
    sector["false_alarms_per_day"] = sector.false_alarms / sector.days
    if sampled:
        metrics["sampled_false_alarms_per_observed_day"] = metrics["false_alarms_per_observed_day"]
        sector["sampled_false_alarms_per_day"] = sector.false_alarms_per_day
    true_positive = detail.loc[truth & alert].copy()
    true_positive["prediction_timestamp"] = true_positive.timestamp
    true_positive["target_window_start"] = true_positive.timestamp + pd.Timedelta(minutes=1)
    true_positive["target_window_completion"] = true_positive.timestamp + pd.Timedelta(minutes=contract.horizon)
    true_positive["completion_lead_minutes"] = contract.horizon
    metrics["mean_completion_lead_minutes"] = float(contract.horizon) if len(true_positive) else None
    metrics["median_completion_lead_minutes"] = float(contract.horizon) if len(true_positive) else None
    # Collapse events on the full eligible truth timeline before intersecting the
    # sampled inference population; gaps in forecasts never invent new episodes.
    full = contract.values()
    if contract.kind == "regression":
        lookup = {entity: values["high"] if values else np.nan for entity, values in contract.thresholds.items()}
        thresholds = panel.meta.entity.map(lookup).to_numpy(dtype=float)
        positives = np.flatnonzero(np.isfinite(full) & (full > thresholds))
    else:
        positives = np.flatnonzero(full == (1 if contract.kind == "binary" else 2))
    episode_by_row = np.full(len(full), -1, dtype="int32")
    if len(positives):
        pmeta = panel.meta.iloc[positives]
        breaks = np.r_[True, (np.diff(pmeta.timestamp.to_numpy(dtype="datetime64[m]")).astype(int) != 1)
                       | (pmeta.entity.astype(str).to_numpy()[1:] != pmeta.entity.astype(str).to_numpy()[:-1])]
        episode_by_row[positives] = np.cumsum(breaks) - 1
    visible = set(episode_by_row[np.asarray(indices)[truth]]) - {-1}
    detected = set(episode_by_row[np.asarray(indices)[truth & alert]]) - {-1}
    metrics["observable_episodes"] = len(visible)
    metrics["detected_episodes"] = len(detected)
    metrics["missed_observable_episodes"] = len(visible - detected)
    metrics["observable_episode_detection_rate"] = len(detected) / len(visible) if visible else None
    true_positive["episode_id"] = episode_by_row[np.asarray(indices)[truth & alert]]
    true_positive = true_positive.sort_values("prediction_timestamp").drop_duplicates("episode_id")
    return metrics, sector, true_positive


def financial(contract, indices, prediction, *, sampled=True):
    if not contract.task.startswith("return"):
        return {"applicable": False, "reason": "No trading policy is defined for volatility/emergence forecasts"}
    panel = contract.panel
    frame = panel.meta.iloc[indices][["timestamp", "trading_date", "entity"]].copy()
    frame["actual_return"] = panel.returns[indices, (5, 15, 30).index(contract.horizon)]
    if contract.kind == "regression":
        position = np.asarray(prediction).reshape(-1) > .0005
    else:
        p = np.asarray(prediction)
        position = (p.argmax(axis=1) if p.ndim == 2 else p) == 2
    frame["position"] = position.astype(int)
    local = frame.timestamp.dt.tz_convert("America/New_York")
    minute = local.dt.hour * 60 + local.dt.minute - 570
    frame = frame.loc[minute.mod(contract.horizon).eq(0)].sort_values(["entity", "timestamp"])
    previous = frame.groupby(["entity", "trading_date"], observed=True).position.shift().fillna(0)
    gaps = frame.groupby("entity", observed=True).timestamp.diff().ne(pd.Timedelta(minutes=contract.horizon))
    previous.loc[gaps] = 0
    trades = (frame.position - previous).abs()
    next_time = frame.groupby("entity", observed=True).timestamp.shift(-1)
    close = next_time.sub(frame.timestamp).ne(pd.Timedelta(minutes=contract.horizon))
    trades += frame.position * close
    frame["net"] = frame.position * frame.actual_return - .0005 * trades
    portfolio = frame.groupby("timestamp").net.mean().sort_index()
    equity = np.r_[1., np.cumprod(1 + portfolio.to_numpy())]
    drawdown = equity / np.maximum.accumulate(equity) - 1
    daily = portfolio.groupby(portfolio.index.tz_convert("America/New_York").date).apply(lambda x: np.prod(1 + x) - 1)
    std = daily.std(ddof=1) if len(daily) > 1 else 0.
    downside = float(np.sqrt(np.mean(np.minimum(daily, 0) ** 2))) if len(daily) else 0.
    losses = abs(portfolio.loc[portfolio < 0].sum())
    return {"applicable": True, "evaluation": "sampled_non_overlapping_diagnostic" if sampled else "eligible_population_non_overlapping_diagnostic", "transaction_cost": .0005,
            "net_return": float(equity[-1] - 1), "maximum_drawdown": float(drawdown.min()),
            "sharpe_daily": float(np.sqrt(252) * daily.mean() / std) if std else None,
            "sortino_daily": float(np.sqrt(252) * daily.mean() / downside) if downside else None,
            "profit_factor": float(portfolio.loc[portfolio > 0].sum() / losses) if losses else None,
            "turnover": float(trades.sum()), "long_windows": int(frame.position.sum()), "observed_days": len(daily),
            "warning": "Equal-weight observed eligible symbols, clock-grid windows, idealized fills; not an executable strategy backtest"}
