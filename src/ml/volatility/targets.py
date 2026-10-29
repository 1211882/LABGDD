from __future__ import annotations

import numpy as np

from src.ml.volatility.data import HORIZONS, WINDOWS, Panel

TASKS = {
    "return_class": ("company", "classification"),
    "return_reg": ("company", "regression"),
    "company_rv": ("company", "regression"),
    "company_regime": ("company", "classification"),
    "sector_rv": ("sector", "regression"),
    "sector_regime": ("sector", "classification"),
    "emergence": ("sector", "binary"),
}


class Contract:
    """Every quantile and baseline parameter is fitted on the supplied past mask."""
    def __init__(self, panel: Panel, company: Panel, task: str, horizon: int, training: np.ndarray):
        self.panel, self.task, self.horizon = panel, task, horizon
        self.kind = TASKS[task][1]
        self.thresholds = {}; self.company_high = {}; self.majority = 1
        self.index = WINDOWS.index(horizon)
        self.training_end = str(panel.meta.loc[training, "trading_date"].astype(str).max())
        if task.startswith("return"):
            labels = self.values()
            valid = training & np.isfinite(labels)
            if self.kind != "regression":
                self.majority = int(np.bincount(labels[valid].astype(int), minlength=3).argmax())
            return
        for entity, indices in panel.meta.groupby("entity", observed=True).indices.items():
            indices = np.asarray(indices); past = indices[training[indices]]
            values = np.asarray(panel.rv[past, self.index])
            values = values[np.isfinite(values)]
            if not len(values):
                self.thresholds[str(entity)] = None
                continue
            quantiles = np.quantile(values, [1 / 3, 2 / 3])
            entry = {"low": float(quantiles[0]), "high": float(quantiles[1]), "degenerate": bool(quantiles[0] == quantiles[1])}
            if task == "emergence":
                components = panel.components[past, self.index]
                for name, column in (("correlation", 7), ("hhi", 4)):
                    observed = components[:, column]; observed = observed[np.isfinite(observed)]
                    entry[name] = float(np.quantile(observed, 2 / 3)) if len(observed) else None
                if entry["correlation"] is not None:
                    entry["correlation"] = max(0., entry["correlation"])
            self.thresholds[str(entity)] = entry
        if task == "emergence":
            dates = company.meta.trading_date.astype(str).to_numpy()
            for entity, indices in company.meta.groupby("entity", observed=True).indices.items():
                indices = np.asarray(indices)
                past = indices[dates[indices] <= self.training_end]
                values = np.asarray(company.rv[past, self.index]); values = values[np.isfinite(values)]
                self.company_high[str(entity)] = float(np.quantile(values, 2 / 3)) if len(values) else None

    def values(self, *, past: bool = False):
        panel = self.panel
        if self.task.startswith("return"):
            value = np.asarray(panel.returns[:, HORIZONS.index(self.horizon)])
            ready = panel.return_ready
            if self.kind == "classification":
                value = np.where(value < -.0005, 0., np.where(value > .0005, 2., 1.))
                value[~np.isfinite(panel.returns[:, HORIZONS.index(self.horizon)])] = np.nan
            else:
                value = value.copy()
            value[~ready] = np.nan
            return value
        source = panel.past_rv if past else panel.rv
        rv = np.asarray(source[:, self.index])
        if self.kind == "regression":
            return rv.astype(float)
        result = np.full(len(panel.meta), np.nan)
        for entity, indices in panel.meta.groupby("entity", observed=True).indices.items():
            indices = np.asarray(indices)
            thresholds = self.thresholds.get(str(entity))
            if thresholds is None:
                continue
            values = rv[indices]
            if self.task != "emergence":
                classes = np.where(values < thresholds["low"], 0., np.where(values > thresholds["high"], 2., 1.))
                classes[~np.isfinite(values)] = np.nan
                result[indices] = classes
                continue
            if thresholds["correlation"] is None or thresholds["hhi"] is None:
                continue
            members = panel.schema["sector_members"][str(entity)]
            high = [self.company_high.get(member) for member in members]
            if any(value is None for value in high):
                continue
            stocks = (panel.past_stock_rv if past else panel.stock_rv)[indices, self.index]
            components = (panel.past_components if past else panel.components)[indices, self.index]
            elevated = (values > thresholds["high"]) & ((stocks > high).sum(axis=1) >= len(members) // 2 + 1)
            valid = np.isfinite(stocks).all(axis=1) & np.isfinite(values)
            valid &= ~elevated | np.isfinite(components[:, [4, 7]]).all(axis=1)
            event = elevated.copy()
            event &= (components[:, 7] > thresholds["correlation"]) & (components[:, 4] <= thresholds["hhi"])
            result[indices[valid]] = event[valid].astype(float)
        return result

    def baseline(self):
        if self.task == "return_reg":
            return np.zeros(len(self.panel.meta))
        if self.task == "return_class":
            return np.full(len(self.panel.meta), self.majority)
        return self.values(past=True)

    def describe(self):
        return {"task": self.task, "horizon": self.horizon, "training_end": self.training_end,
                "entity_thresholds": self.thresholds, "constituent_high_thresholds": self.company_high,
                "baseline_majority": self.majority, "thresholds_used_test": False}
