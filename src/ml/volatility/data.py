from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from src.ml.features import FEATURE_COLUMNS

WINDOWS = (1, 5, 15, 30, 60)
HORIZONS = (5, 15, 30)
EXTRA = [f"past_rv_{h}" for h in WINDOWS] + ["observed_bar", "past_coverage_60"]
SECTOR_FEATURES = ["sector_past_rv_15", "sector_past_rv_dispersion_15", "sector_past_correlation_15", "sector_past_hhi_15", "sector_observed_fraction"]
FEATURES = FEATURE_COLUMNS + EXTRA + SECTOR_FEATURES


def dump(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    temporary.replace(path)


def load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def realized_paths(close: pd.DataFrame, sessions: np.ndarray, horizon: int):
    """Exact clock-minute prices on a session grid; no missing-price filling."""
    returns = np.log(close.where(close > 0)).diff()
    returns.loc[np.r_[True, sessions[1:] != sessions[:-1]]] = np.nan
    past = np.sqrt(returns.pow(2).rolling(horizon, min_periods=horizon).sum())
    future = past.shift(-horizon)
    same_session = pd.Series(sessions).eq(pd.Series(sessions).shift(-horizon)).to_numpy()
    future.loc[~same_session] = np.nan
    endpoint_return = close.shift(-horizon).div(close).sub(1)
    endpoint_return.loc[~same_session] = np.nan
    return returns, past, future, endpoint_return


def sector_components(rv: np.ndarray, returns: pd.DataFrame, horizon: int):
    """Fixed-universe RMS risk and variance-share concentration; NaNs stay missing."""
    complete = np.isfinite(rv).all(axis=1)
    total = np.sum(rv ** 2, axis=1)
    shares = np.divide(rv ** 2, total[:, None], out=np.full_like(rv, np.nan), where=total[:, None] > 0)
    ordered = np.sort(shares, axis=1)
    result = np.column_stack([
        np.sqrt(np.mean(rv ** 2, axis=1)), np.mean(rv, axis=1), np.median(rv, axis=1),
        np.std(rv, axis=1), np.sum(shares ** 2, axis=1), ordered[:, -1], ordered[:, -2:].sum(axis=1),
    ])
    pairs = []
    if horizon >= 3:
        for i in range(returns.shape[1]):
            for j in range(i + 1, returns.shape[1]):
                corr = returns.iloc[:, i].rolling(horizon, min_periods=horizon).corr(returns.iloc[:, j]).to_numpy(copy=True)
                corr[~np.isfinite(corr)] = np.nan
                pairs.append(np.clip(corr, -1, 1))
    if pairs:
        correlations = np.array(pairs).T
        counts = np.isfinite(correlations).sum(axis=1)
        mean = np.divide(np.nansum(correlations, axis=1), counts, out=np.full(len(rv), np.nan), where=counts > 0)
        absolute = np.divide(np.nansum(np.abs(correlations), axis=1), counts, out=np.full(len(rv), np.nan), where=counts > 0)
    else:
        counts = np.zeros(len(rv)); mean = absolute = np.full(len(rv), np.nan)
    result = np.column_stack([result, mean, absolute, counts])
    result[~complete] = np.nan
    return result


COMPONENTS = ["sector_rv", "mean_stock_rv", "median_stock_rv", "dispersion_rv", "hhi", "top1_share", "top2_share", "mean_correlation", "absolute_correlation", "valid_pairs"]


def prepare(root: Path, processed: Path, features: Path, metadata: Path, config: Path):
    """Build disk-backed panels without touching any previous experiment."""
    destination = root / "data"
    if (destination / "schema.json").exists():
        return
    destination.mkdir(parents=True, exist_ok=True)
    mapping = yaml.safe_load(config.read_text(encoding="utf-8"))["symbols"]
    symbols = sorted(mapping)
    sectors = sorted({mapping[s]["sector"] for s in symbols})
    split_spec = load(metadata)["splits"]
    import pyarrow.parquet as pq
    source_columns = pq.read_schema(processed).names
    price_fields = [c for c in ("open", "high", "low", "close", "volume") if c in source_columns]
    bars = pd.read_parquet(processed, columns=["timestamp", "symbol", "trading_date"] + price_fields)
    if bars.duplicated(["symbol", "timestamp"]).any():
        raise ValueError("Duplicate raw bar identities")
    dates = sorted(bars.trading_date.unique())
    grid = pd.DatetimeIndex(np.concatenate([
        pd.date_range(pd.Timestamp(f"{date} 09:30", tz="America/New_York"), periods=390, freq="min").tz_convert("UTC").to_numpy()
        for date in dates
    ]))
    sessions = np.repeat(dates, 390)
    close = bars.pivot(index="timestamp", columns="symbol", values="close").reindex(index=grid, columns=symbols)
    observed_fields = {field: bars.pivot(index="timestamp", columns="symbol", values=field).reindex(index=grid, columns=symbols) for field in price_fields}
    del bars
    t, n = len(grid), len(symbols)
    company = destination / "company"; company.mkdir(exist_ok=True)
    sector_dir = destination / "sector"; sector_dir.mkdir(exist_ok=True)
    x = np.lib.format.open_memmap(company / "x.npy", mode="w+", dtype="float32", shape=(n * t, len(FEATURES)))
    rv = np.lib.format.open_memmap(company / "rv.npy", mode="w+", dtype="float32", shape=(n * t, len(WINDOWS)))
    past_rv = np.lib.format.open_memmap(company / "past_rv.npy", mode="w+", dtype="float32", shape=rv.shape)
    endpoint = np.lib.format.open_memmap(company / "returns.npy", mode="w+", dtype="float64", shape=(n * t, 3))
    present = np.lib.format.open_memmap(company / "present.npy", mode="w+", dtype="bool", shape=(n * t,))
    ready = np.lib.format.open_memmap(company / "return_ready.npy", mode="w+", dtype="bool", shape=(n * t,))
    returns = None
    future_by_window = {}; past_by_window = {}
    for h in WINDOWS:
        returns, backward, forward, future_return = realized_paths(close, sessions, h)
        future_by_window[h] = forward.to_numpy(dtype="float32")
        past_by_window[h] = backward.to_numpy(dtype="float32")
        for i in range(n):
            section = slice(i * t, (i + 1) * t)
            rv[section, WINDOWS.index(h)] = forward.iloc[:, i]
            past_rv[section, WINDOWS.index(h)] = backward.iloc[:, i]
            if h in HORIZONS:
                endpoint[section, HORIZONS.index(h)] = future_return.iloc[:, i]
    for i, symbol in enumerate(symbols):
        print(f"PREPARE company {symbol}", flush=True)
        section = slice(i * t, (i + 1) * t)
        frame = pd.read_parquet(features, filters=[("symbol", "=", symbol)], columns=["timestamp"] + FEATURE_COLUMNS)
        frame = frame.set_index("timestamp").reindex(grid)
        values = frame[FEATURE_COLUMNS].to_numpy(dtype="float32")
        x[section, :len(FEATURE_COLUMNS)] = values
        ready[section] = np.isfinite(values).all(axis=1)
        for field, observed_values in observed_fields.items():
            x[section, FEATURES.index(field)] = observed_values.iloc[:, i]
        local = grid.tz_convert("America/New_York")
        for field, field_values in (("hour", local.hour), ("minute_of_day", local.hour * 60 + local.minute), ("minutes_since_open", local.hour * 60 + local.minute - 570)):
            x[section, FEATURES.index(field)] = field_values
        for h in WINDOWS:
            x[section, FEATURES.index(f"past_rv_{h}")] = past_by_window[h][:, i]
        observed = np.isfinite(close.iloc[:, i].to_numpy()) & (close.iloc[:, i].to_numpy() > 0)
        present[section] = observed
        x[section, FEATURES.index("observed_bar")] = observed
        coverage = pd.Series(observed.astype(float)).groupby(sessions).transform(lambda group: group.rolling(60, min_periods=1).mean())
        x[section, FEATURES.index("past_coverage_60")] = coverage
    sx = np.lib.format.open_memmap(sector_dir / "x.npy", mode="w+", dtype="float32", shape=(len(sectors) * t, len(FEATURES)))
    srv = np.lib.format.open_memmap(sector_dir / "rv.npy", mode="w+", dtype="float32", shape=(len(sectors) * t, len(WINDOWS)))
    sprv = np.lib.format.open_memmap(sector_dir / "past_rv.npy", mode="w+", dtype="float32", shape=srv.shape)
    components = np.lib.format.open_memmap(sector_dir / "components.npy", mode="w+", dtype="float32", shape=(len(sectors) * t, len(WINDOWS), len(COMPONENTS)))
    stock_rv = np.lib.format.open_memmap(sector_dir / "stock_rv.npy", mode="w+", dtype="float32", shape=(len(sectors) * t, len(WINDOWS), 5))
    past_components = np.lib.format.open_memmap(sector_dir / "past_components.npy", mode="w+", dtype="float32", shape=components.shape)
    past_stock_rv = np.lib.format.open_memmap(sector_dir / "past_stock_rv.npy", mode="w+", dtype="float32", shape=stock_rv.shape)
    analytics = []; inter = []
    for si, sector in enumerate(sectors):
        members = [symbols.index(s) for s in symbols if mapping[s]["sector"] == sector]
        if len(members) != 5:
            raise ValueError("This dataset contract expects five configured constituents per sector")
        print(f"PREPARE sector {sector}", flush=True)
        section = slice(si * t, (si + 1) * t)
        arrays = np.stack([np.asarray(x[mi * t:(mi + 1) * t, :len(FEATURE_COLUMNS) + len(EXTRA)]) for mi in members])
        counts = np.isfinite(arrays).sum(axis=0)
        aggregate = np.divide(np.nansum(arrays, axis=0), counts, out=np.full(counts.shape, np.nan, dtype="float32"), where=counts > 0)
        sx[section, :aggregate.shape[1]] = aggregate
        for h in WINDOWS:
            past_values = sector_components(past_by_window[h][:, members], returns.iloc[:, members], h)
            future_values = pd.DataFrame(past_values).shift(-h).to_numpy(copy=True)
            same = pd.Series(sessions).eq(pd.Series(sessions).shift(-h)).to_numpy()
            future_values[~same] = np.nan
            components[section, WINDOWS.index(h)] = future_values
            past_components[section, WINDOWS.index(h)] = past_values
            stock_rv[section, WINDOWS.index(h)] = future_by_window[h][:, members]
            past_stock_rv[section, WINDOWS.index(h)] = past_by_window[h][:, members]
            srv[section, WINDOWS.index(h)] = future_values[:, 0]
            sprv[section, WINDOWS.index(h)] = past_values[:, 0]
            if h == 15:
                shared = np.column_stack([past_values[:, 0], past_values[:, 3], past_values[:, 7], past_values[:, 4], np.isfinite(close.iloc[:, members]).mean(axis=1)])
                sx[section, -len(SECTOR_FEATURES):] = shared
                for member in members:
                    x[member * t:(member + 1) * t, -len(SECTOR_FEATURES):] = shared
            summary = pd.DataFrame(past_values, columns=COMPONENTS)
            summary["timestamp"] = grid; summary["trading_date"] = sessions
            summary["sector"] = sector; summary["window"] = h
            calibration = sessions <= split_spec["train"]["end"]
            high_threshold = np.nanquantile(past_by_window[h][calibration][:, members], 2 / 3, axis=0)
            constituent_rv = past_by_window[h][:, members]
            complete = np.isfinite(constituent_rv).all(axis=1)
            high_count = (constituent_rv > high_threshold).sum(axis=1).astype(float)
            high_count[~complete] = np.nan
            summary["high_constituents"] = high_count
            summary["high_fraction"] = high_count / len(members)
            summary["synchronization_ratio"] = summary["high_fraction"]
            summary["broadness"] = (1 - summary.hhi) / (1 - 1 / len(members))
            cumulative_returns = returns.iloc[:, members].rolling(h, min_periods=h).sum()
            summary["return_dispersion"] = cumulative_returns.std(axis=1, ddof=0, skipna=False).to_numpy()
            # Preserve timestamp-level analytics for plotting and event interpretation.
            summary.to_parquet(destination / f"analytics_{si}_{h}.parquet", index=False)
            grouped = summary.groupby("trading_date")[COMPONENTS + ["high_constituents", "high_fraction", "synchronization_ratio", "broadness", "return_dispersion"]].mean().reset_index()
            grouped["sector"] = sector; grouped["window"] = h
            analytics.append(grouped)
    sector_returns = pd.DataFrame({sector: returns.iloc[:, [symbols.index(s) for s in symbols if mapping[s]["sector"] == sector]].mean(axis=1, skipna=False) for sector in sectors})
    for h in WINDOWS:
        if h < 3:
            continue
        for i, left in enumerate(sectors):
            for right in sectors[i + 1:]:
                correlation = sector_returns[left].rolling(h, min_periods=h).corr(sector_returns[right])
                correlation = correlation.where(np.isfinite(correlation)).clip(-1, 1)
                frame = pd.DataFrame({"trading_date": sessions, "correlation": correlation.to_numpy()})
                frame["absolute_correlation"] = frame.correlation.abs()
                frame = frame.groupby("trading_date").agg(correlation=("correlation", "mean"), absolute_correlation=("absolute_correlation", "mean"), valid_windows=("correlation", "count")).reset_index()
                frame["left_sector"] = left; frame["right_sector"] = right; frame["window"] = h
                inter.append(frame)
    for level, entities in (("company", symbols), ("sector", sectors)):
        records = []
        for entity in entities:
            meta = pd.DataFrame({"timestamp": grid, "trading_date": sessions, "entity": entity,
                                 "sector": mapping[entity]["sector"] if level == "company" else entity})
            meta["split"] = np.where(meta.trading_date <= split_spec["train"]["end"], "train", np.where(meta.trading_date <= split_spec["validation"]["end"], "validation", "test"))
            records.append(meta)
        meta = pd.concat(records, ignore_index=True)
        for column in ("entity", "sector", "split", "trading_date"):
            meta[column] = meta[column].astype("category")
        meta.to_parquet(destination / level / "metadata.parquet", index=False)
    pd.concat(analytics, ignore_index=True).to_parquet(destination / "sector_daily_analytics.parquet", index=False)
    pd.concat(inter, ignore_index=True).to_csv(destination / "inter_sector_correlation.csv", index=False)
    for array in (x, rv, past_rv, endpoint, present, ready, sx, srv, sprv, components, stock_rv, past_components, past_stock_rv):
        array.flush()
    dump(destination / "schema.json", {"features": FEATURES, "symbols": symbols, "sectors": sectors, "windows": WINDOWS,
        "horizons": HORIZONS, "components": COMPONENTS, "splits": split_spec, "sector_members": {s: [symbol for symbol in symbols if mapping[symbol]["sector"] == s] for s in sectors},
        "rows_per_entity": t, "company_rows": n * t, "sector_rows": len(sectors) * t,
        "missing_prices_filled": False, "target_definition": "sqrt(sum of squared exact one-minute future log returns)"})


class Panel:
    def __init__(self, root: Path, level: str):
        root = resolve_data_root(root)
        self.level = level
        self.schema = load(root / "data" / "schema.json")
        self.directory = root / "data" / level
        self.meta = pd.read_parquet(self.directory / "metadata.parquet")
        self.x = np.load(self.directory / "x.npy", mmap_mode="r")
        self.rv = np.load(self.directory / "rv.npy", mmap_mode="r")
        self.past_rv = np.load(self.directory / "past_rv.npy", mmap_mode="r")
        if level == "company":
            self.returns = np.load(self.directory / "returns.npy", mmap_mode="r")
            self.present = np.load(self.directory / "present.npy", mmap_mode="r")
            self.return_ready = np.load(self.directory / "return_ready.npy", mmap_mode="r")
        else:
            self.present = np.ones(len(self.meta), dtype=bool)
            for name in ("components", "stock_rv", "past_components", "past_stock_rv"):
                setattr(self, name, np.load(self.directory / f"{name}.npy", mmap_mode="r"))


def eligible_sequences(panel: Panel, lookback: int) -> np.ndarray:
    n = panel.schema["rows_per_entity"]
    position = np.arange(len(panel.meta)) % n % 390
    invalid = (~panel.present).astype(int)
    prefix = np.r_[0, np.cumsum(invalid)]
    ends = np.arange(len(panel.meta))
    starts = np.maximum(ends - lookback + 1, 0)
    return (position >= lookback - 1) & (prefix[ends + 1] - prefix[starts] == 0)


def representative_indices(meta: pd.DataFrame, eligible: np.ndarray, cap: int | None) -> np.ndarray:
    """Round-robin entity/date strata across the full eligible chronology."""
    indices = np.flatnonzero(eligible)
    if cap is None or len(indices) <= cap:
        return indices
    frame = meta.iloc[indices][["entity", "trading_date"]].copy()
    frame["row"] = indices
    mandatory = []
    for _, group in frame.groupby("entity", observed=True, sort=True):
        mandatory.append(int(group.row.iloc[len(group) // 2]))
    for j, (_, group) in enumerate(frame.groupby("trading_date", observed=True, sort=True)):
        mandatory.append(int(group.row.iloc[(j * 37) % len(group)]))
    mandatory = np.unique(mandatory)
    if len(mandatory) > cap:
        mandatory = mandatory[np.linspace(0, len(mandatory) - 1, cap, dtype=int)]
    groups = [g.row.to_numpy() for _, g in frame.groupby(["trading_date", "entity"], observed=True, sort=True)]
    # Rotating entity priority across dates avoids selecting the same symbols on
    # every date when the cap cannot include every entity/date stratum.
    entity_count = max(1, meta.entity.nunique())
    order = sorted(range(len(groups)), key=lambda j: (j // entity_count, (j + j // entity_count) % entity_count))
    groups = [groups[j] for j in order]
    allocation = np.zeros(len(groups), dtype=int)
    if cap < len(groups):
        allocation[np.linspace(0, len(groups) - 1, cap, dtype=int)] = 1
    else:
        allocation[:] = cap // len(groups)
        allocation[np.linspace(0, len(groups) - 1, cap % len(groups), dtype=int)] += 1
    selected = list(mandatory)
    for values, count in zip(groups, allocation):
        if count:
            selected.extend(values[np.linspace(0, len(values) - 1, min(count, len(values)), dtype=int)])
    selected = list(np.unique(selected))
    if len(selected) > cap:
        remaining = np.setdiff1d(selected, mandatory)
        selected = list(mandatory) + list(remaining[np.linspace(0, len(remaining) - 1, cap - len(mandatory), dtype=int)])
    # If sparse strata exhaust early, fill from unused keys without duplicates.
    if len(selected) < cap:
        remainder = np.setdiff1d(indices, selected)
        selected.extend(remainder[np.linspace(0, len(remainder) - 1, cap - len(selected), dtype=int)])
    return np.sort(np.asarray(selected, dtype=int))


def resolve_data_root(root: Path) -> Path:
    """Resolve an immutable shared target cache without copying its large arrays."""
    if (root / "data" / "schema.json").is_file():
        return root
    reference = load(root / "data_reference.json")
    source = Path(reference["experiment_root"])
    from src.ml.dataset import sha256
    if sha256(source / "data" / "schema.json") != reference["schema_sha256"]:
        raise ValueError("Referenced target schema changed")
    return source
