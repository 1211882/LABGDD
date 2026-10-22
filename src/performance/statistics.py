from __future__ import annotations

import math
import statistics
from typing import Iterable


def percentile(values: Iterable[float], probability: float) -> float | None:
    items = sorted(float(value) for value in values)
    if not items:
        return None
    if not 0 <= probability <= 1:
        raise ValueError("probability must be between zero and one")
    position = (len(items) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return items[lower]
    return items[lower] + (items[upper] - items[lower]) * (position - lower)


def summarize(values: Iterable[float]) -> dict[str, float | None]:
    items = [float(value) for value in values]
    if not items:
        return {key: None for key in ("mean", "median", "standard_deviation", "minimum", "maximum")}
    return {
        "mean": statistics.fmean(items),
        "median": statistics.median(items),
        "standard_deviation": statistics.stdev(items) if len(items) > 1 else 0.0,
        "minimum": min(items),
        "maximum": max(items),
    }
