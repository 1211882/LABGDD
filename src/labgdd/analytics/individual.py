"""Independent scalar reference for v2.0.0 individual-company measurements."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, localcontext
from math import isfinite, sqrt

HORIZONS = (5, 15, 30, 60)


@dataclass(frozen=True, slots=True)
class ReferenceBar:
    event_time: datetime
    open: Decimal
    close: Decimal
    membership_valid_from: datetime
    membership_valid_to: datetime


@dataclass(frozen=True, slots=True)
class ReferenceReturn:
    event_time: datetime
    return_end: datetime
    intrabar_log_return: float | None
    close_to_close_log_return: float | None
    eligible: bool
    reason: str | None


@dataclass(frozen=True, slots=True)
class ReferenceWindow:
    horizon: int
    window_start: datetime
    window_end: datetime
    expected_returns: int
    observed_returns: int
    value: float | None
    realized_variance: float | None
    eligible: bool
    reason: str | None


@dataclass(frozen=True, slots=True)
class ReferenceMeasurements:
    returns: tuple[ReferenceReturn, ...]
    rolling: tuple[ReferenceWindow, ...]
    fixed: tuple[ReferenceWindow, ...]


def _log_ratio(numerator: Decimal, denominator: Decimal) -> float:
    if numerator <= 0 or denominator <= 0:
        raise ValueError("reference measurement: prices must be positive")
    with localcontext() as context:
        context.prec = 50
        result = float((numerator / denominator).ln())
    if not isfinite(result):
        raise ValueError("reference measurement: non-finite result")
    return result


def calculate_individual_measurements(
    bars: tuple[ReferenceBar, ...],
    session_open: datetime,
    session_close: datetime,
    *,
    horizons: tuple[int, ...] = HORIZONS,
) -> ReferenceMeasurements:
    """Calculate expected-slot returns, rolling sample SD and fixed RV without filling gaps."""
    if session_close <= session_open or any(h not in HORIZONS for h in horizons):
        raise ValueError("reference measurement: invalid session or horizon")
    by_time = {bar.event_time: bar for bar in bars}
    if len(by_time) != len(bars):
        raise ValueError("reference measurement: duplicate event time")
    minutes = int((session_close - session_open).total_seconds() // 60)
    returns: list[ReferenceReturn] = []
    values: list[float | None] = []
    memberships: list[tuple[datetime, datetime] | None] = []
    for index in range(minutes):
        event_time = session_open + timedelta(minutes=index)
        current = by_time.get(event_time)
        prior = by_time.get(event_time - timedelta(minutes=1)) if index else None
        intrabar = _log_ratio(current.close, current.open) if current is not None else None
        reason: str | None = None
        value: float | None = None
        if current is None:
            reason = "missing_close"
        elif index == 0:
            reason = "session_boundary"
        elif prior is None:
            reason = "nonconsecutive_bars"
        elif (
            current.membership_valid_from != prior.membership_valid_from
            or current.membership_valid_to != prior.membership_valid_to
            or not (
                current.membership_valid_from <= prior.event_time
                and current.event_time < current.membership_valid_to
            )
        ):
            reason = "membership_boundary"
        else:
            value = _log_ratio(current.close, prior.close)
        returns.append(
            ReferenceReturn(
                event_time,
                event_time + timedelta(minutes=1),
                intrabar,
                value,
                value is not None,
                reason,
            )
        )
        values.append(value)
        memberships.append(
            (current.membership_valid_from, current.membership_valid_to)
            if current is not None
            else None
        )

    rolling: list[ReferenceWindow] = []
    fixed: list[ReferenceWindow] = []
    for horizon in horizons:
        for end_index in range(1, minutes):
            start_index = end_index - horizon + 1
            window_values = values[max(1, start_index) : end_index + 1]
            start = session_open + timedelta(minutes=end_index - horizon + 1)
            end = session_open + timedelta(minutes=end_index + 1)
            complete = start_index >= 1 and len(window_values) == horizon
            eligible = complete and all(item is not None for item in window_values)
            membership_set = (
                {item for item in memberships[start_index - 1 : end_index + 1] if item is not None}
                if complete
                else set()
            )
            if complete and len(membership_set) != 1:
                eligible = False
                reason = "membership_boundary"
            elif not complete:
                reason = "insufficient_history"
            elif not eligible:
                reason = "incomplete_return_window"
            else:
                reason = None
            volatility = None
            if eligible:
                numeric = [item for item in window_values if item is not None]
                mean = sum(numeric) / horizon
                volatility = sqrt(sum((item - mean) ** 2 for item in numeric) / (horizon - 1))
            rolling.append(
                ReferenceWindow(
                    horizon,
                    start,
                    end,
                    horizon,
                    sum(v is not None for v in window_values),
                    volatility,
                    None,
                    eligible,
                    reason,
                )
            )
        complete_windows = (minutes - 1) // horizon
        for index in range(complete_windows):
            first_return = 1 + index * horizon
            window_values = values[first_return : first_return + horizon]
            start = session_open + timedelta(minutes=1 + index * horizon)
            end = start + timedelta(minutes=horizon)
            eligible = len(window_values) == horizon and all(v is not None for v in window_values)
            member_set = {
                item
                for item in memberships[first_return - 1 : first_return + horizon]
                if item is not None
            }
            if len(member_set) != 1:
                eligible = False
                reason = "membership_boundary"
            elif not eligible:
                reason = "incomplete_fixed_window"
            else:
                reason = None
            variance = sum(v * v for v in window_values if v is not None) if eligible else None
            fixed.append(
                ReferenceWindow(
                    horizon,
                    start,
                    end,
                    horizon,
                    sum(v is not None for v in window_values),
                    sqrt(variance) if variance is not None else None,
                    variance,
                    eligible,
                    reason,
                )
            )
    return ReferenceMeasurements(tuple(returns), tuple(rolling), tuple(fixed))
