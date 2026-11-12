"""Pure eligibility checks over explicit schedule bounds, not a calendar engine."""

from collections.abc import Sequence
from datetime import datetime, timedelta

from labgdd.domain.market_bar import ContractError, MarketBar, _utc

_MINUTE = timedelta(minutes=1)


def validate_return_window(
    bars: Sequence[MarketBar],
    *,
    session_open: datetime,
    session_close: datetime,
    window_start: datetime,
    return_count: int,
) -> None:
    """Require H+1 ordered consecutive closes for (start, start+H minutes].

    Bounds come from a caller's versioned session schedule. No schedule lookup,
    sorting, deduplication, correction resolution, gap fill or financial formula
    occurs here. Both trailing and session-anchored windows use this eligibility
    check; callers choose their evaluation cadence.
    """
    for name, value in (
        ("session_open", session_open),
        ("session_close", session_close),
        ("window_start", window_start),
    ):
        _utc(value, name)
        if value.second or value.microsecond:
            raise ContractError(f"{name}: minute alignment required")
    if session_close <= session_open:
        raise ContractError("session: close must follow open")
    if type(return_count) is not int or return_count < 1:
        raise ContractError("return_count: positive integer required")
    # Check available scheduled minutes before constructing a duration from H.
    available = (session_close - window_start) // _MINUTE
    if window_start < session_open + _MINUTE or return_count > available:
        raise ContractError("window: crosses session boundary")
    if len(bars) != return_count + 1:
        raise ContractError("window: H returns require H+1 closes")
    first = bars[0]
    for index, bar in enumerate(bars):
        if bar.event_time != window_start - _MINUTE + index * _MINUTE:
            raise ContractError("window: consecutive ordered minute bars required")
        if (bar.symbol, bar.source, bar.feed) != (first.symbol, first.source, first.feed):
            raise ContractError("window: mixed market series")
