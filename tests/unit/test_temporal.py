"""Hand-enumerated schedule fixtures; these do not test a calendar provider."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from labgdd.domain.market_bar import ContractError, MarketBar
from labgdd.domain.temporal import validate_return_window

MINUTE = timedelta(minutes=1)
OPEN = datetime(2026, 11, 2, 14, 30, tzinfo=UTC)  # 09:30 New York, EST.
CLOSE = datetime(2026, 11, 2, 21, tzinfo=UTC)  # 16:00 New York, EST.


def bars_at(start: datetime, count: int = 6) -> list[MarketBar]:
    return [
        MarketBar(
            event_time=start + index * MINUTE,
            symbol="AAPL",
            open=Decimal("10"),
            high=Decimal("10"),
            low=Decimal("10"),
            close=Decimal("10"),
            volume=0,
            source="alpaca",
            feed="iex",
            ingested_at=start + (index + 1) * MINUTE + timedelta(seconds=1),
            observation_id=f"{index:064x}",
        )
        for index in range(count)
    ]


def check(bars: list[MarketBar], start: datetime = OPEN + MINUTE, count: int = 5) -> None:
    validate_return_window(
        bars,
        session_open=OPEN,
        session_close=CLOSE,
        window_start=start,
        return_count=count,
    )


def test_first_adjacent_return_ends_at_0932() -> None:
    bars = bars_at(OPEN, 2)
    assert bars[0].bar_end == OPEN + MINUTE  # First close, 09:31.
    assert bars[1].bar_end == OPEN + 2 * MINUTE  # First return end, 09:32.
    check(bars, count=1)
    with pytest.raises(ContractError, match="session boundary"):
        check(bars_at(OPEN - MINUTE, 2), start=OPEN, count=1)


def test_five_returns_require_six_closes_0931_through_0936() -> None:
    bars = bars_at(OPEN)
    check(bars)
    assert [bar.bar_end for bar in bars] == [OPEN + i * MINUTE for i in range(1, 7)]
    with pytest.raises(ContractError, match="H\\+1"):
        check(bars[:5])
    with pytest.raises(ContractError, match="H\\+1"):
        check(bars_at(OPEN, 7))


def test_missing_opening_bar_does_not_reanchor_first_window() -> None:
    with pytest.raises(ContractError, match="consecutive"):
        check(bars_at(OPEN + MINUTE))


def test_missing_intermediate_minute_is_not_filled_or_compressed() -> None:
    bars = bars_at(OPEN, 7)
    del bars[2]
    with pytest.raises(ContractError, match="consecutive"):
        check(bars)


def test_unsorted_duplicate_and_mixed_symbol_bars_fail() -> None:
    bars = bars_at(OPEN)
    for invalid in (list(reversed(bars)), [bars[0], bars[0], *bars[2:]]):
        with pytest.raises(ContractError, match="consecutive"):
            check(invalid)
    bars[2] = replace(bars[2], symbol="MSFT")
    with pytest.raises(ContractError, match="mixed"):
        check(bars)


@pytest.mark.parametrize(
    ("day", "offset", "utc_hour"),
    [
        ("2026-03-06", "-05:00", 14),
        ("2026-03-09", "-04:00", 13),
        ("2026-10-30", "-04:00", 13),
        ("2026-11-02", "-05:00", 14),
    ],
)
def test_dst_adjacent_sessions_use_explicit_utc_schedule(
    day: str,
    offset: str,
    utc_hour: int,
) -> None:
    # Independent fixed-offset examples, not an IANA timezone/calendar lookup.
    opening = datetime.fromisoformat(f"{day}T09:30:00{offset}").astimezone(UTC)
    closing = datetime.fromisoformat(f"{day}T16:00:00{offset}").astimezone(UTC)
    assert (opening.hour, opening.minute) == (utc_hour, 30)
    assert closing - opening == 390 * MINUTE
    validate_return_window(
        bars_at(opening),
        session_open=opening,
        session_close=closing,
        window_start=opening + MINUTE,
        return_count=5,
    )


def test_early_close_accepts_last_close_but_rejects_later_bar() -> None:
    opening = datetime(2026, 11, 27, 14, 30, tzinfo=UTC)
    closing = datetime(2026, 11, 27, 18, tzinfo=UTC)  # 13:00 EST; 210 bars.
    assert closing - opening == 210 * MINUTE
    # Trailing five-return window (12:55,13:00] needs closes 12:55..13:00.
    validate_return_window(
        bars_at(closing - 6 * MINUTE),
        session_open=opening,
        session_close=closing,
        window_start=closing - 5 * MINUTE,
        return_count=5,
    )
    with pytest.raises(ContractError, match="session boundary"):
        validate_return_window(
            bars_at(closing - 5 * MINUTE),
            session_open=opening,
            session_close=closing,
            window_start=closing - 4 * MINUTE,
            return_count=5,
        )


def test_overnight_close_cannot_seed_next_session_return() -> None:
    bars = bars_at(OPEN, 5)
    previous_close_bar = bars_at(datetime(2026, 10, 30, 19, 59, tzinfo=UTC), 1)[0]
    with pytest.raises(ContractError):
        check([previous_close_bar, *bars], start=OPEN)
    with pytest.raises(ContractError):
        check([previous_close_bar, *bars])


@pytest.mark.parametrize("count", [0, -1, 10**100])
def test_invalid_or_out_of_session_return_counts(count: int) -> None:
    with pytest.raises(ContractError):
        check(bars_at(OPEN), count=count)


def test_boolean_return_count_is_rejected() -> None:
    with pytest.raises(ContractError):
        check(bars_at(OPEN, 2), count=True)


def test_naive_non_utc_unaligned_and_reversed_schedule_bounds_fail() -> None:
    for opening, closing, start in (
        (OPEN.replace(tzinfo=None), CLOSE, OPEN + MINUTE),
        (OPEN, CLOSE, OPEN.replace(tzinfo=None)),
        (OPEN, CLOSE, datetime.fromisoformat("2026-11-02T09:31:00-05:00")),
        (OPEN, CLOSE + timedelta(seconds=1), OPEN + MINUTE),
        (OPEN, OPEN, OPEN + MINUTE),
        (CLOSE, OPEN, OPEN + MINUTE),
    ):
        with pytest.raises(ContractError):
            validate_return_window(
                bars_at(OPEN),
                session_open=opening,
                session_close=closing,
                window_start=start,
                return_count=5,
            )
