from datetime import UTC, datetime, timedelta
from decimal import Decimal
from math import isclose, sqrt

import pytest

from labgdd.analytics.individual import ReferenceBar, calculate_individual_measurements

OPEN = datetime(2026, 9, 14, 13, 30, tzinfo=UTC)


def bars_from_returns(values: list[Decimal]) -> tuple[ReferenceBar, ...]:
    closes = [Decimal("100")]
    for value in values:
        closes.append(closes[-1] * value.exp())
    return tuple(
        ReferenceBar(
            OPEN + timedelta(minutes=index),
            close,
            close,
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2027, 1, 1, tzinfo=UTC),
        )
        for index, close in enumerate(closes)
    )


def test_alternating_fixture_sample_sd_and_realized_variance() -> None:
    amount = Decimal("0.01")
    result = calculate_individual_measurements(
        bars_from_returns([amount, -amount, amount, -amount, amount]),
        OPEN,
        OPEN + timedelta(minutes=6),
        horizons=(5,),
    )
    assert sum(row.eligible for row in result.returns) == 5
    rolling = result.rolling[-1]
    fixed = result.fixed[0]
    assert rolling.window_start == OPEN + timedelta(minutes=1)
    assert rolling.window_end == OPEN + timedelta(minutes=6)
    assert isclose(rolling.value or -1, sqrt(1.2) * 0.01, rel_tol=1e-10, abs_tol=1e-12)
    assert isclose(fixed.realized_variance or -1, 5 * 0.01**2, rel_tol=1e-10, abs_tol=1e-12)
    assert isclose(fixed.value or -1, sqrt(5) * 0.01, rel_tol=1e-10, abs_tol=1e-12)


def test_gap_is_not_filled_or_reanchored() -> None:
    source = list(bars_from_returns([Decimal("0.01")] * 10))
    del source[2]
    result = calculate_individual_measurements(
        tuple(source),
        OPEN,
        OPEN + timedelta(minutes=11),
        horizons=(5,),
    )
    assert result.returns[2].reason == "missing_close"
    assert result.returns[3].reason == "nonconsecutive_bars"
    assert len(result.fixed) == 2
    assert result.fixed[0].reason == "incomplete_fixed_window"
    assert result.fixed[1].eligible


def test_fixed_counts_regular_and_early_close() -> None:
    fixtures = ((390, {5: 77, 15: 25, 30: 12, 60: 6}), (210, {5: 41, 15: 13, 30: 6, 60: 3}))
    for minutes, expected in fixtures:
        bars = tuple(
            ReferenceBar(
                OPEN + timedelta(minutes=index),
                Decimal("100"),
                Decimal("100"),
                OPEN - timedelta(days=1),
                OPEN + timedelta(days=2),
            )
            for index in range(minutes)
        )
        result = calculate_individual_measurements(bars, OPEN, OPEN + timedelta(minutes=minutes))
        assert sum(row.eligible for row in result.returns) == minutes - 1
        assert {h: sum(row.horizon == h for row in result.fixed) for h in expected} == expected
        assert all(row.value == 0.0 for row in result.fixed)


def test_membership_boundary_invalidates_crossing_windows() -> None:
    source = list(bars_from_returns([Decimal("0.01")] * 5))
    source[3] = ReferenceBar(
        source[3].event_time,
        source[3].open,
        source[3].close,
        OPEN + timedelta(minutes=3),
        source[3].membership_valid_to,
    )
    result = calculate_individual_measurements(
        tuple(source),
        OPEN,
        OPEN + timedelta(minutes=6),
        horizons=(5,),
    )
    assert result.returns[3].reason == "membership_boundary"
    assert result.fixed[0].reason == "membership_boundary"


def test_duplicate_observation_time_is_rejected() -> None:
    source = bars_from_returns([Decimal("0.01")])
    with pytest.raises(ValueError, match="duplicate event time"):
        calculate_individual_measurements(source + (source[0],), OPEN, OPEN + timedelta(minutes=2))
