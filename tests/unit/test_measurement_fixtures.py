"""Independent scalar fixture arithmetic, not production analytics execution."""

import math
import statistics

import pytest


def test_alternating_return_fixture() -> None:
    a = 0.01
    returns = [a, -a, a, -a, a]
    assert math.fsum(r * r for r in returns) == pytest.approx(5 * a * a)
    assert math.sqrt(math.fsum(r * r for r in returns)) == pytest.approx(math.sqrt(5) * a)
    assert statistics.stdev(returns) == pytest.approx(math.sqrt(1.2) * a)


def test_two_member_risk_fixture() -> None:
    risk = [3.0, 4.0]
    total = math.fsum(v * v for v in risk)
    hhi = math.fsum((v * v / total) ** 2 for v in risk)
    assert math.sqrt(total / len(risk)) == pytest.approx(math.sqrt(12.5))
    assert statistics.pstdev(risk) == 0.5
    assert hhi == pytest.approx(337 / 625)
    assert (1 - hhi) / (1 - 1 / len(risk)) == pytest.approx(576 / 625)


def test_mean_log_proxy_differs_from_equal_weight_rebalanced_portfolio() -> None:
    # Gross member returns 2 and 1/2: equal-weight portfolio gross return is 5/4.
    returns = [math.log(2), -math.log(2)]
    proxy = statistics.mean(returns)
    portfolio = math.log1p(statistics.mean([math.expm1(r) for r in returns]))
    assert proxy == 0
    assert portfolio == pytest.approx(math.log(1.25))


def test_constant_and_pair_correlation_fixtures() -> None:
    assert statistics.stdev([0.0] * 5) == 0
    x = [1.0, 2.0, 4.0, 3.0, 5.0]
    assert statistics.correlation(x, x) == pytest.approx(1)
    assert statistics.correlation(x, [-v for v in x]) == pytest.approx(-1)
    with pytest.raises(statistics.StatisticsError):
        statistics.correlation([0.0] * 5, x)
    # Production must map undefined correlation to a reasoned null in Phase 2.
