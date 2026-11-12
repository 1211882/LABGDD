from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from labgdd.domain.market_bar import ContractError, MarketBar


@pytest.fixture
def bar() -> MarketBar:
    return MarketBar(
        event_time=datetime(2026, 10, 30, 14, 30, tzinfo=UTC),
        symbol="AAPL",
        open=Decimal("10"),
        high=Decimal("12"),
        low=Decimal("9"),
        close=Decimal("11"),
        volume=10,
        source="alpaca",
        feed="iex",
        ingested_at=datetime(2026, 10, 30, 14, 31, 1, tzinfo=UTC),
        observation_id="a" * 64,
    )


def test_valid_and_immutable(bar: MarketBar) -> None:
    assert bar.close == Decimal("11")
    with pytest.raises(FrozenInstanceError):
        bar.volume = 20  # type: ignore[misc]  # Exercise runtime immutability.


@pytest.mark.parametrize("field", ["open", "high", "low", "close"])
@pytest.mark.parametrize("price", ["0", "-1", "NaN", "sNaN", "Infinity", "-Infinity"])
def test_nonpositive_and_nonfinite_prices(bar: MarketBar, field: str, price: str) -> None:
    with pytest.raises(ContractError):
        replace(bar, **{field: Decimal(price)})  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("volume", -1),
        ("volume", True),
        ("volume", 1.5),
        ("volume", 2**63),
        ("symbol", "aapl"),
        ("symbol", ""),
        ("symbol", " AAPL"),
        ("source", "other"),
        ("feed", "sip"),
        ("schema_version", "2.0.0"),
        ("observation_id", "x" * 64),
        ("source_event_id", " "),
        ("source_revision", -1),
        ("source_revision", True),
        ("supersedes_observation_id", "a" * 64),
        ("supersedes_observation_id", "invalid"),
        ("open", 10.0),
        ("high", Decimal("10")),
        ("low", Decimal("11")),
        ("open", Decimal("1e18")),
        ("open", Decimal("0.000000001")),
    ],
)
def test_invalid_contract(bar: MarketBar, field: str, value: object) -> None:
    with pytest.raises(ContractError):
        replace(bar, **{field: value})  # type: ignore[arg-type]


@pytest.mark.parametrize("field", ["event_time", "ingested_at"])
def test_reject_naive_and_non_utc(bar: MarketBar, field: str) -> None:
    for zone in (None, timezone(timedelta(hours=1))):
        value = datetime(2026, 10, 30, 14, 30, tzinfo=zone)
        with pytest.raises(ContractError):
            replace(bar, **{field: value})  # type: ignore[arg-type]


def test_time_alignment_and_receive_order(bar: MarketBar) -> None:
    with pytest.raises(ContractError):
        replace(bar, event_time=bar.event_time + timedelta(seconds=1))
    with pytest.raises(ContractError):
        replace(bar, ingested_at=bar.event_time - timedelta(seconds=1))


def test_valid_zero_volume_revision_and_class_symbol(bar: MarketBar) -> None:
    corrected = replace(
        bar,
        symbol="BRK.B",
        volume=0,
        source_revision=0,
        source_event_id="provider-id",
        supersedes_observation_id="b" * 64,
    )
    assert corrected.source_revision == 0
    assert corrected.event_time == bar.event_time
    # Same event time deliberately does not imply a revision ordering.


def test_observation_identity_is_not_logical_or_payload_identity(bar: MarketBar) -> None:
    acquisition = replace(
        bar, observation_id="b" * 64, ingested_at=bar.ingested_at + timedelta(minutes=1)
    )
    assert acquisition != bar
    assert acquisition.logical_key == bar.logical_key
    assert acquisition.ohlcv == bar.ohlcv
    assert replace(bar) == bar  # Replay preserves the entire observation envelope.
    assert bar.logical_key == ("alpaca", "iex", "AAPL", "1Min", bar.event_time, "raw")


def test_conflicting_payload_and_revision_metadata_do_not_change_logical_key(
    bar: MarketBar,
) -> None:
    conflict = replace(bar, close=Decimal("10.5"), observation_id="b" * 64)
    assert conflict.logical_key == bar.logical_key
    assert conflict.ohlcv != bar.ohlcv
    revision = replace(conflict, source_revision=2, supersedes_observation_id=bar.observation_id)
    assert revision.logical_key == conflict.logical_key
    assert revision.ohlcv == conflict.ohlcv
    # The model validates metadata, not whether provider evidence authorizes it.


def test_same_observation_id_different_payload_is_detectable(bar: MarketBar) -> None:
    conflict = replace(bar, volume=20)
    assert conflict.observation_id == bar.observation_id
    assert conflict != bar
    # Cross-observation integrity enforcement belongs to the future resolver.
