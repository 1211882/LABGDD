"""Canonical normalized one-minute observation, contract v1.0.0."""

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

_SYMBOL = re.compile(r"[A-Z][A-Z0-9.-]{0,15}\Z")
_HASH = re.compile(r"[0-9a-f]{64}\Z")


class ContractError(ValueError):
    """Invalid domain data; messages contain field names, never payloads."""


def valid_symbol(value: object) -> bool:
    return isinstance(value, str) and _SYMBOL.fullmatch(value) is not None


def _utc(value: datetime, field: str) -> None:
    if not isinstance(value, datetime) or value.utcoffset() != timedelta(0):
        raise ContractError(f"{field}: timezone-aware UTC required")


@dataclass(frozen=True, slots=True)
class MarketBar:
    """Raw-price, IEX one-minute observation; event_time labels interval start."""

    event_time: datetime
    symbol: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    source: str
    feed: str
    ingested_at: datetime
    observation_id: str
    schema_version: str = "1.0.0"
    source_event_id: str | None = None
    source_revision: int | None = None
    supersedes_observation_id: str | None = None

    @property
    def bar_end(self) -> datetime:
        """Close/return-end label; availability still depends on ingestion."""
        return self.event_time + timedelta(minutes=1)

    @property
    def logical_key(self) -> tuple[str, str, str, str, datetime, str]:
        """Series identity, distinct from an acquisition's observation identity."""
        return self.source, self.feed, self.symbol, "1Min", self.event_time, "raw"

    @property
    def ohlcv(self) -> tuple[Decimal, Decimal, Decimal, Decimal, int]:
        """Market values only; equality does not establish revision ordering."""
        return self.open, self.high, self.low, self.close, self.volume

    def __post_init__(self) -> None:
        _utc(self.event_time, "event_time")
        _utc(self.ingested_at, "ingested_at")
        if self.event_time.second or self.event_time.microsecond:
            raise ContractError("event_time: minute alignment required")
        if self.ingested_at < self.event_time:
            raise ContractError("ingested_at: precedes event_time")
        if not valid_symbol(self.symbol):
            raise ContractError("symbol: invalid canonical symbol")
        for name in ("open", "high", "low", "close"):
            price = getattr(self, name)
            if not isinstance(price, Decimal) or not price.is_finite() or price <= 0:
                raise ContractError(f"{name}: finite positive Decimal required")
            # Fixed transport policy, independent of the caller's Decimal context.
            exponent = price.as_tuple().exponent
            if not isinstance(exponent, int) or exponent < -8 or len(price.as_tuple().digits) > 18:
                raise ContractError(f"{name}: decimal precision exceeds contract")
            if price >= Decimal("1e18"):
                raise ContractError(f"{name}: decimal magnitude exceeds contract")
        if not self.low <= min(self.open, self.close) <= max(self.open, self.close) <= self.high:
            raise ContractError("ohlc: inconsistent price bounds")
        if type(self.volume) is not int or not 0 <= self.volume <= 2**63 - 1:
            raise ContractError("volume: nonnegative int64 required")
        if self.source != "alpaca" or self.feed != "iex":
            raise ContractError("source/feed: unsupported contract")
        if self.schema_version != "1.0.0":
            raise ContractError("schema_version: unsupported version")
        self._validate_id(self.observation_id, "observation_id")
        if self.source_event_id is not None and (
            not isinstance(self.source_event_id, str) or not self.source_event_id.strip()
        ):
            raise ContractError("source_event_id: nonblank string required")
        if self.source_revision is not None and (
            type(self.source_revision) is not int or self.source_revision < 0
        ):
            raise ContractError("source_revision: nonnegative integer required")
        if self.supersedes_observation_id is not None:
            self._validate_id(self.supersedes_observation_id, "supersedes_observation_id")
            if self.supersedes_observation_id == self.observation_id:
                raise ContractError("supersedes_observation_id: self-reference")

    @staticmethod
    def _validate_id(value: str, field: str) -> None:
        if not isinstance(value, str) or not _HASH.fullmatch(value):
            raise ContractError(f"{field}: lowercase SHA-256 required")
