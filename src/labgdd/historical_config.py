"""Validated, bounded acquisition requests; independent of provider libraries."""

import re
import tomllib
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from labgdd.config import ConfigurationError, Settings
from labgdd.domain.market_bar import ContractError, _utc


@dataclass(frozen=True, slots=True)
class HistoricalRequest:
    dataset_version: str
    symbols: tuple[str, ...]
    start: datetime
    end: datetime
    symbol_asof: date
    feed: str = "iex"
    timeframe: str = "1Min"
    adjustment: str = "raw"
    timeout_seconds: float = 15.0
    max_attempts: int = 3
    max_pages: int = 1000

    def __post_init__(self) -> None:
        try:
            _utc(self.start, "start")
            _utc(self.end, "end")
        except ContractError:
            raise ConfigurationError("acquisition: UTC bounds required") from None
        if any(t.second or t.microsecond for t in (self.start, self.end)):
            raise ConfigurationError("acquisition: minute-aligned bounds required")
        if not timedelta(0) < self.end - self.start <= timedelta(days=31):
            raise ConfigurationError("acquisition: interval must be positive and at most 31 days")
        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}", self.dataset_version):
            raise ConfigurationError("dataset_version: portable identifier required")
        if not self.symbols or len(set(self.symbols)) != len(self.symbols):
            raise ConfigurationError("symbols: nonempty unique symbols required")
        if (self.feed, self.timeframe, self.adjustment) != ("iex", "1Min", "raw"):
            raise ConfigurationError("acquisition: only raw one-minute IEX supported")
        if type(self.timeout_seconds) not in {float, int} or not 0 < self.timeout_seconds <= 120:
            raise ConfigurationError("timeout_seconds: must be within (0,120]")
        for field, value, maximum in (
            ("max_attempts", self.max_attempts, 10),
            ("max_pages", self.max_pages, 10000),
        ):
            if type(value) is not int or not 1 <= value <= maximum:
                raise ConfigurationError(f"{field}: outside bounded policy")

    def validate_universe(self, settings: Settings) -> None:
        allowed = {symbol for symbols in settings.universe.values() for symbol in symbols}
        if not set(self.symbols) <= allowed:
            raise ConfigurationError("symbols: outside configured study cohort")

    def parameters(self) -> dict[str, str]:
        return {
            "symbols": ",".join(self.symbols),
            "start": self.start.isoformat(),
            # Provider end is inclusive; project requests use [start,end).
            "end": (self.end - timedelta(microseconds=1)).isoformat(),
            "asof": self.symbol_asof.isoformat(),
            "feed": self.feed,
            "timeframe": self.timeframe,
            "adjustment": self.adjustment,
            "limit": "10000",
            "sort": "asc",
        }


def load_historical_request(path: Path, settings: Settings) -> HistoricalRequest:
    try:
        with path.open("rb") as stream:
            values = tomllib.load(stream)
        required = {
            "dataset_version",
            "symbols",
            "start",
            "end",
            "symbol_asof",
            "feed",
            "timeframe",
            "adjustment",
        }
        optional = {"timeout_seconds", "max_attempts", "max_pages"}
        if not required <= set(values) or set(values) - required - optional:
            raise ValueError
        if not isinstance(values["symbols"], list) or not all(
            isinstance(s, str) for s in values["symbols"]
        ):
            raise ValueError
        request = HistoricalRequest(
            dataset_version=values["dataset_version"],
            symbols=tuple(values["symbols"]),
            start=datetime.fromisoformat(values["start"]),
            end=datetime.fromisoformat(values["end"]),
            symbol_asof=date.fromisoformat(values["symbol_asof"]),
            feed=values["feed"],
            timeframe=values["timeframe"],
            adjustment=values["adjustment"],
            timeout_seconds=values.get("timeout_seconds", 15.0),
            max_attempts=values.get("max_attempts", 3),
            max_pages=values.get("max_pages", 1000),
        )
        request.validate_universe(settings)
        return request
    except (OSError, ValueError, TypeError, KeyError):
        raise ConfigurationError("historical configuration: invalid request") from None
