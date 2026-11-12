"""Strict, credential-free TOML configuration; no side effects at import."""

import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PureWindowsPath
from types import MappingProxyType

from labgdd.domain.market_bar import valid_symbol


class ConfigurationError(ValueError):
    """Invalid configuration, with no configuration values in messages."""


@dataclass(frozen=True, slots=True)
class Settings:
    schema_version: str
    dataset_version: str
    membership_version: str
    data_root: Path
    log_level: str
    universe: Mapping[str, tuple[str, ...]]
    allowed_lateness_minutes: int = 10


def load_settings(path: Path) -> Settings:
    """Resolve paths relative to the file, rejecting external storage targets."""
    try:
        with path.open("rb") as stream:
            raw = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError):
        raise ConfigurationError("configuration: unreadable or invalid TOML") from None
    expected = {
        "schema_version",
        "dataset_version",
        "membership_version",
        "data_root",
        "log_level",
        "universe",
    }
    if not expected <= set(raw) or set(raw) - expected - {"allowed_lateness_minutes"}:
        raise ConfigurationError("configuration: missing or unknown keys")
    lateness = raw.get("allowed_lateness_minutes", 10)
    if type(lateness) is not int or lateness < 0:
        raise ConfigurationError("allowed_lateness_minutes: nonnegative integer required")
    for name in expected - {"universe"}:
        if not isinstance(raw[name], str) or not raw[name].strip():
            raise ConfigurationError(f"{name}: nonblank string required")
    if raw["schema_version"] != "1.0.0":
        raise ConfigurationError("schema_version: unsupported version")
    if raw["log_level"] not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ConfigurationError("log_level: unsupported level")
    relative_root = Path(raw["data_root"])
    if (
        relative_root.is_absolute()
        or PureWindowsPath(raw["data_root"]).drive
        or "\\" in raw["data_root"]
    ):
        raise ConfigurationError("data_root: portable relative path required")
    config_dir = path.resolve().parent
    project_root = config_dir.parent
    data_root = (config_dir / relative_root).resolve()
    if not data_root.is_relative_to(project_root) or data_root in {project_root, config_dir}:
        raise ConfigurationError("data_root: dedicated path inside project required")
    if data_root.is_relative_to(config_dir):
        raise ConfigurationError("data_root: must be outside configuration directory")
    owned_relative = data_root.relative_to(project_root)
    if owned_relative.parts[0] != "data":
        raise ConfigurationError("data_root: must be within project data directory")
    universe = raw["universe"]
    if not isinstance(universe, dict) or not universe:
        raise ConfigurationError("universe: nonempty sector table required")
    seen: set[str] = set()
    groups: dict[str, tuple[str, ...]] = {}
    for sector, symbols in universe.items():
        if not sector.strip() or not isinstance(symbols, list) or not symbols:
            raise ConfigurationError("universe: named sectors with symbol arrays required")
        for symbol in symbols:
            if not valid_symbol(symbol) or symbol in seen:
                raise ConfigurationError("universe: invalid or duplicate symbol")
            seen.add(symbol)
        groups[sector] = tuple(symbols)
    return Settings(
        schema_version=raw["schema_version"],
        dataset_version=raw["dataset_version"],
        membership_version=raw["membership_version"],
        data_root=data_root,
        log_level=raw["log_level"],
        universe=MappingProxyType(groups),
        allowed_lateness_minutes=lateness,
    )
