"""Strict configuration for one approved-Silver to immutable-Gold run."""

import re
import tomllib
from dataclasses import dataclass
from pathlib import Path, PureWindowsPath
from uuid import UUID

from labgdd.config import ConfigurationError, Settings

_IDENTIFIER = re.compile(r"[A-Za-z0-9_.-]+\Z")


@dataclass(frozen=True, slots=True)
class GoldRequest:
    dataset_version: str
    silver_registry: Path
    silver_dataset_version: str
    silver_run_id: UUID
    measurement_version: str
    horizons: tuple[int, ...]
    gold_schema_version: str
    silver_input_authority: str = "approved"


def load_gold_request(path: Path, settings: Settings) -> GoldRequest:
    try:
        with path.open("rb") as stream:
            raw = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError):
        raise ConfigurationError("gold configuration: unreadable or invalid TOML") from None
    required = {
        "dataset_version",
        "silver_registry",
        "silver_dataset_version",
        "silver_run_id",
        "measurement_version",
        "horizons",
        "gold_schema_version",
    }
    optional = {"silver_input_authority"}
    if not required <= set(raw) or set(raw) - required - optional:
        raise ConfigurationError("gold configuration: missing or unknown keys")
    for field in required - {"horizons"}:
        if not isinstance(raw[field], str) or not raw[field].strip():
            raise ConfigurationError(f"{field}: nonblank string required")
    if not _IDENTIFIER.fullmatch(raw["dataset_version"]):
        raise ConfigurationError("dataset_version: invalid identifier")
    if raw["measurement_version"] != "2.0.0" or raw["gold_schema_version"] != "1.0.0":
        raise ConfigurationError("gold configuration: unsupported contract version")
    authority = raw.get("silver_input_authority", "approved")
    if authority not in {"approved", "authorized_bounded_validation"}:
        raise ConfigurationError("silver_input_authority: unsupported authority")
    values = raw["horizons"]
    if (
        not isinstance(values, list)
        or any(type(item) is not int for item in values)
        or tuple(values) != (5, 15, 30, 60)
    ):
        raise ConfigurationError("horizons: exact supported horizon set required")
    registry_value = raw["silver_registry"]
    relative = Path(registry_value)
    if relative.is_absolute() or PureWindowsPath(registry_value).drive or "\\" in registry_value:
        raise ConfigurationError("silver_registry: portable relative path required")
    registry = (path.resolve().parent / relative).resolve()
    project_root = path.resolve().parent.parent
    if not registry.is_relative_to(project_root) or registry.name != "silver_publications.toml":
        raise ConfigurationError("silver_registry: approved project registry required")
    try:
        run_id = UUID(raw["silver_run_id"])
    except ValueError:
        raise ConfigurationError("silver_run_id: UUID required") from None
    return GoldRequest(
        raw["dataset_version"],
        registry,
        raw["silver_dataset_version"],
        run_id,
        raw["measurement_version"],
        tuple(values),
        raw["gold_schema_version"],
        authority,
    )
