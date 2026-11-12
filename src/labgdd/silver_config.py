"""Strict configuration for one immutable Bronze-to-Silver batch run."""

import re
import tomllib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PureWindowsPath

from labgdd.config import ConfigurationError, Settings

_IDENTIFIER = re.compile(r"[A-Za-z0-9_.-]+\Z")


@dataclass(frozen=True, slots=True)
class SilverRequest:
    dataset_version: str
    bronze_manifests: tuple[Path, ...]
    membership_valid_from: datetime
    membership_valid_to: datetime
    membership_basis: str
    resolution_policy_version: str = "silver-resolution-v1"
    silver_schema_version: str = "1.0.0"


def _utc(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ConfigurationError(f"{field}: UTC timestamp required")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise ConfigurationError(f"{field}: UTC timestamp required") from None
    if parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ConfigurationError(f"{field}: UTC timestamp required")
    return parsed


def load_silver_request(path: Path, settings: Settings) -> SilverRequest:
    try:
        with path.open("rb") as stream:
            raw = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError):
        raise ConfigurationError("silver configuration: unreadable or invalid TOML") from None
    expected = {
        "dataset_version",
        "bronze_manifests",
        "membership_valid_from",
        "membership_valid_to",
        "membership_basis",
        "resolution_policy_version",
        "silver_schema_version",
    }
    if set(raw) != expected:
        raise ConfigurationError("silver configuration: missing or unknown keys")
    for field in (
        "dataset_version",
        "membership_basis",
        "resolution_policy_version",
        "silver_schema_version",
    ):
        if not isinstance(raw[field], str) or not raw[field].strip():
            raise ConfigurationError(f"{field}: nonblank string required")
    if not _IDENTIFIER.fullmatch(raw["dataset_version"]):
        raise ConfigurationError("dataset_version: invalid identifier")
    if raw["silver_schema_version"] != "1.0.0":
        raise ConfigurationError("silver_schema_version: unsupported version")
    if raw["resolution_policy_version"] != "silver-resolution-v1":
        raise ConfigurationError("resolution_policy_version: unsupported version")
    start = _utc(raw["membership_valid_from"], "membership_valid_from")
    end = _utc(raw["membership_valid_to"], "membership_valid_to")
    if end <= start:
        raise ConfigurationError("membership applicability: reversed or empty interval")
    values = raw["bronze_manifests"]
    if not isinstance(values, list) or not values or not all(isinstance(v, str) for v in values):
        raise ConfigurationError("bronze_manifests: nonempty path array required")
    manifests: list[Path] = []
    for value in values:
        relative = Path(value)
        if relative.is_absolute() or PureWindowsPath(value).drive or "\\" in value:
            raise ConfigurationError("bronze_manifests: portable relative paths required")
        resolved = (path.resolve().parent / relative).resolve()
        bronze_root = (settings.data_root / "bronze").resolve()
        if resolved.name != "manifest.json" or not resolved.is_relative_to(bronze_root):
            raise ConfigurationError("bronze_manifests: paths must be owned Bronze manifests")
        if resolved in manifests:
            raise ConfigurationError("bronze_manifests: duplicate path")
        manifests.append(resolved)
    return SilverRequest(
        raw["dataset_version"],
        tuple(manifests),
        start,
        end,
        raw["membership_basis"],
        raw["resolution_policy_version"],
        raw["silver_schema_version"],
    )
