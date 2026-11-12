"""Strict loader for the project-declared research universe reference."""

import tomllib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from labgdd.config import ConfigurationError, Settings
from labgdd.research.universe import ResearchMembership, ResearchUniverse, validate_memberships


@dataclass(frozen=True, slots=True)
class ResearchUniverseConfig:
    universe: ResearchUniverse
    path: Path


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


def load_research_universe(path: Path, settings: Settings) -> ResearchUniverseConfig:
    try:
        with path.open("rb") as stream:
            raw = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError):
        raise ConfigurationError("research universe: unreadable or invalid TOML") from None
    expected = {
        "contract_version",
        "universe_version",
        "membership_version",
        "declared_valid_from",
        "declared_valid_to",
        "applicability_basis",
        "evidence_source",
        "evidence_status",
        "historical_listing_status",
        "entity_identity_status",
        "sector_classification_status",
        "exclusions",
        "limitations",
    }
    if set(raw) != expected:
        raise ConfigurationError("research universe: missing or unknown keys")
    strings = expected - {"declared_valid_from", "declared_valid_to", "exclusions", "limitations"}
    if any(not isinstance(raw[field], str) or not raw[field].strip() for field in strings):
        raise ConfigurationError("research universe: nonblank strings required")
    if raw["contract_version"] != "1.0.0":
        raise ConfigurationError("research universe: unsupported contract version")
    if (
        raw["universe_version"] != settings.dataset_version
        or raw["membership_version"] != settings.membership_version
        or raw["universe_version"] != raw["membership_version"]
    ):
        raise ConfigurationError("research universe: version mismatch")
    if raw["evidence_status"] not in {"declared_research_assumption", "independently_verified"}:
        raise ConfigurationError("research universe: unsupported evidence status")
    if raw["historical_listing_status"] not in {"unverified", "verified"}:
        raise ConfigurationError("research universe: unsupported listing status")
    if raw["entity_identity_status"] not in {"unverified", "verified"}:
        raise ConfigurationError("research universe: unsupported entity status")
    if raw["sector_classification_status"] not in {
        "project_defined_unverified",
        "independently_verified",
    }:
        raise ConfigurationError("research universe: unsupported sector status")
    for field in ("exclusions", "limitations"):
        values = raw[field]
        if (
            not isinstance(values, list)
            or not values
            or not all(isinstance(value, str) and value.strip() for value in values)
            or len(values) != len(set(values))
        ):
            raise ConfigurationError(f"research universe: invalid {field}")
    valid_from = _utc(raw["declared_valid_from"], "declared_valid_from")
    valid_to = _utc(raw["declared_valid_to"], "declared_valid_to")
    if valid_to <= valid_from:
        raise ConfigurationError("research universe: invalid applicability interval")
    memberships = tuple(
        ResearchMembership(
            symbol,
            sector,
            raw["membership_version"],
            valid_from,
            valid_to,
            raw["applicability_basis"],
            raw["evidence_source"],
            raw["evidence_status"],
            raw["historical_listing_status"],
            raw["entity_identity_status"],
            raw["sector_classification_status"],
            tuple(raw["exclusions"]),
            tuple(raw["limitations"]),
        )
        for sector, symbols in settings.universe.items()
        for symbol in symbols
    )
    validate_memberships(memberships)
    return ResearchUniverseConfig(
        ResearchUniverse(
            raw["contract_version"],
            raw["universe_version"],
            raw["membership_version"],
            memberships,
        ),
        path.resolve(),
    )
