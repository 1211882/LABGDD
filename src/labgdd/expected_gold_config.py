"""Strict configuration for expected-series Gold publication."""

import re
import tomllib
from dataclasses import dataclass
from datetime import date
from pathlib import Path, PureWindowsPath
from uuid import UUID

from labgdd.config import ConfigurationError, Settings
from labgdd.processing.expected_series_spark import (
    DECLARED_POLICY,
    POPULATION_POLICY_VERSION,
    STRICT_POLICY,
)

_IDENTIFIER = re.compile(r"[A-Za-z0-9_.-]+\Z")


@dataclass(frozen=True, slots=True)
class ExpectedGoldRequest:
    dataset_version: str
    silver_registry: Path
    silver_dataset_version: str
    silver_run_id: UUID
    research_universe: Path
    research_universe_version: str
    requested_sessions: tuple[str, ...]
    expected_series_contract_version: str
    population_policy_version: str
    eligibility_policy: str
    measurement_version: str
    horizons: tuple[int, ...]
    gold_schema_version: str
    path: Path
    silver_input_authority: str = "approved"
    acquisition_scope_symbols: tuple[str, ...] = ()


def _project_path(path: Path, raw: object, expected_name: str, field: str) -> Path:
    if not isinstance(raw, str) or not raw.strip():
        raise ConfigurationError(f"{field}: nonblank string required")
    relative = Path(raw)
    if relative.is_absolute() or PureWindowsPath(raw).drive or "\\" in raw:
        raise ConfigurationError(f"{field}: portable relative path required")
    resolved = (path.resolve().parent / relative).resolve()
    project_root = path.resolve().parent.parent
    if not resolved.is_relative_to(project_root) or resolved.name != expected_name:
        raise ConfigurationError(f"{field}: approved project path required")
    return resolved


def load_expected_gold_request(path: Path, settings: Settings) -> ExpectedGoldRequest:
    """Load a side-effect-free, exact expected-publication request."""
    try:
        with path.open("rb") as stream:
            raw = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError):
        raise ConfigurationError(
            "expected Gold configuration: unreadable or invalid TOML"
        ) from None
    required = {
        "dataset_version",
        "silver_registry",
        "silver_dataset_version",
        "silver_run_id",
        "research_universe",
        "research_universe_version",
        "requested_sessions",
        "expected_series_contract_version",
        "population_policy_version",
        "eligibility_policy",
        "measurement_version",
        "horizons",
        "gold_schema_version",
    }
    optional = {"silver_input_authority", "acquisition_scope_symbols"}
    if not required <= set(raw) or set(raw) - required - optional:
        raise ConfigurationError("expected Gold configuration: missing or unknown keys")
    scalar_fields = required - {"requested_sessions", "horizons"}
    if any(not isinstance(raw[field], str) or not raw[field].strip() for field in scalar_fields):
        raise ConfigurationError("expected Gold configuration: nonblank strings required")
    if not _IDENTIFIER.fullmatch(raw["dataset_version"]):
        raise ConfigurationError("dataset_version: invalid identifier")
    if (
        raw["measurement_version"] != "2.0.0"
        or raw["gold_schema_version"] != "2.0.0"
        or raw["expected_series_contract_version"] != "1.0.0"
        or raw["population_policy_version"] != POPULATION_POLICY_VERSION
    ):
        raise ConfigurationError("expected Gold configuration: unsupported contract version")
    if raw["eligibility_policy"] not in {DECLARED_POLICY, STRICT_POLICY}:
        raise ConfigurationError("eligibility_policy: unsupported policy")
    authority = raw.get("silver_input_authority", "approved")
    if authority not in {"approved", "authorized_bounded_validation"}:
        raise ConfigurationError("silver_input_authority: unsupported authority")
    scope = raw.get("acquisition_scope_symbols", [])
    allowed_symbols = {symbol for symbols in settings.universe.values() for symbol in symbols}
    if (
        not isinstance(scope, list)
        or not all(isinstance(symbol, str) and symbol.strip() for symbol in scope)
        or len(scope) != len(set(scope))
        or not set(scope) <= allowed_symbols
    ):
        raise ConfigurationError("acquisition_scope_symbols: invalid research subset")
    horizons = raw["horizons"]
    if (
        not isinstance(horizons, list)
        or any(type(item) is not int for item in horizons)
        or tuple(horizons) != (5, 15, 30, 60)
    ):
        raise ConfigurationError("horizons: exact supported horizon set required")
    sessions = raw["requested_sessions"]
    if (
        not isinstance(sessions, list)
        or not sessions
        or not all(isinstance(item, str) and item.strip() for item in sessions)
    ):
        raise ConfigurationError("requested_sessions: nonempty date array required")
    try:
        normalized_sessions = tuple(item for item in sessions if date.fromisoformat(item))
    except ValueError:
        raise ConfigurationError("requested_sessions: ISO dates required") from None
    if normalized_sessions != tuple(sorted(set(normalized_sessions))):
        raise ConfigurationError("requested_sessions: unique sorted dates required")
    silver_registry = _project_path(
        path, raw["silver_registry"], "silver_publications.toml", "silver_registry"
    )
    research_universe = _project_path(
        path, raw["research_universe"], "research_universe.toml", "research_universe"
    )
    try:
        silver_run_id = UUID(raw["silver_run_id"])
    except ValueError:
        raise ConfigurationError("silver_run_id: UUID required") from None
    if raw["research_universe_version"] != settings.dataset_version:
        raise ConfigurationError("research_universe_version: project version mismatch")
    return ExpectedGoldRequest(
        raw["dataset_version"],
        silver_registry,
        raw["silver_dataset_version"],
        silver_run_id,
        research_universe,
        raw["research_universe_version"],
        normalized_sessions,
        raw["expected_series_contract_version"],
        raw["population_policy_version"],
        raw["eligibility_policy"],
        raw["measurement_version"],
        tuple(horizons),
        raw["gold_schema_version"],
        path.resolve(),
        authority,
        tuple(scope),
    )
