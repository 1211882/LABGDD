"""Explicit synthetic-only Sector Gold publication request; no real execution mode."""

import re
import tomllib
from dataclasses import dataclass
from pathlib import Path, PureWindowsPath
from uuid import UUID

from labgdd.analytics.sector import ELIGIBILITY_POLICIES
from labgdd.config import ConfigurationError


@dataclass(frozen=True, slots=True)
class SectorPublicationRequest:
    dataset_version: str
    gold_registry: Path
    silver_registry: Path
    research_reference: Path
    input_dataset_version: str
    input_run_id: UUID
    input_manifest_sha256: str
    eligibility_policy: str = "declared_research"
    sector_schema_version: str = "1.0.0"
    evidence_kind: str = "synthetic_fixture"

    def __post_init__(self) -> None:
        if (
            not re.fullmatch(r"sector-synthetic-[A-Za-z0-9_.-]+", self.dataset_version)
            or not re.fullmatch(r"[A-Za-z0-9_.-]+", self.input_dataset_version)
            or not isinstance(self.input_run_id, UUID)
            or not re.fullmatch(r"[0-9a-f]{64}", self.input_manifest_sha256)
            or self.eligibility_policy not in ELIGIBILITY_POLICIES
            or self.sector_schema_version != "1.0.0"
            or self.evidence_kind != "synthetic_fixture"
        ):
            raise ConfigurationError("sector publication: invalid or unsupported request")


def load_sector_publication_request(path: Path) -> SectorPublicationRequest:
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
        required = set(SectorPublicationRequest.__dataclass_fields__)
        if set(raw) != required or any(
            not isinstance(value, str) or not value for value in raw.values()
        ):
            raise ValueError
        for key in ("gold_registry", "silver_registry", "research_reference"):
            value = raw[key]
            relative = Path(value)
            root = path.resolve().parent.parent
            resolved = (path.resolve().parent / relative).resolve()
            if (
                relative.is_absolute()
                or PureWindowsPath(value).drive
                or "\\" in value
                or not resolved.is_relative_to(root)
            ):
                raise ValueError
            raw[key] = resolved
        raw["input_run_id"] = UUID(raw["input_run_id"])
        return SectorPublicationRequest(**raw)
    except (OSError, UnicodeDecodeError, ValueError, TypeError, tomllib.TOMLDecodeError):
        raise ConfigurationError(
            "sector publication: unreadable or invalid configuration"
        ) from None
