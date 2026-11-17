"""Immutable local Gold publication."""

import json
import os
import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from labgdd.adapters.bronze import sha256

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BASE_AUTHORITY_KEYS = {
    "dataset_version",
    "run_id",
    "status",
    "manifest",
    "manifest_sha256",
    "measurement_version",
    "silver_dataset_version",
    "silver_run_id",
    "silver_manifest_sha256",
    "processing_source_sha256",
    "configuration_sha256",
    "approved_at",
    "decision",
}
_V11_AUTHORITY_KEYS = _BASE_AUTHORITY_KEYS | {
    "gold_schema_version",
    "manifest_publication_status",
    "contract_fingerprints",
}
_SUPERSEDED_KEYS = {"run_id", "status", "manifest_sha256", "reason"}


@dataclass(frozen=True, slots=True)
class ApprovedGold:
    registry: Path
    manifest: Path
    manifest_sha256: str
    dataset_version: str
    run_id: str
    metadata: Mapping[str, object]


def _document(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("gold registry: unreadable manifest") from None
    if not isinstance(value, dict):
        raise ValueError("gold registry: invalid manifest")
    return value


def _registry_authorities(registry: dict[str, object]) -> list[dict[str, object]]:
    version = registry.get("registry_version")
    if set(registry) != {"registry_version", "authority", "superseded"}:
        raise ValueError("gold registry: invalid registry schema")
    authority = registry.get("authority")
    if version == "1.0.0":
        if not isinstance(authority, dict) or set(authority) != _BASE_AUTHORITY_KEYS:
            raise ValueError("gold registry: invalid authority")
        normalized = dict(authority)
        normalized.update(
            {
                "gold_schema_version": "1.0.0",
                "manifest_publication_status": "not_recorded",
                "contract_fingerprints": {},
            }
        )
        return [normalized]
    if version == "1.1.0":
        if (
            not isinstance(authority, list)
            or not authority
            or any(
                not isinstance(item, dict) or set(item) != _V11_AUTHORITY_KEYS for item in authority
            )
        ):
            raise ValueError("gold registry: invalid authority inventory")
        return [dict(item) for item in authority]
    raise ValueError("gold registry: unsupported version")


def _validate_authority_inventory(authorities: list[dict[str, object]]) -> None:
    required_strings = _V11_AUTHORITY_KEYS - {"contract_fingerprints"}
    fingerprint_fields = {
        "manifest_sha256",
        "silver_manifest_sha256",
        "processing_source_sha256",
        "configuration_sha256",
    }
    for authority in authorities:
        if any(
            not isinstance(authority.get(field), str) or not str(authority[field]).strip()
            for field in required_strings
        ):
            raise ValueError("gold registry: invalid authority values")
        try:
            UUID(str(authority["run_id"]))
            UUID(str(authority["silver_run_id"]))
        except ValueError:
            raise ValueError("gold registry: invalid authority UUID") from None
        if authority["status"] != "approved" or any(
            _SHA256.fullmatch(str(authority[field])) is None for field in fingerprint_fields
        ):
            raise ValueError("gold registry: invalid authority fingerprints or status")
        contract = authority.get("contract_fingerprints")
        if not isinstance(contract, dict) or any(
            not isinstance(key, str)
            or not key.endswith("_sha256")
            or not isinstance(value, str)
            or _SHA256.fullmatch(value) is None
            for key, value in contract.items()
        ):
            raise ValueError("gold registry: invalid contract fingerprints")


def resolve_approved_gold(
    registry_path: Path, data_root: Path, dataset_version: str, run_id: UUID
) -> ApprovedGold:
    """Resolve one explicitly requested, registered and fully verified Gold run."""
    try:
        registry = tomllib.loads(registry_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError):
        raise ValueError("gold registry: unreadable registry") from None
    authorities = _registry_authorities(registry)
    _validate_authority_inventory(authorities)
    superseded = registry.get("superseded")
    if not isinstance(superseded, list) or any(
        not isinstance(item, dict) or set(item) != _SUPERSEDED_KEYS for item in superseded
    ):
        raise ValueError("gold registry: invalid superseded inventory")
    requested = str(run_id)
    superseded_ids = [item["run_id"] for item in superseded]
    authority_identities = [
        (item.get("dataset_version"), item.get("run_id")) for item in authorities
    ]
    if (
        any(item.get("status") != "superseded" for item in superseded)
        or len(superseded_ids) != len(set(superseded_ids))
        or any(item.get("run_id") in superseded_ids for item in authorities)
        or len(authority_identities) != len(set(authority_identities))
    ):
        raise ValueError("gold registry: inconsistent publication states")
    if requested in superseded_ids:
        raise ValueError("gold registry: requested run is superseded")
    selected = [
        item
        for item in authorities
        if item.get("dataset_version") == dataset_version and item.get("run_id") == requested
    ]
    if len(selected) != 1 or selected[0].get("status") != "approved":
        raise ValueError("gold registry: run is not explicitly approved")
    authority = selected[0]
    manifest_value = authority.get("manifest")
    if not isinstance(manifest_value, str):
        raise ValueError("gold registry: invalid manifest path")
    manifest = (registry_path.parent / manifest_value).resolve()
    gold_root = (data_root.resolve() / "gold").resolve()
    expected_parent = gold_root / f"dataset={dataset_version}" / f"run={requested}"
    if manifest != expected_parent / "manifest.json" or not manifest.is_relative_to(gold_root):
        raise ValueError("gold registry: manifest path mismatch")
    fingerprint_fields = {
        "manifest_sha256",
        "silver_manifest_sha256",
        "processing_source_sha256",
        "configuration_sha256",
    }
    if any(
        not isinstance(authority.get(field), str)
        or _SHA256.fullmatch(str(authority[field])) is None
        for field in fingerprint_fields
    ):
        raise ValueError("gold registry: invalid fingerprints")
    contract_fingerprints = authority.get("contract_fingerprints")
    if not isinstance(contract_fingerprints, dict) or any(
        not isinstance(key, str)
        or not key.endswith("_sha256")
        or not isinstance(value, str)
        or _SHA256.fullmatch(value) is None
        for key, value in contract_fingerprints.items()
    ):
        raise ValueError("gold registry: invalid contract fingerprints")
    if not manifest.is_file() or sha256(manifest) != authority["manifest_sha256"]:
        raise ValueError("gold registry: manifest fingerprint mismatch")
    document = _document(manifest)
    matches = {
        "status": "complete",
        "manifest_version": "1.0.0",
        "gold_schema_version": authority["gold_schema_version"],
        "measurement_version": authority["measurement_version"],
        "dataset_version": dataset_version,
        "run_id": requested,
        "silver_dataset_version": authority["silver_dataset_version"],
        "silver_run_id": authority["silver_run_id"],
        "silver_manifest_sha256": authority["silver_manifest_sha256"],
        "processing_source_sha256": authority["processing_source_sha256"],
        "configuration_sha256": authority["configuration_sha256"],
    }
    publication_status = authority.get("manifest_publication_status")
    if publication_status == "not_recorded":
        matches["publication_status"] = None
    elif isinstance(publication_status, str) and publication_status:
        matches["publication_status"] = publication_status
    else:
        raise ValueError("gold registry: invalid manifest publication status")
    matches.update(contract_fingerprints)
    if any(document.get(key) != value for key, value in matches.items()):
        raise ValueError("gold registry: approved manifest metadata mismatch")
    artifacts = document.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise ValueError("gold registry: invalid artifact inventory")
    expected_files = {"manifest.json"}
    for artifact in artifacts:
        if not isinstance(artifact, dict) or set(artifact) != {"path", "sha256", "bytes"}:
            raise ValueError("gold registry: invalid artifact entry")
        relative, digest, size = artifact["path"], artifact["sha256"], artifact["bytes"]
        if (
            not isinstance(relative, str)
            or not isinstance(digest, str)
            or len(digest) != 64
            or type(size) is not int
            or size < 0
        ):
            raise ValueError("gold registry: invalid artifact entry")
        artifact_path = (manifest.parent / relative).resolve()
        if not artifact_path.is_relative_to(manifest.parent.resolve()):
            raise ValueError("gold registry: artifact path escapes run")
        if (
            not artifact_path.is_file()
            or artifact_path.stat().st_size != size
            or sha256(artifact_path) != digest
        ):
            raise ValueError("gold registry: artifact verification failed")
        expected_files.add(Path(relative).as_posix())
    actual_files = {
        item.relative_to(manifest.parent).as_posix()
        for item in manifest.parent.rglob("*")
        if item.is_file()
    }
    if actual_files != expected_files:
        raise ValueError("gold registry: artifact inventory mismatch")
    return ApprovedGold(
        registry_path.resolve(),
        manifest,
        str(authority["manifest_sha256"]),
        dataset_version,
        requested,
        document,
    )


class GoldRun:
    def __init__(
        self,
        data_root: Path,
        dataset: str,
        run_id: UUID,
        metadata: Mapping[str, object],
        *,
        sector_outputs: bool = False,
    ) -> None:
        if not dataset or any(not (char.isalnum() or char in "_.-") for char in dataset):
            raise ValueError("gold storage: invalid dataset identifier")
        run_id = UUID(str(run_id))
        root = data_root.resolve() / ("sector_gold" if sector_outputs else "gold")
        self.output_names = (
            ("windows", "correlations") if sector_outputs else ("returns", "rolling", "fixed")
        )
        dataset_root = root / f"dataset={dataset}"
        self.path = dataset_root / "_staging" / f"run={run_id}"
        self.completed_path = dataset_root / f"run={run_id}"
        self.failed_path = dataset_root / "_failed" / f"run={run_id}"
        if not all(
            item.resolve().is_relative_to(root)
            for item in (self.path, self.completed_path, self.failed_path)
        ):
            raise ValueError("gold storage: path escapes owned root")
        if self.completed_path.exists() or self.failed_path.exists():
            raise FileExistsError("gold storage: run identity already published")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.mkdir(exist_ok=False)
        self.metadata = dict(metadata)
        self.finished = False
        self._write_json("run.json", self.metadata)

    def _write_json(self, name: str, value: Mapping[str, object]) -> None:
        if self.finished or Path(name).name != name:
            raise ValueError("gold storage: closed run or invalid artifact")
        with (self.path / name).open("xb") as stream:
            stream.write(json.dumps(value, sort_keys=True, allow_nan=False).encode())
            stream.flush()
            os.fsync(stream.fileno())

    def artifact_inventory(self) -> list[dict[str, object]]:
        """Capture the exact bytes to bind a caller's validation to finalization."""
        if self.finished:
            raise ValueError("gold storage: closed run")
        return [
            {
                "path": item.relative_to(self.path).as_posix(),
                "sha256": sha256(item),
                "bytes": item.stat().st_size,
            }
            for item in sorted(self.path.rglob("*"))
            if item.is_file() and item.name not in {"manifest.pending", "manifest.json"}
        ]

    def finish(
        self,
        status: str,
        summary: Mapping[str, object],
        *,
        expected_artifacts: list[dict[str, object]] | None = None,
    ) -> Path:
        if status not in {"complete", "failed"} or self.finished:
            raise ValueError("gold storage: invalid terminal state")
        if status == "complete":
            expected = {"run.json", *self.output_names}
            if {item.name for item in self.path.iterdir()} != expected or any(
                not any((self.path / name).rglob("*.parquet")) for name in self.output_names
            ):
                raise ValueError("gold storage: incomplete output inventory")
        artifacts = self.artifact_inventory()
        if expected_artifacts is not None and artifacts != expected_artifacts:
            raise ValueError("gold storage: artifacts changed after validation")
        document = {
            **self.metadata,
            **summary,
            "status": status,
            "artifacts": artifacts,
            "finished_at": datetime.now(UTC).isoformat(),
        }
        self._write_json("manifest.pending", document)
        os.link(self.path / "manifest.pending", self.path / "manifest.json")
        (self.path / "manifest.pending").unlink()
        target = self.completed_path if status == "complete" else self.failed_path
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.rename(self.path, target)
        except OSError:
            try:
                (self.path / "manifest.json").unlink()
            except OSError:
                pass
            raise
        self.path = target
        self.finished = True
        return target / "manifest.json"
