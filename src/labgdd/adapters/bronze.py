"""Exclusive local Bronze acquisition directories and atomic terminal manifests."""

import hashlib
import json
import os
import re
import tomllib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True, slots=True)
class ApprovedBronze:
    registry: Path
    manifest: Path
    manifest_sha256: str
    dataset_version: str
    acquisition_id: str
    metadata: Mapping[str, object]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_approved_bronze(
    registry_path: Path, data_root: Path, dataset_version: str, acquisition_id: UUID
) -> ApprovedBronze:
    """Resolve one exact, registered Bronze acquisition and verify all raw evidence."""
    # Imported here to avoid a module cycle: Silver owns the established strict
    # Bronze manifest and physical-schema verifier.
    from labgdd.adapters.silver import verify_bronze_manifest

    try:
        registry = tomllib.loads(registry_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError):
        raise ValueError("bronze registry: unreadable registry") from None
    if set(registry) != {"registry_version", "authority", "superseded"}:
        raise ValueError("bronze registry: invalid registry schema")
    if registry.get("registry_version") != "1.0.0":
        raise ValueError("bronze registry: unsupported version")
    authorities = registry.get("authority")
    superseded = registry.get("superseded")
    authority_keys = {
        "dataset_version",
        "acquisition_id",
        "status",
        "manifest",
        "manifest_sha256",
        "manifest_version",
        "schema_version",
        "observation_identity_version",
        "source_contract_version",
        "package_source_sha256",
        "schedule_sha256",
        "approved_at",
        "decision",
    }
    superseded_keys = {"acquisition_id", "status", "manifest_sha256", "reason"}
    if (
        not isinstance(authorities, list)
        or not authorities
        or any(not isinstance(item, dict) or set(item) != authority_keys for item in authorities)
    ):
        raise ValueError("bronze registry: invalid authority inventory")
    if not isinstance(superseded, list) or any(
        not isinstance(item, dict) or set(item) != superseded_keys for item in superseded
    ):
        raise ValueError("bronze registry: invalid superseded inventory")
    identities = [(item.get("dataset_version"), item.get("acquisition_id")) for item in authorities]
    superseded_ids = [item.get("acquisition_id") for item in superseded]
    if (
        len(identities) != len(set(identities))
        or len(superseded_ids) != len(set(superseded_ids))
        or any(item.get("status") != "approved" for item in authorities)
        or any(item.get("status") != "superseded" for item in superseded)
        or any(item.get("acquisition_id") in superseded_ids for item in authorities)
    ):
        raise ValueError("bronze registry: inconsistent publication states")
    requested = str(acquisition_id)
    if requested in superseded_ids:
        raise ValueError("bronze registry: requested acquisition is superseded")
    selected = [
        item
        for item in authorities
        if item.get("dataset_version") == dataset_version
        and item.get("acquisition_id") == requested
    ]
    if len(selected) != 1:
        raise ValueError("bronze registry: acquisition is not explicitly approved")
    authority = selected[0]
    manifest_value = authority.get("manifest")
    if not isinstance(manifest_value, str):
        raise ValueError("bronze registry: invalid manifest path")
    manifest = (registry_path.parent / manifest_value).resolve()
    bronze_root = (data_root.resolve() / "bronze").resolve()
    expected = bronze_root / f"dataset={dataset_version}" / f"acquisition={requested}"
    if manifest != expected / "manifest.json" or not manifest.is_relative_to(bronze_root):
        raise ValueError("bronze registry: manifest path mismatch")
    fingerprint_fields = {"manifest_sha256", "package_source_sha256", "schedule_sha256"}
    if any(
        not isinstance(authority.get(field), str)
        or _SHA256.fullmatch(str(authority[field])) is None
        for field in fingerprint_fields
    ):
        raise ValueError("bronze registry: invalid fingerprints")
    if not manifest.is_file() or sha256(manifest) != authority["manifest_sha256"]:
        raise ValueError("bronze registry: manifest fingerprint mismatch")
    verified = verify_bronze_manifest(manifest)
    document = verified.metadata
    calendar = document.get("calendar")
    schedule = calendar.get("schedule_sha256") if isinstance(calendar, dict) else None
    matches = {
        "manifest_version": authority["manifest_version"],
        "schema_version": authority["schema_version"],
        "observation_identity_version": authority["observation_identity_version"],
        "package_source_sha256": authority["package_source_sha256"],
    }
    source_contract = (
        f"{document.get('source')}-{document.get('feed')}-{document.get('timeframe')}-"
        f"{document.get('adjustment')}-v1"
    )
    if (
        any(document.get(key) != value for key, value in matches.items())
        or schedule != authority["schedule_sha256"]
        or source_contract != authority["source_contract_version"]
        or verified.dataset_version != dataset_version
        or verified.acquisition_id != requested
    ):
        raise ValueError("bronze registry: approved manifest metadata mismatch")
    return ApprovedBronze(
        registry_path.resolve(),
        manifest,
        str(authority["manifest_sha256"]),
        dataset_version,
        requested,
        document,
    )


class BronzeAcquisition:
    def __init__(
        self, data_root: Path, dataset: str, acquisition_id: UUID, metadata: Mapping[str, object]
    ) -> None:
        # dataset is already validated by HistoricalRequest; check ownership here
        # too because this is a public storage boundary.
        if (
            not dataset
            or any(
                c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-"
                for c in dataset
            )
            or dataset in {".", ".."}
        ):
            raise ValueError("storage: invalid dataset identifier")
        bronze_root = data_root.resolve() / "bronze"
        if not bronze_root.resolve().is_relative_to(data_root.resolve()):
            raise ValueError("storage: bronze root escapes data root")
        self.path = bronze_root / f"dataset={dataset}" / f"acquisition={acquisition_id}"
        if not self.path.resolve().is_relative_to(bronze_root.resolve()):
            raise ValueError("storage: path escapes owned root")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.mkdir(exist_ok=False)
        self.metadata = dict(metadata)
        self.finished = False
        self.write_json("acquisition.json", self.metadata)

    def write_json(self, name: str, value: Mapping[str, object]) -> None:
        with self._target(name).open("xb") as stream:
            stream.write(json.dumps(value, sort_keys=True, allow_nan=False).encode())
            stream.flush()
            os.fsync(stream.fileno())

    def _target(self, name: str) -> Path:
        if self.finished or Path(name).name != name or name.startswith("."):
            raise ValueError("storage: closed acquisition or invalid artifact name")
        return self.path / name

    def write_parquet(
        self, name: str, rows: Sequence[Mapping[str, object]], *, pages: bool = False
    ) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Explicit schemas also cover empty pages without inferred null types.
        fields: list[tuple[str, Any]]
        if pages:
            fields = [
                ("page", pa.int64()),
                ("received_at", pa.timestamp("us", tz="UTC")),
                ("request_token", pa.string()),
                ("body", pa.binary()),
                ("body_sha256", pa.string()),
            ]
        else:
            fields = [
                ("observation_id", pa.string()),
                ("symbol", pa.string()),
                ("raw_json", pa.string()),
                ("event_time", pa.timestamp("us", tz="UTC")),
                ("valid", pa.bool_()),
                ("reason", pa.string()),
                ("session", pa.string()),
            ]
        table = pa.Table.from_pylist(list(rows), schema=pa.schema(fields))
        with self._target(name).open("xb") as stream:
            pq.write_table(table, stream, compression="zstd")
            stream.flush()
            os.fsync(stream.fileno())

    def finish(self, status: str, summary: Mapping[str, object]) -> Path:
        if status not in {"complete", "failed"}:
            raise ValueError("storage: invalid terminal state")
        target = self._target("manifest.json")
        artifacts = [
            {"path": p.name, "sha256": sha256(p), "bytes": p.stat().st_size}
            for p in sorted(self.path.iterdir())
            if p.is_file()
        ]
        manifest = {
            **self.metadata,
            **summary,
            "status": status,
            "artifacts": artifacts,
            "finished_at": datetime.now(UTC).isoformat(),
        }
        self.write_json("manifest.pending", manifest)
        # A hard link publishes a fully flushed file atomically and fails if the
        # destination exists. Requires local filesystem hard-link support.
        os.link(self.path / "manifest.pending", target)
        self.finished = True
        # Publication already succeeded. A transient cleanup failure must not
        # misreport a committed acquisition as uncommitted; the alias is harmless.
        try:
            (self.path / "manifest.pending").unlink()
        except OSError:
            pass
        return target
