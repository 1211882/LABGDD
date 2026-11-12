"""Verified Bronze inputs and immutable local Silver run publication."""

import hashlib
import json
import os
import tomllib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import UUID

from labgdd.adapters.bronze import sha256

_V10_AUTHORITY_KEYS = {
    "dataset_version",
    "run_id",
    "status",
    "manifest",
    "manifest_sha256",
    "processing_source_sha256",
    "request",
    "request_sha256",
    "approved_at",
    "decision",
}
_V11_AUTHORITY_KEYS = _V10_AUTHORITY_KEYS | {
    "silver_schema_version",
    "resolution_policy_version",
    "source_contract_version",
    "bronze_inputs",
}
_SUPERSEDED_KEYS = {"run_id", "status", "manifest_sha256", "reason"}


@dataclass(frozen=True, slots=True)
class BronzeInput:
    manifest: Path
    manifest_sha256: str
    acquisition_id: str
    dataset_version: str
    observation_files: tuple[Path, ...]
    metadata: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class ApprovedSilver:
    registry: Path
    manifest: Path
    manifest_sha256: str
    dataset_version: str
    run_id: str
    metadata: Mapping[str, Any]
    authority_status: str = "approved"


def _document(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("bronze input: unreadable manifest") from None
    if not isinstance(value, dict):
        raise ValueError("bronze input: invalid manifest")
    return cast(dict[str, Any], value)


def verify_bronze_manifest(path: Path) -> BronzeInput:
    """Fail closed on status, metadata, inventory, hashes and observation schema."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    doc = _document(path)
    required = {
        "status": "complete",
        "manifest_version": "1.0.0",
        "schema_version": "1.0.0",
        "observation_identity_version": "bronze-observation-v1",
        "source": "alpaca",
        "feed": "iex",
        "timeframe": "1Min",
        "adjustment": "raw",
    }
    if any(doc.get(key) != value for key, value in required.items()):
        raise ValueError("bronze input: incompatible or incomplete manifest")
    if doc.get("revision_resolution") != "not_performed":
        raise ValueError("bronze input: unexpected revision state")
    artifacts = doc.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise ValueError("bronze input: invalid artifact inventory")
    expected_names: set[str] = set()
    observations: list[Path] = []
    responses: list[Path] = []
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            raise ValueError("bronze input: invalid artifact inventory")
        name, digest, size = artifact.get("path"), artifact.get("sha256"), artifact.get("bytes")
        if (
            not isinstance(name, str)
            or Path(name).name != name
            or not isinstance(digest, str)
            or len(digest) != 64
            or type(size) is not int
            or size < 0
        ):
            raise ValueError("bronze input: invalid artifact entry")
        artifact_path = path.parent / name
        if not artifact_path.is_file() or artifact_path.stat().st_size != size:
            raise ValueError("bronze input: artifact size mismatch")
        if sha256(artifact_path) != digest:
            raise ValueError("bronze input: artifact checksum mismatch")
        expected_names.add(name)
        if name.startswith("observations-") and name.endswith(".parquet"):
            observations.append(artifact_path)
        if name.startswith("response-") and name.endswith(".parquet"):
            responses.append(artifact_path)
    actual_names = {item.name for item in path.parent.iterdir() if item.is_file()}
    if actual_names != expected_names | {"manifest.json"}:
        raise ValueError("bronze input: artifact inventory mismatch")
    expected_schema = pa.schema(
        [
            ("observation_id", pa.string()),
            ("symbol", pa.string()),
            ("raw_json", pa.string()),
            ("event_time", pa.timestamp("us", tz="UTC")),
            ("valid", pa.bool_()),
            ("reason", pa.string()),
            ("session", pa.string()),
        ]
    )
    if not {"acquisition.json", "calendar.json"} <= expected_names or not responses:
        raise ValueError("bronze input: required evidence missing")
    calendar_document = _document(path.parent / "calendar.json")
    if calendar_document != doc.get("calendar"):
        raise ValueError("bronze input: calendar provenance mismatch")
    if not observations or any(pq.read_schema(item) != expected_schema for item in observations):
        raise ValueError("bronze input: observation schema mismatch")
    acquisition = doc.get("acquisition_id")
    dataset = doc.get("dataset_version")
    if not isinstance(acquisition, str) or not isinstance(dataset, str):
        raise ValueError("bronze input: missing identity")
    return BronzeInput(path, sha256(path), acquisition, dataset, tuple(observations), doc)


def _registry_authorities(registry: dict[str, Any]) -> list[dict[str, Any]]:
    if set(registry) != {"registry_version", "authority", "superseded"}:
        raise ValueError("silver registry: invalid registry schema")
    version = registry.get("registry_version")
    authority = registry.get("authority")
    if version == "1.0.0":
        if not isinstance(authority, dict) or set(authority) != _V10_AUTHORITY_KEYS:
            raise ValueError("silver registry: invalid authority")
        normalized = dict(authority)
        normalized.update(
            {
                "silver_schema_version": None,
                "resolution_policy_version": None,
                "source_contract_version": None,
                "bronze_inputs": None,
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
            raise ValueError("silver registry: invalid authority inventory")
        return [dict(item) for item in authority]
    raise ValueError("silver registry: unsupported version")


def _resolve_silver(
    registry_path: Path,
    data_root: Path,
    dataset_version: str,
    run_id: UUID,
    *,
    required_status: str,
) -> ApprovedSilver:
    try:
        registry = tomllib.loads(registry_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError):
        raise ValueError("silver registry: unreadable registry") from None
    authorities = _registry_authorities(registry)
    superseded = registry.get("superseded")
    if not isinstance(superseded, list) or any(
        not isinstance(item, dict) or set(item) != _SUPERSEDED_KEYS for item in superseded
    ):
        raise ValueError("silver registry: invalid superseded inventory")
    requested_id = str(run_id)
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
        raise ValueError("silver registry: inconsistent publication states")
    if requested_id in superseded_ids:
        raise ValueError("silver registry: requested run is superseded")
    selected = [
        item
        for item in authorities
        if item.get("status") == required_status
        and item.get("dataset_version") == dataset_version
        and item.get("run_id") == requested_id
    ]
    if len(selected) != 1:
        raise ValueError("silver registry: run is not explicitly approved")
    authority = selected[0]
    manifest_value = authority.get("manifest")
    request_value = authority.get("request")
    if not isinstance(manifest_value, str) or not isinstance(request_value, str):
        raise ValueError("silver registry: invalid authority paths")
    manifest = (registry_path.parent / manifest_value).resolve()
    request = (registry_path.parent / request_value).resolve()
    silver_root = (data_root.resolve() / "silver").resolve()
    expected_parent = silver_root / f"dataset={dataset_version}" / f"run={requested_id}"
    if manifest != expected_parent / "manifest.json" or not manifest.is_relative_to(silver_root):
        raise ValueError("silver registry: manifest path mismatch")
    manifest_hash = authority.get("manifest_sha256")
    request_hash = authority.get("request_sha256")
    source_hash = authority.get("processing_source_sha256")
    if not all(
        isinstance(value, str) and len(value) == 64
        for value in (manifest_hash, request_hash, source_hash)
    ):
        raise ValueError("silver registry: invalid fingerprints")
    if not request.is_file() or sha256(request) != request_hash:
        raise ValueError("silver registry: request fingerprint mismatch")
    if not manifest.is_file() or sha256(manifest) != manifest_hash:
        raise ValueError("silver registry: manifest fingerprint mismatch")
    document = _document(manifest)
    if (
        document.get("status") != "complete"
        or document.get("run_id") != requested_id
        or document.get("dataset_version") != dataset_version
        or document.get("processing_source_sha256") != source_hash
    ):
        raise ValueError("silver registry: approved manifest metadata mismatch")
    if registry.get("registry_version") == "1.1.0":
        bronze_inputs = authority.get("bronze_inputs")
        expected_input_keys = {
            "dataset_version",
            "acquisition_id",
            "manifest_sha256",
        }
        if (
            not isinstance(bronze_inputs, list)
            or not bronze_inputs
            or any(
                not isinstance(item, dict) or set(item) != expected_input_keys
                for item in bronze_inputs
            )
        ):
            raise ValueError("silver registry: invalid Bronze lineage")
        lineage_identities = [
            (item.get("dataset_version"), item.get("acquisition_id")) for item in bronze_inputs
        ]
        if len(lineage_identities) != len(set(lineage_identities)):
            raise ValueError("silver registry: duplicate Bronze lineage")
        manifest_inputs = document.get("input_manifests")
        normalized_manifest_inputs = (
            [
                {
                    "dataset_version": item.get("dataset_version"),
                    "acquisition_id": item.get("acquisition_id"),
                    "manifest_sha256": item.get("sha256"),
                }
                for item in manifest_inputs
                if isinstance(item, dict)
            ]
            if isinstance(manifest_inputs, list)
            else None
        )
        source_contract = (
            f"{document.get('source')}-{document.get('feed')}-"
            f"{document.get('timeframe')}-{document.get('adjustment')}-v1"
        )
        if (
            normalized_manifest_inputs != bronze_inputs
            or authority.get("silver_schema_version") != "1.0.0"
            or document.get("resolution_policy_version")
            != authority.get("resolution_policy_version")
            or source_contract != authority.get("source_contract_version")
        ):
            raise ValueError("silver registry: approved lineage or contract mismatch")
    artifacts = document.get("artifacts")
    if not isinstance(artifacts, list):
        raise ValueError("silver registry: invalid artifact inventory")
    expected_files = {"manifest.json"}
    for artifact in artifacts:
        if not isinstance(artifact, dict) or set(artifact) != {"path", "sha256", "bytes"}:
            raise ValueError("silver registry: invalid artifact entry")
        relative, digest, size = artifact["path"], artifact["sha256"], artifact["bytes"]
        if (
            not isinstance(relative, str)
            or not isinstance(digest, str)
            or len(digest) != 64
            or type(size) is not int
            or size < 0
        ):
            raise ValueError("silver registry: invalid artifact entry")
        artifact_path = (manifest.parent / relative).resolve()
        if not artifact_path.is_relative_to(manifest.parent.resolve()):
            raise ValueError("silver registry: artifact path escapes run")
        if (
            not artifact_path.is_file()
            or artifact_path.stat().st_size != size
            or sha256(artifact_path) != digest
        ):
            raise ValueError("silver registry: artifact verification failed")
        expected_files.add(Path(relative).as_posix())
    actual_files = {
        item.relative_to(manifest.parent).as_posix()
        for item in manifest.parent.rglob("*")
        if item.is_file()
    }
    if actual_files != expected_files:
        raise ValueError("silver registry: artifact inventory mismatch")
    return ApprovedSilver(
        registry_path.resolve(),
        manifest,
        manifest_hash,
        dataset_version,
        requested_id,
        document,
        required_status,
    )


def resolve_approved_silver(
    registry_path: Path, data_root: Path, dataset_version: str, run_id: UUID
) -> ApprovedSilver:
    """Resolve only an explicitly requested, registry-approved and verified Silver run."""
    return _resolve_silver(
        registry_path,
        data_root,
        dataset_version,
        run_id,
        required_status="approved",
    )


def resolve_bounded_validation_silver(
    registry_path: Path, data_root: Path, dataset_version: str, run_id: UUID
) -> ApprovedSilver:
    """Resolve a separately authorized candidate solely for bounded validation."""
    return _resolve_silver(
        registry_path,
        data_root,
        dataset_version,
        run_id,
        required_status="authorized_bounded_validation",
    )


def resolve_silver_input(
    registry_path: Path,
    data_root: Path,
    dataset_version: str,
    run_id: UUID,
    authority_status: str,
) -> ApprovedSilver:
    """Select one explicit authority boundary without broadening approved resolution."""
    if authority_status == "approved":
        return resolve_approved_silver(registry_path, data_root, dataset_version, run_id)
    if authority_status == "authorized_bounded_validation":
        return resolve_bounded_validation_silver(registry_path, data_root, dataset_version, run_id)
    raise ValueError("silver registry: unsupported input authority")


class SilverRun:
    def __init__(
        self, data_root: Path, dataset: str, run_id: UUID, metadata: Mapping[str, object]
    ) -> None:
        if not dataset or any(
            c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-"
            for c in dataset
        ):
            raise ValueError("silver storage: invalid dataset identifier")
        root = data_root.resolve() / "silver"
        dataset_root = root / f"dataset={dataset}"
        self.path = dataset_root / "_staging" / f"run={run_id}"
        self.completed_path = dataset_root / f"run={run_id}"
        self.failed_path = dataset_root / "_failed" / f"run={run_id}"
        if not all(
            item.resolve().is_relative_to(root.resolve())
            for item in (self.path, self.completed_path, self.failed_path)
        ):
            raise ValueError("silver storage: path escapes owned root")
        if self.completed_path.exists() or self.failed_path.exists():
            raise FileExistsError("silver storage: run identity already published")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.mkdir(exist_ok=False)
        self.metadata = dict(metadata)
        self.finished = False
        self.write_json("run.json", self.metadata)

    def write_json(self, name: str, value: Mapping[str, object]) -> None:
        if self.finished or Path(name).name != name or name.startswith("."):
            raise ValueError("silver storage: closed run or invalid artifact name")
        with (self.path / name).open("xb") as stream:
            stream.write(json.dumps(value, sort_keys=True, allow_nan=False).encode())
            stream.flush()
            os.fsync(stream.fileno())

    def write_parquet(
        self, relative: str, rows: Sequence[Mapping[str, object]], *, kind: str
    ) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        schemas = {
            "canonical": pa.schema(
                [
                    ("event_time", pa.timestamp("us", tz="UTC")),
                    ("bar_end", pa.timestamp("us", tz="UTC")),
                    ("symbol", pa.string()),
                    ("open", pa.decimal128(26, 8)),
                    ("high", pa.decimal128(26, 8)),
                    ("low", pa.decimal128(26, 8)),
                    ("close", pa.decimal128(26, 8)),
                    ("volume", pa.int64()),
                    ("source", pa.string()),
                    ("timeframe", pa.string()),
                    ("adjustment", pa.string()),
                    ("selected_observation_id", pa.string()),
                    ("contributing_observation_ids", pa.list_(pa.string())),
                    ("sector", pa.string()),
                    ("membership_version", pa.string()),
                    ("membership_valid_from", pa.timestamp("us", tz="UTC")),
                    ("membership_valid_to", pa.timestamp("us", tz="UTC")),
                    ("session_id", pa.string()),
                    ("session_open", pa.timestamp("us", tz="UTC")),
                    ("session_close", pa.timestamp("us", tz="UTC")),
                    ("calendar_provider", pa.string()),
                    ("calendar_version", pa.string()),
                    ("schedule_sha256", pa.string()),
                    ("quality_status", pa.string()),
                    ("dataset_version", pa.string()),
                    ("schema_version", pa.string()),
                    ("resolution_policy_version", pa.string()),
                    ("input_acquisition_id", pa.string()),
                    ("input_dataset_version", pa.string()),
                    ("input_manifest_sha256", pa.string()),
                ]
            ),
            "quarantine": pa.schema(
                [
                    ("observation_id", pa.string()),
                    ("symbol", pa.string()),
                    ("raw_json", pa.string()),
                    ("event_time", pa.timestamp("us", tz="UTC")),
                    ("quality_reasons", pa.list_(pa.string())),
                    ("input_acquisition_id", pa.string()),
                    ("input_dataset_version", pa.string()),
                    ("input_manifest_sha256", pa.string()),
                ]
            ),
            "duplicates": pa.schema(
                [
                    ("symbol", pa.string()),
                    ("event_time", pa.timestamp("us", tz="UTC")),
                    ("observation_rows", pa.int64()),
                    ("distinct_observation_ids", pa.int64()),
                    ("payload_versions", pa.int64()),
                    ("contributing_observation_ids", pa.list_(pa.string())),
                    ("duplicate_classification", pa.string()),
                ]
            ),
        }
        if kind not in schemas:
            raise ValueError("silver storage: unsupported Parquet schema")
        target = (self.path / relative).resolve()
        if (
            self.finished
            or not target.is_relative_to(self.path.resolve())
            or target.suffix != ".parquet"
        ):
            raise ValueError("silver storage: invalid Parquet target")
        target.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(rows, schema=schemas[kind])
        with target.open("xb") as stream:
            pq.write_table(table, stream, compression="zstd")
            stream.flush()
            os.fsync(stream.fileno())

    def finish(self, status: str, summary: Mapping[str, object]) -> Path:
        if status not in {"complete", "failed"} or self.finished:
            raise ValueError("silver storage: invalid terminal state")
        artifacts = []
        for item in sorted(self.path.rglob("*")):
            if item.is_file() and item.name not in {"manifest.pending", "manifest.json"}:
                artifacts.append(
                    {
                        "path": item.relative_to(self.path).as_posix(),
                        "sha256": sha256(item),
                        "bytes": item.stat().st_size,
                    }
                )
        manifest = {
            **self.metadata,
            **summary,
            "status": status,
            "artifacts": artifacts,
            "finished_at": datetime.now(UTC).isoformat(),
        }
        self.write_json("manifest.pending", manifest)
        os.link(self.path / "manifest.pending", self.path / "manifest.json")
        try:
            (self.path / "manifest.pending").unlink()
        except OSError:
            pass
        target = self.completed_path if status == "complete" else self.failed_path
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.rename(self.path, target)
        except OSError:
            # A terminal manifest is only terminal after directory publication.
            # Leave staging retryable so the orchestrator can publish failed evidence.
            try:
                (self.path / "manifest.json").unlink()
            except OSError:
                pass
            raise
        self.path = target
        self.finished = True
        return target / "manifest.json"


def content_sha256(rows: list[Mapping[str, object]]) -> str:
    encoded = json.dumps(rows, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()
