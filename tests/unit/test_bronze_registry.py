import json
from pathlib import Path
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from labgdd.adapters.bronze import resolve_approved_bronze, sha256

DATASET = "bronze-registry-fixture-v1"
ACQUISITION = UUID("30000000-0000-0000-0000-000000000001")
SUPERSEDED = UUID("30000000-0000-0000-0000-000000000002")
SOURCE_HASH = "a" * 64
SCHEDULE_HASH = "b" * 64


def fixture(root: Path) -> tuple[Path, Path, Path]:
    run = root / "data/bronze" / f"dataset={DATASET}" / f"acquisition={ACQUISITION}"
    run.mkdir(parents=True)
    acquisition = run / "acquisition.json"
    acquisition.write_text("{}", encoding="utf-8")
    calendar_doc = {"schedule_sha256": SCHEDULE_HASH}
    calendar = run / "calendar.json"
    calendar.write_text(json.dumps(calendar_doc), encoding="utf-8")
    observations = run / "observations-000001.parquet"
    pq.write_table(
        pa.table(
            {
                "observation_id": pa.array([], type=pa.string()),
                "symbol": pa.array([], type=pa.string()),
                "raw_json": pa.array([], type=pa.string()),
                "event_time": pa.array([], type=pa.timestamp("us", tz="UTC")),
                "valid": pa.array([], type=pa.bool_()),
                "reason": pa.array([], type=pa.string()),
                "session": pa.array([], type=pa.string()),
            }
        ),
        observations,
    )
    response = run / "response-000001.parquet"
    pq.write_table(pa.table({"body": pa.array([], type=pa.binary())}), response)
    artifacts = [
        {"path": item.name, "sha256": sha256(item), "bytes": item.stat().st_size}
        for item in sorted(run.iterdir())
    ]
    manifest = run / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "status": "complete",
                "manifest_version": "1.0.0",
                "schema_version": "1.0.0",
                "observation_identity_version": "bronze-observation-v1",
                "source": "alpaca",
                "feed": "iex",
                "timeframe": "1Min",
                "adjustment": "raw",
                "revision_resolution": "not_performed",
                "dataset_version": DATASET,
                "acquisition_id": str(ACQUISITION),
                "package_source_sha256": SOURCE_HASH,
                "calendar": calendar_doc,
                "artifacts": artifacts,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    config = root / "config"
    config.mkdir()
    registry = config / "bronze_publications.toml"
    registry.write_text(
        f'''registry_version = "1.0.0"

[[authority]]
dataset_version = "{DATASET}"
acquisition_id = "{ACQUISITION}"
status = "approved"
manifest = "../data/bronze/dataset={DATASET}/acquisition={ACQUISITION}/manifest.json"
manifest_sha256 = "{sha256(manifest)}"
manifest_version = "1.0.0"
schema_version = "1.0.0"
observation_identity_version = "bronze-observation-v1"
source_contract_version = "alpaca-iex-1Min-raw-v1"
package_source_sha256 = "{SOURCE_HASH}"
schedule_sha256 = "{SCHEDULE_HASH}"
approved_at = "2026-11-06T00:00:00Z"
decision = "fixture"

[[superseded]]
acquisition_id = "{SUPERSEDED}"
status = "superseded"
manifest_sha256 = "{"c" * 64}"
reason = "fixture"
''',
        encoding="utf-8",
    )
    return registry, manifest, observations


def test_resolves_only_exact_verified_bronze_authority(tmp_path: Path) -> None:
    registry, manifest, _ = fixture(tmp_path)
    result = resolve_approved_bronze(registry, tmp_path / "data", DATASET, ACQUISITION)
    assert result.manifest == manifest
    with pytest.raises(ValueError, match="not explicitly approved"):
        resolve_approved_bronze(
            registry,
            tmp_path / "data",
            DATASET,
            UUID("30000000-0000-0000-0000-000000000099"),
        )
    with pytest.raises(ValueError, match="requested acquisition is superseded"):
        resolve_approved_bronze(registry, tmp_path / "data", DATASET, SUPERSEDED)


def test_rejects_bronze_manifest_and_artifact_tampering(tmp_path: Path) -> None:
    registry, manifest, observations = fixture(tmp_path)
    observations.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="artifact size mismatch|artifact checksum mismatch"):
        resolve_approved_bronze(registry, tmp_path / "data", DATASET, ACQUISITION)

    registry, manifest, _ = fixture(tmp_path / "second")
    manifest.write_text(manifest.read_text(encoding="utf-8") + " ", encoding="utf-8")
    with pytest.raises(ValueError, match="manifest fingerprint mismatch"):
        resolve_approved_bronze(registry, tmp_path / "second/data", DATASET, ACQUISITION)


def test_rejects_bronze_schema_and_lineage_metadata_mismatch(tmp_path: Path) -> None:
    registry, _, _ = fixture(tmp_path)
    registry.write_text(
        registry.read_text(encoding="utf-8").replace(
            'source_contract_version = "alpaca-iex-1Min-raw-v1"',
            'source_contract_version = "wrong-v1"',
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="approved manifest metadata mismatch"):
        resolve_approved_bronze(registry, tmp_path / "data", DATASET, ACQUISITION)
