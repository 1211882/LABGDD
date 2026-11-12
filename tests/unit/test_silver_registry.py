import json
from pathlib import Path
from uuid import UUID

import pytest

from labgdd.adapters.bronze import sha256
from labgdd.adapters.silver import (
    resolve_approved_silver,
    resolve_bounded_validation_silver,
)

APPROVED = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
SUPERSEDED = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
UNREGISTERED = UUID("ffffffff-ffff-4fff-8fff-ffffffffffff")
DATASET = "silver-registry-fixture-v1"
SOURCE_HASH = "1" * 64


def registry_fixture(root: Path) -> tuple[Path, Path, Path]:
    config = root / "config"
    config.mkdir(parents=True)
    request = config / "silver.toml"
    request.write_text('dataset_version = "fixture"\n', encoding="utf-8")
    run = root / "data/silver" / f"dataset={DATASET}" / f"run={APPROVED}"
    run.mkdir(parents=True)
    artifact = run / "run.json"
    artifact.write_text('{"fixture":true}', encoding="utf-8")
    manifest = run / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "status": "complete",
                "run_id": str(APPROVED),
                "dataset_version": DATASET,
                "processing_source_sha256": SOURCE_HASH,
                "schema_version": "1.0.0",
                "resolution_policy_version": "silver-resolution-v1",
                "source": "alpaca",
                "feed": "iex",
                "timeframe": "1Min",
                "adjustment": "raw",
                "input_manifests": [
                    {
                        "dataset_version": "bronze-fixture-v1",
                        "acquisition_id": "30000000-0000-0000-0000-000000000001",
                        "sha256": "3" * 64,
                    }
                ],
                "artifacts": [
                    {
                        "path": "run.json",
                        "sha256": sha256(artifact),
                        "bytes": artifact.stat().st_size,
                    }
                ],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    registry = config / "silver_publications.toml"
    registry.write_text(
        f'''registry_version = "1.0.0"

[authority]
dataset_version = "{DATASET}"
run_id = "{APPROVED}"
status = "approved"
manifest = "../data/silver/dataset={DATASET}/run={APPROVED}/manifest.json"
manifest_sha256 = "{sha256(manifest)}"
processing_source_sha256 = "{SOURCE_HASH}"
request = "silver.toml"
request_sha256 = "{sha256(request)}"
approved_at = "2026-11-03T00:00:00+00:00"
decision = "fixture approval"

[[superseded]]
run_id = "{SUPERSEDED}"
status = "superseded"
manifest_sha256 = "{"2" * 64}"
reason = "fixture supersession"
''',
        encoding="utf-8",
    )
    return registry, manifest, artifact


def test_resolves_only_explicit_verified_authority(tmp_path: Path) -> None:
    registry, manifest, _ = registry_fixture(tmp_path)
    # A lexically newer-looking directory is irrelevant and must never be selected.
    newest = tmp_path / "data/silver" / f"dataset={DATASET}" / f"run={UNREGISTERED}"
    newest.mkdir(parents=True)
    (newest / "manifest.json").write_text('{"status":"complete"}', encoding="utf-8")
    result = resolve_approved_silver(registry, tmp_path / "data", DATASET, APPROVED)
    assert result.manifest == manifest
    assert result.run_id == str(APPROVED)
    with pytest.raises(ValueError, match="not explicitly approved"):
        resolve_approved_silver(registry, tmp_path / "data", DATASET, UNREGISTERED)


def test_bounded_validation_authority_is_explicit_and_not_approved(tmp_path: Path) -> None:
    registry, manifest, _ = registry_fixture(tmp_path)
    registry.write_text(
        registry.read_text(encoding="utf-8").replace(
            'status = "approved"', 'status = "authorized_bounded_validation"'
        ),
        encoding="utf-8",
    )
    result = resolve_bounded_validation_silver(registry, tmp_path / "data", DATASET, APPROVED)
    assert result.manifest == manifest
    assert result.authority_status == "authorized_bounded_validation"
    with pytest.raises(ValueError, match="not explicitly approved"):
        resolve_approved_silver(registry, tmp_path / "data", DATASET, APPROVED)


def test_rejects_superseded_run_before_filesystem_selection(tmp_path: Path) -> None:
    registry, _, _ = registry_fixture(tmp_path)
    with pytest.raises(ValueError, match="requested run is superseded"):
        resolve_approved_silver(registry, tmp_path / "data", DATASET, SUPERSEDED)


def test_rejects_manifest_artifact_and_request_tampering(tmp_path: Path) -> None:
    registry, _, artifact = registry_fixture(tmp_path)
    artifact.write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="artifact verification failed"):
        resolve_approved_silver(registry, tmp_path / "data", DATASET, APPROVED)

    registry, manifest, _ = registry_fixture(tmp_path / "second")
    manifest.write_text(manifest.read_text(encoding="utf-8") + " ", encoding="utf-8")
    with pytest.raises(ValueError, match="manifest fingerprint mismatch"):
        resolve_approved_silver(registry, tmp_path / "second/data", DATASET, APPROVED)

    registry, _, _ = registry_fixture(tmp_path / "third")
    (tmp_path / "third/config/silver.toml").write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="request fingerprint mismatch"):
        resolve_approved_silver(registry, tmp_path / "third/data", DATASET, APPROVED)


def test_rejects_failed_or_mismatched_authority_metadata(tmp_path: Path) -> None:
    registry, manifest, _ = registry_fixture(tmp_path)
    old_hash = sha256(manifest)
    document = json.loads(manifest.read_text(encoding="utf-8"))
    document["status"] = "failed"
    manifest.write_text(json.dumps(document, sort_keys=True), encoding="utf-8")
    text = registry.read_text(encoding="utf-8").replace(
        f'manifest_sha256 = "{old_hash}"',
        f'manifest_sha256 = "{sha256(manifest)}"',
        1,
    )
    registry.write_text(text, encoding="utf-8")
    with pytest.raises(ValueError, match="approved manifest metadata mismatch"):
        resolve_approved_silver(registry, tmp_path / "data", DATASET, APPROVED)


def test_v11_resolves_multiple_authorities_and_preserves_v10_compatibility(
    tmp_path: Path,
) -> None:
    registry, first_manifest, _ = registry_fixture(tmp_path)
    assert resolve_approved_silver(registry, tmp_path / "data", DATASET, APPROVED)
    second_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaab")
    second_dataset = "silver-registry-second-v1"
    second_run = tmp_path / "data/silver" / f"dataset={second_dataset}" / f"run={second_id}"
    second_run.mkdir(parents=True)
    second_artifact = second_run / "run.json"
    second_artifact.write_text('{"fixture":2}', encoding="utf-8")
    document = json.loads(first_manifest.read_text(encoding="utf-8"))
    document.update({"dataset_version": second_dataset, "run_id": str(second_id)})
    document["artifacts"] = [
        {
            "path": "run.json",
            "sha256": sha256(second_artifact),
            "bytes": second_artifact.stat().st_size,
        }
    ]
    second_manifest = second_run / "manifest.json"
    second_manifest.write_text(json.dumps(document, sort_keys=True), encoding="utf-8")
    request = tmp_path / "config/silver.toml"
    lineage = (
        'bronze_inputs = [{ dataset_version = "bronze-fixture-v1", '
        'acquisition_id = "30000000-0000-0000-0000-000000000001", '
        f'manifest_sha256 = "{"3" * 64}" }}]'
    )

    def authority(dataset: str, run_id: UUID, manifest: Path) -> str:
        return f'''[[authority]]
dataset_version = "{dataset}"
run_id = "{run_id}"
status = "approved"
manifest = "../data/silver/dataset={dataset}/run={run_id}/manifest.json"
manifest_sha256 = "{sha256(manifest)}"
processing_source_sha256 = "{SOURCE_HASH}"
request = "silver.toml"
request_sha256 = "{sha256(request)}"
silver_schema_version = "1.0.0"
resolution_policy_version = "silver-resolution-v1"
source_contract_version = "alpaca-iex-1Min-raw-v1"
{lineage}
approved_at = "2026-11-06T00:00:00Z"
decision = "fixture"
'''

    registry.write_text(
        f'registry_version = "1.1.0"\nsuperseded = []\n\n'
        f"{authority(DATASET, APPROVED, first_manifest)}\n"
        f"{authority(second_dataset, second_id, second_manifest)}\n",
        encoding="utf-8",
    )
    assert resolve_approved_silver(registry, tmp_path / "data", DATASET, APPROVED)
    assert resolve_approved_silver(registry, tmp_path / "data", second_dataset, second_id)

    duplicate = registry.read_text(encoding="utf-8").replace(
        authority(second_dataset, second_id, second_manifest),
        authority(second_dataset, second_id, second_manifest)
        + authority(DATASET, APPROVED, first_manifest),
    )
    registry.write_text(duplicate, encoding="utf-8")
    with pytest.raises(ValueError, match="inconsistent publication states"):
        resolve_approved_silver(registry, tmp_path / "data", DATASET, APPROVED)


def test_v11_rejects_incorrect_bronze_lineage_and_contract(tmp_path: Path) -> None:
    registry, manifest, _ = registry_fixture(tmp_path)
    request = tmp_path / "config/silver.toml"
    incorrect_lineage = (
        'bronze_inputs = [{ dataset_version = "wrong", '
        'acquisition_id = "30000000-0000-0000-0000-000000000001", '
        f'manifest_sha256 = "{"3" * 64}" }}]'
    )
    registry.write_text(
        f'''registry_version = "1.1.0"
superseded = []

[[authority]]
dataset_version = "{DATASET}"
run_id = "{APPROVED}"
status = "approved"
manifest = "../data/silver/dataset={DATASET}/run={APPROVED}/manifest.json"
manifest_sha256 = "{sha256(manifest)}"
processing_source_sha256 = "{SOURCE_HASH}"
request = "silver.toml"
request_sha256 = "{sha256(request)}"
silver_schema_version = "1.0.0"
resolution_policy_version = "silver-resolution-v1"
source_contract_version = "alpaca-iex-1Min-raw-v1"
{incorrect_lineage}
approved_at = "2026-11-06T00:00:00Z"
decision = "fixture"
''',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="approved lineage or contract mismatch"):
        resolve_approved_silver(registry, tmp_path / "data", DATASET, APPROVED)
