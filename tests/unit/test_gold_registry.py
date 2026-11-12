import json
from pathlib import Path
from uuid import UUID

import pytest

from labgdd.adapters.bronze import sha256
from labgdd.adapters.gold import GoldRun, resolve_approved_gold

DATASET = "gold-fixture-v1"
APPROVED = UUID("40000000-0000-0000-0000-000000000001")
SUPERSEDED = UUID("40000000-0000-0000-0000-000000000002")
SILVER_RUN = "50000000-0000-0000-0000-000000000001"
SILVER_HASH = "a" * 64
SOURCE_HASH = "b" * 64
CONFIG_HASH = "c" * 64
EXPECTED_DATASET = "gold-expected-fixture-v1"
EXPECTED_RUN = UUID("40000000-0000-0000-0000-000000000003")
PLAN_HASH = "e" * 64
RESEARCH_HASH = "f" * 64


def completed_run(root: Path) -> Path:
    metadata: dict[str, object] = {
        "manifest_version": "1.0.0",
        "gold_schema_version": "1.0.0",
        "measurement_version": "2.0.0",
        "dataset_version": DATASET,
        "run_id": str(APPROVED),
        "silver_dataset_version": "silver-fixture-v1",
        "silver_run_id": SILVER_RUN,
        "silver_manifest_sha256": SILVER_HASH,
        "processing_source_sha256": SOURCE_HASH,
        "configuration_sha256": CONFIG_HASH,
    }
    run = GoldRun(root, DATASET, APPROVED, metadata)
    for name in ("returns", "rolling", "fixed"):
        (run.path / name).mkdir()
        (run.path / name / "part.parquet").write_bytes(name.encode())
    return run.finish("complete", {})


def registry(root: Path, manifest: Path, *, authority_status: str = "approved") -> Path:
    config = root / "config"
    config.mkdir(exist_ok=True)
    path = config / "gold_publications.toml"
    relative = manifest.relative_to(root).as_posix()
    path.write_text(
        f'''registry_version = "1.0.0"

[authority]
dataset_version = "{DATASET}"
run_id = "{APPROVED}"
status = "{authority_status}"
manifest = "../{relative}"
manifest_sha256 = "{sha256(manifest)}"
measurement_version = "2.0.0"
silver_dataset_version = "silver-fixture-v1"
silver_run_id = "{SILVER_RUN}"
silver_manifest_sha256 = "{SILVER_HASH}"
processing_source_sha256 = "{SOURCE_HASH}"
configuration_sha256 = "{CONFIG_HASH}"
approved_at = "2026-11-03T00:00:00Z"
decision = "fixture"

[[superseded]]
run_id = "{SUPERSEDED}"
status = "superseded"
manifest_sha256 = "{"d" * 64}"
reason = "fixture"
''',
        encoding="utf-8",
    )
    return path


def completed_expected_run(root: Path) -> Path:
    metadata: dict[str, object] = {
        "manifest_version": "1.0.0",
        "gold_schema_version": "2.0.0",
        "measurement_version": "2.0.0",
        "dataset_version": EXPECTED_DATASET,
        "run_id": str(EXPECTED_RUN),
        "publication_status": "pending_architecture_review",
        "silver_dataset_version": "silver-fixture-v1",
        "silver_run_id": SILVER_RUN,
        "silver_manifest_sha256": SILVER_HASH,
        "processing_source_sha256": SOURCE_HASH,
        "configuration_sha256": CONFIG_HASH,
        "approved_expected_plan_sha256": PLAN_HASH,
        "research_reference_sha256": RESEARCH_HASH,
    }
    run = GoldRun(root, EXPECTED_DATASET, EXPECTED_RUN, metadata)
    for name in ("returns", "rolling", "fixed"):
        (run.path / name).mkdir()
        (run.path / name / "part.parquet").write_bytes(name.encode())
    return run.finish("complete", {})


def multi_registry(root: Path, legacy: Path, expected: Path) -> Path:
    config = root / "config"
    config.mkdir(exist_ok=True)
    path = config / "gold_publications.toml"
    legacy_relative = legacy.relative_to(root).as_posix()
    expected_relative = expected.relative_to(root).as_posix()
    expected_fingerprints = (
        "contract_fingerprints = { "
        f'approved_expected_plan_sha256 = "{PLAN_HASH}", '
        f'research_reference_sha256 = "{RESEARCH_HASH}" }}'
    )
    path.write_text(
        f'''registry_version = "1.1.0"

[[authority]]
dataset_version = "{DATASET}"
run_id = "{APPROVED}"
status = "approved"
manifest = "../{legacy_relative}"
manifest_sha256 = "{sha256(legacy)}"
gold_schema_version = "1.0.0"
measurement_version = "2.0.0"
silver_dataset_version = "silver-fixture-v1"
silver_run_id = "{SILVER_RUN}"
silver_manifest_sha256 = "{SILVER_HASH}"
processing_source_sha256 = "{SOURCE_HASH}"
configuration_sha256 = "{CONFIG_HASH}"
manifest_publication_status = "not_recorded"
contract_fingerprints = {{}}
approved_at = "2026-11-03T00:00:00Z"
decision = "legacy_fixture"

[[authority]]
dataset_version = "{EXPECTED_DATASET}"
run_id = "{EXPECTED_RUN}"
status = "approved"
manifest = "../{expected_relative}"
manifest_sha256 = "{sha256(expected)}"
gold_schema_version = "2.0.0"
measurement_version = "2.0.0"
silver_dataset_version = "silver-fixture-v1"
silver_run_id = "{SILVER_RUN}"
silver_manifest_sha256 = "{SILVER_HASH}"
processing_source_sha256 = "{SOURCE_HASH}"
configuration_sha256 = "{CONFIG_HASH}"
manifest_publication_status = "pending_architecture_review"
{expected_fingerprints}
approved_at = "2026-11-05T00:00:00Z"
decision = "expected_fixture"

[[superseded]]
run_id = "{SUPERSEDED}"
status = "superseded"
manifest_sha256 = "{"d" * 64}"
reason = "fixture"
''',
        encoding="utf-8",
    )
    return path


def test_approved_run_resolves_only_by_explicit_identity(tmp_path: Path) -> None:
    manifest = completed_run(tmp_path)
    approved = resolve_approved_gold(registry(tmp_path, manifest), tmp_path, DATASET, APPROVED)
    assert approved.manifest == manifest
    assert approved.manifest_sha256 == sha256(manifest)
    with pytest.raises(ValueError, match="not explicitly approved"):
        resolve_approved_gold(
            tmp_path / "config/gold_publications.toml",
            tmp_path,
            DATASET,
            UUID("40000000-0000-0000-0000-000000000099"),
        )


def test_superseded_and_failed_authority_are_rejected(tmp_path: Path) -> None:
    manifest = completed_run(tmp_path)
    path = registry(tmp_path, manifest)
    with pytest.raises(ValueError, match="requested run is superseded"):
        resolve_approved_gold(path, tmp_path, DATASET, SUPERSEDED)
    registry(tmp_path, manifest, authority_status="failed")
    with pytest.raises(ValueError, match="invalid authority fingerprints or status"):
        resolve_approved_gold(path, tmp_path, DATASET, APPROVED)


def test_artifact_tampering_and_unexpected_inventory_are_rejected(tmp_path: Path) -> None:
    manifest = completed_run(tmp_path)
    path = registry(tmp_path, manifest)
    artifact = manifest.parent / "returns/part.parquet"
    artifact.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="artifact verification failed"):
        resolve_approved_gold(path, tmp_path, DATASET, APPROVED)

    artifact.write_bytes(b"returns")
    (manifest.parent / "unregistered.bin").write_bytes(b"extra")
    with pytest.raises(ValueError, match="artifact inventory mismatch"):
        resolve_approved_gold(path, tmp_path, DATASET, APPROVED)


def test_manifest_metadata_tampering_is_rejected(tmp_path: Path) -> None:
    manifest = completed_run(tmp_path)
    path = registry(tmp_path, manifest)
    document = json.loads(manifest.read_text(encoding="utf-8"))
    document["measurement_version"] = "9.9.9"
    manifest.write_text(json.dumps(document, sort_keys=True), encoding="utf-8")
    with pytest.raises(ValueError, match="manifest fingerprint mismatch"):
        resolve_approved_gold(path, tmp_path, DATASET, APPROVED)


def test_v11_registry_resolves_multiple_explicit_schema_authorities(tmp_path: Path) -> None:
    legacy = completed_run(tmp_path)
    expected = completed_expected_run(tmp_path)
    path = multi_registry(tmp_path, legacy, expected)
    first = resolve_approved_gold(path, tmp_path, DATASET, APPROVED)
    second = resolve_approved_gold(path, tmp_path, EXPECTED_DATASET, EXPECTED_RUN)
    assert first.manifest == legacy
    assert first.metadata["gold_schema_version"] == "1.0.0"
    assert second.manifest == expected
    assert second.metadata["gold_schema_version"] == "2.0.0"
    assert second.metadata["publication_status"] == "pending_architecture_review"


def test_v11_registry_rejects_schema_and_contract_fingerprint_mismatch(tmp_path: Path) -> None:
    legacy = completed_run(tmp_path)
    expected = completed_expected_run(tmp_path)
    path = multi_registry(tmp_path, legacy, expected)
    original = path.read_text(encoding="utf-8")
    invalid_plan = original.replace(
        f'approved_expected_plan_sha256 = "{PLAN_HASH}"',
        f'approved_expected_plan_sha256 = "{"0" * 64}"',
    )
    path.write_text(invalid_plan, encoding="utf-8")
    with pytest.raises(ValueError, match="approved manifest metadata mismatch"):
        resolve_approved_gold(path, tmp_path, EXPECTED_DATASET, EXPECTED_RUN)

    invalid_schema = original.replace(
        'gold_schema_version = "2.0.0"', 'gold_schema_version = "9.9.9"'
    )
    path.write_text(invalid_schema, encoding="utf-8")
    with pytest.raises(ValueError, match="approved manifest metadata mismatch"):
        resolve_approved_gold(path, tmp_path, EXPECTED_DATASET, EXPECTED_RUN)
