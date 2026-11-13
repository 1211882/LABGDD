import json
import os
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

import labgdd.publication_governance as governance
from labgdd.adapters.bronze import sha256
from labgdd.publication_governance import approve_reviewed_step, review_publication_plan

BRONZE_ID = UUID("30000000-0000-0000-0000-000000000001")
SILVER_ID = UUID("50000000-0000-0000-0000-000000000001")
GOLD_ID = UUID("40000000-0000-0000-0000-000000000001")
EXPECTED_ID = UUID("40000000-0000-0000-0000-000000000002")
BRONZE_HASH = "a" * 64
SILVER_HASH = "b" * 64


def proposal_fixture(root: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    config = root / "config"
    config.mkdir(parents=True)
    bronze = config / "bronze.proposed.toml"
    bronze.write_text(
        f'''registry_version = "1.0.0"
superseded = []
[[authority]]
dataset_version = "bronze-v1"
acquisition_id = "{BRONZE_ID}"
status = "approved"
''',
        encoding="utf-8",
    )
    silver = config / "silver.proposed.toml"
    silver.write_text(
        f'''registry_version = "1.1.0"
superseded = []
[[authority]]
dataset_version = "silver-v1"
run_id = "{SILVER_ID}"
status = "approved"
[[authority.bronze_inputs]]
dataset_version = "bronze-v1"
acquisition_id = "{BRONZE_ID}"
manifest_sha256 = "{BRONZE_HASH}"
''',
        encoding="utf-8",
    )
    individual = config / "gold-individual.proposed.toml"
    individual.write_text(
        f'''registry_version = "1.1.0"
superseded = []
[[authority]]
dataset_version = "gold-v1"
run_id = "{GOLD_ID}"
status = "approved"
silver_dataset_version = "silver-v1"
silver_run_id = "{SILVER_ID}"
silver_manifest_sha256 = "{SILVER_HASH}"
''',
        encoding="utf-8",
    )
    expected = config / "gold-expected.proposed.toml"
    expected.write_text(
        individual.read_text(encoding="utf-8")
        + f'''[[authority]]
dataset_version = "expected-v1"
run_id = "{EXPECTED_ID}"
status = "approved"
silver_dataset_version = "silver-v1"
silver_run_id = "{SILVER_ID}"
silver_manifest_sha256 = "{SILVER_HASH}"
''',
        encoding="utf-8",
    )
    plan = config / "plan.toml"
    plan.write_text(
        f'''plan_version = "1.0.0"
proposal_id = "fixture"
status = "pending_explicit_authorization"

[[step]]
name = "bronze"
layer = "bronze"
dataset_version = "bronze-v1"
identity = "{BRONZE_ID}"
active_registry = "bronze.toml"
proposed_registry = "bronze.proposed.toml"
expected_active_sha256 = "absent"
proposed_sha256 = "{sha256(bronze)}"
depends_on = []

[[step]]
name = "silver"
layer = "silver"
dataset_version = "silver-v1"
identity = "{SILVER_ID}"
active_registry = "silver.toml"
proposed_registry = "silver.proposed.toml"
expected_active_sha256 = "absent"
proposed_sha256 = "{sha256(silver)}"
depends_on = ["bronze"]

[[step]]
name = "gold"
layer = "gold"
dataset_version = "gold-v1"
identity = "{GOLD_ID}"
active_registry = "gold.toml"
proposed_registry = "gold-individual.proposed.toml"
expected_active_sha256 = "absent"
proposed_sha256 = "{sha256(individual)}"
depends_on = ["silver"]

[[step]]
name = "expected"
layer = "gold"
dataset_version = "expected-v1"
identity = "{EXPECTED_ID}"
active_registry = "gold.toml"
proposed_registry = "gold-expected.proposed.toml"
expected_active_sha256 = "{sha256(individual)}"
proposed_sha256 = "{sha256(expected)}"
depends_on = ["gold"]
''',
        encoding="utf-8",
    )

    def bronze_resolver(
        registry: Path, data: Path, dataset: str, identity: UUID
    ) -> SimpleNamespace:
        del data
        if not registry.is_file():
            raise ValueError("bronze registry: unreadable registry")
        return SimpleNamespace(
            metadata={"dataset_version": dataset, "acquisition_id": str(identity)},
            manifest_sha256=BRONZE_HASH,
        )

    def silver_resolver(
        registry: Path, data: Path, dataset: str, identity: UUID
    ) -> SimpleNamespace:
        del registry, data
        return SimpleNamespace(
            metadata={
                "dataset_version": dataset,
                "run_id": str(identity),
                "input_manifests": [
                    {
                        "dataset_version": "bronze-v1",
                        "acquisition_id": str(BRONZE_ID),
                        "sha256": BRONZE_HASH,
                    }
                ],
            },
            manifest_sha256=SILVER_HASH,
        )

    def gold_resolver(registry: Path, data: Path, dataset: str, identity: UUID) -> SimpleNamespace:
        del registry, data
        return SimpleNamespace(
            metadata={
                "dataset_version": dataset,
                "run_id": str(identity),
                "silver_dataset_version": "silver-v1",
                "silver_run_id": str(SILVER_ID),
                "silver_manifest_sha256": SILVER_HASH,
            }
        )

    monkeypatch.setattr(governance, "resolve_approved_bronze", bronze_resolver)
    monkeypatch.setattr(governance, "resolve_approved_silver", silver_resolver)
    monkeypatch.setattr(governance, "resolve_approved_gold", gold_resolver)
    return plan, config


def test_review_is_structured_and_does_not_mutate_registries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, config = proposal_fixture(tmp_path, monkeypatch)
    report = tmp_path / "review.json"
    review_publication_plan(plan, tmp_path, tmp_path / "data", report)
    document = json.loads(report.read_text(encoding="utf-8"))
    assert document["status"] == "review_passed_pending_explicit_authorization"
    assert len(document["steps"]) == 4
    assert not (config / "bronze.toml").exists()
    assert not (config / "silver.toml").exists()
    assert not (config / "gold.toml").exists()
    with pytest.raises(FileExistsError):
        review_publication_plan(plan, tmp_path, tmp_path / "data", report)


def test_approval_requires_exact_reviewed_identity_and_updates_atomically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, config = proposal_fixture(tmp_path, monkeypatch)
    report = tmp_path / "review.json"
    review_publication_plan(plan, tmp_path, tmp_path / "data", report)
    with pytest.raises(ValueError, match="explicit target mismatch"):
        approve_reviewed_step(
            plan,
            tmp_path,
            tmp_path / "data",
            report,
            step_name="bronze",
            dataset_version="wrong",
            identity=BRONZE_ID,
        )
    active = approve_reviewed_step(
        plan,
        tmp_path,
        tmp_path / "data",
        report,
        step_name="bronze",
        dataset_version="bronze-v1",
        identity=BRONZE_ID,
    )
    assert active == config / "bronze.toml"
    assert sha256(active) == sha256(config / "bronze.proposed.toml")
    assert (
        approve_reviewed_step(
            plan,
            tmp_path,
            tmp_path / "data",
            report,
            step_name="bronze",
            dataset_version="bronze-v1",
            identity=BRONZE_ID,
        )
        == active
    )


def test_approval_rejects_pending_upstream_and_recovers_replace_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, config = proposal_fixture(tmp_path, monkeypatch)
    report = tmp_path / "review.json"
    review_publication_plan(plan, tmp_path, tmp_path / "data", report)
    with pytest.raises(ValueError, match="unreadable registry"):
        approve_reviewed_step(
            plan,
            tmp_path,
            tmp_path / "data",
            report,
            step_name="silver",
            dataset_version="silver-v1",
            identity=SILVER_ID,
        )

    def fail_replace(source: Path, destination: Path) -> None:
        del source, destination
        raise OSError("fixture replacement failure")

    monkeypatch.setattr(os, "replace", fail_replace)
    with pytest.raises(OSError, match="replacement failure"):
        approve_reviewed_step(
            plan,
            tmp_path,
            tmp_path / "data",
            report,
            step_name="bronze",
            dataset_version="bronze-v1",
            identity=BRONZE_ID,
        )
    assert not (config / "bronze.toml").exists()
    assert not list(config.glob("*.pending"))


def test_review_rejects_changed_proposal_and_incorrect_cross_layer_lineage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan, config = proposal_fixture(tmp_path, monkeypatch)
    (config / "silver.proposed.toml").write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="proposed registry fingerprint mismatch"):
        review_publication_plan(plan, tmp_path, tmp_path / "data", tmp_path / "review.json")

    plan, _ = proposal_fixture(tmp_path / "lineage", monkeypatch)

    def wrong_bronze(registry: Path, data: Path, dataset: str, identity: UUID) -> SimpleNamespace:
        del registry, data
        return SimpleNamespace(
            metadata={"dataset_version": dataset, "acquisition_id": str(identity)},
            manifest_sha256="f" * 64,
        )

    monkeypatch.setattr(governance, "resolve_approved_bronze", wrong_bronze)
    with pytest.raises(ValueError, match="incorrect Bronze lineage"):
        review_publication_plan(
            plan,
            tmp_path / "lineage",
            tmp_path / "lineage/data",
            tmp_path / "lineage/review.json",
        )
