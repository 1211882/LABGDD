from dataclasses import asdict, replace
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from labgdd.adapters.gold import GoldRun
from labgdd.config import ConfigurationError
from labgdd.sector_publication_config import (
    SectorPublicationRequest,
    load_sector_publication_request,
)


def request() -> SectorPublicationRequest:
    return SectorPublicationRequest(
        "sector-synthetic-test-v1",
        Path("gold.toml"),
        Path("silver.toml"),
        Path("research.toml"),
        "synthetic-gold-v1",
        UUID(int=1),
        "a" * 64,
    )


@pytest.mark.parametrize(
    ("key", "value"),
    (
        ("dataset_version", "real-sector-v1"),
        ("input_manifest_sha256", "x" * 64),
        ("eligibility_policy", "unknown"),
        ("evidence_kind", "real"),
        ("sector_schema_version", "2.0.0"),
    ),
)
def test_sector_request_rejects_unsupported_values(key: str, value: str) -> None:
    with pytest.raises(ConfigurationError):
        changes: dict[str, Any] = {key: value}
        replace(request(), **changes)


def test_sector_configuration_requires_exact_keys_and_safe_paths(tmp_path: Path) -> None:
    config = tmp_path / "config"
    config.mkdir()
    path = config / "sector.toml"
    original = request()
    text = "\n".join(f'{key} = "{value}"' for key, value in asdict(original).items())
    path.write_text(text)
    assert load_sector_publication_request(path).input_run_id == original.input_run_id
    for invalid in (
        text + '\nunknown = "value"',
        text + '\nthresholds = "unapproved"',
        text.replace("gold.toml", "../../outside.toml"),
        text.replace("gold.toml", "C:/outside.toml"),
    ):
        path.write_text(invalid)
        with pytest.raises(ConfigurationError):
            load_sector_publication_request(path)


def test_sector_storage_isolated_immutable_and_failed_runs_cannot_complete(tmp_path: Path) -> None:
    identity = uuid4()
    storage = GoldRun(tmp_path, "sector-synthetic-test", identity, {}, sector_outputs=True)
    (storage.path / "windows").mkdir()
    (storage.path / "windows/part.parquet").write_bytes(b"synthetic-storage-fixture")
    with pytest.raises(ValueError):
        storage.finish("complete", {})
    manifest = storage.finish("failed", {})
    assert "sector_gold" in manifest.parts and "_failed" in manifest.parts
    with pytest.raises(FileExistsError):
        GoldRun(tmp_path, "sector-synthetic-test", identity, {}, sector_outputs=True)
    assert not (tmp_path / "gold").exists()


def test_sector_terminal_rename_failure_retains_unpublished_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    storage = GoldRun(tmp_path, "sector-synthetic-atomic", uuid4(), {}, sector_outputs=True)
    for name in ("windows", "correlations"):
        (storage.path / name).mkdir()
        (storage.path / name / "part.parquet").write_bytes(b"synthetic-storage-fixture")
    rename = os.rename

    def fail_once(source: Any, target: Any) -> None:
        monkeypatch.setattr(os, "rename", rename)
        raise OSError("injected same-filesystem rename failure")

    monkeypatch.setattr(os, "rename", fail_once)
    with pytest.raises(OSError, match="injected"):
        storage.finish("complete", {})
    assert not storage.completed_path.exists()
    assert not (storage.path / "manifest.json").exists()
    failed = storage.finish("failed", {})
    assert "_failed" in failed.parts


def test_output_uuid_is_validated_before_any_publication_work() -> None:
    from typing import cast

    from labgdd.processing.sector_publication import publish_synthetic_sector_gold

    with pytest.raises(ValueError):
        publish_synthetic_sector_gold(
            cast(Any, None), cast(Any, None), request(), run_id=cast(Any, "not-a-uuid")
        )
