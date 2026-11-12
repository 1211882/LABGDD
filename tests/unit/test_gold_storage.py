import json
import os
from collections.abc import Mapping
from pathlib import Path
from uuid import UUID

import pytest

from labgdd.adapters.bronze import sha256
from labgdd.adapters.gold import GoldRun


def test_gold_run_is_immutable_and_checksummed(tmp_path: Path) -> None:
    identity = UUID("30000000-0000-0000-0000-000000000001")
    run = GoldRun(tmp_path, "gold-fixture-v1", identity, {"run_id": str(identity)})
    for name in ("returns", "rolling", "fixed"):
        artifact = run.path / name
        artifact.mkdir()
        (artifact / "part.parquet").write_bytes(name.encode())
    manifest = run.finish("complete", {"records": 1})
    document = json.loads(manifest.read_text(encoding="utf-8"))
    assert document["status"] == "complete"
    entry = next(item for item in document["artifacts"] if item["path"].endswith("part.parquet"))
    assert entry["sha256"] == sha256(manifest.parent / entry["path"])
    with pytest.raises(FileExistsError):
        GoldRun(tmp_path, "gold-fixture-v1", identity, {"run_id": str(identity)})


def test_gold_failed_run_cannot_appear_complete(tmp_path: Path) -> None:
    identity = UUID("30000000-0000-0000-0000-000000000002")
    run = GoldRun(tmp_path, "gold-fixture-v1", identity, {"run_id": str(identity)})
    manifest = run.finish("failed", {"failure_stage": "fixture"})
    assert "_failed" in manifest.parts
    assert json.loads(manifest.read_text(encoding="utf-8"))["status"] == "failed"
    assert not run.completed_path.exists()


def test_partial_output_cannot_publish_complete(tmp_path: Path) -> None:
    identity = UUID("30000000-0000-0000-0000-000000000003")
    run = GoldRun(tmp_path, "gold-fixture-v1", identity, {"run_id": str(identity)})
    (run.path / "returns").mkdir()
    (run.path / "returns/part.parquet").write_bytes(b"partial")
    with pytest.raises(ValueError, match="incomplete output inventory"):
        run.finish("complete", {"records": 1})
    assert not (run.path / "manifest.json").exists()


def test_manifest_creation_failure_is_not_terminal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identity = UUID("30000000-0000-0000-0000-000000000004")
    run = GoldRun(tmp_path, "gold-fixture-v1", identity, {"run_id": str(identity)})
    for name in ("returns", "rolling", "fixed"):
        (run.path / name).mkdir()
        (run.path / name / "part.parquet").write_bytes(name.encode())
    original = GoldRun._write_json

    def fail_manifest(self: GoldRun, name: str, value: Mapping[str, object]) -> None:
        if name == "manifest.pending":
            raise OSError("synthetic manifest creation failure")
        original(self, name, value)

    monkeypatch.setattr(GoldRun, "_write_json", fail_manifest)
    with pytest.raises(OSError, match="synthetic manifest"):
        run.finish("complete", {})
    assert not (run.path / "manifest.json").exists()
    assert not run.completed_path.exists()


def test_completion_rename_failure_is_not_terminal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identity = UUID("30000000-0000-0000-0000-000000000005")
    run = GoldRun(tmp_path, "gold-fixture-v1", identity, {"run_id": str(identity)})
    for name in ("returns", "rolling", "fixed"):
        (run.path / name).mkdir()
        (run.path / name / "part.parquet").write_bytes(name.encode())

    def fail_rename(source: object, target: object) -> None:
        raise OSError("synthetic completion rename failure")

    monkeypatch.setattr(os, "rename", fail_rename)
    with pytest.raises(OSError, match="synthetic completion"):
        run.finish("complete", {})
    assert not (run.path / "manifest.json").exists()
    assert not run.completed_path.exists()
