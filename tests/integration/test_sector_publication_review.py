"""Adversarial final review of synthetic candidates; no active registry writes."""

import json
import os
import shutil
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from labgdd.adapters.bronze import sha256
from labgdd.adapters.gold import GoldRun
from labgdd.processing import sector_publication
from labgdd.processing.sector_publication import (
    _read_outputs,
    _validate_frames,
    inspect_sector_candidate,
    publish_synthetic_sector_gold,
)
from tests.integration import test_sector_publication as base

spark = base.spark
fixture_root = base.fixture_root
candidate = base.candidate
pytestmark = pytest.mark.skipif(
    os.name == "nt", reason="native Sector publication review runs on Linux"
)


def _copy(root: Path, candidate_path: Path, target: Path) -> tuple[Path, Path]:
    copy = target / "synthetic-copy"
    shutil.copytree(root, copy)
    return copy, copy / candidate_path.relative_to(root)


def _reseal(manifest: Path, doc: dict[str, Any], *, creation: bool = False) -> None:
    if creation:
        run_path = manifest.parent / "run.json"
        run = json.loads(run_path.read_bytes())
        run.update({key: doc[key] for key in run})
        run_path.write_text(json.dumps(run), encoding="utf-8")
    for path in manifest.parent.rglob("*.crc"):
        path.unlink()
    doc["artifacts"] = [
        {
            "path": p.relative_to(manifest.parent).as_posix(),
            "bytes": p.stat().st_size,
            "sha256": sha256(p),
        }
        for p in sorted(manifest.parent.rglob("*"))
        if p.is_file() and p != manifest
    ]
    manifest.write_text(json.dumps(doc), encoding="utf-8")


@pytest.mark.parametrize(
    "failure",
    (
        "missing",
        "unexpected",
        "same_size",
        "manifest_digest",
        "replaced_after_check",
        "source_manifest",
    ),
)
def test_inspection_rejects_physical_corruption(
    fixture_root: Path,
    candidate: Path,
    tmp_path: Path,
    spark: Any,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    root, manifest = _copy(fixture_root, candidate, tmp_path)
    digest = sha256(manifest)
    path = next((manifest.parent / "windows").glob("*.parquet"))
    if failure == "missing":
        path.unlink()
    elif failure == "unexpected":
        (manifest.parent / "windows/unexpected.txt").write_text("synthetic extra file")
    elif failure == "manifest_digest":
        digest = "f" * 64
    elif failure == "source_manifest":
        request = base._request(root)
        source = (
            root
            / "data/gold"
            / f"dataset={request.input_dataset_version}/run={request.input_run_id}/manifest.json"
        )
        source.write_text("{}", encoding="utf-8")
    elif failure == "same_size":
        raw = bytearray(path.read_bytes())
        raw[len(raw) // 2] ^= 1
        path.write_bytes(raw)
    else:
        verify = sector_publication._verify_inventory
        calls = 0

        def replace_after_check(*args: Any, **kwargs: Any) -> None:
            nonlocal calls
            verify(*args, **kwargs)
            calls += 1
            if calls == 1:
                replacement = path.with_suffix(".replacement")
                replacement.write_bytes(b"replaced synthetic Parquet")
                replacement.replace(path)

        monkeypatch.setattr(sector_publication, "_verify_inventory", replace_after_check)
    with pytest.raises(ValueError):
        inspect_sector_candidate(spark, base._settings(root), base._request(root), manifest, digest)


@pytest.mark.parametrize("failure", ("int96", "naive_timestamp", "wrong_type"))
def test_inspection_rejects_resealed_physical_types(
    fixture_root: Path, candidate: Path, tmp_path: Path, spark: Any, failure: str
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    root, manifest = _copy(fixture_root, candidate, tmp_path)
    doc = json.loads(manifest.read_bytes())
    path = next((manifest.parent / "windows").glob("*.parquet"))
    table = pq.ParquetFile(path).read()
    if failure == "naive_timestamp":
        index = table.schema.get_field_index("window_start")
        table = table.set_column(
            index, "window_start", table["window_start"].cast(pa.timestamp("us"))
        )
    elif failure == "wrong_type":
        index = table.schema.get_field_index("horizon")
        table = table.set_column(index, "horizon", table["horizon"].cast(pa.float64()))
    pq.write_table(table, path, use_deprecated_int96_timestamps=failure == "int96")
    _reseal(manifest, doc)
    with pytest.raises(ValueError, match="UTC microseconds|physical type"):
        inspect_sector_candidate(
            spark, base._settings(root), base._request(root), manifest, sha256(manifest)
        )


@pytest.mark.parametrize("failure", ("coverage", "feed", "source_fingerprint"))
def test_inspection_rejects_resealed_manifest_claims(
    fixture_root: Path, candidate: Path, tmp_path: Path, spark: Any, failure: str
) -> None:
    root, manifest = _copy(fixture_root, candidate, tmp_path)
    doc = json.loads(manifest.read_bytes())
    if failure == "coverage":
        doc["coverage"][0]["observed_slots"] += 1
    elif failure == "feed":
        doc["feed"] = "sip"
    else:
        doc["processing_source_sha256"] = "f" * 64
    _reseal(manifest, doc, creation=True)
    with pytest.raises(ValueError, match="coverage evidence|manifest contract|input authority"):
        inspect_sector_candidate(
            spark, base._settings(root), base._request(root), manifest, sha256(manifest)
        )


@pytest.mark.parametrize(
    "failure",
    (
        "duplicate_pair",
        "pair_count",
        "window_fingerprint",
        "pair_fingerprint",
        "reversed_pair",
        "input_dataset",
        "input_run",
        "input_manifest",
    ),
)
def test_output_identity_and_pair_contracts(candidate: Path, spark: Any, failure: str) -> None:
    from pyspark.sql import functions as F

    windows, pairs = _read_outputs(spark, candidate.parent)
    if failure == "duplicate_pair":
        pairs = pairs.unionByName(pairs.limit(1))
    elif failure == "pair_count":
        windows = windows.withColumn("expected_pairs", F.col("expected_pairs") + 1)
    elif failure == "window_fingerprint":
        windows = windows.withColumn("approved_input_identity_sha256", F.lit("f" * 64))
    elif failure == "reversed_pair":
        pairs = (
            pairs.withColumn("old_left", F.col("left_symbol"))
            .withColumn("left_symbol", F.col("right_symbol"))
            .withColumn("right_symbol", F.col("old_left"))
            .drop("old_left")
        )
    elif failure in {"input_dataset", "input_run", "input_manifest"}:
        key, value = {
            "input_dataset": ("input_gold_dataset_version", "different-dataset"),
            "input_run": ("input_gold_run_id", str(uuid4())),
            "input_manifest": ("input_gold_manifest_sha256", "f" * 64),
        }[failure]
        windows = windows.withColumn(key, F.lit(value))
    else:
        pairs = pairs.withColumn("pair_identity_sha256", F.lit("f" * 64))
    with pytest.raises(ValueError):
        _validate_frames(windows, pairs)


def test_replacement_after_validation_cannot_complete(
    fixture_root: Path, spark: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    identity = uuid4()
    finish = GoldRun.finish

    def swap_then_finish(
        self: GoldRun, status: str, summary: Mapping[str, object], **kwargs: Any
    ) -> Path:
        if status == "complete":
            path = next((self.path / "windows").glob("*.parquet"))
            raw = bytearray(path.read_bytes())
            raw[len(raw) // 2] ^= 1
            path.write_bytes(raw)
            assert kwargs.get("expected_artifacts") is not None
        return finish(self, status, summary, **kwargs)

    monkeypatch.setattr(GoldRun, "finish", swap_then_finish)
    with pytest.raises(ValueError, match="changed after validation"):
        publish_synthetic_sector_gold(
            spark, base._settings(fixture_root), base._request(fixture_root), run_id=identity
        )
    root = base._settings(fixture_root).data_root / "sector_gold/dataset=sector-synthetic-pilot-v1"
    assert not (root / f"run={identity}").exists()
    assert (
        json.loads((root / f"_failed/run={identity}/manifest.json").read_bytes())["status"]
        == "failed"
    )
