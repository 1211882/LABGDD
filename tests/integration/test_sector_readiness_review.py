"""Independent readiness review; fixture mutation never touches frozen evidence."""

import json
import os
import shutil
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from labgdd.adapters.gold import resolve_approved_gold
from labgdd.processing import sector_readiness as readiness
from labgdd.processing.approved_sector import calculate_approved_sector_gold
from tests.integration import test_approved_sector as base
from tests.integration import test_sector_readiness as fixtures

spark = fixtures.spark
evidence = fixtures.evidence
pytestmark = pytest.mark.skipif(os.name == "nt", reason="native readiness review runs on Linux")
UNIVERSE = {"Technology": ("AAPL", "MSFT", "NVDA", "AMD", "INTC")}


def _settings(root: Path) -> Any:
    return replace(base._settings(root), universe=UNIVERSE)


@pytest.mark.parametrize("eligible", [None, True, False])
def test_summary_rejects_contradictory_eligibility(spark: Any, eligible: bool | None) -> None:
    reason = None if eligible is False else "invalid"
    frame = spark.createDataFrame(
        [("Technology", "2026-09-14", 5, eligible, reason, None)],
        "sector string, session_id string, horizon int, eligible boolean, "
        "reason string, value double",
    )
    with pytest.raises(ValueError):
        readiness._summarize(frame)


def test_eligible_pair_requires_value(spark: Any) -> None:
    frame = spark.createDataFrame(
        [("Technology", "2026-09-14", 5, True, None, None)],
        "sector string, session_id string, horizon int, eligible boolean, "
        "reason string, value double",
    )
    with pytest.raises(ValueError):
        readiness._summarize(frame)


def test_constant_pairs_block_scientific_readiness(tmp_path: Path, spark: Any) -> None:
    universe: dict[str, tuple[str, ...]] = {"Energy": ("XOM", "CVX", "COP", "SLB", "OXY")}
    (tmp_path / "constant").mkdir()
    root = base.make_evidence(tmp_path / "constant", spark, universe)
    report = readiness.preflight_real_sector_gold(
        spark, replace(base._settings(root), universe=universe), **base._arguments(root)
    )
    assert report.technical_gate == "passed"
    assert report.scientific_gate == report.governance_gate == "blocked"
    assert report.decision == "blocked_incomplete_pair_eligibility"
    assert all(row["eligible"] for row in report.evidence()["window_reason_counts"])
    assert all(not row["eligible"] for row in report.evidence()["pair_reason_counts"])
    assert report.evidence()["evidence_kind"] == "synthetic_fixture"


@pytest.fixture(scope="module")
def calculation(evidence: Path, spark: Any) -> Any:
    return calculate_approved_sector_gold(spark, _settings(evidence), **base._arguments(evidence))


@pytest.mark.parametrize("failure", ["duplicate_window", "duplicate_pair", "missing_pair"])
def test_inventory_fails_closed(calculation: Any, failure: str) -> None:
    projection, frames = calculation
    windows, pairs = frames.windows, frames.correlations
    if failure == "duplicate_window":
        windows = windows.unionByName(windows.limit(1))
    elif failure == "duplicate_pair":
        pairs = pairs.unionByName(pairs.limit(1))
    else:
        pairs = pairs.filter("left_symbol != 'AAPL'")
    with pytest.raises(ValueError):
        readiness._validate_inventory(projection.population, windows, pairs)


@pytest.mark.parametrize(
    "failure", ["manifest", "source_fingerprint", "lineage", "calendar", "population"]
)
def test_preflight_rejects_invalid_evidence(
    evidence: Path, spark: Any, tmp_path: Path, failure: str
) -> None:
    root = tmp_path / "copy"
    shutil.copytree(evidence, root)
    arguments = base._arguments(root)
    manifest = root / "data/gold" / f"dataset={base.DATASET}/run={base.RUN}/manifest.json"
    doc = json.loads(manifest.read_bytes())
    if failure == "manifest":
        manifest.write_text("{}")
    else:
        key, value = {
            "source_fingerprint": ("processing_source_sha256", "f" * 64),
            "lineage": ("silver_manifest_sha256", "f" * 64),
            "calendar": ("calendar_name", "XNAS"),
            "population": ("expected_symbols", 99),
        }[failure]
        doc[key] = value
        manifest.write_text(json.dumps(doc))
        if failure != "source_fingerprint":
            base._registry(root / "config/gold_publications.toml", manifest, doc, gold=True)
        arguments = base._arguments(root)
    with pytest.raises(ValueError):
        readiness.preflight_real_sector_gold(spark, _settings(root), **arguments)


def test_source_substitution_during_preflight_is_rejected(
    evidence: Path, spark: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "copy"
    shutil.copytree(evidence, root)
    arguments = base._arguments(root)
    resolve = resolve_approved_gold
    calls = 0

    def substitute(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 2:
            path = next((root / "data/gold").rglob("*.parquet"))
            raw = bytearray(path.read_bytes())
            raw[len(raw) // 2] ^= 1
            path.write_bytes(raw)
        return resolve(*args, **kwargs)

    monkeypatch.setattr(readiness, "resolve_approved_gold", substitute)
    with pytest.raises(ValueError, match="artifact verification"):
        readiness.preflight_real_sector_gold(spark, _settings(root), **arguments)
