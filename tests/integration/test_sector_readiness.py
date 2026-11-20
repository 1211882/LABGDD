"""Readiness never grants publication permission, including for complete fixtures."""

import os
from dataclasses import replace
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from labgdd.adapters.bronze import sha256
from labgdd.config import load_settings
from labgdd.processing.sector_readiness import _summarize, preflight_real_sector_gold
from tests.integration import test_approved_sector as base

spark = base.spark
pytestmark = pytest.mark.skipif(os.name == "nt", reason="native readiness runs on Linux")


@pytest.mark.parametrize(
    ("eligible", "value"), [(True, float("inf")), (True, float("nan")), (False, 0.1)]
)
def test_aggregate_forces_financial_validation(spark: Any, eligible: bool, value: float) -> None:
    frame = spark.createDataFrame(
        [("Technology", "2026-09-14", 5, eligible, None, value)],
        "sector string, session_id string, horizon int, eligible boolean, "
        "reason string, value double",
    )
    with pytest.raises(ValueError, match="financial values"):
        _summarize(frame)


@pytest.fixture(scope="module")
def evidence(tmp_path_factory: pytest.TempPathFactory, spark: Any) -> Path:
    return base.make_evidence(
        tmp_path_factory.mktemp("readiness"),
        spark,
        {"Technology": ("AAPL", "MSFT", "NVDA", "AMD", "INTC")},
    )


def test_complete_fixture_replay_strict_and_no_publication(evidence: Path, spark: Any) -> None:
    before = {str(p): sha256(p) for p in evidence.rglob("*") if p.is_file()}
    report = preflight_real_sector_gold(
        spark,
        replace(
            base._settings(evidence),
            universe={"Technology": ("AAPL", "MSFT", "NVDA", "AMD", "INTC")},
        ),
        **base._arguments(evidence),
    )
    assert report.technical_gate == "passed"
    assert report.scientific_gate == "passed"
    assert report.governance_gate == "blocked"
    assert report == preflight_real_sector_gold(
        spark,
        replace(
            base._settings(evidence),
            universe={"Technology": ("AAPL", "MSFT", "NVDA", "AMD", "INTC")},
        ),
        **base._arguments(evidence),
    )
    strict = preflight_real_sector_gold(
        spark,
        replace(
            base._settings(evidence),
            universe={"Technology": ("AAPL", "MSFT", "NVDA", "AMD", "INTC")},
        ),
        **base._arguments(evidence),
        eligibility_policy="verified_historical_applicability",
    )
    assert "blocked_unverified_historical_applicability" in strict.blockers
    assert strict.scientific_gate == "blocked"
    assert before == {str(p): sha256(p) for p in evidence.rglob("*") if p.is_file()}


def test_unknown_policy_and_manifest_fail_closed(evidence: Path, spark: Any) -> None:
    arguments = base._arguments(evidence)
    with pytest.raises(ValueError, match="eligibility policy"):
        preflight_real_sector_gold(
            spark,
            replace(
                base._settings(evidence),
                universe={"Technology": ("AAPL", "MSFT", "NVDA", "AMD", "INTC")},
            ),
            **arguments,
            eligibility_policy="unknown",
        )
    arguments["manifest_sha256"] = "f" * 64
    with pytest.raises(ValueError, match="fingerprint"):
        preflight_real_sector_gold(
            spark,
            replace(
                base._settings(evidence),
                universe={"Technology": ("AAPL", "MSFT", "NVDA", "AMD", "INTC")},
            ),
            **arguments,
        )


def test_optional_real_pilot(spark: Any) -> None:
    root = Path(__file__).resolve().parents[2]
    settings = load_settings(root / "config/project.toml")
    dataset = "gold-study-cohort-20260914-multisymbol-expected-v1"
    identity = UUID("39ab367c-c6d2-480d-8c94-f35f767ed289")
    manifest = settings.data_root / "gold" / f"dataset={dataset}/run={identity}/manifest.json"
    if not manifest.parent.exists() and not manifest.parent.is_symlink():
        pytest.skip("optional exact real evidence absent")
    report = preflight_real_sector_gold(
        spark,
        settings,
        registry=root / "config/gold_publications.toml",
        silver_registry=root / "config/silver_publications.toml",
        research_reference=root / "config/research_universe.toml",
        dataset_version=dataset,
        run_id=identity,
        manifest_sha256="d97b0e48c8463b6a97aa58e576e7f24998b4246f496fabaa286982177ab605b2",
    )
    assert report.decision == "blocked_incomplete_acquisition_scope"
    assert report.scientific_gate == report.governance_gate == "blocked"
    doc = report.evidence()
    assert (
        doc["expected_slots"],
        doc["observed_slots"],
        doc["outside_acquisition_slots"],
        doc["requested_missing_slots"],
    ) == (11700, 1861, 9750, 89)
    assert {
        r["symbol"]: r["observed_slots"] for r in doc["coverage"] if r["acquisition_scope"]
    } == dict(AAPL=390, MSFT=390, NVDA=390, JPM=371, XOM=320)
    assert not any(row["eligible"] for row in doc["window_reason_counts"])
