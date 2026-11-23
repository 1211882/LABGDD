"""Read-only optional pilot evaluation; no publication path is available."""

import os
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from labgdd.config import load_settings
from labgdd.processing.sector_readiness import preflight_real_sector_gold
from labgdd.sector_policy import (
    PublicationPolicy,
    PublicationSourceIdentity,
    evaluate_publication_policy,
)
from tests.integration.test_approved_sector import spark  # noqa: F401

pytestmark = pytest.mark.skipif(
    os.name == "nt", reason="native policy pilot validation runs on Linux"
)


def test_optional_real_pilot_policy_assessments(spark: Any) -> None:  # noqa: F811
    root = Path(__file__).resolve().parents[2]
    settings = load_settings(root / "config/project.toml")
    dataset = "gold-study-cohort-20260914-multisymbol-expected-v1"
    identity = UUID("39ab367c-c6d2-480d-8c94-f35f767ed289")
    manifest = settings.data_root / "gold" / f"dataset={dataset}/run={identity}/manifest.json"
    if not manifest.parent.exists() and not manifest.parent.is_symlink():
        pytest.skip("optional exact real Gold evidence absent")
    readiness = preflight_real_sector_gold(
        spark,
        settings,
        registry=root / "config/gold_publications.toml",
        silver_registry=root / "config/silver_publications.toml",
        research_reference=root / "config/research_universe.toml",
        dataset_version=dataset,
        run_id=identity,
        manifest_sha256="d97b0e48c8463b6a97aa58e576e7f24998b4246f496fabaa286982177ab605b2",
    )
    d = readiness.evidence()
    assert len(d["coverage"]) == 30 and len(d["scope"]) == 6
    assert d["session_bounds"] == [
        {
            "session_id": "2026-09-14",
            "session_open": "2026-09-14T13:30:00Z",
            "session_close": "2026-09-14T20:00:00Z",
        }
    ]
    assert sum(g["requested_members"] for g in d["scope"]) == 5
    assert (
        d["expected_slots"],
        d["observed_slots"],
        d["outside_acquisition_slots"],
        d["requested_missing_slots"],
    ) == (11700, 1861, 9750, 89)
    for purpose in ("diagnostic_research", "eligible_sector_analytics"):
        policy = PublicationPolicy(purpose)
        a = evaluate_publication_policy(
            readiness,
            policy,
            policy_sha256=policy.fingerprint,
            expected_source=PublicationSourceIdentity(dataset, identity, readiness.manifest_sha256),
        )
        assert a.technical_eligible and not a.scientific_eligible
        assert a.evidence_qualifies_for_review == (purpose == "diagnostic_research")
        assert not a.publication_authorized and a.missing_approval_requirements
        assert a == evaluate_publication_policy(
            readiness,
            policy,
            policy_sha256=policy.fingerprint,
            expected_source=PublicationSourceIdentity(dataset, identity, readiness.manifest_sha256),
        )
