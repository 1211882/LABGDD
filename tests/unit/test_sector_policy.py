"""Synthetic report/attestation claims test contracts, never real market authority."""

import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from labgdd.processing.sector_readiness import SectorReadinessReport
from labgdd.sector_policy import (
    HistoricalApplicabilityEvidence,
    PublicationPolicy,
    PublicationSourceIdentity,
    SnapshotCorrectionEvidence,
    evaluate_publication_policy,
)


def report(
    *, pilot: bool = True, verified: bool = False, kind: str = "unspecified", constant: bool = False
) -> SectorReadinessReport:
    """Coherent invented metadata only; no prices, source files or external approval."""
    coverage: list[dict[str, Any]] = []
    groups: list[dict[str, Any]] = []
    windows = []
    pairs = []
    for sector in range(6):
        members: list[dict[str, Any]] = []
        for index in range(5):
            scope = not pilot or (sector == 0 and index < 3) or (sector in (1, 2) and index == 0)
            observed = (
                0
                if not scope
                else (371 if pilot and sector == 1 else 320 if pilot and sector == 2 else 390)
            )
            members.append(
                dict(
                    sector=f"Sector{sector}",
                    symbol=f"T{sector}{index}",
                    session_id="2026-09-14",
                    acquisition_scope=scope,
                    expected_slots=390,
                    observed_slots=observed,
                    missing_slots=390 - observed,
                    upstream_eligible_returns=max(0, observed - 1),
                )
            )
        coverage.extend(members)
        groups.append(
            dict(
                sector=f"Sector{sector}",
                session_id="2026-09-14",
                expected_members=5,
                requested_members=sum(r["acquisition_scope"] for r in members),
                observed_members=sum(r["observed_slots"] > 0 for r in members),
                fully_observed_members=sum(r["observed_slots"] == 390 for r in members),
                upstream_eligible_members=sum(r["upstream_eligible_returns"] > 0 for r in members),
                historically_verified_members=5 if verified else 0,
                applicable_members=5,
            )
        )
        for h in (5, 15, 30, 60):
            row = dict(
                sector=f"Sector{sector}",
                session_id="2026-09-14",
                horizon=h,
                eligible=not pilot,
                reason="member_outside_acquisition_scope" if pilot else None,
            )
            windows.append({**row, "count": 389 // h})
            pairs.append(
                {
                    **row,
                    "eligible": not pilot and not constant,
                    "reason": "member_outside_acquisition_scope"
                    if pilot
                    else "constant_return_series"
                    if constant
                    else None,
                    "count": 389 // h * 10,
                }
            )
    metadata: dict[str, Any] = dict(
        dataset_version="synthetic-report-contract-only",
        run_id=str(UUID(int=12)),
        status="complete",
        gold_schema_version="2.0.0",
        measurement_version="2.0.0",
        source="alpaca",
        feed="iex",
        adjustment="raw",
        timeframe="1Min",
        processing_source_sha256="a" * 64,
        configuration_sha256="b" * 64,
        silver_dataset_version="synthetic-silver",
        silver_run_id=str(UUID(int=13)),
        silver_manifest_sha256="c" * 64,
        research_reference_sha256="d" * 64,
        approved_expected_plan_sha256="e" * 64,
        expected_symbols=30,
        expected_plan_rows=30,
        expected_return_slots=11700,
        research_universe_version="study-cohort-v1",
        horizons=[5, 15, 30, 60],
        requested_sessions=["2026-09-14"],
        acquisition_scope_symbols=sorted(r["symbol"] for r in coverage if r["acquisition_scope"]),
    )
    if kind != "unspecified":
        metadata["evidence_kind"] = kind
    blockers = (
        [
            "blocked_incomplete_acquisition_scope",
            "blocked_missing_observations",
            "blocked_ineligible_sector_windows",
            "blocked_incomplete_pair_eligibility",
        ]
        if pilot
        else []
    )
    if constant and not pilot:
        blockers.append("blocked_incomplete_pair_eligibility")
    d: dict[str, Any] = dict(
        input_metadata=metadata,
        processing_source_sha256="f" * 64,
        eligibility_policy="verified_historical_applicability" if verified else "declared_research",
        eligibility_policy_version="sector-eligibility-policy-v1",
        population_policy_version="sector-complete-population-v1",
        calculation_version="sector-calculation-v1",
        scope=groups,
        coverage=coverage,
        window_reason_counts=windows,
        pair_reason_counts=pairs,
        expected_slots=11700,
        observed_slots=sum(r["observed_slots"] for r in coverage),
        outside_acquisition_slots=sum(
            r["expected_slots"] for r in coverage if not r["acquisition_scope"]
        ),
        requested_missing_slots=sum(r["missing_slots"] for r in coverage if r["acquisition_scope"]),
        warnings=[]
        if verified
        else ["historical_research_classification_not_independently_verified"],
        limitations=["iex_venue_specific_not_consolidated", "provider_finality_unproven"],
        evidence_kind=kind,
    )
    if kind == "unspecified":
        d["warnings"].append("evidence_kind_not_attested")
    blockers.append("blocked_unsupported_publication_policy")
    return SectorReadinessReport(
        "sector-real-publication-readiness-v1",
        metadata["dataset_version"],
        metadata["run_id"],
        "9" * 64,
        "passed",
        "blocked" if len(blockers) > 1 else "passed",
        "blocked",
        blockers[0],
        tuple(blockers),
        json.dumps(d, sort_keys=True),
    )


def history(symbol: str = "T00", sector: str = "Sector0") -> HistoricalApplicabilityEvidence:
    return HistoricalApplicabilityEvidence(
        symbol,
        sector,
        "2026-09-14",
        "fixture-entity",
        "eligible",
        "fixture-classification",
        date(2026, 1, 1),
        date(2027, 1, 1),
        "fixture raw-price corporate-action review",
        "fixture-source",
        "fixture-v1",
        "d" * 64,
        "independently_verified",
        "urn:synthetic:history",
        "1" * 64,
        "fixture-review",
        (),
    )


def correction() -> SnapshotCorrectionEvidence:
    return SnapshotCorrectionEvidence(
        "synthetic-report-contract-only",
        UUID(int=12),
        "9" * 64,
        datetime(2026, 9, 15, tzinfo=UTC),
        datetime(2026, 9, 14, 20, tzinfo=UTC),
        "fixture-v1",
        "urn:synthetic:correction",
        "2" * 64,
        "fixture-review",
        "independently_verified",
    )


def assess(r: SectorReadinessReport, use: str = "diagnostic_research", **kwargs: Any) -> Any:
    policy = PublicationPolicy(use)
    return evaluate_publication_policy(
        r,
        policy,
        policy_sha256=policy.fingerprint,
        expected_source=PublicationSourceIdentity(
            "synthetic-report-contract-only", UUID(int=12), "9" * 64
        ),
        **kwargs,
    )


def test_pilot_diagnostic_qualifies_for_review_only() -> None:
    r = report()
    a = assess(r)
    assert a.technical_eligible and not a.scientific_eligible
    assert a.evidence_qualifies_for_review and not a.publication_authorized
    assert a.missing_approval_requirements
    assert a == assess(r)
    analytical = assess(r, "eligible_sector_analytics")
    assert not analytical.evidence_qualifies_for_review
    assert "independent_historical_evidence_required" in analytical.blocking_reasons
    assert "snapshot_correction_review_required" in analytical.blocking_reasons


def test_complete_analytical_attestations_still_never_authorize() -> None:
    r = report(pilot=False, verified=True, kind="real_market_evidence")
    evidence = tuple(history(f"T{s}{i}", f"Sector{s}") for s in range(6) for i in range(5))
    a = assess(
        r,
        "eligible_sector_analytics",
        historical_evidence=evidence,
        correction_evidence=correction(),
        requested_claims=("official_sector_analytics",),
    )
    assert a.scientific_eligible and a.evidence_qualifies_for_review
    assert not a.publication_authorized and a.missing_approval_requirements
    assert "provider_finality_unproven" in a.evidence_limitations
    assert len(a.historical_attestation_sha256) == 30


@pytest.mark.parametrize(
    "changes",
    [
        dict(contract_version="v99"),
        dict(intended_use="unknown"),
        dict(approval_status="approved"),
        dict(required_approval_roles=()),
        dict(designated_owner=""),
        dict(research_universe_version="current-company-sectors"),
    ],
)
def test_invalid_policy(changes: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        replace(PublicationPolicy("diagnostic_research"), **changes)


def test_policy_fingerprint_binds_definition() -> None:
    p = PublicationPolicy("diagnostic_research")
    changed = replace(p, designated_owner="proposed-owner-not-an-approval")
    assert p.fingerprint != changed.fingerprint
    with pytest.raises(ValueError):
        evaluate_publication_policy(
            report(),
            changed,
            policy_sha256=p.fingerprint,
            expected_source=PublicationSourceIdentity(
                "synthetic-report-contract-only", UUID(int=12), "9" * 64
            ),
        )


@pytest.mark.parametrize("kind", ["synthetic_fixture", "unknown-kind"])
def test_real_policy_refuses_unsupported_classification(kind: str) -> None:
    assert "unsupported_evidence_classification" in assess(report(kind=kind)).blocking_reasons


@pytest.mark.parametrize(
    "claim",
    [
        "official_sector_analytics",
        "consolidated_market_coverage",
        "provider_finality_guaranteed",
        "independently_verified_historical_membership",
    ],
)
def test_diagnostic_cannot_be_relabelled(claim: str) -> None:
    a = assess(report(), requested_claims=(claim,))
    assert not a.evidence_qualifies_for_review and not a.publication_authorized
    assert "prohibited_claim:" + claim in a.blocking_reasons


@pytest.mark.parametrize(
    "mutation",
    [
        "identity",
        "digest",
        "technical",
        "scientific",
        "governance",
        "blockers",
        "source_fingerprint",
        "lineage",
        "coverage",
        "population",
        "pairs",
        "horizon",
        "evidence_kind",
        "false_eligibility",
        "scope",
    ],
)
def test_contradictory_reports_fail_closed(mutation: str) -> None:
    r = report()
    d = r.evidence()
    if mutation in {"identity", "digest", "technical", "scientific", "governance", "blockers"}:
        field, value = {
            "identity": ("run_id", str(UUID(int=99))),
            "digest": ("manifest_sha256", "bad"),
            "technical": ("technical_gate", "blocked"),
            "scientific": ("scientific_gate", "passed"),
            "governance": ("governance_gate", "passed"),
            "blockers": ("blockers", ()),
        }[mutation]
        updates: dict[str, Any] = {field: value}
        r = replace(r, **updates)
    else:
        if mutation == "source_fingerprint":
            d["input_metadata"]["processing_source_sha256"] = "bad"
        elif mutation == "lineage":
            d["input_metadata"]["silver_run_id"] = "bad"
        elif mutation == "coverage":
            d["coverage"].append(d["coverage"][0])
        elif mutation == "population":
            d["scope"][0]["expected_members"] = 4
        elif mutation == "pairs":
            d["pair_reason_counts"][0]["count"] -= 1
        elif mutation == "horizon":
            d["window_reason_counts"].pop()
        elif mutation == "evidence_kind":
            d["evidence_kind"] = "real_market_evidence"
        elif mutation == "false_eligibility":
            d["window_reason_counts"][0].update(eligible=True, reason=None)
        elif mutation == "scope":
            d["input_metadata"]["acquisition_scope_symbols"] = []
        r = replace(r, evidence_json=json.dumps(d, sort_keys=True))
    with pytest.raises(ValueError):
        assess(r)


def test_missing_pairs_and_history_do_not_become_analytical() -> None:
    r = report(pilot=False, verified=False, constant=True, kind="real_market_evidence")
    a = assess(r, "eligible_sector_analytics")
    assert not a.scientific_eligible and not a.evidence_qualifies_for_review
    assert "scientific_readiness_blocked" in a.blocking_reasons


def test_declared_readiness_cannot_satisfy_strict_analytical_science() -> None:
    r = report(pilot=False, verified=False, kind="real_market_evidence")
    assert r.scientific_gate == "passed"
    a = assess(r, "eligible_sector_analytics")
    assert a.readiness_scientific_eligible and not a.scientific_eligible
    assert not a.evidence_qualifies_for_review and not a.publication_authorized


def test_attestations_cannot_rewrite_current_readiness() -> None:
    evidence = tuple(history(f"T{s}{i}", f"Sector{s}") for s in range(6) for i in range(5))
    assert not assess(
        report(),
        "eligible_sector_analytics",
        historical_evidence=evidence,
        correction_evidence=correction(),
    ).evidence_qualifies_for_review
    with pytest.raises(ValueError):
        assess(report(), historical_evidence=(history(), history()))
    with pytest.raises(ValueError):
        assess(report(), historical_evidence=(history("UNKNOWN"),))
    with pytest.raises(ValueError):
        assess(report(), correction_evidence=replace(correction(), run_id=UUID(int=99)))


@pytest.mark.parametrize(
    "changes",
    [
        dict(claim="provider_finality_guaranteed"),
        dict(captured_at=datetime(2026, 9, 13, tzinfo=UTC)),
        dict(captured_at=datetime(2026, 9, 15)),
        dict(evidence_sha256="bad"),
        dict(contract_version="v99"),
    ],
)
def test_false_finality_evidence_rejected(changes: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        replace(correction(), **changes)


def test_historical_uncertainty_cannot_be_hidden() -> None:
    with pytest.raises(ValueError):
        replace(history(), verification_status="unverified")
    with pytest.raises(ValueError):
        replace(history(), evidence_uri=None)
    with pytest.raises(ValueError):
        replace(history(), effective_until=date(2026, 9, 14))
    unverified = replace(
        history(), verification_status="unverified", uncertainty=("not independently checked",)
    )
    assert unverified.verification_status == "unverified"


def test_valid_looking_wrong_manifest_is_rejected_by_explicit_pin() -> None:
    with pytest.raises(ValueError, match="authority identity"):
        assess(replace(report(), manifest_sha256="0" * 64))


def test_owner_designation_is_not_an_approval() -> None:
    p = PublicationPolicy("diagnostic_research", designated_owner="proposed-owner-only")
    a = evaluate_publication_policy(
        report(),
        p,
        policy_sha256=p.fingerprint,
        expected_source=PublicationSourceIdentity(
            "synthetic-report-contract-only", UUID(int=12), "9" * 64
        ),
    )
    assert not a.publication_authorized
    assert "publication_execution_authorization" in a.missing_approval_requirements
    assert "sector_registry_approval" in a.missing_approval_requirements
    forbidden: dict[str, Any] = {"publication_authorized": True}
    with pytest.raises(ValueError):
        replace(a, **forbidden)


def test_unknown_claim_is_not_silently_accepted() -> None:
    with pytest.raises(ValueError):
        assess(report(), requested_claims=("invented-claim",))


@pytest.mark.parametrize("mutation", ["finality", "classification_warning", "nan"])
def test_limitations_cannot_be_rewritten_as_guarantees(mutation: str) -> None:
    r = report()
    d = r.evidence()
    if mutation == "finality":
        d["limitations"].remove("provider_finality_unproven")
    elif mutation == "classification_warning":
        d["warnings"].remove("evidence_kind_not_attested")
    else:
        d["expected_slots"] = float("nan")
    with pytest.raises(ValueError):
        assess(replace(r, evidence_json=json.dumps(d, sort_keys=True)))


def test_policy_evaluation_has_no_publication_side_effects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assess(report())
    assert list(tmp_path.rglob("*")) == []


def test_request_order_is_preserved_but_scope_membership_is_exact() -> None:
    r = report()
    d = r.evidence()
    d["input_metadata"]["acquisition_scope_symbols"].reverse()
    reordered = replace(r, evidence_json=json.dumps(d, sort_keys=True))
    assert assess(reordered).evidence_qualifies_for_review
    assert assess(reordered).readiness_report_sha256 != assess(r).readiness_report_sha256
    d["input_metadata"]["acquisition_scope_symbols"].append(
        d["input_metadata"]["acquisition_scope_symbols"][0]
    )
    with pytest.raises(ValueError):
        assess(replace(r, evidence_json=json.dumps(d, sort_keys=True)))
