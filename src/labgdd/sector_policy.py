"""Proposed publication review contracts. No writer, approval or registry API."""

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from typing import Any, Literal
from uuid import UUID

from labgdd.processing.expected_gold import _source_sha256
from labgdd.processing.sector_readiness import SectorReadinessReport

VERSION = "sector-real-publication-policy-v1"
ROLES = ("data_ownership_review", "independent_scientific_review")
CLAIMS = {
    "diagnostic_research",
    "official_sector_analytics",
    "consolidated_market_coverage",
    "provider_finality_guaranteed",
    "independently_verified_historical_membership",
}


def _sha(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _canonical(value: object) -> str:
    def encode(item: object) -> str:
        if isinstance(item, (date, datetime)):
            return item.isoformat()
        if isinstance(item, UUID):
            return str(item)
        raise TypeError("sector policy: unsupported canonical value")

    return json.dumps(value, sort_keys=True, allow_nan=False, default=encode)


@dataclass(frozen=True, slots=True)
class PublicationPolicy:
    intended_use: str
    research_universe_version: str = "study-cohort-v1"
    designated_owner: str | None = None
    contract_version: str = VERSION
    approval_status: str = "unapproved"
    required_approval_roles: tuple[str, ...] = ROLES

    def __post_init__(self) -> None:
        if (
            self.contract_version != VERSION
            or self.intended_use not in {"diagnostic_research", "eligible_sector_analytics"}
            or self.research_universe_version != "study-cohort-v1"
            or self.approval_status != "unapproved"
            or self.required_approval_roles != ROLES
            or (self.designated_owner is not None and not self.designated_owner.strip())
        ):
            raise ValueError("sector policy: unsupported or conflicting policy")

    def definition(self) -> dict[str, Any]:
        diagnostic = self.intended_use == "diagnostic_research"
        return {
            **asdict(self),
            "policy_id": "sector-" + self.intended_use.replace("_", "-") + "-v1",
            "policy_status": "proposed",
            "dataset_type": "diagnostic_sector_evidence"
            if diagnostic
            else "eligible_sector_analytics",
            "input_authority": "explicitly_approved_expected_gold_v2_with_approved_silver",
            "eligibility_policies": ["declared_research", "verified_historical_applicability"]
            if diagnostic
            else ["verified_historical_applicability"],
            "historical_evidence": "uncertainty_preserved"
            if diagnostic
            else "independently_verified",
            "scientific_completeness": "complete_expected_population_with_explicit_gaps"
            if diagnostic
            else "all_windows_and_expected_pairs_eligible",
            "financial_nullability": "ineligible_null_with_reasons"
            if diagnostic
            else "eligible_required_metrics_present_optional_undefined_states_explicit",
            "source_scope": {
                "source": "alpaca",
                "feed": "iex",
                "adjustment": "raw",
                "timeframe": "1Min",
            },
            "correction_evidence": "immutable_snapshot_limitations_preserved"
            if diagnostic
            else "reviewed_snapshot_correction_protocol",
            "candidate_status": "pending_independent_review_not_authoritative",
            "approval": "external_roles_execution_authorization_and_registry_approval_required",
            "supersession": "fresh_upstream_resolution_and_append_preserving_external_review",
            "prohibited_claims": sorted(
                CLAIMS
                - (
                    {"diagnostic_research"}
                    if diagnostic
                    else {
                        "official_sector_analytics",
                        "independently_verified_historical_membership",
                    }
                )
            ),
        }

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(_canonical(self.definition()).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class HistoricalApplicabilityEvidence:
    symbol: str
    sector: str
    session_id: str
    entity_identity: str | None
    listing_and_trading_status: str
    classification_source: str | None
    effective_from: date
    effective_until: date
    corporate_action_basis: str
    source_identity: str
    source_version: str
    research_sha256: str
    verification_status: str
    evidence_uri: str | None
    evidence_sha256: str | None
    reviewer_reference: str | None
    uncertainty: tuple[str, ...]
    contract_version: str = "sector-historical-applicability-evidence-v1"

    def __post_init__(self) -> None:
        session = date.fromisoformat(self.session_id)
        if (
            self.contract_version != "sector-historical-applicability-evidence-v1"
            or not self.symbol
            or not self.sector
            or not _sha(self.research_sha256)
            or not self.effective_from <= session < self.effective_until
            or not all((self.corporate_action_basis, self.source_identity, self.source_version))
            or self.verification_status not in {"unverified", "independently_verified"}
        ):
            raise ValueError("sector policy: invalid historical evidence")
        if self.verification_status == "independently_verified":
            if (
                not all(
                    (
                        self.entity_identity,
                        self.classification_source,
                        self.evidence_uri,
                        self.reviewer_reference,
                    )
                )
                or not _sha(self.evidence_sha256)
                or self.listing_and_trading_status != "eligible"
                or self.uncertainty
            ):
                raise ValueError("sector policy: incomplete historical attestation")
        elif not self.uncertainty:
            raise ValueError("sector policy: unverified history must preserve uncertainty")


@dataclass(frozen=True, slots=True)
class SnapshotCorrectionEvidence:
    dataset_version: str
    run_id: UUID
    manifest_sha256: str
    captured_at: datetime
    event_interval_end: datetime
    source_version: str
    evidence_uri: str
    evidence_sha256: str
    reviewer_reference: str
    verification_status: str = "unverified"
    claim: str = "reviewed_snapshot_correction_protocol"
    contract_version: str = "sector-snapshot-correction-evidence-v1"

    def __post_init__(self) -> None:
        if (
            self.contract_version != "sector-snapshot-correction-evidence-v1"
            or self.claim != "reviewed_snapshot_correction_protocol"
            or self.verification_status not in {"unverified", "independently_verified"}
            or not isinstance(self.run_id, UUID)
            or not self.dataset_version
            or not _sha(self.manifest_sha256)
            or not _sha(self.evidence_sha256)
            or not all((self.source_version, self.evidence_uri, self.reviewer_reference))
            or any(
                t.utcoffset() != timedelta(0) for t in (self.captured_at, self.event_interval_end)
            )
            or self.captured_at < self.event_interval_end
        ):
            raise ValueError(
                "sector policy: invalid correction evidence or unsupported finality claim"
            )


@dataclass(frozen=True, slots=True)
class PolicyAssessment:
    policy_sha256: str
    input_dataset: str
    input_run: str
    input_manifest_sha256: str
    intended_use: str
    technical_eligible: bool
    scientific_eligible: bool
    evidence_qualifies_for_review: bool
    missing_approval_requirements: tuple[str, ...]
    blocking_reasons: tuple[str, ...]
    evidence_limitations: tuple[str, ...]
    final_assessment: str
    readiness_report_sha256: str
    policy_id: str
    requested_claims: tuple[str, ...]
    historical_attestation_sha256: tuple[str, ...]
    correction_attestation_sha256: str | None
    evaluation_source_sha256: str
    readiness_scientific_eligible: bool
    publication_authorized: Literal[False] = False
    policy_contract_version: str = VERSION
    contract_version: str = "sector-publication-policy-assessment-v1"

    def __post_init__(self) -> None:
        if (
            self.publication_authorized is not False
            or self.contract_version != "sector-publication-policy-assessment-v1"
            or self.policy_contract_version != VERSION
            or not self.missing_approval_requirements
            or self.evidence_qualifies_for_review != (not self.blocking_reasons)
            or self.final_assessment
            != (
                "qualifies_for_review_not_authorized"
                if self.evidence_qualifies_for_review
                else "blocked_not_authorized"
            )
        ):
            raise ValueError("sector policy: assessments cannot grant publication authority")


@dataclass(frozen=True, slots=True)
class PublicationSourceIdentity:
    dataset_version: str
    run_id: UUID
    manifest_sha256: str

    def __post_init__(self) -> None:
        if (
            not re.fullmatch(r"[A-Za-z0-9_.-]+", self.dataset_version)
            or not isinstance(self.run_id, UUID)
            or not _sha(self.manifest_sha256)
        ):
            raise ValueError("sector policy: invalid expected authority identity")


def _validated_report(report: SectorReadinessReport) -> dict[str, Any]:
    """Check internal coherence; this cannot authenticate a caller's fabricated object."""
    if not isinstance(report, SectorReadinessReport):
        raise ValueError("sector policy: validated readiness report required")
    d = report.evidence()
    m = d["input_metadata"]
    if (
        report.contract_version != "sector-real-publication-readiness-v1"
        or report.technical_gate != "passed"
        or report.governance_gate != "blocked"
        or _canonical(d) != report.evidence_json
        or m.get("dataset_version") != report.dataset_version
        or m.get("run_id") != report.run_id
        or not _sha(report.manifest_sha256)
        or m.get("status") != "complete"
        or m.get("gold_schema_version") != "2.0.0"
        or m.get("measurement_version") != "2.0.0"
        or d.get("evidence_kind") != m.get("evidence_kind", "unspecified")
        or d.get("eligibility_policy")
        not in {"declared_research", "verified_historical_applicability"}
        or d.get("eligibility_policy_version") != "sector-eligibility-policy-v1"
        or d.get("population_policy_version") != "sector-complete-population-v1"
        or d.get("calculation_version") != "sector-calculation-v1"
        or m.get("horizons") != [5, 15, 30, 60]
        or not all(
            _sha(m.get(k))
            for k in (
                "processing_source_sha256",
                "configuration_sha256",
                "silver_manifest_sha256",
                "research_reference_sha256",
                "approved_expected_plan_sha256",
            )
        )
        or not _sha(d.get("processing_source_sha256"))
        or not m.get("silver_dataset_version")
        or (m.get("source"), m.get("feed"), m.get("adjustment"), m.get("timeframe"))
        != ("alpaca", "iex", "raw", "1Min")
    ):
        raise ValueError("sector policy: invalid authority or report contract")
    UUID(report.run_id)
    UUID(m["silver_run_id"])
    coverage = d["coverage"]
    keys = [(r["sector"], r["session_id"], r["symbol"]) for r in coverage]
    if not coverage or keys != sorted(set(keys)) or len(coverage) > 1000:
        raise ValueError("sector policy: invalid population identities")
    scope_symbols = m.get("acquisition_scope_symbols")
    if (
        not isinstance(scope_symbols, list)
        or any(not isinstance(symbol, str) for symbol in scope_symbols)
        or len(scope_symbols) != len(set(scope_symbols))
        or m.get("requested_sessions") != sorted({r["session_id"] for r in coverage})
        or sorted(scope_symbols)
        != sorted({r["symbol"] for r in coverage if r["acquisition_scope"]})
    ):
        raise ValueError("sector policy: contradictory requested scope")
    for row in coverage:
        numbers = [
            row[k]
            for k in (
                "expected_slots",
                "observed_slots",
                "missing_slots",
                "upstream_eligible_returns",
            )
        ]
        if (
            any(type(n) is not int or n < 0 for n in numbers)
            or type(row["acquisition_scope"]) is not bool
            or row["expected_slots"] <= 0
            or row["observed_slots"] > row["expected_slots"]
            or row["missing_slots"] != row["expected_slots"] - row["observed_slots"]
            or row["upstream_eligible_returns"] > row["observed_slots"]
            or (not row["acquisition_scope"] and row["observed_slots"] != 0)
        ):
            raise ValueError("sector policy: contradictory coverage")
    totals = {
        "expected_slots": sum(r["expected_slots"] for r in coverage),
        "observed_slots": sum(r["observed_slots"] for r in coverage),
        "outside_acquisition_slots": sum(
            r["expected_slots"] for r in coverage if not r["acquisition_scope"]
        ),
        "requested_missing_slots": sum(
            r["missing_slots"] for r in coverage if r["acquisition_scope"]
        ),
    }
    if any(type(d.get(k)) is not int or d[k] != v for k, v in totals.items()):
        raise ValueError("sector policy: contradictory totals")
    groups = d["scope"]
    group_keys = [(g["sector"], g["session_id"]) for g in groups]
    if group_keys != sorted({(r["sector"], r["session_id"]) for r in coverage}):
        raise ValueError("sector policy: contradictory sector population")
    for g in groups:
        members = [
            r for r in coverage if (r["sector"], r["session_id"]) == (g["sector"], g["session_id"])
        ]
        expected = {
            "expected_members": len(members),
            "requested_members": sum(r["acquisition_scope"] for r in members),
            "observed_members": sum(r["observed_slots"] > 0 for r in members),
            "fully_observed_members": sum(
                r["observed_slots"] == r["expected_slots"] for r in members
            ),
            "upstream_eligible_members": sum(r["upstream_eligible_returns"] > 0 for r in members),
        }
        if any(type(g.get(k)) is not int or g[k] != v for k, v in expected.items()) or any(
            type(g.get(k)) is not int or not 0 <= g[k] <= len(members)
            for k in ("historically_verified_members", "applicable_members")
        ):
            raise ValueError("sector policy: contradictory member counts")
    for evidence_field in ("warnings", "limitations"):
        if (
            not isinstance(d.get(evidence_field), list)
            or any(not isinstance(value, str) or not value for value in d[evidence_field])
            or len(d[evidence_field]) != len(set(d[evidence_field]))
        ):
            raise ValueError("sector policy: malformed limitations")
    if (
        not {"iex_venue_specific_not_consolidated", "provider_finality_unproven"}
        <= set(d["limitations"])
        or (
            (d["evidence_kind"] == "unspecified") != ("evidence_kind_not_attested" in d["warnings"])
        )
        or (
            any(g["historically_verified_members"] != g["expected_members"] for g in groups)
            != ("historical_research_classification_not_independently_verified" in d["warnings"])
        )
    ):
        raise ValueError("sector policy: contradictory evidence limitations")
    for field, pairs in (("window_reason_counts", False), ("pair_reason_counts", True)):
        rows = d[field]
        seen: set[tuple[object, ...]] = set()
        for r in rows:
            key = (r["sector"], r["session_id"], r["horizon"], r["eligible"], r["reason"])
            if (
                key in seen
                or type(r["eligible"]) is not bool
                or type(r["count"]) is not int
                or r["count"] <= 0
                or r["horizon"] not in (5, 15, 30, 60)
                or type(r["horizon"]) is not int
                or (r["eligible"] and r["reason"] is not None)
                or (not r["eligible"] and not r["reason"])
            ):
                raise ValueError("sector policy: invalid reason distribution")
            seen.add(key)
        for g in groups:
            members = [
                r
                for r in coverage
                if (r["sector"], r["session_id"]) == (g["sector"], g["session_id"])
            ]
            if len({r["expected_slots"] for r in members}) != 1:
                raise ValueError("sector policy: inconsistent common session")
            for h in (5, 15, 30, 60):
                expected_count = (members[0]["expected_slots"] - 1) // h
                if pairs:
                    expected_count *= len(members) * (len(members) - 1) // 2
                actual = sum(
                    r["count"]
                    for r in rows
                    if (r["sector"], r["session_id"], r["horizon"])
                    == (g["sector"], g["session_id"], h)
                )
                if expected_count != actual:
                    raise ValueError("sector policy: incomplete window or pair coverage")
        if any((r["sector"], r["session_id"]) not in group_keys for r in rows):
            raise ValueError("sector policy: unknown result population")
    for g in groups:
        if (
            g["requested_members"] < g["expected_members"]
            or g["applicable_members"] < g["expected_members"]
            or (
                d["eligibility_policy"] == "verified_historical_applicability"
                and g["historically_verified_members"] < g["expected_members"]
            )
        ):
            if any(
                r["eligible"]
                for field in ("window_reason_counts", "pair_reason_counts")
                for r in d[field]
                if (r["sector"], r["session_id"]) == (g["sector"], g["session_id"])
            ):
                raise ValueError("sector policy: eligibility contradicts expected membership")
    blockers = []
    if any(not r["acquisition_scope"] for r in coverage):
        blockers.append("blocked_incomplete_acquisition_scope")
    if totals["requested_missing_slots"]:
        blockers.append("blocked_missing_observations")
    if d["eligibility_policy"] == "verified_historical_applicability" and any(
        g["historically_verified_members"] != g["expected_members"] for g in groups
    ):
        blockers.append("blocked_unverified_historical_applicability")
    if any(not r["eligible"] for r in d["window_reason_counts"]):
        blockers.append("blocked_ineligible_sector_windows")
    if not d["pair_reason_counts"] or any(not r["eligible"] for r in d["pair_reason_counts"]):
        blockers.append("blocked_incomplete_pair_eligibility")
    if (
        report.scientific_gate != ("blocked" if blockers else "passed")
        or report.blockers != tuple(blockers + ["blocked_unsupported_publication_policy"])
        or report.decision != report.blockers[0]
    ):
        raise ValueError("sector policy: contradictory readiness decisions")
    if (
        m.get("expected_plan_rows") != len(coverage)
        or m.get("expected_symbols") != len({r["symbol"] for r in coverage})
        or m.get("expected_return_slots") != totals["expected_slots"]
    ):
        raise ValueError("sector policy: contradictory source population")
    return d


def evaluate_publication_policy(
    report: SectorReadinessReport,
    policy: PublicationPolicy,
    *,
    policy_sha256: str,
    expected_source: PublicationSourceIdentity,
    historical_evidence: tuple[HistoricalApplicabilityEvidence, ...] = (),
    correction_evidence: SnapshotCorrectionEvidence | None = None,
    requested_claims: tuple[str, ...] = (),
) -> PolicyAssessment:
    """Review eligibility only. External attestations are claims, not authenticated approvals."""
    if not isinstance(policy, PublicationPolicy) or policy_sha256 != policy.fingerprint:
        raise ValueError("sector policy: fingerprint mismatch")
    if (
        not isinstance(report, SectorReadinessReport)
        or not isinstance(expected_source, PublicationSourceIdentity)
        or (
            report.dataset_version,
            report.run_id,
            report.manifest_sha256,
        )
        != (
            expected_source.dataset_version,
            str(expected_source.run_id),
            expected_source.manifest_sha256,
        )
    ):
        raise ValueError("sector policy: expected authority identity mismatch")
    if set(requested_claims) - CLAIMS or len(set(requested_claims)) != len(requested_claims):
        raise ValueError("sector policy: unknown or duplicate claim")
    try:
        d = _validated_report(report)
    except (KeyError, TypeError, IndexError, AttributeError) as exc:
        raise ValueError("sector policy: incomplete readiness evidence") from exc
    m = d["input_metadata"]
    expected_evidence_keys = {(r["symbol"], r["sector"], r["session_id"]) for r in d["coverage"]}
    supplied = [(e.symbol, e.sector, e.session_id) for e in historical_evidence]
    if (
        len(supplied) != len(set(supplied))
        or not set(supplied) <= expected_evidence_keys
        or any(e.research_sha256 != m["research_reference_sha256"] for e in historical_evidence)
    ):
        raise ValueError("sector policy: conflicting or unrelated historical evidence")
    if correction_evidence is not None and (
        correction_evidence.dataset_version,
        str(correction_evidence.run_id),
        correction_evidence.manifest_sha256,
    ) != (report.dataset_version, report.run_id, report.manifest_sha256):
        raise ValueError("sector policy: correction evidence authority mismatch")
    blockers = []
    if m.get("research_universe_version") != policy.research_universe_version:
        blockers.append("research_universe_mismatch")
    if m["expected_symbols"] != 30 or len({r["sector"] for r in d["scope"]}) != 6:
        blockers.append("required_study_cohort_population_missing")
    if d["evidence_kind"] not in {"real_market_evidence", "unspecified"}:
        blockers.append("unsupported_evidence_classification")
    definition = policy.definition()
    if d["eligibility_policy"] not in definition["eligibility_policies"]:
        blockers.append("unsupported_eligibility_policy")
    blockers.extend(
        "prohibited_claim:" + c
        for c in sorted(set(requested_claims) & set(definition["prohibited_claims"]))
    )
    if policy.intended_use == "eligible_sector_analytics":
        if report.scientific_gate != "passed":
            blockers.append("scientific_readiness_blocked")
        expected = {(r["symbol"], r["sector"], r["session_id"]) for r in d["coverage"]}
        actual = {(e.symbol, e.sector, e.session_id) for e in historical_evidence}
        if (
            actual != expected
            or len(actual) != len(historical_evidence)
            or any(
                e.verification_status != "independently_verified"
                or e.research_sha256 != m["research_reference_sha256"]
                for e in historical_evidence
            )
            or any(g["historically_verified_members"] != g["expected_members"] for g in d["scope"])
        ):
            blockers.append("independent_historical_evidence_required")
        if (
            correction_evidence is None
            or correction_evidence.verification_status != "independently_verified"
            or (
                correction_evidence.dataset_version,
                str(correction_evidence.run_id),
                correction_evidence.manifest_sha256,
            )
            != (report.dataset_version, report.run_id, report.manifest_sha256)
        ):
            blockers.append("snapshot_correction_review_required")
        if d["evidence_kind"] != "real_market_evidence":
            blockers.append("explicit_real_evidence_attestation_required")
    approvals: tuple[str, ...] = (
        *ROLES,
        "publication_execution_authorization",
        "sector_registry_approval",
    )
    if policy.designated_owner is None:
        approvals += ("designated_owner_unresolved",)
    qualifies = not blockers
    scientific_eligible = (
        report.scientific_gate == "passed"
        and d["eligibility_policy"] in definition["eligibility_policies"]
        and "independent_historical_evidence_required" not in blockers
    )
    return PolicyAssessment(
        policy.fingerprint,
        report.dataset_version,
        report.run_id,
        report.manifest_sha256,
        policy.intended_use,
        True,
        scientific_eligible,
        qualifies,
        approvals,
        tuple(blockers),
        tuple(sorted(set(d["warnings"] + d["limitations"]))),
        "qualifies_for_review_not_authorized" if qualifies else "blocked_not_authorized",
        hashlib.sha256(_canonical(asdict(report)).encode()).hexdigest(),
        definition["policy_id"],
        tuple(sorted(requested_claims)),
        tuple(
            sorted(
                hashlib.sha256(_canonical(asdict(e)).encode()).hexdigest()
                for e in historical_evidence
            )
        ),
        hashlib.sha256(_canonical(asdict(correction_evidence)).encode()).hexdigest()
        if correction_evidence is not None
        else None,
        _source_sha256(),
        report.scientific_gate == "passed",
    )
