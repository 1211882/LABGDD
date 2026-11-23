"""Independent policy review: invented claims are never authenticated authority."""

import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import pytest

from labgdd.sector_policy import (
    PublicationPolicy,
    PublicationSourceIdentity,
    evaluate_publication_policy,
)
from tests.unit.test_sector_policy import assess, correction, history, report


@pytest.mark.parametrize(
    "field",
    [
        "reviewer_reference",
        "entity_identity",
        "classification_source",
        "corporate_action_basis",
        "source_identity",
        "source_version",
        "evidence_uri",
    ],
)
def test_blank_historical_provenance_rejected(field: str) -> None:
    with pytest.raises(ValueError):
        changes: dict[str, Any] = {field: "   "}
        replace(history(), **changes)


def test_unknown_listing_state_rejected() -> None:
    with pytest.raises(ValueError):
        replace(
            history(),
            verification_status="unverified",
            uncertainty=("unknown",),
            listing_and_trading_status="invented",
        )


def test_unknown_readiness_reason_rejected() -> None:
    r = report()
    d = r.evidence()
    d["window_reason_counts"][0]["reason"] = "invented_reason"
    with pytest.raises(ValueError):
        assess(replace(r, evidence_json=json.dumps(d, sort_keys=True)))


def test_correction_event_end_must_match_source_session() -> None:
    with pytest.raises(ValueError):
        assess(
            report(),
            correction_evidence=replace(
                correction(), event_interval_end=datetime(2026, 9, 13, 20, tzinfo=UTC)
            ),
        )


def test_wrong_historical_sector_rejected() -> None:
    with pytest.raises(ValueError):
        assess(report(), historical_evidence=(history(sector="Sector1"),))


def test_invalid_historical_interval_rejected() -> None:
    with pytest.raises(ValueError):
        replace(history(), effective_from=date(2026, 9, 15))


@pytest.mark.parametrize(
    "changes",
    [
        {"source": "another-provider"},
        {"feed": "sip"},
        {"adjustment": "all"},
        {"timeframe": "1Day"},
        {"requested_sessions": ()},
        {"requested_sessions": ("2026-09-14", "2026-09-14")},
        {"input_processing_source_sha256": "bad"},
        {"reviewer_reference": " "},
        {"source_version": " "},
        {"evidence_uri": " "},
        {"verification_status": "approved"},
        {"event_interval_start": datetime(2026, 9, 15, tzinfo=UTC)},
        {"event_interval_start": datetime(2026, 9, 14, 13, 30)},
        {"captured_at": datetime(2026, 9, 15, tzinfo=UTC).astimezone(timezone(timedelta(hours=1)))},
    ],
)
def test_invalid_correction_structure(changes: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        replace(correction(), **changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"dataset_version": "another-dataset"},
        {"run_id": UUID(int=999)},
        {"manifest_sha256": "0" * 64},
        {"input_processing_source_sha256": "0" * 64},
        {"requested_sessions": ("2026-09-15",)},
        {"event_interval_start": datetime(2026, 9, 14, 14, tzinfo=UTC)},
        {"event_interval_end": datetime(2026, 9, 14, 19, tzinfo=UTC)},
    ],
)
def test_correction_replay_outside_exact_scope_rejected(changes: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        assess(report(), correction_evidence=replace(correction(), **changes))


@pytest.mark.parametrize("mutation", ["missing", "wrong_date", "wrong_duration", "naive", "extra"])
def test_correction_requires_coherent_session_bounds(mutation: str) -> None:
    r = report()
    d = r.evidence()
    if mutation == "missing":
        del d["session_bounds"]
    elif mutation == "wrong_date":
        d["session_bounds"][0]["session_id"] = "2026-09-15"
    elif mutation == "wrong_duration":
        d["session_bounds"][0]["session_close"] = "2026-09-14T19:00:00+00:00"
    elif mutation == "naive":
        d["session_bounds"][0]["session_open"] = "2026-09-14T13:30:00"
    else:
        d["session_bounds"].append(d["session_bounds"][0])
    with pytest.raises(ValueError):
        assess(
            replace(r, evidence_json=json.dumps(d, sort_keys=True)),
            correction_evidence=correction(),
        )


def test_legacy_report_without_new_bounds_is_diagnostic_only() -> None:
    r = report()
    d = r.evidence()
    del d["session_bounds"]
    legacy = replace(r, evidence_json=json.dumps(d, sort_keys=True))
    assert assess(legacy).evidence_qualifies_for_review
    with pytest.raises(ValueError):
        assess(legacy, correction_evidence=correction())


@pytest.mark.parametrize(
    "changes",
    [
        {"reviewer_reference": None},
        {"sector": " "},
        {"source_version": " "},
        {"research_sha256": "bad"},
        {"contract_version": "v99"},
        {"session_id": "20260914"},
        {"uncertainty": (" ",)},
    ],
)
def test_historical_attestation_structure(changes: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        replace(history(), **changes)


def test_unrelated_historical_research_and_session_rejected() -> None:
    with pytest.raises(ValueError):
        assess(report(), historical_evidence=(replace(history(), research_sha256="0" * 64),))
    with pytest.raises(ValueError):
        assess(report(), historical_evidence=(replace(history(), session_id="2026-09-15"),))


def test_self_consistent_fabrication_is_not_authenticated_or_authorized() -> None:
    # All fixture identities, rows and hashes are invented. Coherence is not authentication.
    a = assess(report())
    assert a.evidence_qualifies_for_review
    assert not a.publication_authorized and a.missing_approval_requirements


def test_noncanonical_json_and_unknown_evidence_kind_rejected() -> None:
    r = report()
    with pytest.raises(ValueError):
        assess(replace(r, evidence_json=json.dumps(r.evidence(), sort_keys=False, indent=2)))
    with pytest.raises(ValueError):
        assess(r, correction_evidence={"verification_status": "independently_verified"})
    with pytest.raises(ValueError):
        assess(r, historical_evidence=({"verification_status": "independently_verified"},))


def test_unrecognized_policy_subclass_rejected() -> None:
    class UnknownPolicy(PublicationPolicy):
        pass

    p = UnknownPolicy("diagnostic_research")
    with pytest.raises(ValueError):
        evaluate_publication_policy(
            report(),
            p,
            policy_sha256=p.fingerprint,
            expected_source=PublicationSourceIdentity(
                "synthetic-report-contract-only", UUID(int=12), "9" * 64
            ),
        )


def test_known_reason_cannot_hide_scope_precedence() -> None:
    r = report()
    d = r.evidence()
    d["pair_reason_counts"][0]["reason"] = "constant_return_series"
    with pytest.raises(ValueError):
        assess(replace(r, evidence_json=json.dumps(d, sort_keys=True)))


def test_missing_mandatory_correction_scope_has_no_positive_defaults() -> None:
    from dataclasses import asdict

    from labgdd.sector_policy import SnapshotCorrectionEvidence

    fields = asdict(correction())
    del fields["feed"]
    with pytest.raises(TypeError):
        SnapshotCorrectionEvidence(**fields)
