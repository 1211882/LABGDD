"""Fail-closed review and explicitly authorized publication-registry activation."""

import json
import os
import tomllib
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import UUID

from labgdd.adapters.bronze import resolve_approved_bronze, sha256
from labgdd.adapters.gold import resolve_approved_gold
from labgdd.adapters.silver import resolve_approved_silver

_PLAN_KEYS = {"plan_version", "proposal_id", "status", "step"}
_STEP_KEYS = {
    "name",
    "layer",
    "dataset_version",
    "identity",
    "active_registry",
    "proposed_registry",
    "expected_active_sha256",
    "proposed_sha256",
    "depends_on",
}


def _toml(path: Path, label: str) -> dict[str, Any]:
    try:
        value = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError):
        raise ValueError(f"{label}: unreadable TOML") from None
    return value


def _plan(path: Path, project_root: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    document = _toml(path, "publication plan")
    if set(document) != _PLAN_KEYS or document.get("plan_version") != "1.0.0":
        raise ValueError("publication plan: invalid schema or version")
    if document.get("status") != "pending_explicit_authorization":
        raise ValueError("publication plan: proposal is not pending explicit authorization")
    steps = document.get("step")
    if (
        not isinstance(steps, list)
        or not steps
        or any(not isinstance(item, dict) or set(item) != _STEP_KEYS for item in steps)
    ):
        raise ValueError("publication plan: invalid step inventory")
    names = [item.get("name") for item in steps]
    if len(names) != len(set(names)):
        raise ValueError("publication plan: duplicate step name")
    root = project_root.resolve()
    for step in steps:
        if step.get("layer") not in {"bronze", "silver", "gold"}:
            raise ValueError("publication plan: invalid layer")
        for field in ("active_registry", "proposed_registry"):
            value = step.get(field)
            if not isinstance(value, str):
                raise ValueError("publication plan: invalid registry path")
            resolved = (path.parent / value).resolve()
            if not resolved.is_relative_to(root / "config"):
                raise ValueError("publication plan: registry path escapes config")
        dependencies = step.get("depends_on")
        if not isinstance(dependencies, list) or any(
            not isinstance(item, str) or item not in names for item in dependencies
        ):
            raise ValueError("publication plan: invalid dependency")
    return document, [dict(item) for item in steps]


def _authorities(document: Mapping[str, Any]) -> list[dict[str, Any]]:
    authority = document.get("authority")
    if isinstance(authority, dict):
        return [dict(authority)]
    if isinstance(authority, list) and all(isinstance(item, dict) for item in authority):
        return [dict(item) for item in authority]
    raise ValueError("publication review: invalid authority inventory")


def _preserves_registry(previous: Path | None, proposed: Path) -> None:
    if previous is None:
        return
    before = _toml(previous, "publication review")
    after = _toml(proposed, "publication review")
    new_authorities = _authorities(after)
    for old in _authorities(before):
        if not any(
            all(candidate.get(key) == value for key, value in old.items())
            for candidate in new_authorities
        ):
            raise ValueError("publication review: existing authority was not preserved")
    old_superseded = before.get("superseded")
    new_superseded = after.get("superseded")
    if not isinstance(old_superseded, list) or not isinstance(new_superseded, list):
        raise ValueError("publication review: invalid superseded inventory")
    if any(item not in new_superseded for item in old_superseded):
        raise ValueError("publication review: superseded evidence was not preserved")


def _resolve_all(layer: str, registry: Path, data_root: Path) -> list[Mapping[str, object]]:
    document = _toml(registry, "publication review")
    results: list[Mapping[str, object]] = []
    for authority in _authorities(document):
        dataset = authority.get("dataset_version")
        identity = authority.get("acquisition_id" if layer == "bronze" else "run_id")
        if not isinstance(dataset, str) or not isinstance(identity, str):
            raise ValueError("publication review: invalid authority identity")
        parsed = UUID(identity)
        if layer == "bronze":
            results.append(resolve_approved_bronze(registry, data_root, dataset, parsed).metadata)
        elif layer == "silver":
            results.append(resolve_approved_silver(registry, data_root, dataset, parsed).metadata)
        else:
            results.append(resolve_approved_gold(registry, data_root, dataset, parsed).metadata)
    return results


def _review_step(
    step: Mapping[str, Any],
    authority: Mapping[str, Any],
    metadata: Mapping[str, object],
) -> dict[str, object]:
    summary_fields = (
        "manifest_version",
        "schema_version",
        "gold_schema_version",
        "measurement_version",
        "status",
        "publication_status",
        "observations",
        "canonical_records",
        "return_records",
        "eligible_return_records",
        "rolling_records",
        "eligible_rolling_records",
        "fixed_records",
        "eligible_fixed_records",
        "expected_symbols",
        "observed_symbols",
    )
    evidence = {key: metadata[key] for key in summary_fields if key in metadata}
    artifacts = metadata.get("artifacts")
    evidence["artifact_count"] = len(artifacts) if isinstance(artifacts, list) else 0
    evidence["manifest_sha256"] = authority.get("manifest_sha256")
    evidence["processing_source_sha256"] = authority.get(
        "processing_source_sha256", authority.get("package_source_sha256")
    )
    if "configuration_sha256" in authority:
        evidence["configuration_sha256"] = authority["configuration_sha256"]
    elif "request_sha256" in authority:
        evidence["configuration_sha256"] = authority["request_sha256"]
    return {
        "name": step["name"],
        "layer": step["layer"],
        "dataset_version": step["dataset_version"],
        "identity": step["identity"],
        "proposed_registry_sha256": step["proposed_sha256"],
        "result": "passed",
        "evidence": evidence,
    }


def _validate_plan(
    plan_path: Path, project_root: Path, data_root: Path
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, object]]]:
    document, steps = _plan(plan_path, project_root)
    state: dict[Path, tuple[Path | None, str]] = {}
    resolved_by_layer: dict[str, Path] = {}
    review_steps: list[dict[str, object]] = []
    completed: set[str] = set()
    for step in steps:
        dependencies = cast(list[str], step["depends_on"])
        if any(item not in completed for item in dependencies):
            raise ValueError("publication plan: dependency order is invalid")
        active = (plan_path.parent / str(step["active_registry"])).resolve()
        proposed = (plan_path.parent / str(step["proposed_registry"])).resolve()
        if not proposed.is_file() or sha256(proposed) != step["proposed_sha256"]:
            raise ValueError("publication review: proposed registry fingerprint mismatch")
        previous, current_hash = state.get(
            active,
            (
                active if active.is_file() else None,
                sha256(active) if active.is_file() else "absent",
            ),
        )
        if current_hash != step["expected_active_sha256"]:
            raise ValueError("publication review: active registry fingerprint mismatch")
        _preserves_registry(previous, proposed)
        proposed_authorities = _authorities(_toml(proposed, "publication review"))
        metadata = _resolve_all(str(step["layer"]), proposed, data_root)
        target_key = "acquisition_id" if step["layer"] == "bronze" else "run_id"
        target_indexes = [
            index
            for index, item in enumerate(metadata)
            if item.get("dataset_version") == step["dataset_version"]
            and item.get(target_key) == step["identity"]
        ]
        if len(target_indexes) != 1:
            raise ValueError("publication review: explicit target is absent")
        state[active] = (proposed, str(step["proposed_sha256"]))
        resolved_by_layer[str(step["layer"])] = proposed
        completed.add(str(step["name"]))
        target_index = target_indexes[0]
        review_steps.append(
            _review_step(step, proposed_authorities[target_index], metadata[target_index])
        )

    bronze_registry = resolved_by_layer.get("bronze")
    silver_registry = resolved_by_layer.get("silver")
    if bronze_registry is None or silver_registry is None:
        raise ValueError("publication review: complete Bronze and Silver governance is required")
    silver_doc = _toml(silver_registry, "publication review")
    for authority in _authorities(silver_doc):
        inputs = authority.get("bronze_inputs")
        if not isinstance(inputs, list):
            raise ValueError("publication review: Silver authority lacks Bronze lineage")
        for item in inputs:
            if not isinstance(item, dict):
                raise ValueError("publication review: invalid Bronze dependency")
            bronze_authority = resolve_approved_bronze(
                bronze_registry,
                data_root,
                str(item.get("dataset_version")),
                UUID(str(item.get("acquisition_id"))),
            )
            if bronze_authority.manifest_sha256 != item.get("manifest_sha256"):
                raise ValueError("publication review: incorrect Bronze lineage")
    for step in steps:
        if step["layer"] != "gold":
            continue
        gold_registry = (plan_path.parent / str(step["proposed_registry"])).resolve()
        for authority in _authorities(_toml(gold_registry, "publication review")):
            silver_authority = resolve_approved_silver(
                silver_registry,
                data_root,
                str(authority.get("silver_dataset_version")),
                UUID(str(authority.get("silver_run_id"))),
            )
            if silver_authority.manifest_sha256 != authority.get("silver_manifest_sha256"):
                raise ValueError("publication review: incorrect Silver lineage")
    return document, steps, review_steps


def review_publication_plan(
    plan_path: Path,
    project_root: Path,
    data_root: Path,
    report_path: Path,
    *,
    reviewed_at: datetime | None = None,
) -> Path:
    """Validate all proposed evidence and write a non-authoritative review report."""
    document, _, steps = _validate_plan(plan_path, project_root, data_root)
    report = {
        "report_version": "1.0.0",
        "proposal_id": document["proposal_id"],
        "proposal_sha256": sha256(plan_path),
        "status": "review_passed_pending_explicit_authorization",
        "reviewed_at": (reviewed_at or datetime.now(UTC)).isoformat(),
        "steps": steps,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("xb") as stream:
        stream.write(json.dumps(report, indent=2, sort_keys=True).encode())
        stream.flush()
        os.fsync(stream.fileno())
    return report_path


def approve_reviewed_step(
    plan_path: Path,
    project_root: Path,
    data_root: Path,
    report_path: Path,
    *,
    step_name: str,
    dataset_version: str,
    identity: UUID,
) -> Path:
    """Atomically activate one exact reviewed step; callers must have external authorization."""
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("publication approval: unreadable review report") from None
    document, steps = _plan(plan_path, project_root)
    report_steps = report.get("steps") if isinstance(report, dict) else None
    report_identity_matches = isinstance(report_steps, list) and len(report_steps) == len(steps)
    if report_identity_matches:
        typed_report_steps = cast(list[object], report_steps)
        for recorded, expected in zip(typed_report_steps, steps, strict=True):
            if not isinstance(recorded, dict) or any(
                recorded.get(key) != value
                for key, value in {
                    "name": expected["name"],
                    "layer": expected["layer"],
                    "dataset_version": expected["dataset_version"],
                    "identity": expected["identity"],
                    "proposed_registry_sha256": expected["proposed_sha256"],
                    "result": "passed",
                }.items()
            ):
                report_identity_matches = False
                break
    if (
        not isinstance(report, dict)
        or report.get("status") != "review_passed_pending_explicit_authorization"
        or report.get("proposal_id") != document["proposal_id"]
        or report.get("proposal_sha256") != sha256(plan_path)
        or not report_identity_matches
    ):
        raise ValueError("publication approval: review report mismatch")
    selected = [item for item in steps if item["name"] == step_name]
    if (
        len(selected) != 1
        or selected[0]["dataset_version"] != dataset_version
        or selected[0]["identity"] != str(identity)
    ):
        raise ValueError("publication approval: explicit target mismatch")
    step = selected[0]
    active = (plan_path.parent / str(step["active_registry"])).resolve()
    proposed = (plan_path.parent / str(step["proposed_registry"])).resolve()
    if not proposed.is_file() or sha256(proposed) != step["proposed_sha256"]:
        raise ValueError("publication approval: proposed registry changed")
    current_hash = sha256(active) if active.is_file() else "absent"
    already_applied = current_hash == step["proposed_sha256"]
    if current_hash != step["expected_active_sha256"] and not already_applied:
        raise ValueError("publication approval: active registry changed")

    if step["layer"] == "bronze":
        resolve_approved_bronze(proposed, data_root, dataset_version, identity)
    elif step["layer"] == "silver":
        bronze = project_root.resolve() / "config/bronze_publications.toml"
        target = resolve_approved_silver(proposed, data_root, dataset_version, identity)
        inputs = target.metadata.get("input_manifests")
        if not isinstance(inputs, list):
            raise ValueError("publication approval: Silver lineage missing")
        for item in inputs:
            if not isinstance(item, dict):
                raise ValueError("publication approval: Silver lineage invalid")
            upstream = resolve_approved_bronze(
                bronze,
                data_root,
                str(item.get("dataset_version")),
                UUID(str(item.get("acquisition_id"))),
            )
            if upstream.manifest_sha256 != item.get("sha256"):
                raise ValueError("publication approval: Bronze authority mismatch")
    elif step["layer"] == "gold":
        silver = project_root.resolve() / "config/silver_publications.toml"
        gold_target = resolve_approved_gold(proposed, data_root, dataset_version, identity)
        silver_upstream = resolve_approved_silver(
            silver,
            data_root,
            str(gold_target.metadata.get("silver_dataset_version")),
            UUID(str(gold_target.metadata.get("silver_run_id"))),
        )
        if silver_upstream.manifest_sha256 != gold_target.metadata.get("silver_manifest_sha256"):
            raise ValueError("publication approval: Silver authority mismatch")

    if already_applied:
        return active

    temporary = active.with_name(f".{active.name}.{step_name}.pending")
    try:
        with temporary.open("xb") as stream:
            stream.write(proposed.read_bytes())
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, active)
    finally:
        if temporary.exists():
            temporary.unlink()
    if sha256(active) != step["proposed_sha256"]:
        raise ValueError("publication approval: atomic update verification failed")
    return active
