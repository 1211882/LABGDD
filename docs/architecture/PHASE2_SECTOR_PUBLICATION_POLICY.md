# Milestone 12: Proposed Sector publication-policy contracts

Governed by Proposed ADR 018. Accepted ADRs 014-017 remain unchanged.

`PublicationPolicy(intended_use)` selects diagnostic_research or
eligible_sector_analytics. `definition()` makes population, authority, nullability,
feed/finality limitations, approval roles, supersession and prohibited claims explicit.
`fingerprint` hashes the canonical full definition. Policies remain unapproved.

Call `evaluate_publication_policy(readiness, policy, policy_sha256=policy.fingerprint,
expected_source=PublicationSourceIdentity(dataset, uuid, manifest_sha256))` only after
a trusted fresh Milestone 11 preflight. The independent expected identity pin and
full policy fingerprint must match. The evaluator checks report coherence; it does
not resolve registries or authenticate a fabricated report. Malformed reports raise;
well-formed but unsuitable evidence produces explainable blocking reasons.

The assessment separately reports technical eligibility, inherited readiness science,
policy-specific scientific eligibility, qualification
for review, limitations and missing approvals. `publication_authorized` is always
false, including when qualification passes. There is no writer, approve command or
filesystem mutation. Hashes identify bytes and claims; they do not sign evidence.
External historical/correction records are separately fingerprinted assertions whose
source bytes and reviewer authority need independent verification before future use.

Diagnostic eligibility preserves all 30 expected members, six sectors, scope gaps,
missing bars, null financial values and reasons. It does not endorse official metrics.
Analytical qualification requires strict history and complete windows/pairs, explicit
real classification, historical provenance and snapshot-correction review. Supplied
attestations cannot rewrite the readiness gate or verify the current declared cohort.

IEX/raw limitations and unproven provider finality remain visible. A reviewed snapshot
correction protocol does not mean correction finality is guaranteed. The existing
pilot is a diagnostic review prospect only; it remains scientifically incomplete
and has no publication authorization. No incomplete sector is filtered out.

No real configuration/registry entry or evidence attestation is installed. Governance
roles and owner designation are proposals, not actual organizational approvals.
Any future publication needs separate authorization, fresh source resolution,
independent evidence checks, immutable staging/inspection and explicit external
registry approval. Keep assessment, candidate completion and approval separate.
