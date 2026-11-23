# ADR 018: Real Sector publication policy and evidence contracts

- Status: Accepted
- Independent final review: 2026-11-23
- Date: 2026-11-20
- Depends on: accepted ADRs 014-017

## Decision

Add the typed `sector-real-publication-policy-v1` definitions in `sector_policy`.
There are two explicit review purposes: diagnostic research evidence and eligible
sector analytics. Neither is an execution policy or an approval. Unknown versions,
purposes, relaxed approval-role lists and approved status are rejected. The required
universe remains study-cohort-v1: all 30 declared symbols and six project sectors.
Do not infer a population from observed survivors or permanently exclude absent members.

Diagnostic evidence may qualify for independent review when an approved, technically
valid snapshot preserves full expectations, scope exclusions, missing observations,
reasons and ineligible null measurements. Scientific eligibility may remain blocked.
Diagnostic evidence cannot claim official sector results, independently verified
historical membership, consolidated coverage or provider correction finality.

Analytical evidence requires strict verified_historical_applicability, every required
window and pair eligible, complete historical attestations bound to research bytes,
explicit real-evidence classification and snapshot-bound correction-protocol review.
Optional undefined metrics such as uncalibrated threshold breadth retain their
accepted null/reason contract. No threshold or emergence capability is introduced.
Analytical review is venue-specific and snapshot-specific. A reviewed correction
protocol is not proof that the provider will never correct its historical data.

## Deterministic assessment boundary

`evaluate_publication_policy` consumes an accepted Milestone 11 readiness report,
an explicitly selected immutable policy, its independently supplied fingerprint,
and the expected dataset/UUID/manifest digest. It checks identity and internal
coherence: source/schema versions, population and coverage arithmetic, reason
inventories across all horizons, scope, historical status, warnings and gate/blocker
consistency. A valid-looking different manifest digest fails the explicit identity pin.

Policy fingerprints cover their full definitions and owner designation. Assessments
bind exact report bytes, external attestation records, source identity, requested
claims and executing source fingerprint. Canonical JSON and stable ordering support
replay. Definitions and assessments are frozen typed objects; an assessment rejects
publication_authorized=true, approved final states or omitted approval requirements.

This pure assessment layer does not reread market artifacts or authenticate external
attestation URIs. The caller must supply a genuinely validated report from a trusted
producer; internal consistency and SHA-256 are not signatures or evidence that a
caller-created object is truthful. Qualification means necessary evidence for further
review, never authority. A future writer must execute fresh readiness/authority checks
and independently verify referenced evidence bytes and reviewer authority.

The assessment distinguishes inherited readiness scientific eligibility from
policy-specific scientific eligibility. A complete declared_research report can
pass its own diagnostic scientific gate while failing the analytical profile
which requires verified history. Correction/classification requirements remain
explicit evidence blockers, and missing approvals remain separate from both gates.

## Historical evidence

`sector-historical-applicability-evidence-v1` records symbol/session/sector, historical
entity identity, listing/trading eligibility, classification source, effective dates
(inclusive start/exclusive end), corporate-action basis, source identity/version,
research fingerprint, verification status, reference URI/digest, reviewer reference
and uncertainty. Verified assertions require complete provenance and eligible status;
unverified assertions require explicit uncertainty. Dates alone do not establish truth.
Records outside the report population, duplicates and research mismatches are rejected.

The evaluator cannot upgrade current research membership: analytical qualification
also requires the independently generated readiness population to be fully verified.
No present-day classification is evidence of historical membership. The current
study-cohort-v1 stays research-declared. No real historical attestation is supplied here.

## Coverage and corrections

Alpaca IEX is a venue-specific source, not consolidated coverage. Unrequested symbols
are acquisition exclusions; requested gaps are missing observations and do not prove
exchange-wide inactivity. Prices remain raw; adjustment/corporate-action policies
require explicit evidence, not automatic substitution. Event time identifies market
instants; capture time identifies observation/snapshot creation, not provider finality.

`sector-snapshot-correction-evidence-v1` binds dataset/run/manifest, event-end and
capture UTC times, source version, review reference and evidence URI/digest. Capture
must follow event-end; this ordering alone proves no finality. Its only supported
claim is reviewed_snapshot_correction_protocol, with explicit verification status.
Provider-finality guarantees are rejected and the readiness limitation remains visible.
The existing pilot supplies no reviewed correction evidence and cannot qualify analytically.

## Approval and registry governance

Proposed responsibilities are data-ownership and independent scientific review, plus
separate execution authorization and Sector registry approval. These are role labels,
not invented organizational approvers. Owner defaults to unresolved. Declaring an
owner changes the policy fingerprint but grants no approval. Missing approval
requirements remain nonempty even when evidence qualifies for review.

A future candidate must remain pending independently of completeness. A separate
Sector registry and explicit activation plan would preserve existing authority and
superseded inventories, bind exact manifests/report/policy fingerprints, reject stale
preconditions and superseded inputs, and replace local registries atomically. Existing
Bronze/Silver/Gold governance demonstrates these principles; it is not extended here.
Corrections or supersession require renewed evidence and approval, never overwritten
frozen artifacts or automatic selection of the newest directory.

Unresolved human decisions: operational adoption for a particular real dataset, whether and how
snapshot-specific IEX analytical claims are permitted, named ownership/reviewer
responsibilities, independent evidence standards and the future activation mechanism.
No real writer, candidate, registry, acquisition or publication is authorized.
Acceptance covers assessment contracts only; do not begin Milestone 13.

## Independent architecture review: 2026-11-23

The review distinguishes structural coherence checks, SHA-256 byte integrity and
trusted operational assumptions. A fabricated internally coherent report can qualify
as an assertion for review, but is not authenticated or authorized. No URI is fetched
or treated as proof. The caller must use fresh `preflight_real_sector_gold`; a future
writer must independently resolve registries, reference bytes and actual approvers.
This in-process typed boundary is not a sandbox against hostile Python code.

Bounded corrections reject whitespace-only historical/correction provenance,
unknown listing states, noncanonical historical session labels, unknown reason
codes, inappropriate metric-specific reasons and contradicted primary precedence.
Only the exact supported policy class is recognized. Unknown evidence object kinds
fail explicitly. No scientific formula, row reason or approved publication changes.

Correction attestations require explicit source/feed/adjustment/timeframe, sorted
unique requested sessions, input processing-source digest and interval start in
addition to exact dataset/run/manifest, interval end, capture time and review fields.
All times must be UTC. Readiness adds bounded `session_bounds`, formatted by Spark
in its verified UTC session rather than converted through the host timezone.
The evaluator checks scope/provenance equality, session labels/durations and exact
first-open/last-close interval coverage. Snapshot capture must not precede event end;
this is still not proof that every later provider correction has arrived.

These are pre-acceptance corrections to previously Proposed evidence v1 contracts;
no persisted real attestation is migrated or invented. Original readiness reports
without the additive bounds remain usable for diagnostic review without correction
evidence. They cannot validate a correction interval: regenerate a fresh trusted
readiness report instead of inventing missing bounds. Frozen upstream manifests and
Individual/Expected/Sector financial schemas remain unchanged.

After all required gates passed, architecture acceptance covers only
these two review-purpose contracts. Policy instances remain unapproved; their
`policy_status=proposed` definitions/fingerprints are intentionally unchanged.
Actual ownership, independent reference verification, permitted analytical claims,
execution and registry activation still require separately authorized decisions.

The final reviewed source passed 512 full Linux tests with zero skips and seven
pinned-Spark API warnings in 1485.40s, including independent scalar/native parity,
publication lifecycle/corruption, readiness and all policy adversarial regressions.
Formatting, lint, strict typing, dependency checks, build, clean-wheel smoke and
security checks passed. Acceptance grants no real-publication permission, dataset
approval, Sector registry authority or independently verified historical facts.
