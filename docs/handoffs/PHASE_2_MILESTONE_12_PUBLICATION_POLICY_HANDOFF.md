# LABGDD V2 - Milestone 12 Publication Policy Handoff

Date: 2026-11-20. VERIFIED denotes direct inspection/execution in this task;
REPORTED denotes inherited evidence. Assessment, architecture acceptance, execution
permission and dataset approval remain separate.

## 1. Executive Summary

Phase 2 Milestone 12 implements proposed real Sector publication-policy/evidence
contracts. Status: Completed for implementation and validation; authorized Git finalization
is recorded below and in the final response.
Diagnostic research and eligible analytical purposes are explicit and distinct.
All assessments retain publication_authorized=false and outstanding external approvals.
The real pilot qualifies only as diagnostic evidence for review, not official sector
analytics or publication. ADR 018 remains Proposed. No writer, registry activation,
market acquisition, financial formula change or Milestone 13 implementation.

## 2. Repository and Git State

Canonical path: `C:\Users\Deus\Documents\LABGDD`; branch main; origin:
`https://github.com/1211882/LABGDD.git`. Starting clean HEAD and fetched origin/main:
`2a5c2e8dd352899d9b816237a20da574486ec250`. No unrelated changes.
Current implementation is uncommitted pending required gates. The final response
records exact commit/push identities and remote verification; a commit cannot embed
its own SHA. No force-push, rebase, reset, approval tag or history rewrite. Historical
LABGDD_NEW remains untouched.

## 3. Initial State and Dependencies

Read AGENTS.md, accepted ADRs 014-017, final Milestone 9/10/11 handoffs, approved-input,
Sector calculation/publication/inspection/readiness boundaries, and existing
publication_governance/runners/publications contracts. Existing governance preserves
prior authority/superseded inventory and uses exact preconditions/content-bound review
and atomic replacement; no Sector layer or policy activation was added to it.
Reuse local Python 3.11.9, Linux Python 3.11.18, Spark 4.3.0, PyArrow 21.0.0 and
exchange_calendars 4.11.2. No dependencies or production-image changes. Prior accepted
readiness validation was REPORTED as 412 passing Linux tests.

## 4. Implementation Details

`sector_policy.PublicationPolicy` selects diagnostic_research or eligible_sector_analytics.
Its full definition specifies required study-cohort-v1, approved Expected Gold v2
and Silver, eligibility/history, completeness/nullability, Alpaca IEX/raw scope,
correction evidence, roles, pending status, prohibited claims and supersession.
Unknown versions/purposes, approved status or relaxed role lists fail. Owner is
explicitly unresolved by default; declaring one changes the fingerprint, not authority.

`evaluate_publication_policy` requires the full policy fingerprint and an independent
`PublicationSourceIdentity` pin (dataset/UUID/manifest SHA-256). It consumes a validated
Milestone 11 report and checks internal coherence: versions, source identity, scope,
coverage arithmetic, population, all-horizon window/pair counts, reasons, limitations,
historical state and gate/blocker agreement. Invalid inputs raise; unsuitable valid
evidence produces explainable blockers. `PolicyAssessment` separately records
technical/policy-specific scientific eligibility, inherited readiness scientific
eligibility, qualification for review, limitations, missing
approvals, identities and fingerprints. It rejects any authorized/approved state.

## 5. Architecture and Data Flow

Trusted fresh Milestone 11 preflight -> explicitly pinned policy/source -> report
coherence and evidence-contract checks -> deterministic policy assessment. No financial
row collection, write path, candidate constructor, approval operation or registry API.
Canonical definitions and assessments bind report bytes, requested claims, policy,
source and external attestation records. Executing source bytes are separately hashed.

This layer does not authenticate caller-created reports, fetch attestation URIs or
verify external evidence files. A self-consistent object/hash is not proof of truth.
Trusted producer validation is a precondition. Future writers must freshly resolve
authority and independently verify reference bytes, reviewer authority and approvals.
Qualification is necessary evidence for review only; missing approvals never disappear.

## 6. Scientific and Mathematical Contracts

No change to measurement 2.0.0, H={5,15,30,60}, UTC/XNYS semantics, population,
eligibility or identity contracts. For expected N: q_i=sum(r_it^2), sector
RMS=sqrt(sum(q_i)/N), exact portfolio p_t=log1p(sum(expm1(r_it))/N), portfolio
RV=sqrt(sum(p_t^2)). Mean-log proxy stays separate. Dispersion uses N;
HHI=sum((q_i/sum(q))^2); effective breadth=(1-HHI)/(1-1/N), with N>=2 and positive
total variance. Complete pair requirements and stable constant detection remain.
Inherited parity tolerances remain absolute 1e-12/relative 1e-10.

Normative primary reasons remain scope exclusion, incomplete population, unverified
history, missing member, missing bar, ineligible return. Policy blockers are separate.
Diagnostic evidence preserves all expected members, explicit gaps/nulls/reasons;
analytical qualification requires strict history and complete windows/pairs.
Optional uncalibrated threshold breadth keeps its existing null/reason contract.
No observed-survivor subset, zero/forward filling, calibration or emergence decision.

## 7. Data Contracts and Provenance

New contracts: sector-real-publication-policy-v1, sector-publication-policy-assessment-v1,
sector-historical-applicability-evidence-v1, sector-snapshot-correction-evidence-v1.
Historical records cover symbol/entity/listing/trading/classification, effective dates
(inclusive/exclusive), corporate-action basis, source/version, research hash,
verification/reference/digest/reviewer and uncertainty. Duplicate/out-of-population/
unrelated evidence fails. Provided claims cannot upgrade the report's declared history.

Correction records bind the exact snapshot, UTC event-end/capture ordering, source
version and review/reference digests. A reviewed snapshot-correction protocol is
not a guarantee of provider finality; finality limitations remain visible.
No actual historical/correction attestations or organizational approvers were invented.
Positive analytical unit fixtures use invented metadata/attestation claims only.

Processing-source SHA-256, matched between staged and working bytes before full testing:
`8359e4f180a04e477127e54d2cdb7680ccdebc81b71f6fa67adcd46ccd98d5fc`.
Default proposed diagnostic policy SHA-256:
`6e8f896f742f1e88585fb88f3df6336fd5f87424fa0ae3a3b202a061768f8710`.
Default analytical policy SHA-256:
`0e89706da556cc74cee233b0ef1d38685477bb8d5c68a0926105b211b47e2edd`.
These use unresolved owner and required proposed roles, not external approval.

Real input: gold-study-cohort-20260914-multisymbol-expected-v1,
run 39ab367c-c6d2-480d-8c94-f35f767ed289, manifest
`d97b0e48c8463b6a97aa58e576e7f24998b4246f496fabaa286982177ab605b2`.
Read-only inspection confirms declared_research and unspecified evidence classification;
absence of a marker is not converted to an invented real-evidence attestation.

## 8. Files Changed

- src/labgdd/sector_policy.py: typed policies, evidence, identity and assessments/evaluator.
- tests/unit/test_sector_policy.py: mandatory portable synthetic contract/adversarial tests.
- tests/integration/test_sector_policy.py: optional exact real-pilot read-only evaluation.
- scripts/smoke_wheel.py: isolated import of the new module.
- docs/decisions/018-real-sector-publication-policy.md: Proposed ADR and unresolved decisions.
- docs/architecture/PHASE2_SECTOR_PUBLICATION_POLICY.md: interfaces, trust and source limits.
- README.md: discovery link to the policy architecture.
- docs/handoffs/PHASE_2_MILESTONE_12_PUBLICATION_POLICY_HANDOFF.md: complete evidence/handoff.

No removed files, dependency locks, active registries, approved ADRs 014-017 or
immutable datasets changed. Temporary fixtures/builds/logs remain excluded from Git.

## 9. Validation Evidence

Executed: Ruff formatting/lint passed; strict mypy passed for 93 files; pip check
passed; repository scanner passed for 201 candidates before this report.
`python -m pytest tests/unit/test_sector_policy.py -q -p no:cacheprovider`:
47 passed in 0.43s. Native optional pilot command:
`python -m pytest tests/integration/test_sector_policy.py -x -q -p no:cacheprovider`:
1 passed in 49.05s, no skip. Complete final Linux
`python -m pytest -q -p no:cacheprovider`: 460 passed, zero skips, seven pinned-Spark API warnings,
1458.14s (24m18s reported test time).
Dedicated image labgdd-portability-test:with-git, UID 10001, network disabled,
secret-free source copy and frozen evidence mounted read-only. Includes all existing
Sector lifecycle/readiness, authority and independent scalar/Spark regressions.

`python -m build --no-isolation <temporary-copy>` built sdist/wheel 0.4.0.
`python scripts/smoke_wheel.py <copy>/dist/labgdd_v2-0.4.0-py3-none-any.whl
--config config/project.toml` passed clean installation, imports, offline CLIs and
pip check, with zero mandatory runtime dependencies. Source was normalized to LF
and index parity checked before full testing. Final staged security/whitespace/diff
gates are repeated before commit; no source changes followed full validation.

Intermediate fixes: Ruff formatting/import/line-length findings; strict mypy dictionary
inference, tuple-width and reused-variable issues corrected. Initial real-pilot test
failed in 53.48s because the evaluator incorrectly required sorted acquisition-scope
metadata. Accepted source manifests preserve request order. Exact duplicate-free
membership comparison now preserves original bytes/order; a regression proves both
permutation compatibility and duplicate rejection. No frozen artifact was changed,
failure suppressed or test disabled. A partial full-suite run was deliberately
stopped (exit 137) to separate inherited readiness science from policy-specific
science: declared-research readiness cannot satisfy strict analytical eligibility.
The added regression passes; the restarted full suite uses the final source bytes.
A later wait returned after 1861s instead of the requested 50s; host/VM suspension
was inferred from the discrepancy with guest process elapsed time. After the user
resumed, the same active test process completed without restart or source changes.
The final duration above is pytest-reported active guest test time, not total wall
time across the interruption.

Baseline CI verified successful on Ubuntu and Windows for runs 44657094927 and
44657269599. Filtered authenticated job logs confirm respectively: Ubuntu
408 passed/4 skipped/7 warnings (587.31s and 661.77s); Windows 335 passed/77 skipped
(1226.64s and 1234.81s). Optional real artifacts are absent in clean Ubuntu checkouts;
Windows also retains existing native Linux-only exclusions. These prove baseline
commits, not unpushed Milestone 12 changes. New CI must be reported at observed status.

## 10. Quantitative Results

Actual pilot integration verified 30 expected symbols, six sectors, five acquired,
25 intentionally unrequested, 11,700 expected slots, 1,861 observed bars, 9,750
outside-scope slots and 89 requested-symbol gaps. Total missing slots are 9,839.
No sector is complete. Diagnostic: technical eligible, scientific ineligible,
qualifies for review only, authorization false. Analytical: blocked by scientific
incompleteness, unsupported declared policy, missing independent history/correction
review and missing explicit real-evidence classification. Approvals remain outstanding.

Positive analytical unit fixtures are invented metadata/attestation claims only,
not real observations or verified project evidence. Even those assessments cannot
authorize publication. Requested official/consolidated/finality claims cannot relabel
diagnostic evidence. Replay and a no-filesystem-side-effects test passed.

## 11. Architecture Decisions

ADR 018 remains Proposed. Preserve accepted ADRs 014-017 and all frozen schemas.
Choose pure contract/coherence assessment rather than extending approval CLI,
installing a registry, fetching evidence or writing outputs. Two fixed safe profile
definitions prevent weakening analytical requirements through arbitrary flags.
Historical/correction attestations remain externally reviewable claims. Snapshot
analytics with a reviewed correction protocol are proposed venue-specific evidence,
not proof of final provider data; human review must decide permitted intended use.

## 12. Security and Governance

No Alpaca requests, secret-environment reads, candidate creation, dataset mutation
or active registry writes. GitHub log authentication stayed in memory; credentials/
headers were not printed or persisted. Generic proposed review roles and unresolved
owner are not actual organizational approvers. Publication authorization is always
false and cannot be changed via the assessment constructor. New hashes bind exact
definitions/evidence; they are not signatures. Future supersession requires fresh
authority checks, append-preserved history and explicit external approval.

## 13. Problems and Limitations

Full final validation passed; Git finalization remains to be recorded. Coherence validation cannot
authenticate a fabricated internally consistent report or external attestation URI;
trusted producer input and independent reference/role verification are required.
Provider corrections, consolidated coverage and historical membership remain unproven.
Capture/event ordering is a required metadata check, not a finality proof. No real
correction or historical evidence is supplied here. Proposed profiles need independent
review; assessments cannot substitute for fresh publication preflight or approval.

## 14. Current Project State

Policy/evidence contracts are implemented and focused-tested, including actual pilot
assessment. Package/static/security checks and the final 460-test Linux suite passed.
No publication service, real writer, Sector registry, calibration, emergence or later
milestone was implemented. Existing approved artifacts and authority remain intact.

## 15. Recommended Next Step

Complete final validation/Git evidence, then independently review Proposed ADR 018
and both policy profiles. Review classification, independent evidence standards,
intended-use claims and external ownership/approval requirements before considering
separately authorized implementation. No automatic Milestone 13 progression.

## 16. Decisions Requiring Approval

ADR 018/profile acceptance; whether snapshot-specific IEX analytical claims are
permitted; independent historical/correction verification standards; actual owner/
reviewer assignment; handling incomplete diagnostic evidence; future writer and
registry activation design/execution. None is granted by assessment qualification.

## 17. Next-Agent Continuation Instructions

Use canonical LABGDD/main; verify HEAD/origin/cleanliness and read AGENTS.md, ADRs
014-018 and Milestone 11/12 handoffs. Never modify LABGDD_NEW. Use a trusted fresh
readiness producer and independently pinned source/policy hashes; do not trust a
manually constructed report as authority. Preserve all 30 members and six sectors,
frozen artifacts, UTC/time contracts and financial formulas. Absent optional real
evidence may skip; present-invalid evidence must fail. Keep tests network-disabled
and frozen data read-only. Do not fetch Alpaca data, publish candidates/outputs,
activate registries, calibrate thresholds, implement emergence or begin another phase.

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
