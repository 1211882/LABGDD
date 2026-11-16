# ADR 015: Read-only approved Expected-Series Gold to Sector Gold

- Status: Proposed
- Date: 2026-11-16
- Depends on: ADR 009, 010, 011, 013 and accepted ADR 014

## Decision proposed

Add a file-owning integration in `processing.approved_sector`. The public APIs
`project_approved_expected_gold` and `calculate_approved_sector_gold` accept explicit
registry, dataset, UUID, manifest digest, research reference and upstream Silver
registry. They accept no arbitrary DataFrame, authority-label switch or caller-built
ApprovedGold object. Resolve through `resolve_approved_gold` and
`resolve_approved_silver`, including inventory/checksum verification and lineage.

Require Gold schema 2.0.0 and measurement 2.0.0. Verify the research-file fingerprint,
regenerate the XNYS plan with pinned calendar provenance, and match the approved plan
fingerprint. Read the declared return files, check physical types and UTC timestamps,
then compare all expected-authority fields and scheduled slots bidirectionally to
the regenerated grid. Duplicate, absent or contradictory slots fail. Validate
financial nullability, canonical/session/membership provenance and acquisition scope.
No upstream financial formula is recomputed.

Construct the complete sector population from the independent plan. Map existing
close-to-close returns into the SectorMemberReturn-shaped Spark contract, excluding
the opening bar (which has no adjacent-close return) and intentionally unrequested
symbols. The latter remain population members with acquisition_scope=false; they
are not provider failures. Coverage retains every upstream scheduled slot, including
opening bars, and separates upstream eligibility from downstream sector eligibility.

The existing public Sector Gold transform remains synthetic-only. An internal engine
entry supports the verified file-owning projection. This is an in-process architecture
boundary, not a sandbox against malicious Python code or monkeypatching. Parquet
reader nullability widening is tolerated only alongside mandatory value checks and
exact regenerated-grid equality. No Python UDF or financial-row collection is used
by the production projection or calculation path; only aggregate coverage counts
reach the driver. Independent tests may collect bounded synthetic relations to run
the scalar reference.

Preserve population/eligibility/output contracts from ADR 014. A declared input may
be evaluated with verified_historical_applicability to make unverified results null;
a strict input cannot be downgraded to declared_research. Never invent evidence.
Preserve source dataset/run/manifest identities on windows and pairs, with
`approved_input_identity_sha256=SHA256` over the pipe-delimited tuple
`(sector-approved-input-v1, output_identity_sha256, input dataset, run, manifest hash)`.
This supplements, rather than redefines, sector-output-identity-v1. Threshold
calibration remains unavailable at this boundary.

## Consequences and limitations

The five-symbol pilot is incomplete for every sector: all complete-sector financial
metrics must remain null. The 25 unrequested symbols are distinct from missing IEX
minutes within requested JPM/XOM. Research membership remains an unverified declared
classification; IEX is venue-specific and provider correction finality is unproven.

The API is read-only and produces calculation frames, not a publication. Embedded
pending status on a historical manifest remains unchanged; authority comes from the
external approved registry. No registry entry, writer, acquisition, calibration or
Sector Gold publication is authorized by this ADR. Immutable files are reverified
after projection validation; continued immutability remains a storage precondition
for later lazy Spark actions. A future publication requires separate review.
