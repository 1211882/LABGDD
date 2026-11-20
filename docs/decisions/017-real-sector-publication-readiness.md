# ADR 017: Read-only real Sector Gold publication readiness

- Status: Accepted
- Independent final review: 2026-11-20
- Date: 2026-11-19
- Depends on: accepted ADRs 014, 015 and 016

## Decision

Expose `preflight_real_sector_gold` using explicit registry, dataset, UUID, manifest
digest, upstream registry and research reference. It accepts no financial frames.
Reuse the accepted verified snapshot/calculation boundary; authority/schema,
checksum, calendar, duplicate-slot, research and lineage failures raise rather than
produce a seemingly usable report. Unsupported policies also raise. No error text
from a provider or arbitrary exception is serialized into readiness evidence.

The frozen typed report uses `sector-real-publication-readiness-v1`. Canonical
sorted JSON contains immutable input metadata, processing-source fingerprint,
sector/session population, per-symbol coverage, horizon/eligibility/reason aggregates,
limitations and blockers. No execution timestamp or random run ID is added.
Technical validity, scientific eligibility and governance permission are separate.
The governance gate is always blocked; no real execution policy exists here.
Synthetic fixture evidence retains its classification and cannot authorize real use.
Invalid authority is represented by an exception, not a successful technical gate.

Evaluate every sector/session of the explicitly selected run; no implicit newest-run
selection or observed-survivor sector selection. Preserve all four horizons and
complete populations. Acquisition exclusions are separate from requested IEX gaps.
Declared research permits diagnostic eligibility with explicit historical warnings;
strict historical policy requires independently verified listing, entity and sector
classification. Undefined metrics remain distinct from primary ineligibility.
Source finality remains unproven and IEX remains venue-specific.

## Independent review contract

Check the full independently generated sector/session/horizon/window grid before
aggregation. Require unique non-null window identities and the exact multiset of
ordered expected symbol pairs for each window. Missing, extra or duplicate rows
are technical failures, never reduced populations. Reuse ADR 016's read-only grid
validator; no publication writer is called by this dependency.

Aggregate invalid-value indicators depend on every double financial expression,
so Spark cannot prune required financial evaluation. Reject NULL eligibility,
contradictory eligibility/reasons, NaN/infinity, non-null metrics on ineligible rows,
and absent essential measurements on eligible rows. Optional undefined quantities
retain their accepted reasons. A constant-return sector can retain valid zero
intensity while its Pearson pairs are undefined: technical validity passes but
complete-pair scientific readiness is blocked with
`blocked_incomplete_pair_eligibility`. This does not change any mathematical formula,
window eligibility or metric-specific undefined state.

Do not infer real-market classification merely because a synthetic marker is absent.
Preserve explicit input classification; an absent marker is `unspecified` with
`evidence_kind_not_attested`. A known real pilot's acquisition evidence does not
invent an absent manifest field. Future real publication must independently establish
the classification and intended use. Governance is blocked regardless of classification.

Source/configuration fingerprints on historical approved inputs are checked against
registry authority, not recomputed from unavailable historical implementation files.
The executing readiness source has its own byte fingerprint, rechecked before return.
Registry/research ownership, approved code and local snapshot/checkpoint isolation
remain trusted operational prerequisites; this is not a sandbox or signature system.
Fresh authority and checksum resolution detects source substitution during preflight.
Reports may immediately become stale and must never substitute for fresh resolution
at a separately authorized future publication/approval boundary.

Limit reference population to 1,000 rows and retain the 256 MiB snapshot bound.
Only bounded reference/aggregate rows reach Python. Financial data remains in Spark.
Private temporary snapshots/checkpoint/spill files are operational necessities;
the preflight writes no report file, candidate, Sector Parquet or registry entry.
Recheck source bytes, Gold authority, Silver lineage and research bytes before return.
Reports are point-in-time evidence, never capabilities to publish later.

## Future publisher design; not implemented or authorized

A future reviewed publication intent must identify real input authority, exact scope,
policy and permitted use. Revalidate immediately before writing, retain the verified
snapshot until native calculations finish, and preserve all lineage and nullability.
Use exclusive staging, exact readback schemas, captured validated artifact inventory,
comparison before immutable same-filesystem finalization and independent inspection.
Recover failed/interrupted attempts with new UUIDs; never overwrite evidence.
Resolve authority again before finalization and external approval. Source correction
or supersession invalidates an earlier readiness decision and requires fresh review.

Use a separate Sector registry with explicit manifest hashes, versions, policy and
source fingerprints, upstream identities, supersession and independent approval.
Activation needs an exact registry precondition, content-bound review, atomic
replacement and preservation of prior authorities. Readiness, complete evidence and
ADR acceptance cannot grant approval. Signatures, object-store transactions, larger
snapshots, calibrated thresholds and emergence detection remain outside this proposal.

Unresolved: permitted real publication policy, historical-evidence requirements,
review of source finality, handling of entirely ineligible diagnostic publications,
operational ownership/write isolation and the separately authorized activation plan.
Independent review accepted the read-only assessment contract after 412 full Linux
tests passed with no skips, including independent scalar/Spark parity, exact
inventories, adversarial evidence/nullability and source-substitution checks.
Acceptance grants no real-publication execution permission, no Sector dataset
approval and no registry activation. All future policy decisions above remain
separately governed; the readiness governance gate stays blocked.
