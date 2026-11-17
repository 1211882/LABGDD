# ADR 016: Immutable Sector Gold publication foundation

- Status: Proposed
- Date: 2026-11-17
- Depends on: accepted ADRs 014 and 015; existing local Gold publication lifecycle
- Scientific measurement specification: 2.0.0

## Context

Sector calculations now consume exact approved Expected-Series Gold evidence through
a file-owning verification boundary. Calculation completion cannot confer publication
authority. A separate immutable candidate contract is needed before real publication
can be considered. The existing real pilot has no complete five-member sector.

## Proposed decision

Use `processing.sector_publication.publish_synthetic_sector_gold` with a strict
`SectorPublicationRequest`. This milestone provides no real-publication execution
mode or approval CLI. The source Gold manifest must contain
`evidence_kind=synthetic_fixture` within its exact approved manifest fingerprint.
An argument or DataFrame label alone cannot mark an existing real manifest synthetic.
Require explicit input dataset, UUID, manifest SHA-256, registries, research reference
and eligibility policy. Dataset identifiers begin `sector-synthetic-`.

Call the accepted Milestone 9 API, preserving its bounded verified byte snapshot
and eager local checkpoint. Do not return to lazy reads of original source files.
Build the independent population before examining financial availability. Record
the full bounded reference population and coverage in the manifest, not financial
observations on the driver. Input and output lineage remain distinct.

Reuse `GoldRun`'s exclusive staging directory, terminal manifest, checksums and
same-filesystem directory rename. Its default Individual/Expected Gold behavior
remains unchanged. An explicit sector option uses `data/sector_gold` and required
`windows` and `correlations` datasets; it cannot replace existing Gold authorities.
Completed and failed UUIDs cannot be reused. An interrupted process leaves staging
unpublished; a handled failure is isolated under `_failed`. Filesystem failure may
leave recoverable staging, never authority. Recovery inspects evidence and uses a
new UUID; it must not silently overwrite, resume or promote an interrupted run.

## Dataset and physical contract

Sector schema `1.0.0`, calculation contract `sector-calculation-v1`. Preserve every
accepted metric, reason, policy and identity field; widen integral publication
counts/indices to int64 without recasting financial values. Exact ordered schemas
are defined by `sector_publication_schema`. Parquet's nullable physical fields do
not relax mandatory-value validation. All timestamps must be UTC microseconds;
INT96, naive Arrow timestamps and incompatible types are rejected. No PyArrow
financial writer or driver-side financial-row collection is introduced.

Windows preserve population identities/counts, membership applicability, historical
verification counts, policy versions, calendar/source provenance, exact boundaries,
financial nullability and metric reasons. Pairs retain the same population/source
provenance, ordered left/right symbols, nullable Pearson values and pair reasons.
`sector-pair-identity-v1` hashes the source-bound window identity and ordered symbols;
it supplements the existing window/source identity contracts rather than changing them.

Native Spark writes one compressed Parquet file per logical dataset in this bounded
foundation. There is no symbol/sector partition explosion. Row order is not a Spark
relation contract; replay compares logical multisets and deterministic keys, not
run-specific filenames or binary Parquet identity.

Validate exact schemas, independent window grid, pair completeness/uniqueness,
pair/window provenance, mandatory values, counts, finite metrics, eligibility,
reasons and recomputed identities. Compare physical readback bidirectionally against
the calculated frames. Tests independently compare windows and pairs to Python.
Revalidate source Gold, upstream Silver, research bytes and processing fingerprint
immediately before finalization. No partial output may receive complete status.

## Manifest and inspection

Record dataset/run/schema/calculation/measurement versions; pending publication
status; synthetic evidence kind; exact input Gold and Silver identities/digests;
input and sector source/configuration fingerprints; research/plan/calendar hashes;
runtime versions; population/evidence rows and coverage; row/eligibility/reason
counts; logical schemas; physical artifact inventory, sizes and checksums.
`inspect_sector_candidate` requires exact manifest hash, re-resolves upstream
authority and independently expected population, checks physical/semantic evidence,
and verifies inventory again after inspection. It also recalculates through the
accepted native engine and compares every inherited financial/provenance column
bidirectionally, rejecting semantically forged artifacts even when their candidate
checksums have been resealed. It returns metadata, not lazy financial frames, and
confers no approval. Native recomputation is not an independent mathematical
implementation: Python scalar parity remains a separate test/review requirement.

## Future external authority contract (not implemented or activated)

Propose separate `config/sector_publications.toml`, registry version `1.0.0`, with
append-preserving authority inventory and explicit superseded inventory. An authority
entry must bind dataset/UUID, exact manifest path/hash, Sector schema/calculation and
measurement versions, evidence kind and intended use, source/configuration/research/plan/policy fingerprints, exact
upstream Gold/Silver identities, approval timestamp and independent decision.
Resolution must reject missing, unregistered, pending and superseded identities,
verify manifest/artifact inventories/checksums, and resolve upstream Gold authority
again. Never choose the newest directory. Preserve all existing Gold registries.

Any future activation must use the existing governance principles: explicit plan,
exact current-registry precondition, content-bound review report, atomic local
replacement and prior-authority preservation. Extending the governance CLI or adding
entries requires separate architecture review and execution authorization. A complete
candidate alone cannot satisfy authority resolution. Synthetic candidates must never
be promoted as real market evidence.

## Scientific safeguards and deferred decisions

Preserve complete-population denominators, exact equal-weight portfolio returns,
stable Pearson degeneracy, all four horizons and normative primary reason precedence.
Do not fill missing bars or reduce populations to survivors. The threshold-breadth
field is preserved, but calibration authority does not yet exist at this boundary;
the value stays null with its explicit reason. Requests cannot inject thresholds.

Future real publication needs a separately authorized API using Milestone 9's
file-owning calculation, immediate authority revalidation, explicit evidence-kind
contracts, independently reviewed results and the external registry above. The
current one-session five-symbol pilot cannot justify official complete-sector
financial values. IEX coverage, provider corrections and historical research-sector
classification remain limited evidence. No acquisition or real publication is
authorized here. Scaling beyond bounded reference collection/snapshot size, durable
checkpoint recovery and object-store transactions are deferred. This ADR remains
Proposed until independent review; test success does not accept it automatically.
