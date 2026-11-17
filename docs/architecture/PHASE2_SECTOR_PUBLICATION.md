# Milestone 10: Sector Gold publication foundation

Implementation candidate governed by Proposed ADR 016. ADRs 014/015 remain accepted
and unchanged. Real Sector Gold publication and external approval are deferred.

## Interfaces

`SectorPublicationRequest` validates exact identity, source hash, eligibility and
schema contracts. `load_sector_publication_request(path)` accepts TOML with exactly
the dataclass fields: dataset_version, gold_registry, silver_registry,
research_reference, input_dataset_version, input_run_id, input_manifest_sha256,
eligibility_policy, sector_schema_version and evidence_kind. References are portable
relative project paths. Unknown keys, versions, policies or modes fail closed.
There is no default real-data configuration and no publication CLI in this milestone.

`publish_synthetic_sector_gold(spark, settings, request, run_id=...)` requires a
synthetic marker in the verified source manifest, invokes
`calculate_approved_sector_gold`, and publishes native Spark windows/correlations
under `data/sector_gold/dataset=<id>/run=<UUID>`. It accepts no external financial
frames and never changes active registries. The source is revalidated before the
terminal rename. Dataset prefixes alone do not establish synthetic provenance.

`inspect_sector_candidate(spark, settings, request, manifest, manifest_sha256)` is
read-only and idempotent. It verifies exact input authority, manifest contract,
inventory, schemas, physical UTC timestamps, source-bound identities, independent
expected windows, complete pairs, financial nullability, reason/count summaries and
population evidence. It returns a metadata document, never an approved-publication
object or lazy source-dependent financial frame.
Native recomputation additionally binds inherited output values/provenance to the
approved input. Independent scalar parity is exercised separately in tests.

## Lifecycle and contracts

`GoldRun(..., sector_outputs=True)` reuses existing local finalization semantics in
a separate namespace. Writes use unique staging UUIDs and error-if-exists Parquet.
Successful physical/scientific validation precedes terminal manifest creation and
atomic directory rename. Handled failures retain `_failed` evidence; crashes or
filesystem failures can retain staging, which is never complete or authoritative.
Use a fresh UUID after inspecting an interrupted attempt. Existing completed/failed
evidence is never overwritten. These are local same-filesystem semantics, not
distributed transactions or object-store atomicity.

Sector schema 1.0.0 preserves accepted metrics and extends pair provenance/identity.
Integral counts are int64; financial values remain doubles. Mandatory values are
checked explicitly despite Parquet reader nullability widening. All timestamp fields
are UTC microseconds. Complete expected population and pair inventories are checked
independently of observed financial availability. Every ineligible financial value
remains null. Primary reasons and metric-specific undefined states remain distinct.

Bounded reference collection is limited to 1,000 population rows; financial rows
stay in Spark. The Milestone 9 snapshot limit is unchanged at 256 MiB. Local checkpoint
blocks belong to the Spark session; stop it to release resources. Block loss fails
closed. One output file per dataset avoids small partition files for bounded tests;
larger workloads require separate review.

## Authority and scientific limits

Candidates have `status=complete`, `publication_status=pending_architecture_review`,
and `evidence_kind=synthetic_fixture`. Completion, independent review and external
approval are separate states. Existing Gold resolvers and governance commands do
not activate Sector candidates. ADR 016 specifies a future separate registry; none
is created or populated here.

The complete/incomplete/constant synthetic fixtures exercise three five-member
sectors and all horizons. They are not observations from Alpaca. Existing real
evidence is used only for read-only regressions and a refusal test proving it cannot
enter this synthetic publication API. The original 30-symbol six-sector research
cohort and all frozen Bronze/Silver/Gold evidence remain unchanged. No research
classification is upgraded to verified historical membership. Threshold calibration,
emergence detection and any later platform milestone remain outside this scope.
