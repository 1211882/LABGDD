# ADR 006: Capture pages first and publish immutable local acquisition manifests

Date: 2026-11-03. Status: implemented for the first Phase 1 milestone.

Decision: use standard-library HTTPS with a concrete injectable transport rather
than an SDK/framework. Preserve successful response bytes in Parquet before JSON
validation, with individual observation projections and stable acquisition/page/row
identity. Archive invalid data and duplicates, without selecting source revisions.
Use a new exclusive UUID directory per acquisition, exclusive artifact creation,
checksums and a terminal manifest published last via an atomic no-replace hard link.
`complete` means exhausted pagination; `failed` preserves the acquired subset;
missing terminal manifest means incomplete. Coverage/source completeness are
separate from execution completion. These local filesystem guarantees are tested
on NTFS; object storage and power-loss durability are not claimed.

Keep calendar/Parquet dependencies in an optional ingestion extra, leaving domain
and base imports independent. Record explicit raw IEX parameters, versions,
package fingerprint, calendar UTC schedule/tzdata hashes and cohort limitations.
Do not turn a common calendar into invented listing or sector-membership history.

Consequences: one bounded historical acquisition can be audited independently;
no overwrite, automatic recovery/delete or source-revision inference is needed.
No Silver/analytics state is introduced. Detailed contracts, failure semantics,
identity encoding, limits and tests are in `docs/architecture/PHASE1_INGESTION.md`.
Future recovery and object-store publication require their own reviewed design.
