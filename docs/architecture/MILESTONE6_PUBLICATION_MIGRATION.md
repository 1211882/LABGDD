# Milestone 6 candidate publication migration

Date: 2026-11-11. Status: implemented, independently reviewed and activated by
explicit user authorization. The prior Milestone 6 implementation tag did not grant
publication authority; Milestone 7 completed the separate migration described here.

## Silver authority migration

`config/silver_publications.toml` registry v1.0 currently has one approved AAPL
authority. It must not be replaced. Before approving the five-symbol Silver run,
extend the Silver registry and resolver to a backward-compatible multi-authority
schema, analogous to Gold registry v1.1. Acceptance tests must prove v1.0 reading,
explicit dataset/run selection, duplicate-identity rejection, superseded-run
rejection, and exact manifest/request/source/artifact verification.

Only after independent publication review may an additional `approved` entry pin:

- dataset `silver-pilot-five-symbol-20260914-v1`;
- run `f9d4bcc6-5a51-4c37-8d74-86cf9cee1ca2`;
- manifest SHA-256 `f0ff790cfe0200e94c407010cfc4f2a8bfe93110d580f751b9f934095f9a15f8`;
- processing source SHA-256 `46caa29f76fe5f0d381c2ba4800fbd67f76d8940b8de1dedc12d4ccd37297de8`;
- request SHA-256 `0185df0f7e9ad375069ad43e99cf604e2c0b6387763ab7f5471044b943cb1d70`.

Silver resolver v1.1 and the reviewed proposal
`config/silver_publications.proposed.toml` implement this contract. The proposal was
activated as `config/silver_publications.toml` with SHA-256
`969309c9d1df647095351cd7069f6be19c97d09940cd07f816f2bcf71affdc2a`.
The current `config/milestone6/silver_publications.toml` remains validation-only and
must not be renamed, copied, or interpreted as publication approval.

## Gold authority migration

Gold registry v1.1 already supports multiple explicit authorities. A future review
may append entries without replacing the approved AAPL or expected-series datasets.
Each entry must be constructed from the immutable manifest and independently verify
all artifact checksums, Silver lineage, schema/measurement versions, source and
configuration fingerprints, and contract fingerprints where applicable.

Candidate identities are:

- Individual Gold dataset `gold-pilot-five-symbol-20260914-individual-v1`, run
  `310941bd-7ff9-4e25-971a-febec753c9e5`, manifest SHA-256
  `c9ecea6009363f589bb57365acbccd6454fe8e122c3d737fbe3fd71363badad0`.
- Expected-Series Gold dataset
  `gold-study-cohort-20260914-multisymbol-expected-v1`, run
  `39ab367c-c6d2-480d-8c94-f35f767ed289`, manifest SHA-256
  `d97b0e48c8463b6a97aa58e576e7f24998b4246f496fabaa286982177ab605b2`.

Individual Gold schema v1 legitimately has no embedded publication-status field;
use `manifest_publication_status = "not_recorded"` if approved, consistent with the
existing schema-v1 authority. Expected-Series Gold retains immutable
`pending_architecture_review` creation metadata; an external registry decision may
approve the exact evidence without rewriting it.

No migration may select a filesystem directory by recency, mutate an existing
manifest, or infer approval from `status = "complete"`. A new acquisition or
reprocessing run requires new identities and a separate review.

## Activation plan

`config/publication_governance_milestone7.toml` defines four exact, ordered steps:
Bronze, Silver, Individual Gold and Expected-Series Gold. The inactive proposed
registries preserve all existing AAPL/Silver/Gold authorities and superseded runs.
`docs/architecture/PHASE2_MILESTONE7_APPROVAL_REVIEW.json` records the successful
read-only review, exact proposal hashes and evidence summaries. Explicit authorization
then executed each step in order with the exact dataset and UUID. Final active
registry SHA-256 values are Bronze
`87872458f765223dfbd3619c3ab054584f7967889d645a5053c1c28f0f89c439`, Silver
`969309c9d1df647095351cd7069f6be19c97d09940cd07f816f2bcf71affdc2a`, and Gold
`2f9aaf217ead86fea588dc6fb25f4faa9e4ceb8ed832545218ecf9ac95badf51`.
