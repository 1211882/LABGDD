# Bronze v2 observation identity migration proposal

Date: 2026-11-03. Status: proposed, not implemented. This document does not
authorize migration, acquisition, or mutation of Bronze v1 evidence.

## Decision boundary

Bronze v1 remains sufficient for the approved bounded AAPL Silver publication:
the acquisition manifest and artifacts are immutable and checksummed, Silver
validates lowercase SHA-256 form and cross-row collisions, and no cryptographic
reconstruction claim is made. Bronze v1 therefore remains readable and supported.

Bronze v2 is required before either of these stronger claims is made:

1. independently reconstruct every observation ID without inferring its source
   locator from response ordering; or
2. combine the full research universe across multiple acquisitions/transports
   while treating observation IDs as cryptographically verified provenance.

The limitation is concrete: `processing.historical.observation_id` hashes the
length-prefixed tuple `(identity_version, acquisition_id, page, symbol, row,
response_body_sha256)`, but Bronze v1 observation rows retain only the resulting
ID, symbol and normalized raw JSON. The response evidence retains page/body data,
but there is no explicit observation-to-page/row/body-hash mapping. Reconstructing
that association would depend on parsing and ordering assumptions rather than
stored provenance.

## Proposed additive row provenance

A future Bronze v2 observation row must retain these nonsecret fields:

| Field | Contract |
|---|---|
| `observation_identity_version` | Exact algorithm identifier, initially `bronze-observation-v1`; a new algorithm requires a new value |
| `acquisition_id` | UUID of the immutable acquisition namespace |
| `transport` | Controlled value such as `alpaca_rest`; prevents locator namespaces from being conflated |
| `response_page` | Positive provider-request page ordinal used by the current encoder |
| `provider_symbol` | Exact symbol string used by the encoder, separate from later entity mapping |
| `provider_row_index` | Zero-based row ordinal within the symbol list used by the encoder |
| `response_body_sha256` | SHA-256 of the exact retained response body bytes |
| `response_artifact` | Relative path to the checksummed response artifact containing those bytes |
| `raw_json` | Lossless observation representation used for inspection; exact response bytes remain authoritative |
| `observation_id` | Stored lowercase SHA-256 result reconstructed from the versioned tuple |

`received_at`, request parameters, source, feed, adjustment, timeframe and schema
version remain required provenance but do not enter the current ID tuple. Secrets
never enter rows, manifests or the hash envelope. A streaming identity version
would need an explicit connection/sequence namespace and must not reuse REST page
semantics.

## Compatibility and migration

- Bronze v1 is never rewritten or relabelled as v2.
- Readers continue accepting v1 with verification level
  `format_and_collision_checks_only_provenance_locator_unavailable`.
- V2 readers recompute the identity and fail closed on any mismatch, missing
  response artifact, body checksum mismatch or locator outside the response.
- Any v1-to-v2 conversion creates a new dataset version with source manifest
  hashes and an explicit `derived_from_bronze_v1` limitation. It cannot claim the
  locator was observed unless the mapping is independently proven.
- Silver manifests record the Bronze schema/identity version and verification
  level for every input. V1 and v2 may be processed together only if the weaker v1
  verification level remains explicit; v2 does not retroactively strengthen v1.

## Acceptance tests before implementation approval

1. Recompute every ID from stored fields and exact response-body hash.
2. Reject changed acquisition, page, symbol, row, body hash or algorithm version.
3. Reject missing/dangling response artifacts and body checksum mismatches.
4. Prove locator uniqueness within an acquisition and deterministic replay.
5. Show independent acquisitions produce different IDs for equal market payloads.
6. Preserve invalid observations and extra provider fields without normalization
   affecting identity.
7. Cover empty symbol lists, pagination, repeated bodies, duplicate market bars,
   partial acquisitions and controlled failure publication.
8. Demonstrate v1 compatibility without upgrading its verification claim.
9. Validate REST and future streaming locator namespaces cannot collide.
10. Run repository security checks proving no credentials enter the envelope.

No Bronze v2 schema or ingestion change is implemented in Phase 2 Silver
finalization. A separate reviewed task is required.
