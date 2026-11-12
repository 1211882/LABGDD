# ADR 011: Separate expected-series Gold publication

**Status:** Accepted
**Date:** 2026-11-05

## Decision

Publish the reviewed expected-series Spark path only under a new dataset identity
and a dedicated request/runner. Resolve one explicit approved Silver run through
`config/silver_publications.toml`; never discover inputs or outputs by directory
recency. Derive the expected population independently from the versioned research
reference and pinned XNYS schedule, and bind the run to an independently computed
expected-plan SHA-256.

Reuse the existing local `GoldRun` protocol: write native Spark Parquet to a unique
staging run, validate physical schemas and distributed readback, checksum the exact
artifact inventory, write a terminal manifest, then atomically rename. Validation
failure publishes only sanitized failed evidence. Completed run identities are
immutable and cannot be overwritten.

Preserve expected, observed, canonical, financial-eligibility, historical-evidence
and source-completeness states as separate fields. Retain missing expected slots;
never fabricate prices, fill minutes or infer historical absence from a missing IEX
bar. Use Gold schema v2.0.0 because these rows add the expected-series authority and
observation-provenance columns to the frozen individual Gold schema.

Extend the Gold registry to v1.1 as an array of explicit authorities. Each authority
pins dataset/run identity, Gold schema and measurement versions, Silver lineage,
manifest/source/configuration fingerprints, optional contract fingerprints and the
publication status embedded when the immutable manifest was created. Continue to
read the v1.0 singleton format. Registry approval is an external review decision; it
does not rewrite a manifest whose embedded status records
`pending_architecture_review` at creation time.

## Consequences

The bounded 30-symbol session has 11,700 return slots even though canonical input
exists only for AAPL. The other 11,310 slots remain explicit null observations and
financial measurements. The published manifest records Silver, research, calendar,
configuration, source and expected-plan fingerprints plus counts and reason
distributions. This makes the evidence reproducible without claiming consolidated
market coverage, correction finality or verified historical listing/membership.

The reviewed candidate is approved externally by explicit dataset and run identity.
Its candidate-generating source is preserved at commit
`cbf126beb732d3a2f43c4ffae3381a69a3a624f0`, whose package fingerprint exactly
matches the manifest. The original AAPL authority remains independently approved.

The publication protocol remains local-filesystem scoped; it is not an object-store
transaction and does not establish distributed scale. Architecture approval and a
registry entry are required before any future run becomes authoritative.
