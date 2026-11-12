# ADR 010: Expected-series authority in bounded Spark Gold processing

**Status:** Accepted
**Date:** 2026-11-04

## Decision

Add an optional expected-series authority to
`processing.gold_batch.transform_individual_gold`. The existing two-argument path
continues to derive slots from observed canonical series and remains unchanged for
the frozen AAPL publication. The new path requires an explicitly supplied Spark
relation produced from an approved `ExpectedSeriesPlan`, expands scheduled minute
slots from that authority, and left-joins canonical observations.

Version the new population policy as `expected-series-left-join-v1`. Preserve the
expected-plan and expected-identity SHA-256 values on every output. An expected
identity is research evidence, not proof of listing or provider completeness.
Strict historical mode marks unverified applicability ineligible.

Treat the Spark relation as untrusted input. Require the exact typed schema and
validate nulls, states, temporal bounds, membership consistency, logical uniqueness
and recomputed identity fingerprints. The caller must also provide an approved-plan
SHA-256 derived independently from the approved `ExpectedSeriesPlan`; a syntactically
valid digest carried only by the relation is not an authority boundary. Exact
duplicate rows are idempotent, while contradictory definitions fail closed.

Before the left join, validate canonical logical-key uniqueness, one-minute temporal
geometry, accepted quality status and relevant source, calendar and membership
provenance. Do not deduplicate conflicting canonical observations. A missing,
ineligible, invalid or provenance-inconsistent observation remains visible as an
observation state, but every affected financial value is null with a controlled
reason.

Do not connect this path to the frozen Gold runner or publication registry in this
milestone. A persisted run requires a distinct dataset/configuration identity and
must remain unapproved until a separate publication review.

## Consequences

Wholly absent symbols and scheduled sessions produce null measurement slots with
controlled reasons rather than disappearing. No bar is fabricated and no missing
close becomes a zero return. Existing window anchors, membership boundaries and
v2.0.0 formulas remain unchanged. Spark global row ordering is not promised;
logical identities and fingerprints are deterministic.
