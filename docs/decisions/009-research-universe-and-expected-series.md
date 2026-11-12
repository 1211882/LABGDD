# ADR 009: Research universe and expected-series authority

**Status:** Accepted
**Date:** 2026-11-03

## Decision

Preserve `study-cohort-v1` as the project-defined six-group/30-symbol research
taxonomy. Store its declared applicability and evidence limitations separately from
observed bars. Generate expected symbol/session identities from that versioned
reference plus explicitly requested approved XNYS sessions. Never infer the expected
population from observed Silver rows.

Keep the generator pure and deterministic and expose only a narrow calendar adapter.
Do not integrate it into the frozen Silver/Gold pipelines until the expected Spark
relation and migration acceptance tests receive separate approval.

## Consequences

Wholly missing symbols and observed-data sessions can remain in future coverage
denominators. Holidays produce no scheduled rows. Membership changes remain explicit
and overlapping definitions fail. No synthetic prices are created. The reference is
a declared research assumption with unverified historical listing, entity and sector
evidence; it does not support official historical industry claims.
