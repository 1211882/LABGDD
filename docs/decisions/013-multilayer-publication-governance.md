# ADR 013: Explicit multi-layer publication governance

**Status:** Accepted
**Date:** 2026-11-11

## Context

Milestone 6 produced immutable Bronze, Silver, Individual Gold and Expected-Series
Gold candidates under bounded validation authority. Filesystem completion could not
make them authoritative, Silver registry v1.0 could represent only one authority,
and Bronze had no external approval registry. Approval also had to preserve the
previously accepted AAPL publications without selecting directories by recency or
rewriting immutable manifests.

## Decision

Adopt explicit external publication authority at every persisted layer. Bronze
registry v1.0 pins acquisition identity, manifest, raw inventory, schema, source and
calendar fingerprints. Silver registry v1.1 supports multiple exact dataset/run
authorities while retaining valid v1.0 read compatibility and adds ordered Bronze
lineage plus schema, resolution, source, request and processing fingerprints. Gold
continues to use registry v1.1 with independent schema-v1 and schema-v2 authorities.

Separate publication into two non-interactive operations. `review` validates the
complete proposal and creates a hash-bound report without registry mutation.
`approve` requires an exact reviewed step, dataset and UUID, verifies the current
registry precondition and upstream active authorities, then performs one atomic
same-filesystem registry replacement. Steps are ordered Bronze, Silver, Individual
Gold and Expected-Series Gold. Exact repeated activation is idempotent; divergent or
tampered state fails closed.

The reviewed Milestone 7 plan was explicitly authorized and activated on 2026-11-11.
The active authority inventories are two Bronze acquisitions, two Silver runs and
four Gold runs. Immutable data and manifests were not changed.

## Consequences

Downstream consumers can resolve the five-symbol pilot only by explicit registered
identity. The original AAPL Bronze, Silver and Gold authorities remain independently
resolvable. A complete but unregistered directory remains non-authoritative, and the
Milestone 6 `authorized_bounded_validation` registry remains historical validation
evidence rather than an alternative approved path.

Atomic replacement is scoped to a local same-filesystem registry file, not a
distributed catalog transaction. A hard interruption before replacement can leave
an orphan pending file, but cannot expose a partially written active registry; the
next attempt fails closed until that file is inspected. Registry approval does not
upgrade the scientific evidence: IEX coverage, provider correction finality and
historical listing/entity/sector membership remain unverified limitations.
