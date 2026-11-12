# Phase 2 Milestone 4: bounded Spark expected-series integration

Date: 2026-11-04. This milestone adds a review-only Spark population path. It does
not publish a new Gold dataset, change an approved registry, acquire data or
calculate sector statistics.

## Explicit authority relation

`processing.expected_series_spark.expected_series_frame` converts an approved
in-memory `ExpectedSeriesPlan` into an explicit-schema Spark DataFrame. Callers must
supply source, feed, timeframe and adjustment; those fields identify the expected
observation series but do not assert source completeness.

Each row retains symbol/sector, session ID and UTC bounds, membership/universe
versions and applicability, intra-session membership status, evidence/listing/entity
statuses, XNYS calendar/tzdata/schedule provenance, eligibility policy,
`expected-series-left-join-v1`, a deterministic plan SHA-256 and a deterministic
identity SHA-256. `expectation_authority=approved_research_reference` means the
research reference is approved; it does not mean historical listing is verified.

Default `declared_research` mode permits a full-session declared cohort row while
preserving its unverified evidence fields. `strict_historical=True` makes listing,
entity or evidence that is not independently verified explicitly ineligible with
`unverified_historical_applicability`. Partial or changing membership is ineligible
with `membership_not_full_session`.

## Gold left join

`transform_individual_gold(canonical, horizons, expected_series=None, *,
required_eligibility_policy=None, approved_expected_plan_sha256=None)` preserves
the legacy behavior when the optional relation is absent. When present, it requires
an independently derived approved-plan digest and validates the exact schema and
types, mandatory values, authority and enumerated states, temporal bounds,
membership and eligibility consistency, XNYS provenance, logical uniqueness and
recomputed row/plan fingerprints. A 64-character value supplied only inside the
relation is not accepted as verified provenance. Exact duplicate expectations are
idempotent; conflicting definitions for one observation-series/session identity
fail closed. A caller requiring strict historical applicability must pass
`required_eligibility_policy=verified_historical_applicability`, so changing an
eligibility boolean or presenting the declared-research policy cannot bypass the
evidence requirements.

The transform explodes scheduled minutes from expected session bounds and left-joins
canonical Silver using symbol, source, feed, timeframe, adjustment, session and
event time. Before the join, canonical logical keys must be unique and their types,
source/feed/timeframe/adjustment, session geometry, one-minute bar end, calendar,
membership, observation identity, prices and canonical quality status are validated.
No conflicting observation is arbitrarily selected. Canonical calendar and
membership provenance must match the expectation. Missing, ineligible or
provenance-inconsistent observations retain null financial measurements and
controlled reasons; observation existence remains a separate fact. Expected
identities therefore survive even when a symbol or entire scheduled session has no
canonical rows.

The v2.0.0 financial definitions are unchanged: no overnight link, gap compression,
zero substitution or reanchoring is allowed. Rolling and fixed windows preserve the
existing scheduled anchors and reasons. Research expectation, provider observation,
canonical validity, financial eligibility, source completeness and correction
finality remain distinct states.

## Bounded evidence

Deterministic fixtures cover multiple symbols/sectors, absent symbols, a wholly
absent scheduled session, opening/interior gaps, membership changes, variable
constituents, holiday/DST/early-close geometry, duplicate and contradictory
expectations, mandatory nulls, incorrect Spark types, forged fingerprints, duplicate
canonical keys, invalid bar ends/quality states, contradictory provenance, strict
historical mode with observed prices, and all horizons against the Python reference.

The approved 390-row AAPL Silver evidence was loaded read-only using bounded PyArrow
on Windows because native Hadoop directory enumeration is unavailable. The
calculation then ran in native Spark. Filtering the new full 30-symbol expected
population to AAPL reproduced 390 return rows, 389 eligible adjacent returns, 1,556
rolling rows, 1,450 eligible rolling rows and 120 eligible fixed windows exactly on
the legacy output columns. The other 29 expected symbols remained null and labeled
with unverified research evidence; this is not a verified historical-absence claim.

Milestone 5 exercises that reviewed path through the separate
`labgdd-expected-gold` runner and `config/expected_gold.toml`. It uses a new dataset
identity, retains expected-plan fingerprints and approved Silver lineage, and uses
the established staging/readback/checksum protocol. The bounded native Linux run
retains `pending_architecture_review` as immutable creation metadata. After
independent validation, registry v1.1 explicitly approves its dataset/run identity,
schema and fingerprints without rewriting the manifest or selecting by recency.
