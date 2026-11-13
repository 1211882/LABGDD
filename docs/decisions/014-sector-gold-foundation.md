# ADR 014: Sector Gold calculation foundation

- Status: Proposed for architecture review
- Date: 2026-11-12
- Measurement specification: 2.0.0

## Context

The approved Gold authorities preserve individual and independently expected
symbol/session results, but a completed sector calculation could still be biased if
its population were inferred from available observations. Sector formulas also mix
several distinct estimands: constituent RMS intensity, exact equal-weight portfolio
risk, constituent-volatility dispersion, dependence, concentration and breadth.
They require explicit eligibility and cannot share a silent observed-subset
denominator.

## Decision

Introduce a calculation-only Sector Gold foundation with two implementations:

1. `labgdd.analytics.sector.calculate_sector_measurements` is the independent pure
   Python scalar reference.
2. `labgdd.processing.sector_gold.transform_sector_gold` is a native Spark
   implementation over explicit population and individual-return relations.

The population relation is independent of return availability. It records expected
members, acquisition scope, session membership, historical evidence,
`sector-complete-population-v1`, and the research-reference fingerprint. The Spark
boundary in this milestone accepts only `synthetic_fixture`; it cannot claim that an
arbitrary frame is approved real evidence. A future real-data runner must start with
`resolve_approved_gold`, bind the resolved manifest and contract fingerprints to the
population projection, and receive separate architecture approval.

Complete-sector financial metrics are null when any expected member is outside
scope, absent, invalid, ineligible, historically unverified under strict policy, or
not applicable for the full session. Missing observations are never filled, zeroed
or time-compressed. Pair correlations have explicit pair rows and require every
expected pair for the headline mean. Threshold breadth is unavailable unless every
member/horizon threshold is supplied by a separate calibration authority.

`volatility_dispersion` retains the v2.0.0 population standard deviation of member
realized volatilities. The milestone does not invent a distinct per-minute
cross-sectional return-dispersion estimand despite informal use of that phrase.

## Consequences

Synthetic fixtures can now verify scalar/Spark parity and fail-closed population
semantics for H in 5, 15, 30 and 60. The exact equal-weight calculation uses
`log1p(mean(expm1(r)))`, never the mean-log proxy. No runner, persistence schema,
publication registry, approved Sector Gold dataset, emergence calibration or real
sector result is introduced. ADR approval and a publication design remain future
review actions.
