# ADR 014: Sector Gold calculation foundation

- Status: Accepted
- Date: 2026-11-13
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

Eligibility is an explicit input contract rather than a calculation flag. Every
population row carries `eligibility_policy` (`declared_research` or
`verified_historical_applicability`) and
`eligibility_policy_version=sector-eligibility-policy-v1`. Missing, unknown,
mixed or differently versioned values fail closed. Both fields are preserved in
scalar and Spark outputs and in `sector-output-identity-v1`, whose SHA-256 also
binds sector, session, horizon, window index, sorted symbols, population policy and
research-reference fingerprint.

Pearson degeneracy is determined from the mathematical range (`min == max`), not
from a rounded centered sum or an epsilon. Nonconstant vectors are translated by
their minimum before centering and scaled before products are accumulated. This
preserves representable small variation and avoids overflow/underflow without
reclassifying legitimate variation as constant.
The Spark headline mean folds pair values in sorted symbol-pair order rather than
using an unordered floating aggregate, making replay invariant to partition order.

Primary fail-closed reasons have normative precedence:
`member_outside_acquisition_scope`, `incomplete_sector_population`,
`unverified_historical_applicability`, `missing_expected_member`,
`missing_observed_bar`, then `ineligible_individual_return`. Metric-specific
undefined states such as constant correlation, insufficient pairs and incomplete
threshold calibration remain separate.

`volatility_dispersion` retains the v2.0.0 population standard deviation of member
realized volatilities. The milestone does not invent a distinct per-minute
cross-sectional return-dispersion estimand despite informal use of that phrase.

## Consequences

Synthetic fixtures can now verify scalar/Spark parity and fail-closed population
semantics for H in 5, 15, 30 and 60. The exact equal-weight calculation uses
`log1p(mean(expm1(r)))`, never the mean-log proxy. No runner, persistence schema,
publication registry, approved Sector Gold dataset, emergence calibration or real
sector result is introduced. Acceptance approves the calculation contract only;
publication design and any real Sector Gold authority remain future review actions.
