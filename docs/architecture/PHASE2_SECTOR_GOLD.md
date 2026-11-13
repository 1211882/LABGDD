# Phase 2 Milestone 8: Sector Gold calculation foundation

Status: synthetic calculation candidate; no Sector Gold publication exists.
Date: 2026-11-12. Measurement specification: 2.0.0.

## Scope and authority

Sector inputs are two independent relations. The population relation declares every
expected member for a sector/session and records membership, historical-evidence,
acquisition-scope, population-policy and research-reference provenance. The return
relation contains only explicitly supplied individual close-to-close return results.
Expected members do not disappear when their returns are absent.

The Spark schema currently accepts only `input_authority=synthetic_fixture`. This is
deliberate: Milestone 8 proves formulas and execution parity without converting a
64-character string or local directory into publication authority. A future real
runner must call `adapters.gold.resolve_approved_gold` for an explicitly selected
Expected-Series Gold dataset/run, verify its manifest and artifacts, derive the
population relation from that evidence, and bind the resulting fingerprint before
calling the calculation layer. No newest-directory discovery is permitted.

## Window and population contract

For each expected sector/session and H in `{5,15,30,60}`, fixed windows retain the
v2.0.0 nonoverlapping anchors. A window `(a,b]` has H scheduled return ends and uses
the complete expected member set. The outputs preserve:

- sorted expected symbols and expected-member count;
- independently verified, acquisition-scope, completely observed, canonically valid
  and financially eligible member counts;
- missing and excluded counts, full-session membership applicability;
- expected/valid correlation-pair counts;
- population-policy version and research-reference SHA-256.

`strict_historical=True` requires independently verified applicability, listing,
entity identity and sector classification for every expected member. Declared-only
synthetic policy is supported explicitly for formula testing but must not be
described as verified history.

The primary fail-closed reasons are `missing_expected_member`,
`member_outside_acquisition_scope`, `missing_observed_bar`,
`ineligible_individual_return`, `incomplete_calculation_window`,
`insufficient_valid_pairs`, `constant_return_series`,
`unverified_historical_applicability` and `incomplete_sector_population`.
`incomplete_threshold_calibration` applies specifically to threshold breadth.

## Measurements

The implementation preserves the definitions in `MEASUREMENTS.md`:

- constituent intensity: `sqrt(mean_i(sum_t r[i,t]^2))`;
- exact equal-weight portfolio return per minute:
  `p[t]=log1p(mean_i(expm1(r[i,t])))`, then `sqrt(sum_t p[t]^2)`;
- separately named mean-log proxy: `sqrt(sum_t(mean_i r[i,t])^2)`;
- volatility dispersion: population standard deviation of member realized
  volatilities;
- pairwise Pearson correlation over synchronized H-return vectors, with a null
  headline if any expected pair is undefined;
- HHI from member realized-variance shares and effective breadth
  `(1-HHI)/(1-1/N)` when defined;
- threshold breadth only with all member/horizon thresholds and strict `>`.

Zero realized variance is valid. Zero total variance makes HHI/effective breadth
and correlations undefined. A single-member sector has zero volatility dispersion
but no pair correlation. Financial values are null for incomplete populations.

## Implementation boundaries

`src/labgdd/analytics/sector.py` has no Spark or persistence dependency and is the
scalar arithmetic authority. `src/labgdd/processing/sector_gold.py` validates exact
Spark schemas, rejects duplicate/contradictory rows, constructs scheduled slots from
the population relation, and uses native Spark expressions only. It does not use
Python UDFs or collect financial data to the driver. The Spark session must be UTC;
tests compare timestamp instants as epoch microseconds so host-local timezone display
cannot alter window identity.

No output writer, manifest, registry entry or runner is present. Existing Bronze,
Silver and Gold files and registries are unchanged.

## Scientific limitations and future gates

Synthetic results are not market evidence. Before real Sector Gold publication:

1. historical symbol/entity/listing/sector membership must be independently
   reviewed for the intended dates;
2. an approved Gold resolver-to-population projection must bind exact dataset/run,
   manifest, artifacts and contract fingerprints;
3. threshold breadth and emergence require a separately versioned, train-only
   calibration artifact with the v2.0.0 minimum-history policy;
4. storage schema, immutable publication and external authority require separate
   ADR and acceptance tests;
5. one-session IEX observations remain venue-specific and cannot establish provider
   correction finality or exchange-wide absence.

Milestone 8 does not publish sector metrics or calculate them from the approved
five-symbol pilot.
