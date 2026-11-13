# LABGDD V2 – Development Handoff

**Phase:** Phase 2, Milestone 8 – Sector Gold Architecture and Measurement Contracts
**Task:** Implement synthetic scalar/native-Spark Sector Gold foundations
**Status:** Completed
**Date:** 2026-11-12
**Git branch and commit:** `main`; based on approved Milestone 7 commit `94c8a769cb92a50cb20da2bc4de6cb65797934a0`. The final Milestone 8 commit is recorded by Git and in the execution response because a tracked file cannot contain the hash of its own commit.

## 1. Objective

Implement a calculation-only scientific foundation for Sector Gold using deterministic
synthetic evidence. Preserve measurement specification 2.0.0, keep the expected
research population independent of observed returns, establish pure-Python/native-
Spark parity, and stop without calculating or publishing real sector results.

## 2. Changes Implemented

`labgdd.analytics.sector.calculate_sector_measurements()` is the independent scalar
reference. It accepts explicit `SectorPopulationMember`, `SectorMemberReturn` and
optional `SectorThreshold` relations and produces sector windows plus explicit
pairwise-correlation rows. It validates UTC minute alignment, temporal bounds,
population uniqueness, acquisition scope, return consistency, finite values,
membership applicability, historical evidence and research fingerprints.

`labgdd.processing.sector_gold.transform_sector_gold()` implements equivalent native
Spark expressions. Exact non-nullable schemas reject incorrect column types and SQL
NULL bypasses. The transformation constructs scheduled return slots from the expected
population, left-joins individual return evidence, rejects duplicates and cross-sector
symbol contradictions, and does not use Python UDFs or collect financial data to the
driver. Spark must run in UTC. The input boundary accepts only
`input_authority=synthetic_fixture`; arbitrary real frames cannot masquerade as an
approved Gold authority.

Complete-sector output includes expected symbols; verified, acquired, observed,
canonical and eligible member counts; missing/excluded counts; membership status;
expected/valid pair counts; population-policy version; and research fingerprint.
Reasons distinguish absent expected members, acquisition exclusions, missing bars,
ineligible returns, incomplete windows/populations, insufficient pairs, constant
series, unverified history and unavailable threshold calibration.

## 3. Files Created or Modified

- `src/labgdd/analytics/sector.py`: scalar contracts, validation, formulas and reasons.
- `src/labgdd/processing/sector_gold.py`: exact Spark schemas and native transforms.
- `tests/unit/test_sector_measurements.py`: adversarial scalar fixtures.
- `tests/integration/test_spark_sector_gold.py`: all-horizon Spark parity, replay and
  fail-closed validation.
- `docs/architecture/PHASE2_SECTOR_GOLD.md`: population, formula, authority and future
  publication contracts.
- `docs/decisions/014-sector-gold-foundation.md`: proposed architecture decision.
- `README.md`, `docs/architecture/ARCHITECTURE.md`,
  `docs/architecture/IMPLEMENTATION_PLAN.md` and
  `docs/architecture/MEASUREMENTS.md`: current capability and limitation updates.
- This handoff records the completed execution.

No publication registry, runtime configuration, immutable dataset or dependency was
changed.

## 4. Architecture and Design Decisions

The independent population relation is authoritative for denominators. Missing
companies cannot disappear from a sector merely because no observation exists.
Strict historical mode requires verified applicability, listing, entity identity and
sector classification for every member. Declared-only mode exists for explicit
synthetic formula testing and is not historical evidence.

The informal request for “cross-sectional return dispersion” is resolved conservatively
to the existing v2.0.0 `volatility_dispersion` definition: population standard
deviation of constituent realized volatilities. No new per-minute return-dispersion
estimand was invented.

ADR 014 remains Proposed. A future real-data runner must begin with
`resolve_approved_gold`, bind an exact Expected-Series Gold dataset/run, manifest,
artifacts and contract fingerprints to the population projection, and undergo
separate review. There is intentionally no Sector Gold writer, runner, manifest,
registry entry or approval tag.

## 5. Scientific Formulas and Population Semantics

For fixed nonoverlapping H-return windows, H in `{5,15,30,60}`:

- sector RMS intensity is `sqrt(mean_i(sum_t r[i,t]^2))`;
- exact equal-weight return is `p[t]=log1p(mean_i(expm1(r[i,t])))`, with portfolio
  realized volatility `sqrt(sum_t p[t]^2)`;
- the mean-log proxy remains separately named and never substitutes for the exact
  portfolio;
- volatility dispersion uses population denominator N;
- Pearson correlation uses every unordered synchronized member pair; if any expected
  pair is undefined, the headline correlation is null;
- HHI uses realized-variance shares, and effective breadth is
  `(1-HHI)/(1-1/N)` when defined;
- threshold breadth requires every member/horizon threshold and uses strict `>`.

Zero realized variance is valid. Zero total variance makes HHI/effective breadth and
correlations null. Gaps are never filled, zeroed or compressed. Incomplete populations
produce null complete-sector financial outputs, while population counts remain
available.

## 6. Tests and Validation

Actual results:

- Ruff formatting: **78 files formatted**, passed after one formatting correction.
- Ruff lint: passed.
- Strict mypy: passed for **78 source files**.
- Full Windows pytest: **285 passed, 2 skipped** in 1,305.43 seconds. The two skips
  are the existing Linux-only publication tests.
- New scalar suite: **8 passed**.
- New Windows Spark suite: **7 passed**; the expanded all-horizon parity case passed
  in 174.52 seconds.
- Dedicated Linux Sector Gold Spark suite: **7 passed, 0 skipped** in 87.01 seconds.
- Existing Linux-only publication suite: **2 passed, 0 skipped** in 52.15 seconds.
- The first Linux launch attempt did not start Spark because a network-disabled
  container hostname was not resolvable. The rerun used a fixed hostname mapped to
  loopback, retained `--network none`, and passed. This was an environment-launch
  failure, not a financial-test failure.
- `pip check`: passed with no broken requirements.
- Sdist and `labgdd_v2-0.4.0-py3-none-any.whl`: built successfully.
- Clean-wheel install, imports, module/console CLI and dependency smoke: passed.
- Repository security validation: passed; `.env` remained ignored and untracked.

Linux used an existing dedicated test image, a 167-file secret-free temporary package,
a read-only `/workspace` mount and no external network. Temporary packages were
removed afterward.

Fixtures cover a complete five-member sector, a second single-member sector, absent
and intentionally unrequested members, opening/interior gaps, membership changes,
unverified history, ineligible returns, constant series, insufficient pairs, HHI,
breadth, exact-versus-proxy portfolio divergence, shuffled partitions, deterministic
replay, all horizons, UTC window identity and scalar/Spark numerical tolerance.

## 7. Issues and Limitations

Synthetic validation is not market evidence. Historical entity/listing/sector
membership remains unverified for real observations. IEX is venue-specific, provider
correction finality remains unproven, and one session/five acquired symbols cannot
support scientifically valid sector claims. Threshold breadth has no fitted
calibration authority, and emergence detection remains unimplemented.

No multi-node Spark, distributed filesystem or production-scale performance claim is
made. ADR 014 and any physical Sector Gold schema still require architecture review.
Credentials visible in prior IDE context were not read or used; they require rotation
before any future authenticated Alpaca request.

## 8. Scientific and Data Integrity

Measurement specification 2.0.0 was not changed. Existing Bronze, Silver, Individual
Gold, Expected-Series Gold and all publication registries remain unchanged. The
30-symbol/six-sector cohort is unchanged. No real dataset was read or rewritten, no
network request was made, and no sector metric was published.

## 9. Current Project Status

The repository can calculate reproducible synthetic Sector Gold windows with pure
Python and native Spark while preserving explicit population/missingness semantics.
It cannot yet produce an approved real Sector Gold dataset. Milestone 8 is committed
as a development foundation without an approval tag; all later phases remain not
started.

## 10. Recommended Next Step

Rotate the exposed Alpaca credentials, then perform architecture review of ADR 014,
the population schema, reason precedence and synthetic parity evidence. If accepted,
define a separate resolver-bound real-input and immutable publication milestone. Do
not calculate real sector results automatically.

## 11. Review Questions

1. Should ADR 014 and `sector-complete-population-v1` be accepted as the production
   calculation contract?
2. Does the research require a separately versioned per-minute cross-sectional
   return-dispersion estimand, or is v2.0.0 volatility dispersion sufficient?

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
