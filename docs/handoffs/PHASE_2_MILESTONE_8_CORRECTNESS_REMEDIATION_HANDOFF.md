# LABGDD V2 – Development Handoff

**Phase:** Phase 2, Milestone 8 correctness remediation
**Task:** Correct Sector Gold correlation, eligibility provenance and threshold validation
**Status:** Completed
**Date:** 2026-11-13
**Git branch and commit:** `main`; baseline `b96e4cd9af3d60362b4ee4e8395b88b8bb066489`; remediation commit recorded in the execution report

## 1. Objective

Resolve the three blocking findings from the Milestone 8 architecture audit in the canonical `LABGDD` repository: incorrect degeneracy handling for constant nonzero return vectors, absence of eligibility-policy provenance from Sector Gold identity, and scalar/Spark threshold-validation divergence. Preserve measurement specification v2.0.0, frozen Individual/Expected-Series Gold contracts, and the calculation-only boundary.

## 2. Changes Implemented

`labgdd.analytics.sector._pearson` now decides mathematical constancy by exact representable range (`min == max`). For nonconstant finite vectors, it subtracts the minimum before computing the mean, centers those translated values, and scales centered values before accumulating covariance products. Translation prevents the mean calculation from erasing one-ULP variation; scaling avoids underflow/overflow without an arbitrary epsilon. The Spark implementation applies the same algorithm with native higher-order expressions. Positive, negative and zero constant vectors remain null with `constant_return_series`; genuinely varying near-constant vectors remain eligible.

Sector population input now requires `eligibility_policy` and `eligibility_policy_version`. The supported policies are exactly `declared_research` and `verified_historical_applicability`; the version is `sector-eligibility-policy-v1`. The previous calculation boolean was removed, preventing policy changes that were absent from provenance. Missing, blank, unknown, mixed or differently versioned policies fail closed.

Scalar `SectorWindow` and `SectorPairCorrelation` results and Spark window/pair outputs preserve the policy and version. `sector-output-identity-v1` hashes a canonical pipe-delimited payload containing sector, session, horizon, window index, sorted symbols, population policy, eligibility policy/version and research-reference SHA-256. Reserved delimiters are rejected. Policy changes therefore change identity even when values coincide.

Scalar thresholds are now rejected unless `(symbol, sector)` belongs to the expected population, matching Spark. Both boundaries reject unsupported horizons, negative/nonfinite values and duplicate keys, including duplicates with conflicting values. Spark's headline correlation mean now folds a sorted symbol-pair array instead of using unordered floating `avg`, making exact output replay invariant to partition order.

Primary reason precedence remains acquisition scope, complete membership, historical applicability, wholly missing member, missing observed bar, then ineligible individual return. Constant correlation, insufficient pairs, concentration and incomplete calibration remain separate metric-specific states.

## 3. Files Created or Modified

- `src/labgdd/analytics/sector.py`: stable Pearson arithmetic, mandatory policy contract, result provenance, deterministic identity and population-bound thresholds.
- `src/labgdd/processing/sector_gold.py`: matching Spark schema/validation, native stable Pearson expressions, policy-derived eligibility, deterministic hashes and sorted pair aggregation.
- `tests/unit/test_sector_measurements.py`: all-horizon positive/negative/zero constant cases, one-ULP variation, identity, threshold and precedence tests.
- `tests/integration/test_spark_sector_gold.py`: scalar/Spark parity, schema rejection, policy identity, partition-order replay and adversarial threshold coverage.
- `docs/decisions/014-sector-gold-foundation.md`: ADR 014 accepted with the corrected contract.
- `docs/architecture/ARCHITECTURE.md`, `DATA_CONTRACTS.md`, `MEASUREMENTS.md`, and `PHASE2_SECTOR_GOLD.md`: numerical method, schema, identity payload and reason precedence.
- `docs/handoffs/PHASE_2_MILESTONE_8_CORRECTNESS_REMEDIATION_HANDOFF.md`: this report.

No publication registry, generated financial dataset or frozen Gold schema was changed.

## 4. Architecture and Design Decisions

Policy is data provenance, not an unrecorded function switch. Making it part of the population authority and output identity prevents declared research assumptions from being silently interpreted as verified history. A separate policy-version field permits future semantic evolution without overloading the policy name.

Range equality is used only to identify mathematical constancy in float64 values. No epsilon is used because an absolute cutoff would incorrectly null legitimate small variation. Translation and normalization preserve Pearson's mathematical value while improving finite arithmetic. Deterministic sorted aggregation is required because Spark reduction order otherwise changes low bits across partitions.

ADR 014 is Accepted for this synthetic calculation contract. Acceptance does not authorize a Sector Gold publication, real-data runner, calibration, emergence detection or registry entry.

## 5. Tests and Validation

Final passed gates:

- `python -m ruff format --check .`: 78 files formatted.
- `python -m ruff check .`: all checks passed.
- `python -m mypy --cache-dir <temporary>`: no issues in 78 source files.
- `python -m pip check`: no broken requirements.
- `pytest tests/unit/test_sector_measurements.py`: 27 passed.
- Complete Windows `pytest -p no:cacheprovider`: 307 passed, 2 skipped in 3,396.74 seconds. The skips are the two supported-Linux publication tests. Pytest exited zero; Windows emitted a post-exit temporary-cleanup `Access denied` message.
- Network-disabled Linux Docker, read-only tracked-file mount: `tests/integration/test_spark_sector_gold.py` plus `tests/integration/test_expected_gold_publication.py`: 12 passed, no skips, in 173.79 seconds.
- `python -m build --no-isolation`: sdist and wheel built successfully from a secret-free tracked-file copy.
- `scripts/smoke_wheel.py`: clean installation, imports, module/console CLI, configuration validation and dependency consistency passed.
- `scripts/check_repository.py`: repository security checks passed before handoff creation; repeated after the final handoff/staging review.

Development failures were not suppressed. Initial near-constant fixtures exposed loss of one-ULP variation in the ordinary mean; translation corrected it. An initial Spark run exposed an omitted final identity projection and a timestamp-comparison fixture issue. The first complete suite then produced 306 passed, 2 skipped and 1 failed because unordered Spark `avg` was not bitwise partition-invariant. The sorted fold correction passed its focused regression and the complete clean rerun above.

## 6. Issues and Limitations

Sector evidence remains synthetic. No approved real-data projection into this schema exists. Research membership is still project-defined rather than independently verified historical classification. Thresholds remain supplied fixtures; there is no fitted calibration authority or emergence detector. IEX venue-specific observations, correction finality and broader market coverage remain outside this milestone.

The Windows Spark/pytest process continues to emit a nonfatal cleanup warning after successful exit; Linux validation provides the authoritative execution evidence for the two Windows-skipped publication tests.

## 7. Scientific and Data Integrity

The v2.0.0 formulas are unchanged: exact equal-weight returns remain `log1p(mean(expm1(r)))`; HHI uses each member's realized-variance share over positive total member variance; effective breadth remains `(1-HHI)/(1-1/N)` for `N>=2`; complete-sector denominators never shrink to observed survivors. Missing observations remain null and are never filled or zeroed.

The remediation changes numerical correctness, provenance and reproducibility, not the estimands. Existing frozen Bronze, Silver, Individual Gold and Expected-Series Gold artifacts and registries are untouched.

## 8. Current Project Status

The scalar and native Spark Sector Gold calculation foundations implement the accepted ADR 014 contract and agree on eligibility, nullability, reason codes, policy identity and financial results across all four horizons. They remain calculation-only and synthetic-only. No Sector Gold publication exists.

## 9. Recommended Next Step

Perform an architecture review of this bounded remediation and ADR 014 acceptance. Any future real Sector Gold publication must be separately authorized and begin with explicit approved Gold resolution and manifest-bound population projection. Milestone 9 is not started.

## 10. Review Questions

None for the bounded correctness remediation.

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
