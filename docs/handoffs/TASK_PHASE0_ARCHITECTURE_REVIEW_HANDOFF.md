### LABGDD V2 – Development Handoff

**Phase:** Phase 0 — architecture-review remediation  
**Task:** Resolve temporal, finalization, observation, calendar and measurement ambiguities before Phase 1  
**Status:** Completed  
**Date:** 2026-11-03 (Europe/Lisbon)  
**Git branch and commit:** `main`; no commits exist. Changes remain uncommitted; no commit, tag or remote was created.

#### 1. Objective

Preserve the approved modular monolith while making its contracts precise and adding executable regression coverage. Scope excludes ingestion, distributed engines, infrastructure, ML and serving. Phase 1 was not started.

Issues identified: first-observed-close wording could shift windows; watermark progress was insufficiently separated from completeness; idle-input behavior and acquisition/correction cases needed explicit policies; calendar selection was unresolved; and the mean-log-return proxy needed separation from exact equal-weight portfolio returns.

#### 2. Changes Implemented

Added `labgdd.domain.temporal.validate_return_window(bars, *, session_open, session_close, window_start, return_count)`. It validates UTC, minute-aligned supplied bounds and H+1 ordered consecutive same-series closes for H returns. It rejects missing/duplicate minutes, mixed symbols and session-boundary crossings. It neither sorts/fills data nor generates a calendar or financial result.

Added computed `MarketBar.bar_end`, `MarketBar.logical_key` and `MarketBar.ohlcv` properties without changing serialized dataclass fields. Tests distinguish observation identity, logical-bar identity and market-payload equality; they do not implement correction selection.

Added optional `Settings.allowed_lateness_minutes`, default 10, to `load_settings`, with nonnegative-integer validation and backward-compatible loading when absent. The shipped configuration sets it explicitly. It is not an active finalization mechanism.

Documented scheduled-open window anchors, explicit idle-source handling, bounded terminal offsets, capture cutoffs, immutable publication revisions, raw-price policy, variable membership and calendar provenance. Added independent scalar numerical fixtures without building an analytics engine.

#### 3. Files Created or Modified

Created:

- `src/labgdd/domain/temporal.py` — pure consecutive-close eligibility contract.
- `tests/unit/test_temporal.py` — opening, first return, five-return, gaps, DST-offset, early-close and overnight fixtures.
- `tests/unit/test_measurement_fixtures.py` — independent formula arithmetic and portfolio/proxy counterexample.
- `docs/decisions/005-calendar-and-raw-price-policy.md` — new calendar-provider proposal and raw-price decision.
- `docs/handoffs/TASK_PHASE0_ARCHITECTURE_REVIEW_HANDOFF.md` — this report.

Modified:

- `src/labgdd/domain/market_bar.py` — computed temporal/identity properties.
- `src/labgdd/config.py` and `config/project.toml` — configurable lateness.
- `tests/unit/test_market_bar.py` and `tests/unit/test_config.py` — identity and lateness regressions.
- `tests/unit/test_architecture.py` — permit internal domain imports while retaining the external-dependency guard.
- `scripts/smoke_wheel.py` — include the new temporal module in installed-package import checks.
- `docs/architecture/ARCHITECTURE.md` — actual executable scope and remaining risks.
- `docs/architecture/DATA_CONTRACTS.md` — temporal examples, correction cases, finalization and adjustment policies.
- `docs/architecture/MEASUREMENTS.md` — v2.0.0 definitions and calibration separation.
- `docs/architecture/IMPLEMENTATION_PLAN.md` — explicit future acceptance fixtures and Phase 1 gates.
- `docs/decisions/003-event-time-and-revisions.md` and `docs/decisions/004-measurements-and-experiments.md` — compatible clarifications and versioned formula correction.
- `README.md` — corrected storage-path description, review summary and handoff link.

#### 4. Architecture and Design Decisions

**Confirmed:** standard-library-only domain, immutable observations, raw IEX prices, original six-sector/30-company taxonomy, variable constituent counts, no gap filling, no overnight returns, explicit correction evidence and immutable research outputs. No new dependencies, services or speculative interfaces were added.

**Clarified:** fixed H-return windows start at scheduled open + one minute + k*H minutes. At a 09:30 opening, first close is 09:31, first adjacent-close return ends 09:32, and the first five-return window is (09:31,09:36], requiring six closes. Missing opening data does not move the anchor.

Proposed watermark W=min(required-partition maximum observed bar ends)-L deducts lateness once. Idle/uninitialized required inputs hold progress; live silence leaves affected windows provisional. Explicit bounded completion can finalize an exhausted snapshot, not certify complete market history. Publications record accepted input, capture cutoff, offsets, policy and coverage; subsequent corrections require new versions.

**New calendar decision:** propose `exchange_calendars`/XNYS for the common regular-session benchmark, based on its explicit session/minute interfaces. `pandas_market_calendars` is a reasonable alternative with documented historical and early-close schedules; choosing one avoids competing authorities. This is a design judgment, not a performance/accuracy benchmark. Installation, version pinning, listing/venue applicability and generated-schedule verification wait for Phase 1. Sources: [exchange_calendars documentation](https://github.com/gerrymanoim/exchange_calendars), [pandas_market_calendars guide](https://pandas-market-calendars.readthedocs.io/en/latest/usage.html).

**Changed measurement definition:** specification v2.0.0 gives exact equal-weight rebalanced portfolio log return `p=ln(mean(exp(r_i)))`; portfolio RV is `sqrt(sum(p^2))`. The old mean-log proxy retains the explicit identifier `sector_mean_log_return_realized_volatility`. Gross returns [2,0.5] demonstrate proxy=0 versus exact portfolio return ln(1.25). The bar schema remains 1.0.0. Detector-rule and fitted-calibration identities remain separate from estimands; emergence thresholds are experimental hypotheses.

#### 5. Tests and Validation

Executed locally on Windows, CPython 3.11.9, using the existing isolated project environment:

| Command | Actual result |
|---|---|
| `.venv/Scripts/python -m ruff format --check .` | Passed; 17 files formatted |
| `.venv/Scripts/python -m ruff check .` | Passed |
| `.venv/Scripts/python -m mypy` | Passed; 17 files, strict mode |
| `.venv/Scripts/python -m pytest` | Passed; 109 tests, no skips, final full run 0.70 seconds |
| `.venv/Scripts/python -m pip check` | Passed; no broken requirements |
| `.venv/Scripts/python -m pip install --no-build-isolation --no-deps -e .` | Passed |
| `.venv/Scripts/python -m build --no-isolation` | Passed; source distribution and wheel built from it |
| `.venv/Scripts/python scripts/smoke_wheel.py dist/labgdd_v2-0.1.0-py3-none-any.whl --config config/project.toml` | Passed; clean offline temporary environment outside repository, installed imports, module CLI, console CLI and dependency check |
| `.venv/Scripts/labgdd --config config/project.toml` | Passed; exit 0, structured valid-config event |
| `.venv/Scripts/labgdd --config config/does-not-exist.toml` | Expected rejection; exit 2, structured invalid-config event |

Added 31 tests: 16 temporal, eight lateness, three identity and four scalar arithmetic cases. Existing domain-independence, variable-universe, secret-path, logging and audit-integrity tests still pass. DST cases use hand-enumerated fixed offsets around both transitions; they do not test IANA timezone lookup or a provider library.

Final scope review used `.venv/Scripts/python -` scripts comparing all repository files against a pre-task snapshot because Git has no committed baseline. Source/document diffs were inspected; dependency files, workflow instructions, prior handoffs, ADRs 001/002 and the reference audit remain unchanged. Audit SHA-256 remains `553d34b93258fe35bd061956b616f30c72e3c8759419a4cfd849920d071eb069`.

Failed validation checks: none. Skipped tests: none. Not executed: Linux/remote CI, authenticated APIs, calendar integration, correction resolver, engine parity and infrastructure tests; those implementations do not exist yet.

#### 6. Issues and Limitations

The temporal validator trusts caller-supplied bounds for one session; it does not prove that a date is a trading day or select fixed-window cadence. Phase 1 must pin provider/tzdb versions, freeze schedule hashes, verify historical exceptions and establish declared cohort applicability without inventing industry-membership dates. IEX entitlements and venue comparability remain unverified.

Observation hashing, revision-evidence validation and resolution are specified but unimplemented. Alpaca's documented original/updated-bar fields do not supply a global ordered revision counter; channel/timestamp alone cannot resolve ambiguous conflicts. See [Alpaca streaming documentation](https://docs.alpaca.markets/us/docs/real-time-stock-pricing-data).

Finalization, null propagation, calibrated detection and distributed numeric parity have future acceptance fixtures, not runtime evidence. No new known regression was exposed by executed checks.

#### 7. Scientific and Data Integrity

Temporal wording is tightened to scheduled anchors and executable same-session eligibility. Portfolio measurement meaning changes explicitly under v2.0.0; other reviewed numerical fixtures remain mathematically consistent. No production V2 analytics outputs exist to migrate, and no historical datasets/models/experiments were altered. Raw-price adjustment is separate from return formulas. Selection/survivorship bias and retrospective sector-label limitations are explicit. Batch/streaming equality remains a future requirement over an identical accepted observation snapshot.

#### 8. Current Project Status

Verified operational: package installation/imports, configuration validation, safe JSON logging, offline CLI, bar validation and explicit-bound temporal eligibility. Formula fixture arithmetic is checked independently. Provider acquisition, calendar integration, production analytics, Kafka/Spark, ML, PostgreSQL, Prometheus/Grafana and Power BI remain unimplemented. Zero third-party runtime dependencies are retained.

#### 9. Recommended Next Step

Review the revised contracts and ADR 005, then explicitly authorize Phase 1 if accepted. Its first work should pin/integrate the calendar, verify study-date/venue applicability and source access, then test acquisition/persistence. No Phase 1 work was performed.

#### 10. Review Questions

- Accept the exact-portfolio definition and separately named proxy in measurement specification v2.0.0?
- Accept the proposed XNYS common-session benchmark, subject to Phase 1 listing/venue checks, and the conservative policy that idle live inputs remain provisional until explicit completion?

**Baseline recommendation:** Phase 0 is ready to be frozen as the foundation after reviewer acceptance of these decisions. Future integration gates are documented and do not require implementing Phase 1 to freeze Phase 0. No baseline commit/tag was created automatically.

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
