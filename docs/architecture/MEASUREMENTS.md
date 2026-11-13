# Financial measurements v2.0.0 (reference specification)

Reviewed 2026-11-03. Supersedes proposed v1.0.0: the equal-weight portfolio
definition is corrected below and the original mean-log proxy gets its own name.
No historical V2 calculation outputs exist to migrate. Bar schema remains 1.0.0.
Independent scalar fixture arithmetic and temporal eligibility tests began in Phase 0.
Phase 2 implements individual calculations and a synthetic-only scalar/native-Spark
Sector Gold foundation; no real Sector Gold publication exists. Measurement IDs include
this version; changes to estimands/sampling/eligibility require a new version.
Detection-rule and fitted-calibration versions are separate from measurement IDs.

## Common conventions

Prices are positive closes; use natural logarithms and dimensionless returns,
never percentages. `C[i,t]` is the close at bar end `t`, not bar-start timestamp.
Sampling is exact adjacent one-minute regular-session closes. Windows `(a,b]`
contain H returns, with H in {5,15,30,60}; H+1 closes are necessary. Lookbacks reset
each session. No gap filling, zero substitution, overnight linking or annualization.
Missing any required price makes that member/window null with a reason. A full
sector statistic requires every expected constituent; publish null plus coverage
otherwise. Membership is not inferred from the observed rows. N is variable.
Finite float64 results use the architecture parity tolerance; null/count/key
comparisons are exact. Zero RV is valid; undefined ratios/correlations are null.
Rolling SD is evaluated at each eligible return end using its trailing H returns.
Fixed realized-volatility and emergence outputs use the nonoverlapping session
anchors in DATA_CONTRACTS.md; rolling RV would require a separate output identifier.

| ID | Definition | Window / eligibility |
|---|---|---|
| intrabar_log_return | `ln(close/open)` | One bar; distinct from adjacent-close return |
| close_to_close_log_return | `r[i,t]=ln(C[i,t]/C[i,t-1min])` | Adjacent same-session bars only |
| rolling_return_volatility | `sqrt(sum((r-mean(r))^2)/(H-1))` | Trailing H returns, sample SD, H>=2 |
| realized_variance | `RV2[i,W]=sum(t in W) r[i,t]^2` | Complete fixed H-return window |
| realized_volatility | `RV[i,W]=sqrt(RV2[i,W])` | Same window; no demeaning |
| sector_realized_volatility | `sqrt(sum(i=1..N) RV2[i,W]/N)` | N>=1, all member windows valid; RMS risk intensity, not portfolio RV |
| sector_mean_log_return_realized_volatility | `sqrt(sum(t in W) (mean_i r[i,t])^2)` | Geometric-return proxy; not exact equal-weight portfolio risk |
| sector_portfolio_realized_volatility | `sqrt(sum(t in W) p[t]^2)`, `p[t]=ln(mean_i exp(r[i,t]))` | Exact frictionless equal-weight portfolio, rebalanced every minute; full synchronized membership |
| volatility_dispersion | `sqrt(sum_i (RV[i,W]-mean_i RV[i,W])^2/N)` | Population SD, N>=1; single member gives zero |
| volatility_concentration_hhi | `sum_i q[i]^2`, `q[i]=RV2[i,W]/sum_j RV2[j,W]` | Positive total variance; otherwise null |
| volatility_breadth | `sum_i 1[RV[i,W]>theta[i,H]]/N` | All train-only member thresholds available; strict > |
| volatility_effective_breadth | `(1-HHI)/(1-1/N)` | N>=2 and positive total variance; distinct from threshold breadth |
| intra_sector_correlation | Mean Pearson correlation over all unordered member pairs | Same H synchronized returns, H>=3; N>=2 |
| inter_sector_correlation | Pearson correlation of `s[g,t]=mean_i r[i,t]` for two sectors | H synchronized sector-proxy returns, H>=3; all members required |
| emergence_event | Boolean rule below | Finalized complete H-return window with frozen calibration |

Pearson uses centered sums `sum((x-xbar)*(y-ybar))/sqrt(sum((x-xbar)^2)*sum((y-ybar)^2))`.
Constant series are identified by `min(x)==max(x)` or `min(y)==max(y)` and produce
null. Implementations translate nonconstant vectors by their minimum before
centering and scale normalized deviations before accumulating products. This is
algebraically equivalent for finite vectors, avoids rounded zero-variance decisions,
and preserves small representable variation without an arbitrary epsilon. Store expected/valid pair counts; if any expected
intra-sector pair is undefined, the headline aggregate is null (do not silently
change the denominator). Single-constituent sector correlation is null. Dispersion
and intensity are separate outputs. No pooling of constituent intrabar returns
is described as sector realized volatility.

For the exact portfolio, gross simple returns are `exp(r[i,t])`; equal capital
weights are reset before each interval. A numerically stable implementation is
`p[t]=log1p(mean_i expm1(r[i,t]))`. This assumes no fees/slippage; it is a research
return definition, not a trading strategy. Cross-member covariance enters through
the synchronized return vector. The mean-log proxy is a distinct approximation;
inter-sector correlation continues to use that explicitly named proxy, not p[t].

## Emergence rule and calibration

Proposed deterministic baseline `emergence_threshold_v1`: event iff sector RV is
strictly above its frozen 95th-percentile training threshold, threshold breadth
is >=0.6, and intra-sector mean signed correlation is >=0.5. Every input must be
eligible; otherwise event is null, not false. Each member's theta is its 90th
percentile complete training RV at the same H. Quantiles use linear interpolation:
sorted x, h=(n-1)*p, interpolate floor(h)/ceil(h). Calibration requires at least
100 complete training windows per entity/H; otherwise calibration is unavailable.
Persist period, population, quantile algorithm, thresholds, dataset and feature
versions. Constants are research hypotheses, not established economic truths.
The rule version is `emergence_threshold_v1`; it references v2.0.0 measurement
IDs and a separately hashed calibration artifact. Defining RV or correlation does
not fit thresholds or establish event-detection accuracy. Changing the rule's
constants/eligibility creates a new rule version; refitting unchanged quantiles
on another training snapshot creates a new calibration ID, not a new RV formula.
Score each finalized window independently; consecutive positives are not merged
into incidents in v1. No causal contagion or profitability claim follows.

## Hand-computable fixtures required before engine work

- Constant closes: all close returns/RV/dispersion zero; HHI/correlation null.
- Alternating log returns `[a,-a,a,-a,a]`: RV=`sqrt(5)*abs(a)`, realized
  variance=`5*a^2`, sample rolling SD=`sqrt(1.2)*abs(a)`.
- Two members with RV `[3,4]`: sector intensity=`sqrt(12.5)`, dispersion=0.5,
  HHI=`337/625`, effective breadth=`576/625`.
- Identical nonconstant return vectors: pair correlation 1; opposite vectors -1.
- One minute with member gross returns `[2,0.5]`: mean log proxy=0, exact
  equal-weight gross portfolio return=1.25, p=`ln(1.25)`, portfolio RV=`abs(p)`.
  The one-minute arithmetic fixture does not change production H in {5,15,30,60}.
- Missing a minute, wholly missing member, zero total variance, one member,
  membership boundary, early close, DST, correction conflict and late revision.
- Same frozen logical input replayed in different orders/partitions must produce
  identical eligible windows and results within declared tolerance.

`tests/unit/test_measurement_fixtures.py` checks alternating-return, two-member,
proxy-versus-portfolio and correlation scalar arithmetic independently with
`math`/`statistics`. `tests/unit/test_temporal.py` checks temporal eligibility.
Null propagation, full-sector eligibility, calibration, detection and engine parity
are implemented for deterministic Sector Gold fixtures except fitted calibration and
emergence detection, which remain future reviewed work. Synthetic parity is not real
market evidence.

## Research protocol (Phase 5)

Predict future H-return RV on `(t,t+H]` using information actually ingested by
decision time t, not merely event timestamps. Start with persistence/seasonal
statistical forecasts and a scikit-learn regularized regression baseline.
Use chronological train/validation/test with an explicit time boundary table;
purge any training label horizon overlapping validation, likewise validation/test.
Fit scalers, imputers, feature selection and thresholds on training only. Missing
targets are never imputed. Seed 42 initially; record library/thread versions.
Choose hyperparameters using validation MAE versus persistence, report RMSE and
per-sector coverage, freeze selection, then evaluate held-out test once.
Features and labels have separate versions and manifests; persist all eligible
row IDs and exclusion reasons, not just successful predictions.

Training and inference use separate entry points. Immutable experiment stages:
registered inputs -> train -> validation selection -> frozen selection -> held-out
predictions/evaluation -> completed. Write checksummed artifacts first and an
atomic completion record last; resume checks hashes and stage identity, including
post-selection recovery without retuning on test. Incomplete stages are never
reported as completed. Detailed fault-injection tests belong to Phase 5.
