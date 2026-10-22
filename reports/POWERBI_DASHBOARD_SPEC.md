# Power BI Dashboard Specification

Every page should display `Academic research; not investment advice`. Financial diagnostic pages must use `Diagnostic signal evaluation; not trading strategy performance`.

1. **Executive Overview:** historical/ML rows, 252 trading days, 30 symbols, six sectors, zero equivalence mismatches, Random Forest 5m Macro F1, best regression R-squared, and the explicit no-profitability conclusion.
2. **Market Overview:** price, volume, return, pooled symbol volatility, and coverage with date/symbol/sector/window filters.
3. **Sector Analysis:** pooled constituent price/return dispersion, volume, coverage, active symbols. Label sector `volatility` as pooled constituent-return dispersion, not synthetic-index temporal volatility.
4. **Window Analysis:** 1/5/15/30/60m coverage, complete/incomplete windows, returns, event counts, and volatility availability. State that 1m sample volatility is undefined for one bar.
5. **Batch vs Streaming:** Phase 7 compared rows, zero mismatches, maximum difference/tolerance; Phase 8 duration, throughput, Streaming latency, and memory by workload. Subtitle: `Local controlled benchmark`.
6. **Classification Overview:** FULL/COMMON selector, model/horizon table, Macro F1 as primary metric, and Random Forest highlighted without hiding other models.
7. **Classification Detail:** confusion-matrix heatmap from the prepared fact, actual/predicted class distributions, confidence, symbol/sector evidence, and model/horizon filters.
8. **Classification Financial Diagnostics:** diagnostic/non-overlap selector, signals, directional accuracy, cumulative return, drawdown, turnover, and 5 bps cost. Do not show unavailable Sharpe as zero.
9. **Regression Overview:** zero-return baseline beside learned models; MAE, RMSE, R-squared, correlation, and directional accuracy. Apply FULL/COMMON explicitly.
10. **Regression Detail:** actual-vs-predicted scatter, residual distribution, and error by horizon/symbol/sector/magnitude using predictions plus persisted Phase 11 aggregate evidence.
11. **Classification vs Regression:** compare direct and regression-derived class metrics from frozen evidence. Never place Macro F1 and RMSE on one numerical axis. Conclusion: direct classification is the stronger directional formulation.
12. **Methodology / Limitations:** Alpaca IEX, dual Batch/Streaming architecture, one-year ML dataset, exact temporal labels, train-only scaling, local infrastructure, gaps/non-stationarity, CPU limits, and no demonstrated profitable strategy.

Grafana operational panels, Kafka lag, active query state, and container health are intentionally excluded from these pages.
