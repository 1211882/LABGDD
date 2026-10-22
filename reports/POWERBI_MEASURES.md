# Recommended Power BI Measures

Persisted scientific metrics should normally be displayed directly from their metric facts under a single model, horizon, population, and evaluation-type filter. Do not average Macro F1, R-squared, Sharpe, or cumulative return across models or horizons.

```DAX
Average Return = AVERAGE(fact_financial_analytics[window_return])

Average Volatility = AVERAGE(fact_financial_analytics[volatility])

Average Sector Dispersion = AVERAGE(fact_sector_analytics[volatility])

Average Coverage = AVERAGE(fact_financial_analytics[coverage_ratio])

Total Volume = SUM(fact_financial_analytics[total_volume])

Complete Windows =
CALCULATE(COUNTROWS(fact_financial_analytics), fact_financial_analytics[coverage_ratio] >= 1)

Incomplete Windows =
CALCULATE(COUNTROWS(fact_financial_analytics), fact_financial_analytics[coverage_ratio] < 1)

Classification Accuracy = SELECTEDVALUE(fact_classification_metrics[accuracy])

Balanced Accuracy = SELECTEDVALUE(fact_classification_metrics[balanced_accuracy])

Macro F1 = SELECTEDVALUE(fact_classification_metrics[macro_f1])

Regression MAE = SELECTEDVALUE(fact_regression_metrics[mae])

Regression RMSE = SELECTEDVALUE(fact_regression_metrics[rmse])

Regression R Squared = SELECTEDVALUE(fact_regression_metrics[r2])

Signal Count =
COALESCE(SUM(fact_classification_financial[signal_count]), SUM(fact_regression_financial[signal_count]))

Diagnostic Return =
COALESCE(SELECTEDVALUE(fact_classification_financial[cumulative_return]), SELECTEDVALUE(fact_regression_financial[cumulative_return]))

Maximum Drawdown =
COALESCE(SELECTEDVALUE(fact_classification_financial[maximum_drawdown]), SELECTEDVALUE(fact_regression_financial[maximum_drawdown]))

Equivalence Mismatches = SUM(fact_equivalence[mismatches])
```

`SELECTEDVALUE` deliberately returns blank under an ambiguous filter context instead of creating a scientifically invalid average. Format returns and scores as decimals or percentages consistently; keep regression errors in raw return units. Classification financial Sharpe is unavailable and must remain blank.
