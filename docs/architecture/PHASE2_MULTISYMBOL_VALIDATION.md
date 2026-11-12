# Phase 2 Milestone 6: Multi-symbol end-to-end validation

Date: 2026-11-06. Status: implementation accepted; candidate datasets remain
pending publication review. This milestone does not implement sector analytics or
approve a new publication.

The bounded acquisition requested raw one-minute IEX bars for MSFT, NVDA, JPM and
XOM during the XNYS regular session on 2026-09-14. Existing immutable AAPL evidence
was reused. The provider completed in one page and one HTTP attempt with 1,471 new
observations: MSFT 390, NVDA 390, JPM 371 and XOM 320. The 89 absent scheduled
symbol-minutes were not filled. Provider fields were `t,o,h,l,c,v,n,vw`; original
JSON and the complete response page remain in immutable Bronze evidence.

The native Linux Spark Silver candidate combined the two explicit Bronze manifests
and produced 1,861 unique canonical observations: AAPL 390, MSFT 390, NVDA 390,
JPM 371 and XOM 320. It produced no quarantine or duplicate/conflict rows. Physical
timestamps are Parquet microsecond UTC and all artifact checksums verify.

Individual Gold produced 1,950 scheduled return slots, 7,780 rolling rows and 600
fixed windows. Expected-Series Gold independently retained the full 30-symbol
population: 11,700 returns, 46,680 rolling rows and 3,600 fixed windows. Its manifest
records the five acquisition-scope symbols; the other 25 symbols have null
observations and financial values and are not classified as provider failures.

Every requested-symbol financial key, reason, eligibility flag and value was
compared with `analytics.individual.calculate_individual_measurements` using
`abs(actual - expected) <= 1e-12 + 1e-10 * abs(expected)`. AAPL also matches both
approved legacy Gold datasets exactly. New datasets are absent from the approved
Silver and Gold registries and remain immutable review candidates.

IEX is venue-specific. The project taxonomy, historical entity/listing status and
sector membership remain unverified. Complete pagination proves exhaustion of this
bounded response, not consolidated-market completeness or provider correction
finality.

ADR 012 accepts the separate bounded-validation authority. Future publication is a
different governance action and must follow `MILESTONE6_PUBLICATION_MIGRATION.md`.
