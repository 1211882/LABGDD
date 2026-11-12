# ADR 008: Individual-company Gold measurement boundary

**Status:** Accepted
**Date:** 2026-11-03

## Decision

Implement only v2.0.0 individual-company intrabar returns, adjacent-close returns,
rolling sample volatility, fixed realized variance and fixed realized volatility.
Require an explicitly approved Silver run and retain its manifest identity in every
Gold output. Use an independent pure-Python Decimal-based reference and native
Spark SQL production implementation. Publish scheduled ineligible rows with reason
and coverage counts. Publish immutable staged native Parquet with a terminal
checksummed manifest. Authorize selection only through the version-controlled Gold
registry and an explicit dataset/run UUID request.

## Consequences

The implementation cannot silently select superseded Silver, connect sessions,
fill gaps or reanchor fixed windows. Engine parity is measurable independently.
The approved output is suitable as individual-company evidence, but it does not
authorize sector aggregation or later infrastructure. `config/gold_publications.toml`
pins the sole approved run, its Silver input, measurement version, source/configuration
fingerprints, manifest and artifacts. Unregistered, failed and superseded runs are
not authority. The registry does not turn local rename into an object-store transaction.

Expected series in this bounded implementation are generated from distinct series
metadata present in canonical Silver rows. This is correct for the approved AAPL
session because at least one canonical row exists and its approved schedule supplies
all 390 expected slots. It cannot represent a wholly absent symbol/session. Full-cohort
work must instead supply an approved expected symbol/session relation built from the
versioned cohort/membership evidence and approved exchange-calendar sessions, then
left-join canonical observations to that relation. Observed bars must not invent the
expected research population.
