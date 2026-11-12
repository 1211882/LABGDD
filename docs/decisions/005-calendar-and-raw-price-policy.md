# ADR 005: Explicit US-equity session schedule and raw IEX research prices

Date: 2026-11-03. Status: proposed provider selected for Phase 1 integration;
no calendar dependency is installed in Phase 0.

Phase 1 update (2026-11-03): provider 4.11.2 is installed through the optional
ingestion extra and generated calendar edge fixtures are tested. Calendar/tzdata
and schedule hashes are persisted. The common-session policy is approved, while
security-level historical listing applicability remains unverified and explicitly
marked in raw acquisition metadata. See PHASE1_INGESTION.md; the frozen Phase 0
commit and this ADR's original rationale remain preserved by the baseline tag.

## Context and choice

The initial US-equity study needs a reproducible session grid, independent of
whether IEX emits a bar. Select `exchange_calendars`, calendar `XNYS`, as the
proposed common US-equity regular-session benchmark. Its
[official documentation](https://github.com/gerrymanoim/exchange_calendars)
provides explicit session schedules and configurable minute-side conventions.
Derive start-labeled intervals `[open,close)` directly from schedule bounds;
do not rely on a library's default inclusive minute index. This selection does
not assert that IEX is NYSE or that every cohort security is NYSE-listed.
Before use, verify the common-session policy against relevant listing/venue
exceptions; use an explicitly versioned override or exclude an unsupported case.

Alternative `pandas_market_calendars` has documented NYSE schedules, early closes
and historical market-time changes in its
[usage guide](https://pandas-market-calendars.readthedocs.io/en/latest/usage.html).
It is reasonable, but installing both would duplicate schedule authorities.
`exchange_calendars` is preferred here for its explicit session/minute semantics;
this is a design judgment, not a measured accuracy/performance comparison.
Hand-maintained holiday rules or a generic weekday/federal-holiday calendar are
rejected because exchange exceptions and historical changes need evidence.
Provider APIs can cross-check an acquisition, but mutable remote answers alone
are insufficient provenance for replay.

## Required adapter policy and provenance

- Exchange timezone is America/New_York; derive UTC per session with a pinned
  IANA timezone-data source. Never hardcode a permanent -04:00/-05:00 offset.
- Use regular sessions only, usually 09:30–16:00 local in the contemporary study
  period; exclude extended hours. Holiday dates produce no expected bars.
- Use explicit holiday/early-close/exception schedules. The
  [NYSE reference](https://www.nyse.com/markets/hours-calendars) lists a 13:00
  local close on 2026-11-27. Historical study dates need historical hours and
  exception evidence; do not extrapolate current schedules indefinitely.
- Phase 1 pins a Python-3.11-compatible provider version only after dependency
  resolution and generated-schedule tests. Record package/version, calendar ID,
  requested/supported date bounds, tzdb/version, override version, retrieved
  authoritative reference/checksum, and the generated UTC schedule hash.
- Freeze that schedule with the dataset. A holiday-rule/provider correction
  creates a new calendar/dataset version, never rewrites historical evidence.
  Unknown/out-of-range sessions fail closed; no weekday fallback.
- Listing dates, ticker/entity changes and cohort applicability are separate
  reference data. Never invent historical sector dates from this calendar.

Executable Phase 0 tests consume enumerated UTC bounds around both 2026 DST
changes and an early close. They do not invoke this provider or validate its
historical schedule generation. Phase 1 acceptance requires generated fixtures
for holidays/weekends, spring/fall DST, early closes, exceptions, endpoint
inclusion, out-of-range rejection and reproducible schedule fingerprints.

## Adjustment decision

Confirm initial raw IEX OHLCV, explicit `adjustment=raw` for REST, and no implicit
adjustment inside return formulas. Preserve raw corporate-action discontinuities
and record flags/eligibility. This matches the raw option described in
[Alpaca historical bars](https://docs.alpaca.markets/us/reference/stockbars).
An adjusted research dataset would be a separate version with a documented
adjustment source/as-of policy and a live comparability analysis.

## Consequences

Domain stays independent: it validates supplied UTC session bounds without
calendar I/O. Calendar/price selection is documented now; downloading packages,
generating production schedules and authenticated data checks wait for Phase 1.
Provider correctness, historical applicability and entitlements remain unverified.
