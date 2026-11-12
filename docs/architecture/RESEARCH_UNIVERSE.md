# Research universe and expected-series contract v1.0.0

Date: 2026-11-03. This contract establishes research expectations; it does not
establish historical market facts or calculate financial sector measurements.

## Purpose and evidence status

`study-cohort-v1` preserves the six project-defined research groups and 30 symbols
in `config/project.toml`. The labels Technology, Healthcare, Defense, Aerospace,
Energy and Financials are a study taxonomy, not GICS and not a claim about official
historical classifications. Selecting present-day recognizable companies creates
selection and survivorship bias.

`config/research_universe.toml` declares applicability only for
`2026-09-14T00:00:00Z` through `2026-09-15T00:00:00Z`, matching the bounded approved
Silver/Gold study day. This interval is a project research assumption. It is not an
inferred listing date, incorporation history, sector effective date, permanent
membership claim or corporate-action record.

Each `ResearchMembership` carries symbol, sector, membership version, half-open
declared applicability, applicability basis, evidence source/status, listing and
entity verification statuses, sector-classification status, exclusions and
limitations. Current listing and entity statuses are `unverified`; sector status is
`project_defined_unverified`. These values must not be upgraded without reviewed
external evidence.

## Expected symbol/session generation

`research.universe.generate_expected_series` is a pure deterministic function over
a versioned `ResearchUniverse`, explicit requested session labels, and approved
regular-session bounds/calendar provenance.

`processing.expected_series.generate_xnys_expected_series` is the narrow adapter
from the pinned `CalendarSnapshot`. It requires XNYS/America/New_York provenance.
The function has no observed-bar argument, so absent symbols and sessions cannot
silently disappear based on market-data availability. Requested holidays/weekends
remain visible in `requested_sessions` but have no `scheduled_sessions` or expected
rows. A requested XNYS session receives a row for every declared membership interval
intersecting that session, even if no observation exists. Membership beginning after
the open is retained as `partial_session` rather than silently omitted.

Rows preserve UTC open/close, membership bounds/version, calendar/tzdata/schedule
provenance and evidence status. A membership expiring within a session is marked
`partial_session`; an adjacent membership interval starting during the session is
marked `membership_change_during_session` with explicit boundary instants. No price,
volume or synthetic financial observation is generated. Overlapping membership
intervals for the same symbol/version fail closed.

## Completeness and eligibility distinctions

- **Expected research membership:** entities declared by the versioned research
  reference for a requested scheduled session, independent of observations.
- **Observed market data:** provider observations actually captured; absence may be
  a halt, no qualifying IEX trade, entitlement/source issue or another unknown cause.
- **Valid canonical observations:** observed rows satisfying schema, calendar,
  membership and conservative resolution contracts in one Silver snapshot.
- **Eligible return windows:** canonical H+1 consecutive same-session closes meeting
  temporal and membership rules. Expected rows do not make prices eligible.
- **Complete sector windows:** future sector calculations must have every expected
  constituent and required eligible return. Missing/ineligible expected constituents
  stay in expected counts and make complete-membership statistics null.
- **Source completeness/correction finality:** separate external claims. Full expected
  coverage does not prove that IEX is consolidated or no correction can arrive.

## Spark integration boundary

Frozen Silver and approved Gold publications are unchanged. Silver currently
constructs membership rows from project settings and its explicit request interval.
A future reviewed migration can supply `ResearchMembership` rows to the existing
membership validation boundary.

Milestone 4 adds an optional Spark relation from `ExpectedSeriesPlan.rows` and a
left-join processing path in `transform_individual_gold`. The legacy two-argument
path remains unchanged. The optional path makes a wholly missing symbol/session
measurable as missing without fabricating bars and carries deterministic plan and
identity fingerprints. Gold requires the approved plan fingerprint through a
separate caller argument and recomputes all relation fingerprints; strict consumers
can require the verified-historical policy. Milestone 5 connects this path only to
a separate runner and dataset. External registry v1.1 approves it independently;
the frozen runner and original AAPL authority remain unchanged.

Future sector analytics must group the expected relation, not only observed rows,
and retain expected/observed/eligible constituent counts. No sector metric is
implemented here.

## Evidence needed before sector claims

Historical sector claims require reviewed symbol-to-entity identifiers, listing and
delisting applicability, taxonomy source/effective dates, ticker changes, mergers
and relevant corporate-action/adjustment evidence. Provider venue coverage and
correction finality also require independent evidence. Until then, results may only
be described as analysis of the declared `study-cohort-v1` research sample.
