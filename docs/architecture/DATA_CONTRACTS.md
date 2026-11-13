# Data contracts v1.0.0

Reviewed 2026-11-03. Bar schema remains 1.0.0; the pre-freeze window-policy
clarification below is executable. Resolver/finalization policies remain designs.
Phase 1 Bronze implementation and exact identity/publication algorithms are
specified in PHASE1_INGESTION.md. Bar schema and measurement semantics are unchanged.
Phase 2 Milestone 1 implements the Silver v1 projection and conservative
equivalent-only resolver described below; see PHASE2_BATCH.md and ADR 007.

## Canonical one-minute bar

`labgdd.domain.market_bar.MarketBar` is immutable and validated on construction.
It is a normalized observation, not proof of session eligibility or membership.

| Field | Python / wire representation | Rule |
|---|---|---|
| event_time | aware datetime / RFC3339 UTC | Interval start; UTC, minute aligned |
| symbol | string | Uppercase US symbol, 1–16 letters/digits/dot/hyphen; starts with a letter |
| open, high, low, close | Decimal / decimal strings | Finite, positive, <=18 significant digits, <=8 decimal places |
| volume | int / integer | 0 through signed int64 maximum; bool/fraction rejected |
| source, feed | strings | v1 supports `alpaca`, `iex` only |
| ingested_at | aware datetime / RFC3339 UTC | Receive time; UTC, not before event_time |
| schema_version | string | Exactly `1.0.0` |
| observation_id | lowercase SHA-256 hex | ID of raw observation envelope, not logical bar |
| source_event_id | optional nonblank string | Provider identifier if supplied |
| source_revision | optional nonnegative int | Only populated for a documented ordered provider revision |
| supersedes_observation_id | optional SHA-256 | Explicit correction linkage if available; cannot reference itself |

OHLC invariant: `low <= min(open,close) <= max(open,close) <= high`.
UTC offsets other than zero and naive timestamps fail; adapters must explicitly
convert aware timestamps to UTC. Decimal prices avoid transport rounding;
Silver Parquet uses decimal128(26,8), volume int64, timestamps microsecond UTC.
Do not cast fractional volume before checking. Float64 conversion occurs only at
the measurement boundary with tolerance tests. The contract excludes credentials.
The executable dataclass is the v1 validation authority; no duplicate speculative
JSON schema is maintained before a wire adapter exists.

Logical key: `(source, feed, symbol, timeframe=1Min, event_time, adjustment=raw)`.
`MarketBar.logical_key`, `.ohlcv` and `.bar_end` expose series identity, market
values and interval end without altering serialized fields or selecting revisions.
Observation ID is SHA-256 of a versioned, canonical raw envelope containing the
exact payload, acquisition ID and page/row or connection/receive-sequence locator;
credentials and receive wall-clock guesses are excluded. Replay preserves that ID.
It differs from logical identity and content hash. Bronze manifest stores both.
Price-only hashing cannot distinguish different acquisitions or order revisions.

Alpaca labels bars with interval start. Updated bars can revise that same timestamp;
it therefore cannot order corrections. See [Alpaca FAQ](https://docs.alpaca.markets/us/docs/market-data-faq)
and [streaming documentation](https://docs.alpaca.markets/us/docs/real-time-stock-pricing-data).

## Layers and provenance

Bronze retains original payloads, including invalid ones, in immutable acquisition
objects, with receive timestamp, request parameters without authentication,
page/connection locator, run UUID, source/feed, adjustment, checksum and acquisition
as-of time. Canonical validation occurs after raw capture; invalid observations
are quarantined with stable reason codes and Bronze references. A timeout or
partial acquisition never receives a successful complete-dataset manifest.

Silver includes the canonical bar, logical key, selected observation ID,
resolution-policy version, session ID, exchange-calendar version, membership
version and sector. Unknown study symbols remain in Bronze but are excluded from
study Silver with `unknown_membership`. Deduplication distinguishes:

1. Same observation ID + same content: idempotent replay.
2. Same ID + different content: integrity failure.
3. Same logical key + same market payload: equivalent duplicate, retain lineage.
4. Different payload: choose only an explicit valid supersession chain or a
   documented source revision order within one comparable revision namespace.
5. No comparable revision order, equal revisions with different payloads, forks
   or cycles: quarantine conflict; never choose max price/event/ingestion time.

A later REST snapshot may differ from a stream snapshot without source revision
ordering. Compare them as separate dataset snapshots; do not silently overwrite.
Different feeds/adjustments never deduplicate together.

### Acquisition and correction cases (future resolver acceptance fixtures)

| Case | Required outcome |
|---|---|
| Repeat a REST acquisition | New acquisition/observation IDs; identical logical key and OHLCV may share one canonical value, with both provenance references |
| Identical REST and WebSocket OHLCV | Equivalent only under the same feed, timeframe, symbol/time and raw adjustment; preserve both observations and transport metadata |
| Conflicting REST snapshots | Distinct as-of dataset snapshots; unresolved in a combined snapshot absent comparable revision evidence |
| Updated bar (`T=u`) | Capture the update channel and payload; target logical bar is known, but timestamp/channel alone does not totally order conflicting updates |
| Explicit ordered revisions | Select highest documented provider revision only inside one recorded revision namespace; validate same logical key, retain all predecessors |
| Explicit supersession | Resolve only a unique acyclic same-key chain with evidence for each edge; reject forks, dangling targets and contradictions with ordered revisions |
| No revision evidence | Quarantine the entire conflicting logical key; arrival order, receive time, event time, max price and hash order cannot select a winner |
| Replay | Keep original IDs, payloads, capture timestamps and source locators; replay execution timestamps are separate provenance |

The [Alpaca bar schema](https://docs.alpaca.markets/us/docs/real-time-stock-pricing-data)
documents original/updated bar types but does not expose an ordered bar revision
counter in those fields. Do not synthesize `source_revision` from receive sequence,
trade count or timestamp. `source_event_id` is not automatically an ordering key.
Leave revision/linkage null unless evidence exists. A type-u update signals a
revised aggregate, not a globally ordered stream across reconnects/acquisitions.

Observation hashing must preserve exact raw bytes (or a separately checksummed
lossless payload) and use unambiguous length-prefixed canonical envelope encoding;
version the algorithm before Phase 1. A retry within the same acquisition reuses
its persisted locator; an independent acquisition uses a new acquisition ID.
The normalized MarketBar is not the entire Bronze envelope: extra raw fields,
channel, revision namespace and evidence references remain in Bronze/manifests.
Silver stores all contributing observation IDs, the selected value and evidence,
and a resolution-policy version. Equivalent payloads can share a value without
arbitrarily promoting one acquisition to the source of truth. Domain tests
distinguish identities/payloads. Phase 1 implements the Bronze observation hash
encoder in `processing.historical.observation_id`, as specified in
PHASE1_INGESTION.md; a source-correction resolver remains unimplemented.

Bronze v1 does not place the page, provider row index and response-body hash on
each observation row, so Silver records a limited identity-verification level and
does not claim independent cryptographic reconstruction. The additive Bronze v2
proposal and compatibility policy are in `BRONZE_V2_IDENTITY_PROPOSAL.md`; no v2
schema is implemented by Phase 2.

Gold keys include dataset snapshot, source/feed/adjustment, entity, UTC window
bounds, measurement version, membership version, calendar version and publication
revision. Store expected/observed bars, constituents, returns and valid pairs;
missing/invalid reason, finalization cutoff, input manifest hashes, run UUID and
code/environment identity. Counts and quality cannot be inferred only from
observed constituents.

## Time, membership and completeness

Processing time is execution-clock time; ingestion time is capture-clock time;
event time is the interval start. They are separate fields. Negative receive lag
fails this v1 contract and goes to quarantine for clock investigation.
Use an exchange calendar with America/New_York timezone, holidays, DST and early
closes; never assume 390 minutes every day. The proposed provider is
`exchange_calendars`/XNYS, with explicit bounds; see ADR 005 for alternatives,
historical applicability and provenance. Installation/pin/integration is a Phase 1 gate.
Exclude extended sessions initially; do not infer a missing session from observed
rows. Trading halts/no qualifying trades/outages are missing, never zero returns.

Membership reference rows are `(symbol, sector, valid_from, valid_to,
membership_version)` with half-open UTC effectiveness and no overlapping groups.
`config/project.toml` supplies only the versioned study cohort, not historical
effective dates. Phase 1 must attach an explicit applicability interval before
producing sector-enriched historical data. Windows crossing a membership change
are ineligible in v1; use the expected universe at the window start otherwise.
The six labels and all 30 configured symbols remain the original study taxonomy,
not GICS or a historically reconstructed investable index. A retrospective fixed
cohort may be used only with an explicitly declared study interval and eligibility
evidence for each security; this is not an invented historical industry-membership
date. Record ticker/entity mapping, listing availability and exclusions. Selected
survivors and current sector labels introduce survivorship/selection bias. Sector
statistics work for variable N; nothing requires five constituents.

Return-end windows are `(a,b]`: a return ending at `t` needs closes of adjacent
bars whose end times are `t-1min` and `t`. Thus H returns need H+1 closes within
one session; the first session bar cannot produce an overnight return. A bar at
event_time `u` closes at `u+1min`. Availability cannot precede bar end or ingestion.
Window starts are `a_k = session_open + 1min + k*H*1min`, k>=0; ends are
`b_k = a_k + H*1min`. Anchor to the scheduled first close even when it is missing,
never to the first observed close. Retain only complete scheduled windows with
`b_k <= session_close`; report leftover session-edge minutes as unsampled rather
than compressing them into another window. Rolling SD uses the same eligibility
rules with a one-minute cadence rather than these fixed anchors.
The same explicit start/end keys govern Batch and Streaming, including DST days.

`domain.temporal.validate_return_window` checks supplied UTC minute-aligned session
bounds and H+1 ordered consecutive same-series bars. It rejects gaps, duplicates,
mixed symbols and cross-session windows, without sorting or filling. It does not
generate schedules, enforce fixed-window cadence, resolve revisions or compute
returns. Callers must resolve each bar and choose anchors before calling it.

### Concrete schedule fixtures

All local times below are America/New_York. Deterministic tests supply fixed-offset
UTC bounds independently; they test geometry, not calendar-provider or IANA tzdb
generation. [NYSE hours and exceptions](https://www.nyse.com/markets/hours-calendars)
are the schedule reference; Phase 1 must verify generated schedules against them.

| Fixture | Expected result |
|---|---|
| 2026-11-02 open 09:30 EST = 14:30 UTC | First bar [09:30,09:31), first close at 09:31; first adjacent-close return ends 09:32 |
| First five-return window | (09:31,09:36], closes at 09:31..09:36, six bars starting 09:30..09:35 |
| Missing opening or 09:32-start bar | First window ineligible; no reanchor/fill; a later intact window may be eligible |
| Spring DST: March 6 / March 9, 2026 | 09:30 local opens at 14:30 / 13:30 UTC respectively |
| Autumn DST: October 30 / November 2, 2026 | 09:30 local opens at 13:30 / 14:30 UTC respectively |
| November 27, 2026 early close | 09:30–13:00 EST = 14:30–18:00 UTC; 210 scheduled bars, not 390 |
| Last trailing five-return early-close window | (12:55,13:00], six closes; next minute crosses the session and is rejected |
| Previous session close + next opening bar | Never an adjacent same-session return, even if both exist |

For H=5, a 390-bar session supplies 389 adjacent returns, 77 complete anchored
windows and four unsampled final returns. A 210-bar early-close session supplies
209 returns, 41 complete windows and four unsampled final returns. A trailing
window ending at the session close is eligible but is not an extra fixed window.

## Sector calculation identity and eligibility

The calculation-only Sector Gold population relation requires
`population_policy_version=sector-complete-population-v1`, an
`eligibility_policy` of `declared_research` or
`verified_historical_applicability`, and
`eligibility_policy_version=sector-eligibility-policy-v1`. A sector/session cannot
mix these authorities. Missing or unknown values fail closed.

Every scalar and Spark sector window and pair row preserves the eligibility policy
and version. `sector-output-identity-v1` is the lowercase SHA-256 of a UTF-8,
pipe-delimited payload containing, in order: identity version, sector, session ID,
horizon, zero-based window index, comma-joined sorted expected symbols, population
policy version, eligibility policy, eligibility-policy version and research-reference
SHA-256. The delimiter is forbidden by the validated source fields. Changing policy
therefore changes output identity even when financial values happen to be equal.

Primary ineligibility precedence is acquisition scope, complete membership,
historical applicability under the declared policy, wholly absent member, missing
observed bar, then ineligible individual return. Correlation degeneracy,
insufficient-pair, concentration and threshold-calibration reasons are separate
metric states.

## Lateness and finalization policy (future processing)

- **Allowed lateness L:** configurable `allowed_lateness_minutes`, default 10,
  nonnegative integer, recorded in each run. This is a research eligibility policy,
  not a provider guarantee. Phase 0 validates configuration only.
- **Event-time watermark:** progress estimate in the bar-END clock. Proposed
  conservative policy `W=min_p(max_observed_bar_end[p])-L`, over a fixed manifest
  of required partitions. A window ending b is a candidate when W>=b. If an engine
  tracks bar-start time, convert to ends first; subtract L once, not twice.
- **Source completeness:** a separate claim about expected observations and
  corrections. A watermark does not prove this. Gold records expected/observed
  coverage, unresolved keys and `source_completeness=unproven` unless independently
  evidenced; a fully observed window may still receive a future correction.
- **Idle/uninitialized partition:** holds the conservative watermark back. Do not
  remove it silently or substitute processing time. Idle-source timeout/health is
  operational telemetry, not evidence that no future bar/correction can arrive.
  If the entire live source goes idle, keep affected windows provisional. A future
  explicit session reconciliation/bounded snapshot run can close them; there is
  no automatic wall-clock-only finalization in v1.
- **Bounded replay completion:** a manifest fixes dataset hashes and terminal
  offsets for every required partition. Finalize after consuming those offsets,
  without synthetic financial bars to push a watermark. This proves exhaustion
  of the declared snapshot, not completeness of all market observations.
- **Finalization cutoff:** publication records input manifest/offset vector,
  capture-time cutoff C, W (if applicable), L, revision-policy version and reason
  (`watermark_snapshot` or `bounded_snapshot`). Include only original observations
  eligible under that snapshot and capture cutoff; replay time never substitutes
  for original capture time. Persist the accepted set before comparing engines.
  Never label output “market complete” solely because it is finalized.
- **Publication:** resolve accepted observations first. Incomplete or ambiguous
  windows produce a finalized null with coverage/reason for that immutable
  snapshot. A finalization marker commits all keys, including null results.
- **Late correction after publication:** retain raw evidence and register a new
  snapshot/recomputation request. A reviewed recomputation publishes a new Gold
  revision with predecessor lineage; it never mutates the old publication.

Future acceptance fixtures: fully idle input never finalizes by elapsed wall time;
one idle/uninitialized partition blocks W; changing L changes eligibility but not
market truth; W=b is eligible, W<b is not; empty bounded partitions still require
terminal markers; consumed terminal offsets allow finalization without new events;
an ambiguous revision makes a null window; corrections after cutoff produce new
versions; Batch and replay compare the identical accepted set, keys/nulls/counts
and declared numeric tolerance. These behaviors are specified, not executed today.

## Source adjustment policy

Initial research uses raw IEX prices/volume and regular sessions only. Phase 1
must request `feed=iex`, `timeframe=1Min`, `adjustment=raw` explicitly and preserve
parameters/provider symbol-as-of mapping in the acquisition manifest. Alpaca's
[historical API](https://docs.alpaca.markets/us/reference/stockbars) distinguishes
raw from corporate-action adjustments. Adjustment choice precedes return formulas:
log ratios do not themselves split- or dividend-adjust prices. Same-session rules
exclude overnight jumps, but do not repair every corporate-action discontinuity.
Flag affected data; never silently transform it. Adjusted future experiments need
distinct dataset identities and comparable source policies; do not mix with live
raw IEX. No adjusted dataset is implemented or acquired in Phase 0.

## Schema evolution and storage ownership

Major version for changed field meanings/types or identity rules; minor for
optional backward-compatible additions; patch for clarifications that do not
alter values. Readers explicitly accept supported versions; v1 rejects unknown
versions. Migrations produce new datasets with input hashes, never mutate Bronze.
Measurement and membership versions evolve independently of bar schema versions.

Future layouts: `data/{bronze,silver,gold}/dataset=<id>/run=<uuid>/...` and
`artifacts/experiments/<experiment-id>/...`; query checkpoints under a separate
V2-owned root coupled to run/output identity. Partition Bronze by acquisition date
and feed, Silver by session date/feed (avoid tiny symbol files), Gold by measure
and session date. Validate ownership before writes/deletion. No legacy volumes,
paths or checkpoint IDs are reused. Storage is created by the owning future
runner, not by Phase 0 imports or configuration checks.
