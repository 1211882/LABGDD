# Phase 1, milestone 1: mocked historical acquisition and local Bronze

Date: 2026-11-03. Phase 0 is immutable at commit
`81be7794ffa4c197e866f3fc89d147287abb3731`, annotated tag `phase0-approved-v1`.
This milestone does not implement Silver, analytics or infrastructure. No live
market acquisition was executed during development.

## Boundaries and dependencies

`historical_config.HistoricalRequest` and `load_historical_request` validate a
separate TOML request against the existing configurable study universe. Symbols
are a nonempty unique subset; the cohort/sector taxonomy is unchanged. Requests
use UTC minute-aligned `[start,end)` bounds, maximum 31 days for this milestone,
raw IEX and 1Min. The CLI also rejects a future historical endpoint before I/O.
Dataset identifiers cannot escape owned storage. Attempts, timeout and page counts
are explicitly bounded. `--check` validates configuration without API credentials,
calendar generation or data-directory creation.

`adapters.alpaca.AlpacaREST` uses standard-library urllib and an injectable
transport callable. Authentication comes only from `ALPACA_API_KEY` and
`ALPACA_API_SECRET`. Credential representations omit values; logs use only
allowlisted events. HTTPS endpoint is fixed to the Alpaca data host, and redirects
are rejected to prevent forwarding authentication. No secrets enter manifests.
Responses are capped at 16 MiB, request timeout defaults to 15 seconds, attempts
to three and pages to 1,000. Retry 429/500/502/503/504 and transport failures with
exponential backoff. Retry-After accepts seconds or HTTP dates; invalid values or
delays over 30 seconds fail instead of retrying before the provider deadline.
Total theoretical wait/request budgets remain bounded by attempts and pages.

The [provider API](https://docs.alpaca.markets/us/reference/stockbars) has inclusive
end and continuation tokens. Map project end to end minus one microsecond; preserve
actual parameters and symbol-as-of date. Detect repeated tokens, identical response
pages and page-limit exhaustion. Empty/null bar maps are exhausted snapshots with
missing coverage, not claims of complete market observations. Reject malformed
envelopes, duplicate JSON keys and nonstandard JSON numeric constants after capture.

Calendar/Parquet code is isolated in adapters and installed via optional extra
`ingestion`. Exact selected versions: exchange_calendars 4.11.2, PyArrow 21.0.0,
NumPy 2.3.4, pandas 2.3.3, tzdata 2025.2. The latter two numerical libraries are
calendar dependencies, not an analytics engine. Versions are compatible with the
tested Python 3.11.9 environment and deliberately pinned; they are not claimed to
be newest. Official release metadata:
[exchange_calendars](https://pypi.org/project/exchange_calendars/4.11.2/),
[PyArrow](https://pypi.org/project/pyarrow/21.0.0/).
The development lock includes all optional dependencies and transitive hashes.
The base wheel still imports and validates configuration without this extra.

## Calendar and applicability

`adapters.calendar.xnys_snapshot` builds an explicit-date XNYS schedule with
left-labeled intervals. It records provider version, date bounds, timezone name,
pinned tzdata distribution/version/file checksum, schedule rows/hash and no-override
version. Generated opening/closing offsets are cross-checked against a ZoneInfo
loaded directly from the pinned tzdata package, independent of system tzdb lookup.
The tests actually generate spring/fall DST schedules, exchange holidays including
Good Friday, weekends, the 2026-11-27 early close and regular-session endpoints.
Session rules follow the [NYSE schedule reference](https://www.nyse.com/markets/hours-calendars).

All configured symbols are checked for cohort applicability, not for historical
listing/industry membership. XNYS is an approved common-session benchmark, not an
assertion of listing venue or a guarantee of historical IEX availability for each
symbol. Metadata explicitly records historical listing membership as unverified.
Do not use these raw acquisitions as sector-enriched historical evidence until
listing/entity/as-of applicability is established for the study interval. No
industry membership dates or asset master were invented.

## Raw-first persistence and identity

`processing.historical.acquire_historical` captures each successful response before
JSON or bar validation, then validates each observation using the existing
MarketBar schema. No casts truncate fractional volume. Decimal prices and exact
minute labels are checked, including sub-microsecond timestamps that Python would
otherwise truncate. Preserve unexpected-symbol/out-of-range/invalid observations
with a reason, and classify valid extended-session data without dropping it.

`adapters.bronze.BronzeAcquisition` exclusively creates:

```text
data/bronze/dataset=<dataset-version>/acquisition=<UUID>/
  acquisition.json
  calendar.json
  response-000001.parquet
  observations-000001.parquet
  ...
  manifest.json
```

Response Parquet stores page number, original request token, UTC receive time,
exact body bytes and body checksum. Observation Parquet stores observation ID,
symbol, a readable raw-JSON projection, optional event time, validity/reason and
session classification. Decimal values in the readable projection become strings;
the exact response bytes are authoritative and retain original numeric spelling,
extra provider fields and malformed payloads. All rows, including duplicates and
conflicts, remain Bronze. Source/feed/schema and acquisition lineage are inherited
through the acquisition manifest. No canonical revision winner is selected.

Observation ID algorithm `bronze-observation-v1`: SHA-256 of each UTF-8 field
prefixed by its unsigned eight-byte big-endian byte length, in order: algorithm
name, acquisition UUID, 1-based page index, symbol, 0-based within-symbol row index,
SHA-256 hex of the exact response body. Independent acquisitions have new UUIDs;
replaying stored pages with original locators preserves IDs. Receive wall-clock
timestamps, credential headers and price ordering never order revisions.

## Completion, failure and manifests

All artifact writes use exclusive file creation. Parquet and JSON files are
flushed/fsynced. `manifest.pending` is linked atomically to `manifest.json`, failing
if the target exists, then the pending name is removed. This local-filesystem
publication is considered committed once the terminal link exists; if pending-name
cleanup fails, its harmless alias may remain without changing the published state.
The local-filesystem
protocol requires hard-link support; it is tested on NTFS, not object storage or
a power-loss durability guarantee. Completed acquisition directories are never
reopened for mutation; repeated requests create distinct acquisitions. UUID reuse
fails before any provider request. An interrupted run lacking a terminal manifest
is incomplete; no automatic resume or cleanup deletes its evidence.

Manifest fields include dataset/acquisition/run/schema/identity versions; source,
feed, timeframe, adjustment, exact base parameters, UTC start/finish timestamps;
Python/package versions and source fingerprint; calendar snapshot; cohort version;
policy budgets; page/row/invalid/duplicate/conflicting-key counts; expected/observed
unique regular symbol-minutes and missing counts; relative artifact paths, sizes
and SHA-256; terminal status and controlled error code. Raw-page tokens reconstruct
per-page continuation requests. Expected minutes come from schedule bounds clipped
to the request, never a fixed 390-minute assumption. Coverage counts are not proof
that conflicting values were resolved or all provider corrections have arrived.

`complete` means the declared pagination exhausted successfully, even with invalid
or missing observations. `failed` marks controlled API/pagination/envelope failures
and retains prior pages. Disk failure may leave only started metadata/partial
artifacts and raises a controlled uncommitted-storage error; it cannot publish a
successful manifest. Do not treat started metadata as completion. A manifest's
artifact hashes can be checked without interpreting market values.

## Remaining acceptance and review gates

Mocked integration tests exercise HTTP mapping, successful and empty acquisition,
pagination/cycles/limits, malformed/invalid/duplicate observations, gaps and partial
failures, immutable storage, checksums and calendar provenance. No authenticated
provider entitlement, live rate limit, historical listing master, object-store
commit protocol, correction resolution, replay executor or Silver engine is
claimed. Review this milestone before extending ingestion or authorizing Phase 2.

## Milestone 2: September 14 integration validation (2026-11-03)

Credentials were unavailable to the execution's process environment, so no real
Alpaca request ran. Dependencies were available. The installed XNYS adapter
generated AAPL's requested common-session interval as
`[2026-09-14T13:30:00+00:00,2026-09-14T20:00:00+00:00)`, 390 scheduled minutes.
This validates schedule generation, not listing history or actual IEX availability.

`tests/integration/test_milestone2_session_validation.py` exercises that exact
scope with synthetic responses. Its primary acquisition archives two response
pages and all five supplied observations: four valid records, one invalid
fractional-volume record, one equivalent duplicate, three unique valid regular
minutes and 387 missing scheduled minutes. Missing observations are not filled.
These counts are mocked evidence, not real AAPL market-data statistics.

The tests verify exact response bytes and numerical spelling, OHLCV/provider-extra
preservation, UTC normalization including a synthetic offset timestamp, independently
reconstructed observation IDs, exact Parquet schemas/row counts, manifest/calendar
consistency, all artifact hashes, identical repeated reads, and unchanged completed
files after a duplicate acquisition attempt. HTTP 403 and retry-exhausted 503
fixtures retain their partial observations under failed terminal manifests.

No production defect was exposed by these cases; production components and
dependencies were unchanged. All artifacts for this milestone's executed checks
were rooted under the ignored V2-owned `.venv/milestone2-runs/` directory, including
pytest temporary directories and temporary wheel-installation environments.
Live provider validation remains pending. See
`docs/handoffs/PHASE_1_MILESTONE_2_HANDOFF.md` for executed checks and evidence paths.

## Milestone 3: authenticated session validation (2026-11-03)

Following explicit authorization, the existing pipeline acquired real AAPL raw
IEX one-minute bars for the XNYS-derived September 14, 2026 session:
`[2026-09-14T13:30:00+00:00,2026-09-14T20:00:00+00:00)`.
One HTTP request returned one terminal page and 390 observations. All 390 passed
validation and covered 390 unique scheduled minutes; invalid, duplicate,
conflicting and missing-minute counts were zero for this acquisition only.
This does not establish general feed completeness or correction finality.

Acquisition ID: `f2345e41-da10-420f-8b60-6079a9df2006`. Manifest:
`data/bronze/dataset=milestone3-live-aapl-20260914-v1/acquisition=f2345e41-da10-420f-8b60-6079a9df2006/manifest.json`.
Exact payloads, field preservation, UTC labels, observation IDs, physical schemas,
row counts, manifest/calendar consistency, all four artifact checksums and repeat
reads were verified. Reusing the same acquisition ID failed without another HTTP
request or any changes to completed files. No synthetic data was mixed into it.

The response envelope and fields match mocked contracts; whole-number OHLC values
appear as JSON integers within a batch otherwise containing decimal numbers. The
existing parser handles this; a new synthetic mixed-numeric regression fixture
records that format without copying real prices into Git. Live continuation-token
pagination, throttling and invalid-provider-record behavior were not exercised by
this successful single-page acquisition and remain mocked evidence.

Credentials initially supplied in tracked `.env.example` were moved, at the user's
request, to ignored/untracked `.env`; the example was restored blank. Only the
authorized validation process explicitly loaded that file into its environment.
Production code retains environment-only authentication and no automatic dotenv
loading. Credentials are excluded from manifests, logs and commits. See
`docs/handoffs/PHASE_1_MILESTONE_3_HANDOFF.md` for the complete validation handoff.
