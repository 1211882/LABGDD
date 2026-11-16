# Milestone 9: Approved Expected-Series Gold projection

Status: implementation candidate; ADR 015 remains Proposed. No Sector Gold publication.

`processing.approved_sector.project_approved_expected_gold` resolves an explicit
approved Gold identity, checks upstream approved Silver and research/calendar/plan
fingerprints, and projects complete independent populations plus existing upstream
return values. `calculate_approved_sector_gold` owns that entire read path and calls
the existing native Sector Gold engine. Externally supplied frames cannot enter the
approved path through the public calculation API.

The population relation preserves every expected symbol/session, membership bounds,
research evidence and acquisition scope. Return rows preserve close-to-close log
returns, exact minute ends, observed-bar counts, financial nullability and reasons.
Opening bar slots remain in coverage but are not adjacent returns. Financial values
are never filled, recomputed or compressed. Real-input outputs retain exact upstream
identities, fingerprints and an additional source-bound hash without changing the
accepted ADR 014 output identity contract.

Coverage has expected_slots, observed_slots, missing_slots, acquisition_scope and
upstream_eligible_returns. These count upstream bar-indexed Gold slots, not sector
fixed-window members or exchange-wide availability. The proposed bridge accepts
declared_research and verified_historical_applicability; a stricter downstream policy
cannot manufacture historical verification.

All fields must match the approved regenerated expectation grid; physical Parquet
types, UTC instants, canonical provenance, duplicates, acquisition scope and manifest
coverage counts are validated before engine execution. Aggregate checks execute in
Spark; the production path does not collect financial rows or use Python UDFs.

The pilot identity is gold-study-cohort-20260914-multisymbol-expected-v1,
39ab367c-c6d2-480d-8c94-f35f767ed289, manifest
d97b0e48c8463b6a97aa58e576e7f24998b4246f496fabaa286982177ab605b2.
It has 30 research symbols, 11,700 scheduled Gold slots, 1,861 observed bars and
9,839 missing slots. Only AAPL/MSFT/NVDA/JPM/XOM were requested; the other 25 are
out-of-scope research members. No sector has full coverage, so complete-sector
financial metrics must remain null with member_outside_acquisition_scope precedence.

These counts are an expected validation target until executed evidence is recorded
in the Milestone 9 handoff. Fixtures are synthetic and isolated; optional real tests
skip only when the exact run directory is absent. Present-invalid evidence fails.
All data and registries remain immutable. No approval tag is created.
