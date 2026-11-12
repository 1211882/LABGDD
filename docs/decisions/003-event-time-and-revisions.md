# ADR 003: Immutable observations and explicit revision resolution

Date: 2026-11-02. Clarified: 2026-11-03. Status: accepted foundation;
calendar integration and live processing remain future work.

Decision: separate logical bars from source observation identities, preserve raw
Bronze including rejects, and resolve corrections only through documented source
revision order or explicit supersession. Otherwise quarantine conflicts. Normalize
UTC, label intervals by their start, compute returns on bar ends, version calendar
and membership independently. Finalized outputs reference an as-of snapshot;
late revisions create new publications rather than mutating scientific evidence.

Consequences: event-time ordering, arrival-time last-write-wins and lexicographic
OHLCV choice are rejected as unreliable correction policies. Historical/live
snapshots are comparable only after deliberate reconciliation. Ten-minute lateness
is an initial proposal, not a guarantee of complete market reporting.

Review clarification: window starts are scheduled-open + one minute + k*H minutes,
never first-observed-close. H+1 consecutive same-session closes supply H returns.
The pure temporal validator checks supplied schedule bounds, not calendar truth.
`allowed_lateness_minutes` is configurable (default 10). Proposed watermarks use
bar-end time and deduct lateness once. Idle required partitions hold progress;
no live wall-clock-only finalization is authorized. Bounded terminal offsets prove
snapshot exhaustion, not provider completeness. Every publication freezes its
accepted input set and capture cutoff, preserving nulls and all lineage.

Alpaca updated-bar type and bar timestamps do not provide a global revision order.
Do not synthesize source revisions from arrival sequence. Duplicate acquisitions
retain distinct observations; identical payloads may share canonical values.
Conflict handling, evidence namespaces and post-publication revisions are explicit
in DATA_CONTRACTS.md and require future resolver tests. ADR 005 records the newly
selected calendar proposal and confirmed raw IEX adjustment policy.
