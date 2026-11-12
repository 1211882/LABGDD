### LABGDD V2 – Development Handoff

**Phase:** Phase 2 — Silver finalization
**Task:** Authoritative publication, timezone regression closure and baseline freeze
**Status:** Completed
**Date:** 2026-11-03 (Europe/Lisbon)
**Git branch and commit:** `main`; this report is included in annotated baseline tag `phase2-silver-approved-v1`. The actual commit and annotated tag object identities are reported by the execution that creates the tag; parent before this milestone was `d148b45b24d22cb6d0a0c8e20b71cba3b3900c8a`.

#### 1. Objective

Finalize the accepted bounded Bronze-to-Silver architecture, make the corrected AAPL run the only selectable authority, preserve and supersede invalid historical outputs without mutation, close timezone regressions across Spark and compatibility persistence, assess the Bronze identity limitation, and freeze a clean Git baseline before any Gold work.

#### 2. Changes Implemented

`config/silver_publications.toml` is the minimal external publication registry. It approves only dataset `silver-aapl-20260914-v1`, run `aecb53c1-d7a8-40c7-939b-a6901f435ff5`, and pins its manifest SHA-256, processing-source fingerprint and `config/silver.toml` fingerprint. It classifies `0c822d2b-be75-4497-a99a-06246013973a` as superseded for timestamp-invalid Windows compatibility output and `ed452342-931f-4549-809c-b072b4e8199a` as superseded for timezone-implicit INT96 output. All run files remain unchanged.

`labgdd.adapters.silver.resolve_approved_silver` requires callers to provide both dataset version and run UUID. It validates the strict registry schema, rejects superseded and unregistered IDs, resolves only the pinned path under the V2 Silver root, verifies request/manifest/source fingerprints, requires the Silver manifest to be `complete`, and recomputes every artifact size and SHA-256 against an exact inventory. It never scans for or selects the newest directory.

The bounded `pyarrow_compat` writer now projects every timestamp to Spark `unix_micros` before collecting and reconstructs aware UTC datetimes from integer microseconds. Python and the host timezone therefore cannot reinterpret naive Spark datetimes. The native writer continues using `TIMESTAMP_MICROS`, explicit physical-schema validation and no result collection.

Timezone fixtures now cover summer EDT, winter EST, the spring DST transition and a 210-minute early close. They validate `event_time`, `bar_end`, `session_open`, `session_close`, `membership_valid_from` and `membership_valid_to`. The Linux smoke validates four physical session partitions and replaces `DataFrame.collect` with a failing function on the native path.

`docs/architecture/BRONZE_V2_IDENTITY_PROPOSAL.md` records a separate, unimplemented migration proposal. Bronze v1 remains supported for the approved bounded evidence with the limited verification claim. V2 becomes a gate before independently reconstructing observation IDs or treating full-universe, multi-acquisition identities as cryptographically verified.

#### 3. Files Created or Modified

- `config/silver_publications.toml`: authoritative and superseded Silver decisions with pinned fingerprints.
- `src/labgdd/adapters/silver.py`: `ApprovedSilver` and fail-closed `resolve_approved_silver`.
- `src/labgdd/processing/silver.py`: host-independent compatibility timestamp conversion via epoch microseconds.
- `tests/unit/test_silver_registry.py`: explicit authority, no-newest selection, supersession, tamper and failed-status tests.
- `tests/integration/test_spark_silver.py`: summer/winter/DST/early-close logical and compatibility persistence assertions.
- `scripts/smoke_silver_container.py`: four-session native physical/logical UTC validation and collection guard.
- `docs/architecture/BRONZE_V2_IDENTITY_PROPOSAL.md`: exact v2 provenance fields, compatibility and acceptance gates.
- `README.md`, `docs/architecture/ARCHITECTURE.md`, `DATA_CONTRACTS.md`, `PHASE2_BATCH.md`, and ADR 007: explicit publication authority, timezone and identity decisions.
- This handoff. Existing Phase 0/1/2 evidence and prior handoffs were not overwritten.

The milestone commit also includes the previously reviewed Phase 2 Silver implementation, Docker runtime, locks, tests and documentation that were still uncommitted at task start.

#### 4. Architecture and Design Decisions

A completed immutable directory is evidence, not authority. Authority is a separate version-controlled decision, selected by explicit dataset/run identity and verified against immutable artifacts. This avoids conflating filesystem discovery, creation time or lexical UUID order with scientific approval. Unregistered intermediate runs remain unselectable.

The registry is intentionally a single-dataset TOML contract rather than a database or catalog service. Its strict v1 schema can evolve only through review. Gold must call the resolver; direct “latest directory” discovery is prohibited.

Native Spark persistence remains the production path. Compatibility collection remains bounded and test-only. Using Spark epoch microseconds before collection is necessary because Python `Row` timestamp conversion on Windows can apply the host zone even when Spark SQL is configured for UTC.

Bronze v2 is justified but not implemented. The current identity encoder hashes `(identity_version, acquisition_id, page, symbol, row, response_body_sha256)`. V1 observation rows omit page, row and response-body hash linkage, so they cannot independently reconstruct the ID. Proposed v2 rows add the algorithm version, acquisition, transport, response page, provider symbol, provider row index, response-body hash and response-artifact reference. V1 is never rewritten; mixed inputs retain the weaker v1 verification level.

#### 5. Tests and Validation

Passed on Windows with CPython 3.11.9:

- `.venv/Scripts/python -m ruff format --check .` — 41 files.
- `.venv/Scripts/python -m ruff check .`.
- `.venv/Scripts/python -m mypy` — strict typing, 41 source files.
- `.venv/Scripts/python -m pytest --basetemp .venv/phase2-final-pytest` — **214 passed** in 308.62 seconds; no skips.
- `.venv/Scripts/python -m pip check` — no broken requirements.
- `.venv/Scripts/python scripts/check_repository.py` — 84 candidates passed before the final handoff was added; rerun during the staging gate.
- Silver configuration check — passed without starting Spark.
- Actual publication-registry resolution — approved run, manifest hash and all artifact hashes verified.
- `.venv/Scripts/python -m build --no-isolation` — package 0.3.0 sdist and wheel built.
- Clean wheel smoke — install, imports, module/console CLIs and `pip check` passed.
- `git diff --check` — passed with line-ending notices only.

Docker validation passed with the digest-pinned non-root image `labgdd-spark-batch:phase2-final`. The same four-session smoke passed under both `TZ=Europe/Lisbon` and `TZ=America/Los_Angeles`: four canonical records, zero quarantine, four physical partitions, UTC microsecond physical timestamps, exact logical instants and membership bounds, and no native `.collect()`.

The authoritative manifest remains SHA-256 `33209b03a5f5f6d39efea922376e13f08c59756aa5a7cc01e3cae367711933d6`, with 390 canonical records, zero quarantine/duplicates and 390/390 XNYS coverage. Source and request fingerprints matched the registry. No Alpaca request or historical reprocessing ran during finalization.

#### 6. Issues and Limitations

The registry is local/version-controlled governance, not a concurrent catalog, database transaction or object-store commit service. It supports the one approved dataset and must be versioned deliberately for multiple authorities.

Bronze v1 identity remains non-reconstructable from each observation row. Historical entity/listing/sector membership, IEX consolidated-market completeness, provider correction finality, multi-node Spark, distributed filesystem recovery and object-store publication remain unverified. These do not invalidate the bounded approved evidence but block broader scientific claims.

#### 7. Scientific and Data Integrity

No financial formula, measurement v2.0.0 definition, raw-adjustment policy, missing-minute policy or event-time contract changed. The compatibility fix enforces the existing UTC interval-start contract. Invalid and superseded artifacts remain immutable evidence but are excluded from authority. No missing bars were filled and no historical membership was invented.

#### 8. Current Project Status

Phase 2 Silver is finalized for the bounded AAPL session: immutable Bronze verification, strict Spark normalization, native Parquet publication, failure-safe manifests, explicit authority resolution and cross-timezone regression evidence are operational. Gold and all later systems remain not started.

#### 9. Recommended Next Step

Review the frozen Silver baseline and publication-registry interface. Before Gold, approve a separate task defining Gold input selection exclusively through `resolve_approved_silver`, and decide whether Bronze v2 must be implemented before any full-universe/multi-acquisition expansion.

#### 10. Review Questions

None. The accepted decisions were implemented as requested.

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
