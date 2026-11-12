# LABGDD V2 – Development Handoff

**Phase:** Phase 2, Milestone 2 – Individual-company Gold financial analytics
**Task:** Implement and validate v2.0.0 returns, rolling volatility and fixed realized volatility
**Status:** Completed
**Date:** 2026-11-03
**Git branch and commit:** `main` at Silver baseline `0d1584bd4b159f0af3a7b780b46a2472419581d3`; Gold changes are intentionally uncommitted for review

## 1. Objective

Implement the first bounded Gold analytics milestone without changing the approved
financial or temporal contracts. Consume only explicitly approved Silver evidence,
calculate individual-company intrabar and adjacent-close log returns, trailing
sample volatility, fixed realized variance and realized volatility for H in
{5,15,30,60}, prove independent Python/Spark parity, publish immutable native
Parquet evidence, and stop before sector analytics or later infrastructure.

## 2. Changes Implemented

`labgdd.analytics.individual.calculate_individual_measurements` is an independent,
engine-free reference. `ReferenceBar`, `ReferenceReturn`, `ReferenceWindow` and
`ReferenceMeasurements` model scheduled observations and results. Decimal prices
remain Decimal through natural-log ratio construction; only measurement outputs
become finite float64.

`labgdd.processing.gold_batch.transform_individual_gold` generates expected
one-minute slots from Silver session bounds and uses Spark SQL/window functions for
the five approved measurements. It emits `returns`, `rolling` and `fixed` frames.
Missing bars are not filled, zeroed or used to move anchors. Eligibility rows retain
expected/observed counts and controlled reasons including `session_boundary`,
`missing_close`, `nonconsecutive_bars`, `insufficient_history`,
`incomplete_return_window`, `incomplete_fixed_window` and `membership_boundary`.

`labgdd.processing.gold.process_gold` calls the existing four-argument
`resolve_approved_silver` before reading input. It pins the approved Silver UUID and
manifest SHA-256, adds measurement/dataset/publication/finalization lineage to every
row, records configuration/source/runtime fingerprints, performs native Spark
staged writes, verifies physical UTC-microsecond schemas, counts and distributed
logical equality, then publishes a checksummed terminal manifest. `GoldRun` rejects
reuse of completed or failed UUIDs. `labgdd-gold` is the new bounded CLI.

## 3. Files Created or Modified

- `src/labgdd/analytics/individual.py`: pure v2.0.0 scalar reference.
- `src/labgdd/processing/gold_batch.py`: native Spark calculations and eligibility.
- `src/labgdd/processing/gold.py`, `src/labgdd/adapters/gold.py`: orchestration,
  validation and immutable publication.
- `src/labgdd/gold_config.py`, `config/gold.toml`,
  `src/labgdd/runners/gold.py`: strict approved-input request and CLI.
- `tests/unit/test_individual_measurements.py`, `tests/unit/test_gold_storage.py`,
  `tests/integration/test_spark_gold.py`: arithmetic, temporal, storage and parity tests.
- `scripts/validate_gold_evidence.py`, `scripts/smoke_gold_container.py`: independent
  real-evidence validation and Linux Spark smoke.
- `docker/spark-batch/Dockerfile`, `pyproject.toml`, `README.md`: Gold runtime,
  package v0.4.0/entry point and project status.
- `docs/architecture/PHASE2_GOLD.md` and
  `docs/decisions/008-individual-gold-measurements.md`: bounded design and proposed ADR.
- This handoff. Existing measurement/data contracts and immutable data artifacts
  were not modified.

## 4. Architecture and Design Decisions

The approved Silver registry is the sole input authority; filesystem recency is
never a selection policy. Fixed windows use `a_k=session_open+1min+k*H` and
`b_k=a_k+H`, while rolling windows advance at each expected adjacent-return end.
Rolling sample SD uses H-1; fixed realized variance is `sum(r^2)` and realized
volatility its square root. A 390-bar session therefore produces 389 adjacent
returns and 77/25/12/6 fixed windows. Trailing incomplete fixed returns are
unsampled, not compressed into a new window. Window membership timestamps and
calendar provenance remain explicit.

Spark never collects full result frames in production. It writes partitions by
session and symbol without global output ordering. The local staging/rename protocol
is failure-safe on this filesystem but is not claimed as an object-store transaction.
ADR 008 remains proposed pending review. No new third-party dependency was added.

## 5. Tests and Validation

Passed:

- `python -m ruff format --check .` – 53 files formatted.
- `python -m ruff check .` – passed.
- `python -m mypy` – strict typing passed for 53 source files.
- `python -m pytest` – **223 passed in 337.09s**. Pytest emitted post-success
  cleanup warnings for a damaged external Windows temporary directory; no repository
  file or test assertion failed.
- `python -m pip check` – no broken requirements.
- `python -m build --no-isolation` – built v0.4.0 sdist and wheel.
- `scripts/smoke_wheel.py ...0.4.0...` – clean install, imports, module/console CLI
  and pip consistency passed.
- `scripts/check_repository.py` – passed for 101 baseline candidates after this
  handoff; credentials, environments, data and generated build products remained ignored.
- Docker image `labgdd-spark-batch:phase2-gold` built from pinned bases; image ID
  `sha256:738cf7d13f4bd6e05a1449c2f6b98972efafc614c1b7df0177f8aef3cf94e39b`.
- Gold container smoke: `390/1556/120`; upstream Silver container smoke:
  `canonical=4`, `quarantine=0`, `sessions=4`.

The final real bounded publication is run
`2c36a9ab-78ce-45a8-869d-8c0225267d17`; manifest SHA-256 is
`cadbbcf50150b62ffc06aa348f503837c87281bbca2690d0179a788ec599d7ce`.
Its processing-source fingerprint is
`61a13645c92841deb3564b79d27d7c9e0a8e20d348009f36e21a770dc4859778`
and configuration fingerprint is
`a0bc3d8a7abe92a3b97226a730ab076a961206715b7746137f3a1719faf35094`.
It consumed approved Silver manifest SHA-256
`33209b03a5f5f6d39efea922376e13f08c59756aa5a7cc01e3cae367711933d6`.
Independent validation rehashed all seven Silver and seven Gold artifacts and found:
390 return rows, 389 eligible adjacent returns; 1,556 rolling rows, 1,450 eligible
(H=5/15/30/60: 385/375/360/330); 120 eligible fixed rows
(77/25/12/6). Maximum absolute errors were `1.2723112494117395e-16` rolling,
`6.869437202232376e-19` variance and `2.1640675362810669e-16` fixed volatility,
all well within `1e-12 + 1e-10*abs(expected)`.

The native Windows attempt failed safely at Silver read with Hadoop `Access denied`
and published failed UUID `dcd48e76-...`. Intermediate complete development runs
`e43ada7f-...` and `c6f9dc52-...` are preserved but superseded by the final source-
and-configuration-fingerprinted run above. No live Alpaca request was made.

## 6. Issues and Limitations

Evidence covers one AAPL IEX session, not the 30-symbol cohort or multi-session
scale. IEX venue coverage and provider correction finality remain unproven.
Historical listing/entity/sector membership is not independently established.
Bronze v1 identity reconstruction limits remain. The Windows local Hadoop reader is
unsupported; Linux container execution is the verified native path. Gold has no
approved-run registry yet, so downstream use must await architecture review rather
than discover a directory. Distributed-cluster and object-store behavior are not
verified.

## 7. Scientific and Data Integrity

No approved formula, timestamp convention, membership rule or Silver evidence was
changed. `event_time` remains interval start and return end is the ending close.
There is no overnight linking, gap fill, zero substitution, annualization, implicit
correction ordering or membership inference. Fixed and rolling outputs are separate.
The first bar has no adjacent return; warm-up and gap rows remain explicit nulls.
Real results are reproducible from the pinned Silver manifest, source/configuration
fingerprints, runtime versions and checksummed Gold artifacts.

## 8. Current Project Status

Phase 0 remains frozen; Phase 1 Bronze and approved Phase 2 Silver remain immutable.
The project can now produce reviewed individual-company Gold return/volatility
datasets from an explicitly approved Silver run in the supported Linux container.
Sector aggregation, correlations, concentration, emergence detection, Kafka,
Structured Streaming, ML, databases and dashboards are not implemented.

## 9. Recommended Next Step

Architecture-review ADR 008, schemas, null/reason policy and the final Gold evidence.
If accepted, establish a small external approved-Gold registry before any downstream
consumer, then define the next explicitly authorized milestone. Do not start sector
analytics automatically.

## 10. Review Questions

1. Approve ADR 008 and final Gold run `2c36a9ab-...` as the individual-company
   evidence baseline?
2. Require a Gold publication registry analogous to Silver before any next milestone?

**Review status:** READY FOR REVIEW

**Next phase:** NOT STARTED
