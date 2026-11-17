"""Portable approved-file boundary tests; all publications here are synthetic."""

import json
import os
import shutil
import sys
from dataclasses import fields, replace
from datetime import UTC, date, timedelta
from decimal import Decimal
from hashlib import sha256 as new_sha256
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from labgdd.adapters.bronze import sha256
from labgdd.adapters.calendar import xnys_snapshot
from labgdd.adapters.gold import resolve_approved_gold
from labgdd.adapters.silver import resolve_approved_silver
from labgdd.analytics.sector import (
    SectorMemberReturn,
    SectorPopulationMember,
    calculate_sector_measurements,
)
from labgdd.config import load_settings
from labgdd.expected_gold_config import ExpectedGoldRequest
from labgdd.processing.approved_sector import (
    calculate_approved_sector_gold,
    project_approved_expected_gold,
)
from labgdd.processing.expected_gold import PreparedExpectedGold, process_expected_gold
from labgdd.processing.expected_series import generate_xnys_expected_series
from labgdd.processing.expected_series_spark import expected_plan_fingerprint
from labgdd.processing.sector_gold import transform_sector_gold
from labgdd.research_config import load_research_universe

ROOT = Path(__file__).resolve().parents[2]
RUN = UUID("90000000-0000-4000-8000-000000000001")
SILVER = UUID("90000000-0000-4000-8000-000000000002")
DATASET = "gold-expected-sector-fixture-v1"
pytestmark = pytest.mark.skipif(
    os.name == "nt", reason="native approved Parquet boundary tested on Linux"
)


@pytest.fixture(scope="module")
def spark() -> Any:
    from pyspark.sql import SparkSession

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[2]")
        .appName("approved-sector-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _registry(path: Path, manifest: Path, metadata: dict[str, Any], *, gold: bool) -> None:
    keys = ["dataset_version", "run_id", "processing_source_sha256"]
    authority = {key: metadata[key] for key in keys}
    authority.update(
        status="approved",
        manifest="../" + manifest.relative_to(path.parent.parent).as_posix(),
        manifest_sha256=sha256(manifest),
        approved_at="2026-11-16T00:00:00Z",
        decision="synthetic_fixture",
    )
    if gold:
        for key in (
            "gold_schema_version",
            "measurement_version",
            "silver_dataset_version",
            "silver_run_id",
            "silver_manifest_sha256",
            "configuration_sha256",
        ):
            authority[key] = metadata[key]
        authority["manifest_publication_status"] = metadata["publication_status"]
        fingerprints = {
            key: metadata[key]
            for key in (
                "approved_expected_plan_sha256",
                "research_reference_sha256",
                "schedule_sha256",
            )
            if key in metadata
        }
        inline = ", ".join(f"{key} = {json.dumps(value)}" for key, value in fingerprints.items())
        extra = f"contract_fingerprints = {{ {inline} }}\n"
    else:
        authority.update(request="silver.toml", request_sha256=sha256(path.parent / "silver.toml"))
        extra = ""
    header = (
        'registry_version = "1.1.0"\nsuperseded = []\n[[authority]]\n'
        if gold
        else ('registry_version = "1.0.0"\nsuperseded = []\n[authority]\n')
    )
    path.write_text(
        header
        + "".join(f"{key} = {json.dumps(value)}\n" for key, value in authority.items())
        + extra,
        encoding="utf-8",
    )


@pytest.fixture(scope="module")
def evidence(tmp_path_factory: pytest.TempPathFactory, spark: Any) -> Path:
    root = tmp_path_factory.mktemp("approved-sector")
    return make_evidence(root, spark)


def make_evidence(
    root: Path, spark: Any, universe: dict[str, tuple[str, ...]] | None = None
) -> Path:
    config = root / "config"
    config.mkdir()
    (config / "research_universe.toml").write_bytes(
        (ROOT / "config/research_universe.toml").read_bytes()
    )
    settings = replace(
        load_settings(ROOT / "config/project.toml"),
        data_root=root / "data",
        universe=universe or {"Technology": ("AAPL", "MSFT")},
    )
    research = load_research_universe(config / "research_universe.toml", settings)
    calendar = xnys_snapshot(date(2026, 9, 14), date(2026, 9, 14))
    plan = generate_xnys_expected_series(research.universe, calendar, ("2026-09-14",))
    rows = []
    for member in plan.rows:
        for minute in range(390):
            if universe is None and member.symbol == "MSFT" and minute == 10:
                continue
            if universe is not None and (
                member.symbol == "V" or (member.symbol == "JPM" and minute == 10)
            ):
                continue
            event = member.session_open + timedelta(minutes=minute)
            price = Decimal("100") + Decimal(minute % 7) / Decimal("100")
            if universe is not None and member.sector == "Energy":
                price = Decimal("100")
            rows.append(
                dict(
                    symbol=member.symbol,
                    sector=member.sector,
                    source="alpaca",
                    feed="iex",
                    timeframe="1Min",
                    adjustment="raw",
                    session_id=member.session_id,
                    session_open=member.session_open,
                    session_close=member.session_close,
                    calendar_provider="exchange_calendars",
                    calendar_version="4.11.2",
                    schedule_sha256=calendar.metadata["schedule_sha256"],
                    membership_version=member.membership_version,
                    event_time=event,
                    bar_end=event + timedelta(minutes=1),
                    open=price,
                    close=price,
                    selected_observation_id=f"{minute + 1:064x}",
                    membership_valid_from=member.membership_valid_from,
                    membership_valid_to=member.membership_valid_to,
                    quality_status="canonical",
                )
            )
    silver = settings.data_root / "silver/dataset=silver-sector-fixture-v1" / f"run={SILVER}"
    spark.createDataFrame(rows).write.parquet(str(silver / "canonical"))
    (config / "silver.toml").write_text('dataset_version = "synthetic"\n', encoding="utf-8")
    artifacts = [
        {"path": p.relative_to(silver).as_posix(), "sha256": sha256(p), "bytes": p.stat().st_size}
        for p in sorted(silver.rglob("*"))
        if p.is_file()
    ]
    silver_doc = dict(
        status="complete",
        dataset_version="silver-sector-fixture-v1",
        run_id=str(SILVER),
        processing_source_sha256="a" * 64,
        artifacts=artifacts,
    )
    silver_manifest = silver / "manifest.json"
    silver_manifest.write_text(json.dumps(silver_doc), encoding="utf-8")
    _registry(config / "silver_publications.toml", silver_manifest, silver_doc, gold=False)
    approved = resolve_approved_silver(
        config / "silver_publications.toml", settings.data_root, "silver-sector-fixture-v1", SILVER
    )
    request = ExpectedGoldRequest(
        DATASET,
        config / "silver_publications.toml",
        approved.dataset_version,
        SILVER,
        research.path,
        "study-cohort-v1",
        ("2026-09-14",),
        "1.0.0",
        "expected-series-left-join-v1",
        "declared_research",
        "2.0.0",
        (5, 15, 30, 60),
        "2.0.0",
        config / "expected_gold.toml",
        acquisition_scope_symbols=tuple(sorted({row.symbol for row in plan.rows})),
    )
    plan_hash = expected_plan_fingerprint(
        plan, source="alpaca", feed="iex", timeframe="1Min", adjustment="raw"
    )
    prepared = PreparedExpectedGold(
        approved, research.universe, plan, calendar, sha256(research.path), plan_hash, "b" * 64
    )
    result = process_expected_gold(settings, request, spark, run_id=RUN, prepared=prepared)
    document = json.loads(result.manifest.read_text())
    document["evidence_kind"] = "synthetic_fixture"
    result.manifest.write_text(json.dumps(document), encoding="utf-8")
    _registry(config / "gold_publications.toml", result.manifest, document, gold=True)
    return root


def _arguments(root: Path) -> dict[str, Any]:
    manifest = root / "data/gold" / f"dataset={DATASET}" / f"run={RUN}/manifest.json"
    return dict(
        registry=root / "config/gold_publications.toml",
        silver_registry=root / "config/silver_publications.toml",
        research_reference=root / "config/research_universe.toml",
        dataset_version=DATASET,
        run_id=RUN,
        manifest_sha256=sha256(manifest),
    )


def _settings(root: Path) -> Any:
    return replace(
        load_settings(ROOT / "config/project.toml"),
        data_root=root / "data",
        universe={"Technology": ("AAPL", "MSFT")},
    )


def test_approved_projection_scalar_spark_parity_and_strict_policy(
    evidence: Path, spark: Any
) -> None:
    projection, result = calculate_approved_sector_gold(
        spark, _settings(evidence), **_arguments(evidence)
    )
    # The original public frame API cannot promote these labelled frames to real
    # authority; only the file-owning projection API may use approved mode.
    with pytest.raises(ValueError):
        transform_sector_gold(projection.population, projection.returns, (5, 15, 30, 60))
    population = []
    for row in projection.population.collect():
        values = row.asDict()
        for name in (
            "session_open",
            "session_close",
            "membership_valid_from",
            "membership_valid_to",
        ):
            values[name] = values[name].replace(tzinfo=UTC)
        population.append(
            SectorPopulationMember(
                **{f.name: values[f.name] for f in fields(SectorPopulationMember)}
            )
        )
    returns = tuple(
        SectorMemberReturn(
            row.symbol,
            row.sector,
            row.session_id,
            row.return_end.replace(tzinfo=UTC),
            row.observed_bars,
            row.canonical_valid,
            row.financial_eligible,
            row.close_to_close_log_return,
            row.individual_reason,
        )
        for row in projection.returns.collect()
    )
    scalar = calculate_sector_measurements(tuple(population), returns)
    actual = {row.output_identity_sha256: row for row in result.windows.collect()}
    assert len(actual) == len(scalar.windows) == 120
    lineage = {
        "input_processing_source_sha256": "processing_source_sha256",
        "input_configuration_sha256": "configuration_sha256",
        "input_gold_schema_version": "gold_schema_version",
        "input_measurement_version": "measurement_version",
        "input_silver_dataset_version": "silver_dataset_version",
        "input_silver_run_id": "silver_run_id",
        "input_silver_manifest_sha256": "silver_manifest_sha256",
        "input_approved_expected_plan_sha256": "approved_expected_plan_sha256",
        "input_research_reference_sha256": "research_reference_sha256",
    }
    for row in [*actual.values(), *result.correlations.collect()]:
        assert row.input_gold_dataset_version == DATASET and row.input_gold_run_id == str(RUN)
        assert row.sector_publication_status == "calculation_only_not_published"
        for output, source in lineage.items():
            assert row[output] == projection.gold.metadata[source]
        payload = "|".join(
            (
                "sector-approved-input-v1",
                row.output_identity_sha256,
                DATASET,
                str(RUN),
                projection.gold.manifest_sha256,
            )
        )
        assert row.approved_input_identity_sha256 == new_sha256(payload.encode()).hexdigest()
    for expected in scalar.windows:
        row = actual[expected.output_identity_sha256]
        assert row.reason == expected.reason
        for metric in (
            "sector_realized_volatility",
            "portfolio_realized_volatility",
            "volatility_dispersion",
            "mean_pairwise_correlation",
            "volatility_concentration_hhi",
            "volatility_effective_breadth",
        ):
            value = getattr(expected, metric)
            assert (row[metric] is None) == (value is None)
            if value is not None:
                assert row[metric] == pytest.approx(value, abs=1e-12, rel=1e-10)
    spark.conf.set("spark.sql.shuffle.partitions", "3")
    _, replay = calculate_approved_sector_gold(spark, _settings(evidence), **_arguments(evidence))
    assert result.windows.exceptAll(replay.windows).limit(1).count() == 0
    assert replay.windows.exceptAll(result.windows).limit(1).count() == 0
    assert result.correlations.exceptAll(replay.correlations).limit(1).count() == 0
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    _, strict = calculate_approved_sector_gold(
        spark,
        _settings(evidence),
        **_arguments(evidence),
        eligibility_policy="verified_historical_applicability",
    )
    assert (
        strict.windows.filter("eligible or reason != 'unverified_historical_applicability'").count()
        == 0
    )
    assert (
        strict.windows.select("output_identity_sha256")
        .intersect(result.windows.select("output_identity_sha256"))
        .count()
        == 0
    )
    total = projection.coverage.agg({"observed_slots": "sum"}).first()
    assert total is not None and total[0] == 779


@pytest.mark.parametrize(
    "failure", ("missing", "tampered", "unregistered", "fingerprint", "policy", "research")
)
def test_approved_projection_rejects_invalid_authority(
    evidence: Path, tmp_path: Path, spark: Any, failure: str
) -> None:
    root = tmp_path / "copy"
    shutil.copytree(evidence, root)
    args = _arguments(root)
    manifest = root / "data/gold" / f"dataset={DATASET}" / f"run={RUN}/manifest.json"
    if failure == "missing":
        manifest.unlink()
    elif failure == "tampered":
        manifest.write_text("{}")
    elif failure == "unregistered":
        args["run_id"] = UUID("90000000-0000-4000-8000-000000000099")
    elif failure == "fingerprint":
        args["manifest_sha256"] = "f" * 64
    elif failure == "policy":
        args["eligibility_policy"] = "unsupported"
    elif failure == "research":
        (root / "config/research_universe.toml").write_text("invalid=true")
    with pytest.raises(ValueError):
        project_approved_expected_gold(spark, _settings(root), **args)


@pytest.mark.parametrize(
    "mutation",
    (
        "duplicate",
        "timestamp",
        "source",
        "membership",
        "scope",
        "missing",
        "nan",
        "eligible_missing_close",
        "silver_lineage",
        "plan_fingerprint",
        "calendar",
        "quality",
        "equivalent_duplicate",
    ),
)
def test_approved_rows_are_validated(
    evidence: Path, tmp_path: Path, spark: Any, mutation: str
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    root = tmp_path / "synthetic"
    shutil.copytree(evidence, root)
    manifest = root / "data/gold" / f"dataset={DATASET}" / f"run={RUN}/manifest.json"
    document = json.loads(manifest.read_text())
    path = next((manifest.parent / "returns").rglob("*.parquet"))
    table = pq.ParquetFile(path).read()
    values = table.to_pylist()
    if mutation == "duplicate":
        values.append(dict(values[0]))
    elif mutation == "timestamp":
        values[0]["event_time"] += timedelta(seconds=30)
    elif mutation == "source":
        values[0]["feed"] = "sip"
    elif mutation == "membership":
        values[0]["evidence_status"] = "independently_verified"
    elif mutation == "scope":
        document["acquisition_scope_symbols"] = ["AAPL"]
    elif mutation == "nan":
        next(row for row in values if row["eligible"])["close_to_close_log_return"] = float("nan")
    elif mutation == "eligible_missing_close":
        next(row for row in values if row["eligible"])["close"] = None
        document["observed_bar_slots"] -= 1
    elif mutation == "silver_lineage":
        document["silver_manifest_sha256"] = "f" * 64
    elif mutation == "plan_fingerprint":
        del document["approved_expected_plan_sha256"]
    elif mutation == "calendar":
        document["calendar_version"] = "unverified"
    elif mutation == "quality":
        next(row for row in values if row["eligible"])["quality_status"] = "invalid"
    elif mutation == "equivalent_duplicate":
        next(row for row in values if row["eligible"])["quality_status"] = (
            "canonical_equivalent_duplicate"
        )
    changed = pa.Table.from_pylist(values, schema=table.schema)
    if mutation == "missing":
        changed = changed.drop(["quality_status"])
    pq.write_table(changed, path)
    # Re-register only this synthetic fixture to test semantic validation after
    # genuine resolver checksum validation, not merely a byte-tampering rejection.
    for checksum in manifest.parent.rglob("*.crc"):
        checksum.unlink()
    document["artifacts"] = [
        {
            "path": item.relative_to(manifest.parent).as_posix(),
            "sha256": sha256(item),
            "bytes": item.stat().st_size,
        }
        for item in sorted(manifest.parent.rglob("*"))
        if item.is_file() and item.name != "manifest.json"
    ]
    manifest.write_text(json.dumps(document), encoding="utf-8")
    _registry(root / "config/gold_publications.toml", manifest, document, gold=True)
    if mutation == "equivalent_duplicate":
        projection = project_approved_expected_gold(spark, _settings(root), **_arguments(root))
        assert (
            projection.returns.filter("financial_eligible").count()
            == document["eligible_return_records"]
        )
        return
    with pytest.raises(ValueError):
        project_approved_expected_gold(spark, _settings(root), **_arguments(root))


@pytest.mark.parametrize(
    "boundary", ("before_resolve", "after_resolve", "after_projection", "snapshot_limit")
)
def test_source_mutation_cannot_change_verified_calculation(
    evidence: Path, tmp_path: Path, spark: Any, monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    from labgdd.processing import approved_sector

    root = tmp_path / "mutation"
    shutil.copytree(evidence, root)
    args = _arguments(root)
    manifest = root / "data/gold" / f"dataset={DATASET}" / f"run={RUN}/manifest.json"
    path = next((manifest.parent / "returns").rglob("*.parquet"))

    def replace_source() -> None:
        # Replacement of controlled synthetic evidence only; frozen data is never touched.
        changed = bytearray(path.read_bytes())
        changed[len(changed) // 2] ^= 1
        path.unlink()
        # Preserve the size so the digest, not merely the size guard, must reject it.
        path.write_bytes(changed)

    if boundary == "after_projection":
        projection, calculated = calculate_approved_sector_gold(spark, _settings(root), **args)
        before = calculated.windows.orderBy("output_identity_sha256").collect()
        pairs = calculated.correlations.orderBy(
            "output_identity_sha256", "left_symbol", "right_symbol"
        ).collect()
        replace_source()
        assert projection.returns.inputFiles() == []
        assert calculated.windows.orderBy("output_identity_sha256").collect() == before
        assert (
            calculated.correlations.orderBy(
                "output_identity_sha256", "left_symbol", "right_symbol"
            ).collect()
            == pairs
        )
        assert sum(row.observed_slots for row in projection.coverage.collect()) == 779
        return
    if boundary == "before_resolve":
        replace_source()
    elif boundary == "snapshot_limit":
        monkeypatch.setattr(approved_sector, "_MAX_SNAPSHOT_BYTES", 0)
    else:

        def replace_after_resolve(*positional: Any, **keywords: Any) -> Any:
            approved = resolve_approved_gold(*positional, **keywords)
            replace_source()
            return approved

        monkeypatch.setattr(approved_sector, "resolve_approved_gold", replace_after_resolve)
    with pytest.raises(ValueError, match="artifact|snapshot"):
        project_approved_expected_gold(spark, _settings(root), **args)


def test_different_approved_run_has_distinct_source_identity(
    evidence: Path, tmp_path: Path, spark: Any
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    root = tmp_path / "new-run"
    shutil.copytree(evidence, root)
    other = UUID("90000000-0000-4000-8000-000000000003")
    parent = root / "data/gold" / f"dataset={DATASET}"
    (parent / f"run={RUN}").rename(parent / f"run={other}")
    manifest = parent / f"run={other}/manifest.json"
    document = json.loads(manifest.read_text())
    document["run_id"] = str(other)
    for path in (manifest.parent / "returns").rglob("*.parquet"):
        table = pq.ParquetFile(path).read()
        values = table.to_pylist()
        for row in values:
            row["gold_run_id"] = str(other)
        pq.write_table(pa.Table.from_pylist(values, schema=table.schema), path)
    for checksum in manifest.parent.rglob("*.crc"):
        checksum.unlink()
    document["artifacts"] = [
        {
            "path": p.relative_to(manifest.parent).as_posix(),
            "sha256": sha256(p),
            "bytes": p.stat().st_size,
        }
        for p in sorted(manifest.parent.rglob("*"))
        if p.is_file() and p.name != "manifest.json"
    ]
    manifest.write_text(json.dumps(document), encoding="utf-8")
    _registry(root / "config/gold_publications.toml", manifest, document, gold=True)
    _, original = calculate_approved_sector_gold(spark, _settings(evidence), **_arguments(evidence))
    args = _arguments(evidence)
    args.update(
        registry=root / "config/gold_publications.toml",
        silver_registry=root / "config/silver_publications.toml",
        research_reference=root / "config/research_universe.toml",
        run_id=other,
        manifest_sha256=sha256(manifest),
    )
    _, changed = calculate_approved_sector_gold(spark, _settings(root), **args)
    for old, new in (
        (original.windows, changed.windows),
        (original.correlations, changed.correlations),
    ):
        assert (
            old.select("output_identity_sha256")
            .exceptAll(new.select("output_identity_sha256"))
            .count()
            == 0
        )
        assert (
            old.select("approved_input_identity_sha256")
            .intersect(new.select("approved_input_identity_sha256"))
            .count()
            == 0
        )


def test_optional_real_approved_pilot_projection(spark: Any) -> None:
    dataset = "gold-study-cohort-20260914-multisymbol-expected-v1"
    run = UUID("39ab367c-c6d2-480d-8c94-f35f767ed289")
    settings = load_settings(ROOT / "config/project.toml")
    path = settings.data_root / "gold" / f"dataset={dataset}" / f"run={run}"
    if not path.exists() and not path.is_symlink():
        pytest.skip("optional exact approved multi-symbol Gold evidence absent")
    projection, result = calculate_approved_sector_gold(
        spark,
        settings,
        registry=ROOT / "config/gold_publications.toml",
        silver_registry=ROOT / "config/silver_publications.toml",
        research_reference=ROOT / "config/research_universe.toml",
        dataset_version=dataset,
        run_id=run,
        manifest_sha256="d97b0e48c8463b6a97aa58e576e7f24998b4246f496fabaa286982177ab605b2",
    )
    assert projection.population.count() == 30
    coverage = projection.coverage.collect()
    assert sum(row.expected_slots for row in coverage) == 11700
    assert sum(row.observed_slots for row in coverage) == 1861
    assert sum(row.missing_slots for row in coverage) == 9839
    assert {row.symbol: row.observed_slots for row in coverage if row.acquisition_scope} == {
        "AAPL": 390,
        "MSFT": 390,
        "NVDA": 390,
        "JPM": 371,
        "XOM": 320,
    }
    assert sum(not row.acquisition_scope for row in coverage) == 25
    assert result.windows.count() == 720
    assert result.correlations.count() == 7200
    assert result.windows.select("output_identity_sha256").distinct().count() == 720
    assert (
        result.windows.filter(
            "expected_members != 5 or expected_pairs != 10 or valid_pairs != 0"
        ).count()
        == 0
    )
    assert (
        result.windows.filter("eligible or reason != 'member_outside_acquisition_scope'").count()
        == 0
    )
    assert (
        result.windows.filter(
            "portfolio_realized_volatility is not null or volatility_concentration_hhi is not null"
        ).count()
        == 0
    )
