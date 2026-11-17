"""Synthetic lifecycle validation; never publishes the real pilot."""

import json
import os
import shutil
from dataclasses import fields, replace
from datetime import UTC
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from labgdd.adapters.bronze import sha256
from labgdd.analytics.sector import (
    SectorMemberReturn,
    SectorPopulationMember,
    calculate_sector_measurements,
)
from labgdd.config import load_settings
from labgdd.processing.approved_sector import project_approved_expected_gold
from labgdd.processing.sector_publication import (
    _read_outputs,
    _validate_expected_grid,
    _validate_frames,
    inspect_sector_candidate,
    publish_synthetic_sector_gold,
)
from labgdd.sector_publication_config import SectorPublicationRequest
from tests.integration import test_approved_sector as approved_fixtures
from tests.integration.test_approved_sector import (
    DATASET,
    ROOT,
    RUN,
    _arguments,
    _registry,
    make_evidence,
)

spark = approved_fixtures.spark

pytestmark = pytest.mark.skipif(
    os.name == "nt", reason="native Sector Parquet lifecycle validated on Linux"
)
UNIVERSE: dict[str, tuple[str, ...]] = {
    "Technology": ("AAPL", "MSFT", "NVDA", "GOOGL", "AMD"),
    "Energy": ("XOM", "CVX", "COP", "SLB", "OXY"),
    "Financials": ("JPM", "BAC", "GS", "MS", "V"),
}


@pytest.fixture(scope="module")
def fixture_root(tmp_path_factory: pytest.TempPathFactory, spark: Any) -> Path:
    return make_evidence(tmp_path_factory.mktemp("sector-publication"), spark, UNIVERSE)


def _request(root: Path, *, strict: bool = False) -> SectorPublicationRequest:
    args = _arguments(root)
    return SectorPublicationRequest(
        "sector-synthetic-pilot-v1",
        args["registry"],
        args["silver_registry"],
        args["research_reference"],
        DATASET,
        RUN,
        args["manifest_sha256"],
        "verified_historical_applicability" if strict else "declared_research",
    )


def _settings(root: Path) -> Any:
    return replace(
        load_settings(ROOT / "config/project.toml"), data_root=root / "data", universe=UNIVERSE
    )


@pytest.fixture(scope="module")
def candidate(fixture_root: Path, spark: Any) -> Path:
    before = {path.name: sha256(path) for path in (fixture_root / "config").glob("*.toml")}
    result = publish_synthetic_sector_gold(spark, _settings(fixture_root), _request(fixture_root))
    assert before == {path.name: sha256(path) for path in (fixture_root / "config").glob("*.toml")}
    assert not (fixture_root / "config/sector_publications.toml").exists()
    return result


def test_complete_synthetic_publication_matches_scalar_and_is_immutable(
    fixture_root: Path, candidate: Path, spark: Any
) -> None:
    settings = _settings(fixture_root)
    request = _request(fixture_root)
    document = inspect_sector_candidate(spark, settings, request, candidate, sha256(candidate))
    assert document["counts"]["windows"] == {"rows": 360, "eligible": 240}
    assert document["counts"]["correlations"]["rows"] == 3600
    projection = project_approved_expected_gold(spark, settings, **_arguments(fixture_root))
    population = []
    for row in projection.population.collect():
        values = row.asDict()
        for key in (
            "session_open",
            "session_close",
            "membership_valid_from",
            "membership_valid_to",
        ):
            values[key] = values[key].replace(tzinfo=UTC)
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
    reference = calculate_sector_measurements(tuple(population), returns)
    windows, pairs = _read_outputs(spark, candidate.parent)
    actual = {row.output_identity_sha256: row for row in windows.collect()}
    for expected in reference.windows:
        row = actual[expected.output_identity_sha256]
        assert row.reason == expected.reason
        for metric in (
            "sector_realized_variance",
            "sector_realized_volatility",
            "portfolio_realized_variance",
            "portfolio_realized_volatility",
            "mean_log_return_realized_volatility",
            "volatility_dispersion",
            "mean_pairwise_correlation",
            "volatility_concentration_hhi",
            "volatility_effective_breadth",
            "volatility_breadth",
        ):
            value = getattr(expected, metric)
            assert (row[metric] is None) == (value is None)
            if value is not None:
                assert row[metric] == pytest.approx(value, abs=1e-12, rel=1e-10)
        assert row.window_start.replace(tzinfo=UTC) == expected.window_start
        assert row.window_end.replace(tzinfo=UTC) == expected.window_end
    actual_pairs = {
        (row.output_identity_sha256, row.left_symbol, row.right_symbol): row
        for row in pairs.collect()
    }
    assert len(actual_pairs) == len(reference.correlations) == 3600
    for expected_pair in reference.correlations:
        row = actual_pairs[
            (
                expected_pair.output_identity_sha256,
                expected_pair.left_symbol,
                expected_pair.right_symbol,
            )
        ]
        assert row.eligible == expected_pair.eligible and row.reason == expected_pair.reason
        assert (row.value is None) == (expected_pair.value is None)
        if expected_pair.value is not None:
            assert row.value == pytest.approx(expected_pair.value, abs=1e-12, rel=1e-10)
    assert pairs.filter("sector = 'Energy' and value is not null").count() == 0
    before = {
        p.relative_to(candidate.parent).as_posix(): sha256(p)
        for p in candidate.parent.rglob("*")
        if p.is_file()
    }
    assert (
        inspect_sector_candidate(spark, settings, request, candidate, sha256(candidate)) == document
    )
    from uuid import UUID

    with pytest.raises(FileExistsError):
        publish_synthetic_sector_gold(spark, settings, request, run_id=UUID(document["run_id"]))
    assert before == {
        p.relative_to(candidate.parent).as_posix(): sha256(p)
        for p in candidate.parent.rglob("*")
        if p.is_file()
    }


def test_strict_candidate_replay_and_explicit_pending_authority(
    fixture_root: Path, candidate: Path, spark: Any
) -> None:
    spark.conf.set("spark.sql.shuffle.partitions", "3")
    replay = publish_synthetic_sector_gold(spark, _settings(fixture_root), _request(fixture_root))
    first = _read_outputs(spark, candidate.parent)
    second = _read_outputs(spark, replay.parent)
    for left, right in zip(first, second, strict=True):
        columns = [c for c in left.columns if c != "sector_run_id"]
        assert left.select(*columns).exceptAll(right.select(*columns)).count() == 0
        assert right.select(*columns).exceptAll(left.select(*columns)).count() == 0
    strict = publish_synthetic_sector_gold(
        spark, _settings(fixture_root), _request(fixture_root, strict=True)
    )
    doc = inspect_sector_candidate(
        spark, _settings(fixture_root), _request(fixture_root, strict=True), strict, sha256(strict)
    )
    assert doc["counts"]["windows"]["eligible"] == 0
    windows, _ = _read_outputs(spark, strict.parent)
    assert windows.filter("reason != 'unverified_historical_applicability'").count() == 0
    assert doc["publication_status"] == "pending_architecture_review"
    spark.conf.set("spark.sql.shuffle.partitions", "2")


def test_corruption_source_mismatch_and_unmarked_real_guard(
    fixture_root: Path, candidate: Path, tmp_path: Path, spark: Any
) -> None:
    root = tmp_path / "copy"
    shutil.copytree(fixture_root, root)
    copied = root / candidate.relative_to(fixture_root)
    digest = sha256(copied)
    artifact = next((copied.parent / "windows").glob("*.parquet"))
    artifact.write_bytes(artifact.read_bytes() + b"corrupt")
    with pytest.raises(ValueError, match="checksum"):
        inspect_sector_candidate(spark, _settings(root), _request(root), copied, digest)
    with pytest.raises(ValueError, match="source authority"):
        publish_synthetic_sector_gold(
            spark, _settings(root), replace(_request(root), input_manifest_sha256="f" * 64)
        )
    source = root / "data/gold" / f"dataset={DATASET}/run={RUN}/manifest.json"
    doc = json.loads(source.read_text())
    del doc["evidence_kind"]
    source.write_text(json.dumps(doc))
    _registry(root / "config/gold_publications.toml", source, doc, gold=True)
    with pytest.raises(ValueError, match="real or unmarked"):
        publish_synthetic_sector_gold(spark, _settings(root), _request(root))


def test_output_validation_and_partial_write_failure(
    fixture_root: Path, candidate: Path, spark: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyspark.sql import functions as F

    from labgdd.processing import sector_publication

    windows, pairs = _read_outputs(spark, candidate.parent)
    for changed in (
        windows.unionByName(windows.limit(1)),
        windows.withColumn("observed_members", F.lit(-1).cast("long")),
        windows.withColumn("eligible", F.lit(True)).withColumn("reason", F.lit("invalid")),
        windows.drop("window_start"),
        windows.withColumn("sector_realized_volatility", F.lit(None).cast("double")),
    ):
        with pytest.raises(ValueError):
            _validate_frames(changed, pairs)
    projection = project_approved_expected_gold(
        spark, _settings(fixture_root), **_arguments(fixture_root)
    )
    with pytest.raises(ValueError, match="independent window grid"):
        _validate_expected_grid(windows.filter("window_index != 0"), projection.population)
    with pytest.raises(ValueError, match="pair inventory"):
        _validate_frames(windows, pairs.filter("left_symbol != 'AAPL'"))
    identity = uuid4()

    def reject(*args: Any, **kwargs: Any) -> Any:
        raise ValueError("injected physical readback failure")

    monkeypatch.setattr(sector_publication, "_read_outputs", reject)
    with pytest.raises(ValueError, match="injected"):
        publish_synthetic_sector_gold(
            spark, _settings(fixture_root), _request(fixture_root), run_id=identity
        )
    root = _settings(fixture_root).data_root / "sector_gold/dataset=sector-synthetic-pilot-v1"
    assert not (root / f"run={identity}").exists()
    assert (
        json.loads((root / f"_failed/run={identity}/manifest.json").read_text())["status"]
        == "failed"
    )


def test_optional_real_input_cannot_be_published(spark: Any) -> None:
    from uuid import UUID

    settings = load_settings(ROOT / "config/project.toml")
    dataset = "gold-study-cohort-20260914-multisymbol-expected-v1"
    run = UUID("39ab367c-c6d2-480d-8c94-f35f767ed289")
    manifest = settings.data_root / "gold" / f"dataset={dataset}/run={run}/manifest.json"
    if not manifest.parent.exists() and not manifest.parent.is_symlink():
        pytest.skip("optional exact approved real Gold evidence absent")
    request = SectorPublicationRequest(
        "sector-synthetic-real-guard-v1",
        ROOT / "config/gold_publications.toml",
        ROOT / "config/silver_publications.toml",
        ROOT / "config/research_universe.toml",
        dataset,
        run,
        sha256(manifest),
    )
    target = settings.data_root / "sector_gold/dataset=sector-synthetic-real-guard-v1"
    before = target.exists()
    with pytest.raises(ValueError, match="real or unmarked"):
        publish_synthetic_sector_gold(spark, settings, request)
    assert target.exists() == before


def test_invalid_individual_return_propagates_through_publication(
    fixture_root: Path, tmp_path: Path, spark: Any
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    root = tmp_path / "invalid-individual"
    shutil.copytree(fixture_root, root)
    source = root / "data/gold" / f"dataset={DATASET}/run={RUN}/manifest.json"
    doc = json.loads(source.read_text())
    changed = False
    for path in (source.parent / "returns").rglob("*.parquet"):
        table = pq.ParquetFile(path).read()
        rows = table.to_pylist()
        for row in rows:
            if (
                row["symbol"] == "AMD"
                and row["event_time"].minute == 51
                and row["event_time"].hour == 13
            ):
                assert row["eligible"]
                row.update(
                    eligible=False,
                    reason="incomplete_return_window",
                    close_to_close_log_return=None,
                )
                changed = True
        pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path)
    assert changed
    # This is construction of an adversarial synthetic source, never frozen evidence.
    for path in source.parent.rglob("*.crc"):
        path.unlink()
    doc["eligible_return_records"] -= 1
    doc["artifacts"] = [
        {
            "path": p.relative_to(source.parent).as_posix(),
            "sha256": sha256(p),
            "bytes": p.stat().st_size,
        }
        for p in sorted(source.parent.rglob("*"))
        if p.is_file() and p != source
    ]
    source.write_text(json.dumps(doc))
    _registry(root / "config/gold_publications.toml", source, doc, gold=True)
    candidate = publish_synthetic_sector_gold(spark, _settings(root), _request(root))
    evidence = inspect_sector_candidate(
        spark, _settings(root), _request(root), candidate, sha256(candidate)
    )
    assert evidence["counts"]["windows"]["eligible"] == 236
    assert evidence["reason_counts"]["windows"]["ineligible_individual_return"] == 4
    windows, _ = _read_outputs(spark, candidate.parent)
    assert (
        windows.filter(
            "reason = 'ineligible_individual_return' and portfolio_realized_volatility is not null"
        ).count()
        == 0
    )


def test_resealed_candidate_cannot_forge_source_provenance(
    fixture_root: Path, candidate: Path, tmp_path: Path, spark: Any
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    root = tmp_path / "forged-candidate"
    shutil.copytree(fixture_root, root)
    manifest = root / candidate.relative_to(fixture_root)
    doc = json.loads(manifest.read_text())
    for name in ("windows", "correlations"):
        for path in (manifest.parent / name).glob("*.parquet"):
            table = pq.ParquetFile(path).read()
            rows = table.to_pylist()
            for row in rows:
                row["feed"] = "forged_venue"
            pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path)
    for path in manifest.parent.rglob("*.crc"):
        path.unlink()
    doc["artifacts"] = [
        {
            "path": p.relative_to(manifest.parent).as_posix(),
            "bytes": p.stat().st_size,
            "sha256": sha256(p),
        }
        for p in sorted(manifest.parent.rglob("*"))
        if p.is_file() and p != manifest
    ]
    manifest.write_text(json.dumps(doc))
    with pytest.raises(ValueError, match="calculation evidence mismatch"):
        inspect_sector_candidate(spark, _settings(root), _request(root), manifest, sha256(manifest))
