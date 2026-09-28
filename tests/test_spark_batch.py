from __future__ import annotations

import math
import os
import statistics
import sys
from datetime import datetime, timedelta, timezone

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from src.spark.batch_processor import (
    _write_partitioned_with_pyarrow,
    discover_parquet_files,
    transform_batch,
)
from src.spark.transformations import add_financial_features, add_sector


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("financial-market-batch-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"Spark runtime unavailable: {exc}")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _rows(symbol: str, count: int) -> list[tuple]:
    start = datetime(2026, 9, 1, 9, 30, tzinfo=timezone.utc)
    return [
        (
            start + timedelta(minutes=index),
            symbol,
            float(index + 1),
            float(index + 2),
            float(index),
            float(index + 1),
            100 + index,
        )
        for index in range(count)
    ]


def _dataframe(spark: SparkSession, rows: list[tuple]):
    return spark.createDataFrame(
        rows, ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
    )


def test_discovers_one_symbol_or_all_available_symbols(tmp_path) -> None:
    for symbol in ("AAPL", "GE", "JPM"):
        symbol_dir = tmp_path / symbol
        symbol_dir.mkdir()
        (symbol_dir / "bars.parquet").touch()

    all_files, all_symbols = discover_parquet_files(tmp_path)
    aapl_files, aapl_symbols = discover_parquet_files(tmp_path, ["aapl"])

    assert len(all_files) == 3
    assert all_symbols == ["AAPL", "GE", "JPM"]
    assert len(aapl_files) == 1
    assert aapl_symbols == ["AAPL"]


def test_unknown_symbol_receives_explicit_unknown_sector(spark: SparkSession) -> None:
    result = add_sector(_dataframe(spark, _rows("UNKNOWN", 1)), {"AAPL": "Technology"})

    assert result.select("sector").first().sector == "Unknown"


def test_single_symbol_sector_validation_and_duplicates(spark: SparkSession) -> None:
    rows = _rows("AAPL", 2)
    rows.append(rows[1])
    rows.append(
        (
            datetime(2026, 9, 1, 9, 32, tzinfo=timezone.utc),
            "AAPL",
            10.0,
            8.0,
            9.0,
            11.0,
            100,
        )
    )

    processed, quarantined, quality_valid = transform_batch(
        _dataframe(spark, rows), {"AAPL": "Technology"}
    )

    assert quality_valid.count() == 3
    assert processed.count() == 2
    assert {row.sector for row in processed.select("sector").collect()} == {"Technology"}
    errors = [error for row in quarantined.collect() for error in row.validation_errors]
    assert "duplicate_symbol_timestamp" in errors
    assert "high_below_low" in errors
    assert "high_below_open" in errors
    assert "high_below_close" in errors
    assert "low_above_open" not in errors


def test_multiple_symbols_do_not_share_window_history(spark: SparkSession) -> None:
    dataframe = _dataframe(spark, _rows("AAPL", 2) + _rows("TSLA", 2))
    enriched = add_sector(
        dataframe, {"AAPL": "Technology", "TSLA": "Automotive"}
    )
    result = add_financial_features(enriched)
    ordered_rows = result.orderBy("symbol", "timestamp").collect()
    first_rows = {}
    for row in ordered_rows:
        first_rows.setdefault(row.symbol, row)

    assert set(first_rows) == {"AAPL", "TSLA"}
    assert all(row.previous_close is None for row in first_rows.values())
    sectors = {symbol: row.sector for symbol, row in first_rows.items()}
    assert sectors == {"AAPL": "Technology", "TSLA": "Automotive"}


def test_lag_change_return_and_moving_average_features(spark: SparkSession) -> None:
    result = add_financial_features(_dataframe(spark, _rows("AAPL", 31))).orderBy(
        "timestamp"
    )
    rows = result.collect()

    assert rows[0].previous_close is None
    assert rows[0].previous_volume is None
    assert rows[1].previous_close == pytest.approx(1.0)
    assert rows[1].price_change == pytest.approx(1.0)
    assert rows[1].return_pct == pytest.approx(1.0)
    assert rows[1].log_return == pytest.approx(math.log(2.0))
    assert rows[1].previous_volume == 100
    assert rows[1].volume_change == 1

    assert rows[3].ma_5 is None
    assert rows[4].ma_5 == pytest.approx(3.0)
    assert rows[13].ma_15 is None
    assert rows[14].ma_15 == pytest.approx(8.0)
    assert rows[28].ma_30 is None
    assert rows[29].ma_30 == pytest.approx(15.5)

    assert rows[29].rolling_volatility_30 is None
    expected_returns = [index / (index - 1) - 1 for index in range(2, 32)]
    assert rows[30].rolling_volatility_30 == pytest.approx(
        statistics.stdev(expected_returns)
    )


def test_local_windows_writer_preserves_spark_schema(
    spark: SparkSession, tmp_path
) -> None:
    processed, _, _ = transform_batch(
        _dataframe(spark, _rows("AAPL", 2)), {"AAPL": "Technology"}
    )
    output_path = tmp_path / "batch"
    stale_partition = output_path / "symbol=STALE"
    stale_partition.mkdir(parents=True)
    (stale_partition / "old.parquet").touch()

    _write_partitioned_with_pyarrow(processed, output_path, replace_all=True)

    files = [path.resolve().as_posix() for path in output_path.glob("*/*.parquet")]
    assert not stale_partition.exists()
    loaded = spark.read.option("basePath", output_path.resolve().as_posix()).parquet(
        *files
    )
    schema = {field.name: field.dataType.simpleString() for field in loaded.schema.fields}
    assert schema["validation_errors"] == "array<string>"
    assert schema["previous_volume"] == "bigint"
    assert schema["volume_change"] == "bigint"
    assert loaded.count() == 2
