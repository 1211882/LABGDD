from __future__ import annotations

import json
import math
import os
import statistics
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config.settings import load_spark_settings


FEATURE_COLUMNS = [
    "previous_close",
    "price_change",
    "return_pct",
    "log_return",
    "previous_volume",
    "volume_change",
    "ma_5",
    "ma_15",
    "ma_30",
    "rolling_volatility_30",
]


def main() -> int:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    settings = load_spark_settings()
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("batch-output-validation")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        output_root = settings.output_path.resolve()
        output_files = [path.resolve().as_posix() for path in output_root.glob("*/*.parquet")]
        if not output_files:
            raise FileNotFoundError(f"No processed Parquet files found under: {output_root}")
        dataframe = (
            spark.read.option("basePath", output_root.as_posix())
            .parquet(*output_files)
            .cache()
        )
        print("[SCHEMA]")
        dataframe.printSchema()

        counts = {
            row.symbol: row["count"]
            for row in dataframe.groupBy("symbol").count().orderBy("symbol").collect()
        }
        duplicate_groups = (
            dataframe.groupBy("symbol", "timestamp")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        sectors = {
            row.symbol: row.sectors
            for row in dataframe.groupBy("symbol")
            .agg(F.collect_set("sector").alias("sectors"))
            .orderBy("symbol")
            .collect()
        }
        sector_counts = {
            row.sector: row["count"]
            for row in dataframe.groupBy("sector").count().orderBy("sector").collect()
        }
        null_row = dataframe.agg(
            *[
                F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)).alias(name)
                for name in FEATURE_COLUMNS
            ]
        ).first()
        null_counts = {name: null_row[name] for name in FEATURE_COLUMNS}
        invalid_valid_output = dataframe.filter(~F.col("is_valid")).count()
        unknown_sectors = dataframe.filter(
            F.col("sector").isNull() | (F.col("sector") == "Unknown")
        ).count()

        print("[AAPL_FIRST_10]")
        aapl = dataframe.filter(F.col("symbol") == "AAPL").orderBy("timestamp").cache()
        selected_columns = [
            "timestamp",
            "close",
            "previous_close",
            "price_change",
            "return_pct",
            "log_return",
            "volume",
            "previous_volume",
            "volume_change",
            "ma_5",
            "ma_15",
            "ma_30",
            "rolling_volatility_30",
        ]
        aapl.select(*selected_columns).show(10, truncate=False)

        rows = aapl.select(*selected_columns).limit(31).collect()
        if len(rows) < 31:
            raise AssertionError("AAPL requires at least 31 rows for window validation.")
        target = rows[30]
        closes = [row.close for row in rows]
        returns = [
            (rows[index].close - rows[index - 1].close) / rows[index - 1].close
            for index in range(1, 31)
        ]
        expected = {
            "previous_close": rows[29].close,
            "price_change": target.close - rows[29].close,
            "return_pct": (target.close - rows[29].close) / rows[29].close,
            "log_return": math.log(target.close / rows[29].close),
            "previous_volume": rows[29].volume,
            "volume_change": target.volume - rows[29].volume,
            "ma_5": statistics.mean(closes[-5:]),
            "ma_15": statistics.mean(closes[-15:]),
            "ma_30": statistics.mean(closes[-30:]),
            "rolling_volatility_30": statistics.stdev(returns),
        }
        actual = {name: target[name] for name in expected}
        for name, expected_value in expected.items():
            if not math.isclose(actual[name], expected_value, rel_tol=1e-12, abs_tol=1e-12):
                raise AssertionError(
                    f"Feature mismatch for {name}: expected={expected_value}, actual={actual[name]}"
                )

        expected_null_counts = {
            "previous_close": len(counts),
            "price_change": len(counts),
            "return_pct": len(counts),
            "log_return": len(counts),
            "previous_volume": len(counts),
            "volume_change": len(counts),
            "ma_5": sum(min(4, count) for count in counts.values()),
            "ma_15": sum(min(14, count) for count in counts.values()),
            "ma_30": sum(min(29, count) for count in counts.values()),
            "rolling_volatility_30": sum(min(30, count) for count in counts.values()),
        }
        if null_counts != expected_null_counts:
            raise AssertionError(
                f"Unexpected feature nulls: expected={expected_null_counts}, actual={null_counts}"
            )

        invalid_path = settings.invalid_output_path
        invalid_files = list(invalid_path.glob("*/*.parquet")) if invalid_path.exists() else []
        invalid_output_count = (
            spark.read.option("basePath", invalid_path.resolve().as_posix())
            .parquet(*[path.resolve().as_posix() for path in invalid_files])
            .count()
            if invalid_files
            else 0
        )

        summary = {
            "row_counts": counts,
            "total_rows": sum(counts.values()),
            "duplicate_identity_groups": duplicate_groups,
            "sectors": sectors,
            "rows_per_sector": sector_counts,
            "number_of_sectors": len(sector_counts),
            "null_counts": null_counts,
            "invalid_rows_in_valid_output": invalid_valid_output,
            "unknown_sector_rows": unknown_sectors,
            "invalid_output_rows": invalid_output_count,
            "manual_aapl_row_31": {
                "timestamp": target.timestamp.isoformat(),
                "close": target.close,
                "expected": expected,
                "spark": actual,
            },
        }
        print("[VALIDATION_SUMMARY]")
        print(json.dumps(summary, indent=2, sort_keys=True))

        assert duplicate_groups == 0
        assert invalid_valid_output == 0
        assert unknown_sectors == 0
        assert sectors == {
            symbol: [sector] for symbol, sector in settings.symbol_sectors.items()
        }
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
