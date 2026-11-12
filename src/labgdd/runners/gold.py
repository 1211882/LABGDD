"""Run one bounded approved-Silver to individual-company Gold publication."""

import argparse
import os
import sys
from pathlib import Path
from uuid import uuid4

from labgdd.adapters.silver import resolve_silver_input
from labgdd.config import ConfigurationError, load_settings
from labgdd.gold_config import load_gold_request
from labgdd.observability.logging import configure_logging
from labgdd.processing.gold import GoldProcessingError, process_gold


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("config/project.toml"))
    parser.add_argument("--request", type=Path, default=Path("config/gold.toml"))
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    identity = uuid4()
    logger = configure_logging(identity)
    spark = None
    try:
        settings = load_settings(args.config)
        request = load_gold_request(args.request, settings)
        resolve_silver_input(
            request.silver_registry,
            settings.data_root,
            request.silver_dataset_version,
            request.silver_run_id,
            request.silver_input_authority,
        )
        if args.check:
            logger.info("", extra={"event": "configuration_valid"})
            return 0
        from pyspark.sql import SparkSession

        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        spark = (
            SparkSession.builder.master(args.master)
            .appName(f"labgdd-gold-{identity}")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", "true")
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.pyspark.python", sys.executable)
            .config("spark.pyspark.driver.python", sys.executable)
            .config("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
            .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        result = process_gold(settings, request, spark, run_id=identity)
        if result.status != "complete":
            logger.error("", extra={"event": "gold_failed"})
            return 1
        logger.info("", extra={"event": "gold_complete"})
        return 0
    except (ConfigurationError, GoldProcessingError, OSError, ValueError, ImportError):
        logger.error("", extra={"event": "gold_failed"})
        return 2
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
