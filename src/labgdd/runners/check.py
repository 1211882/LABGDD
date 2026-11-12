"""Offline configuration validation; never creates lake directories or services."""

import argparse
from pathlib import Path
from uuid import uuid4

from labgdd.config import ConfigurationError, load_settings
from labgdd.observability.logging import configure_logging


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("config/project.toml"))
    args = parser.parse_args(argv)
    run_id = uuid4()
    logger = configure_logging(run_id)
    try:
        settings = load_settings(args.config)
    except ConfigurationError:
        logger.error("", extra={"event": "configuration_invalid"})
        return 2
    logger = configure_logging(run_id, settings.log_level)
    logger.info("", extra={"event": "configuration_valid"})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
