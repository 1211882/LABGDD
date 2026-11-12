"""Small historical acquisition CLI; no automatic live execution."""

import argparse
from pathlib import Path
from uuid import uuid4

from labgdd.adapters.alpaca import AcquisitionError, AlpacaREST, Credentials
from labgdd.config import ConfigurationError, load_settings
from labgdd.historical_config import load_historical_request
from labgdd.observability.logging import configure_logging
from labgdd.processing.historical import acquire_historical


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("config/project.toml"))
    parser.add_argument("--request", type=Path, default=Path("config/historical.toml"))
    parser.add_argument(
        "--check", action="store_true", help="Validate configuration without credentials or network"
    )
    args = parser.parse_args(argv)
    identity = uuid4()
    logger = configure_logging(identity)
    try:
        settings = load_settings(args.config)
        request = load_historical_request(args.request, settings)
        if args.check:
            logger.info("", extra={"event": "configuration_valid"})
            return 0
        result = acquire_historical(
            settings, request, AlpacaREST(Credentials.from_environment()), acquisition_id=identity
        )
        if result.status != "complete":
            logger.error("", extra={"event": "acquisition_failed"})
            return 1
        logger.info("", extra={"event": "acquisition_complete"})
        return 0
    except (ConfigurationError, AcquisitionError, OSError, ValueError, ImportError):
        logger.error("", extra={"event": "acquisition_failed"})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
