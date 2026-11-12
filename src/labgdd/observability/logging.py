"""Allowlisted JSON logging: arbitrary messages, args and exceptions are omitted."""

import json
import logging
from datetime import UTC, datetime
from typing import TextIO
from uuid import UUID

_EVENTS = {
    "configuration_valid",
    "configuration_invalid",
    "acquisition_complete",
    "acquisition_failed",
    "silver_complete",
    "silver_failed",
}


class JsonFormatter(logging.Formatter):
    def __init__(self, run_id: UUID) -> None:
        super().__init__()
        self.run_id = str(run_id)

    def format(self, record: logging.LogRecord) -> str:
        event = getattr(record, "event", None)
        if not isinstance(event, str) or event not in _EVENTS:
            event = "application_event"
        return json.dumps(
            {
                "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
                "level": record.levelname,
                "run_id": self.run_id,
                "event": event,
            },
            sort_keys=True,
        )


def configure_logging(
    run_id: UUID, level: str = "INFO", stream: TextIO | None = None
) -> logging.Logger:
    """Configure only the project's logger; callers own when this occurs."""
    logger = logging.getLogger("labgdd")
    logger.setLevel(level)
    logger.propagate = False
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter(run_id))
    logger.addHandler(handler)
    return logger
