import io
import json
import logging
from pathlib import Path
from uuid import uuid4

import pytest

from labgdd.observability.logging import configure_logging
from labgdd.runners.check import main


def test_logging_drops_arbitrary_payload_and_exception() -> None:
    stream = io.StringIO()
    run_id = uuid4()
    logger = configure_logging(run_id, stream=stream)
    try:
        raise ValueError("secret-in-exception")
    except ValueError:
        logger.exception("api_key=%s", "secret-in-args", extra={"event": "secret-in-extra"})
    output = stream.getvalue()
    record = json.loads(output)
    assert record["run_id"] == str(run_id)
    assert record["event"] == "application_event"
    assert record["level"] == "ERROR"
    assert "secret" not in output


def test_reconfiguration_is_idempotent() -> None:
    root_handlers = logging.getLogger().handlers[:]
    stream = io.StringIO()
    configure_logging(uuid4(), stream=stream)
    logger = configure_logging(uuid4(), stream=stream)
    logger.info("", extra={"event": "configuration_valid"})
    assert len(stream.getvalue().splitlines()) == 1
    assert logging.getLogger().handlers == root_handlers


def test_cli_valid_and_missing_config(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config = Path(__file__).resolve().parents[2] / "config/project.toml"
    assert main(["--config", str(config)]) == 0
    assert json.loads(capsys.readouterr().err)["event"] == "configuration_valid"
    assert main(["--config", str(tmp_path / "missing")]) == 2
    assert json.loads(capsys.readouterr().err)["event"] == "configuration_invalid"
