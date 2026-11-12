import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/check_repository.py"
SPEC = importlib.util.spec_from_file_location("repository_checks", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
checks = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checks)


@pytest.mark.parametrize(
    "text",
    [
        "ALPACA_API_KEY=\nALPACA_API_SECRET=\n",
        "ALPACA_API_KEY=\r\nALPACA_API_SECRET=\r\n",
        "ALPACA_API_KEY = ''\nALPACA_API_SECRET = \"\"\n",
        "# ALPACA_API_KEY=synthetic\n",
        '    "ALPACA_API_KEY=synthetic-only\\n",\n',
    ],
)
def test_safe_credential_fixtures(text: str) -> None:
    assert checks.credential_lines(text) == []


@pytest.mark.parametrize(
    "text",
    [
        "ALPACA_API_KEY=synthetic-only\n",
        "ALPACA_API_KEY=\nALPACA_API_SECRET=synthetic-only\n",
        "export APCA_API_SECRET_KEY = 'synthetic-only'\n",
        '  "API_SECRET": "synthetic-only"\n',
        "-----BEGIN " + "PRIVATE KEY-----\n",
    ],
)
def test_nonempty_credentials_detected_without_values(text: str) -> None:
    assert checks.credential_lines(text)
    assert all(isinstance(number, int) for number in checks.credential_lines(text))


@pytest.mark.parametrize(
    "name",
    [
        ".env",
        "config/.env.local",
        ".venv/a",
        "models/x",
        "data/x",
        "checkpoints/x",
        "dist/x",
        "pyvenv.cfg",
    ],
)
def test_local_artifacts_excluded(name: str) -> None:
    assert checks.forbidden_path(name)


def test_template_allowed() -> None:
    assert not checks.forbidden_path(".env.example")
