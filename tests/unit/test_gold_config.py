from pathlib import Path

import pytest

from labgdd.config import ConfigurationError, load_settings
from labgdd.gold_config import load_gold_request

ROOT = Path(__file__).resolve().parents[2]


def test_existing_gold_defaults_to_approved_authority() -> None:
    settings = load_settings(ROOT / "config/project.toml")
    request = load_gold_request(ROOT / "config/gold.toml", settings)
    assert request.silver_input_authority == "approved"


def test_milestone6_gold_requires_bounded_validation_authority() -> None:
    settings = load_settings(ROOT / "config/project.toml")
    request = load_gold_request(ROOT / "config/milestone6/gold.toml", settings)
    assert request.silver_input_authority == "authorized_bounded_validation"


def test_gold_rejects_unknown_input_authority(tmp_path: Path) -> None:
    settings = load_settings(ROOT / "config/project.toml")
    source = (ROOT / "config/milestone6/gold.toml").read_text(encoding="utf-8")
    config = tmp_path / "config/milestone6"
    config.mkdir(parents=True)
    path = config / "gold.toml"
    path.write_text(source.replace("authorized_bounded_validation", "unreviewed"), encoding="utf-8")
    with pytest.raises(ConfigurationError, match="silver_input_authority"):
        load_gold_request(path, settings)
