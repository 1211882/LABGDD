from dataclasses import replace
from pathlib import Path

import pytest

from labgdd.config import ConfigurationError, load_settings
from labgdd.silver_config import load_silver_request

ROOT = Path(__file__).resolve().parents[2]


def template(manifest: str) -> str:
    return f'''dataset_version = "silver-fixture-v1"
bronze_manifests = ["{manifest}"]
membership_valid_from = "2026-09-14T00:00:00+00:00"
membership_valid_to = "2026-09-15T00:00:00+00:00"
membership_basis = "declared_test_cohort"
resolution_policy_version = "silver-resolution-v1"
silver_schema_version = "1.0.0"
'''


def test_silver_configuration_is_strict_and_side_effect_free(tmp_path: Path) -> None:
    project = load_settings(ROOT / "config/project.toml")
    request = load_silver_request(ROOT / "config/silver.toml", project)
    assert request.dataset_version == "silver-aapl-20260914-v1"
    assert request.bronze_manifests[0].is_relative_to(project.data_root / "bronze")
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    settings = replace(project, data_root=tmp_path / "data")
    for name, text in {
        "unknown": template("../data/bronze/x/manifest.json") + "extra = true\n",
        "external": template("../../outside/manifest.json"),
        "absolute": template("C:/secrets/manifest.json"),
        "reversed": template("../data/bronze/x/manifest.json").replace("2026-09-15", "2026-09-13"),
        "schema": template("../data/bronze/x/manifest.json").replace('"1.0.0"', '"2.0.0"'),
    }.items():
        path = config_dir / f"{name}.toml"
        path.write_text(text, encoding="utf-8")
        with pytest.raises(ConfigurationError):
            load_silver_request(path, settings)
    assert not settings.data_root.exists()
