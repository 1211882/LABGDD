from pathlib import Path

import pytest

from labgdd.config import ConfigurationError, load_settings

TEMPLATE = """schema_version = "1.0.0"
dataset_version = "fixture-v1"
membership_version = "cohort-v1"
data_root = "../data"
log_level = "INFO"
[universe]
Technology = ["AAPL", "MSFT"]
Energy = ["XOM"]
"""


def write_config(tmp_path: Path, text: str = TEMPLATE) -> Path:
    path = tmp_path / "config" / "project.toml"
    path.parent.mkdir(exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_relative_paths_and_variable_universe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_config(tmp_path)
    monkeypatch.chdir(tmp_path.parent)
    settings = load_settings(path)
    assert settings.data_root == tmp_path / "data"
    assert settings.universe["Energy"] == ("XOM",)
    assert not settings.data_root.exists()
    with pytest.raises(TypeError):
        settings.universe["New"] = ("V",)  # type: ignore[index]


@pytest.mark.parametrize(
    ("before", "after"),
    [
        ('"1.0.0"', '"2.0.0"'),
        ('"INFO"', '"verbose"'),
        ('"../data"', '"../../outside"'),
        ('"../data"', '"/tmp/external"'),
        ('"../data"', '"C:/external"'),
        ('"../data"', '".."'),
        ('"../data"', '"."'),
        ('"../data"', '"inside"'),
        ('"../data"', '"../src"'),
        ('"../data"', '"../.git"'),
        ('"XOM"', '"AAPL"'),
        ('["XOM"]', "[]"),
        ('["XOM"]', '"XOM"'),
        ('"AAPL"', '"aapl"'),
        ('"fixture-v1"', '""'),
        ('log_level = "INFO"', "log_level = 2"),
        ('log_level = "INFO"', 'unknown = "INFO"'),
    ],
)
def test_reject_invalid_config(tmp_path: Path, before: str, after: str) -> None:
    path = write_config(tmp_path, TEMPLATE.replace(before, after))
    with pytest.raises(ConfigurationError):
        load_settings(path)


def test_missing_and_malformed_do_not_echo_values(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError):
        load_settings(tmp_path / "absent.toml")
    path = write_config(tmp_path, 'secret = "sensitive-fixture')
    with pytest.raises(ConfigurationError) as error:
        load_settings(path)
    assert "sensitive-fixture" not in str(error.value)


def test_shipped_universe() -> None:
    settings = load_settings(Path(__file__).resolve().parents[2] / "config/project.toml")
    assert len(settings.universe) == 6
    assert sum(map(len, settings.universe.values())) == 30


@pytest.mark.parametrize("value", ["0", "10", "15"])
def test_configurable_lateness(tmp_path: Path, value: str) -> None:
    path = write_config(tmp_path, f"allowed_lateness_minutes = {value}\n" + TEMPLATE)
    assert load_settings(path).allowed_lateness_minutes == int(value)


@pytest.mark.parametrize("value", ["-1", "true", "1.5", '"10"'])
def test_invalid_lateness(tmp_path: Path, value: str) -> None:
    path = write_config(tmp_path, f"allowed_lateness_minutes = {value}\n" + TEMPLATE)
    with pytest.raises(ConfigurationError, match="allowed_lateness_minutes"):
        load_settings(path)


def test_lateness_default_preserves_existing_config(tmp_path: Path) -> None:
    assert load_settings(write_config(tmp_path)).allowed_lateness_minutes == 10
