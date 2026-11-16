import builtins
import hashlib
import socket
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from labgdd.adapters.silver import ApprovedSilver
from labgdd.config import ConfigurationError, load_settings
from labgdd.expected_gold_config import load_expected_gold_request
from labgdd.runners.expected_gold import main

ROOT = Path(__file__).resolve().parents[2]


def _inventory(path: Path) -> tuple[tuple[str, int, str], ...]:
    if not path.exists():
        return ()
    return tuple(
        (
            item.relative_to(path).as_posix(),
            item.stat().st_size,
            hashlib.sha256(item.read_bytes()).hexdigest(),
        )
        for item in sorted(candidate for candidate in path.rglob("*") if candidate.is_file())
    )


def test_expected_gold_configuration_is_strict_and_side_effect_free(tmp_path: Path) -> None:
    settings = load_settings(ROOT / "config/project.toml")
    request = load_expected_gold_request(ROOT / "config/expected_gold.toml", settings)
    assert request.dataset_version == "gold-study-cohort-20260914-expected-v1"
    assert request.requested_sessions == ("2026-09-14",)
    assert request.eligibility_policy == "declared_research"
    assert request.gold_schema_version == "2.0.0"
    assert not tmp_path.exists() or not any(tmp_path.iterdir())


def test_milestone6_scope_and_validation_authority_are_explicit() -> None:
    settings = load_settings(ROOT / "config/project.toml")
    request = load_expected_gold_request(ROOT / "config/milestone6/expected_gold.toml", settings)
    assert request.silver_input_authority == "authorized_bounded_validation"
    assert request.acquisition_scope_symbols == ("AAPL", "MSFT", "NVDA", "JPM", "XOM")


def test_acquisition_scope_rejects_duplicates_and_symbols_outside_cohort(
    tmp_path: Path,
) -> None:
    settings = load_settings(ROOT / "config/project.toml")
    source = (ROOT / "config/milestone6/expected_gold.toml").read_text(encoding="utf-8")
    config = tmp_path / "config/milestone6"
    config.mkdir(parents=True)
    for name, replacement in (
        ("duplicate", '["AAPL", "AAPL"]'),
        ("unknown", '["AAPL", "NOT_IN_COHORT"]'),
    ):
        path = config / f"{name}.toml"
        path.write_text(
            source.replace('["AAPL", "MSFT", "NVDA", "JPM", "XOM"]', replacement),
            encoding="utf-8",
        )
        with pytest.raises(ConfigurationError, match="acquisition_scope_symbols"):
            load_expected_gold_request(path, settings)


def test_expected_gold_configuration_rejects_unknown_paths_policies_and_sessions(
    tmp_path: Path,
) -> None:
    settings = load_settings(ROOT / "config/project.toml")
    source = (ROOT / "config/expected_gold.toml").read_text(encoding="utf-8")
    config = tmp_path / "config"
    config.mkdir()
    cases = {
        "unknown": source + "unknown = true\n",
        "external": source.replace(
            'research_universe = "research_universe.toml"',
            'research_universe = "../../outside.toml"',
        ),
        "policy": source.replace(
            'eligibility_policy = "declared_research"', 'eligibility_policy = "unsafe"'
        ),
        "sessions": source.replace('["2026-09-14"]', '["2026-09-14", "2026-09-14"]'),
        "schema": source.replace('gold_schema_version = "2.0.0"', 'gold_schema_version = "1.0.0"'),
    }
    for name, value in cases.items():
        path = config / f"{name}.toml"
        path.write_text(value, encoding="utf-8")
        with pytest.raises(ConfigurationError):
            load_expected_gold_request(path, settings)


def test_both_eligibility_policies_are_explicitly_supported(tmp_path: Path) -> None:
    settings = load_settings(ROOT / "config/project.toml")
    declared = (ROOT / "config/expected_gold.toml").read_text(encoding="utf-8")
    config = tmp_path / "config"
    config.mkdir()
    strict_path = config / "strict.toml"
    strict_path.write_text(
        declared.replace(
            'eligibility_policy = "declared_research"',
            'eligibility_policy = "verified_historical_applicability"',
        ),
        encoding="utf-8",
    )
    request = load_expected_gold_request(strict_path, settings)
    assert request.eligibility_policy == "verified_historical_applicability"


@pytest.mark.parametrize("valid_request", (True, False))
def test_configuration_check_does_not_start_spark_or_write(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, valid_request: bool
) -> None:
    original_import = builtins.__import__

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name.startswith("pyspark"):
            raise AssertionError("configuration check started Spark")
        return original_import(name, *args, **kwargs)

    config = tmp_path / "config"
    config.mkdir()
    for name in ("project.toml", "expected_gold.toml", "research_universe.toml"):
        (config / name).write_bytes((ROOT / "config" / name).read_bytes())
    if not valid_request:
        request_path = config / "expected_gold.toml"
        request_path.write_text(
            request_path.read_text(encoding="utf-8").replace(
                'eligibility_policy = "declared_research"',
                'eligibility_policy = "invalid"',
            ),
            encoding="utf-8",
        )
    # Only the external publication authority boundary is mocked. Configuration,
    # research-plan generation and check-mode control flow remain real.
    authority = Mock(
        return_value=ApprovedSilver(
            config / "silver_publications.toml",
            tmp_path / "external-evidence/manifest.json",
            "a" * 64,
            "silver-aapl-20260914-v1",
            "aecb53c1-d7a8-40c7-939b-a6901f435ff5",
            {},
        )
    )
    monkeypatch.setattr("labgdd.processing.expected_gold.resolve_silver_input", authority)

    def reject_network(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("configuration check attempted network access")

    monkeypatch.setattr(socket, "create_connection", reject_network)
    monkeypatch.setattr(socket.socket, "connect", reject_network)
    before = _inventory(tmp_path)
    monkeypatch.setattr(builtins, "__import__", guarded_import)
    assert main(
        [
            "--config",
            str(config / "project.toml"),
            "--request",
            str(config / "expected_gold.toml"),
            "--check",
        ]
    ) == (0 if valid_request else 2)
    assert _inventory(tmp_path) == before
    assert not (tmp_path / "data").exists()
    if valid_request:
        authority.assert_called_once()
    else:
        authority.assert_not_called()
