"""Install a wheel offline in a temporary clean venv and run outside the repo."""

import argparse
import subprocess
import sys
import tempfile
import venv
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    wheel = args.wheel.resolve(strict=True)
    config = args.config.resolve(strict=True)
    with tempfile.TemporaryDirectory(prefix="labgdd-v2-smoke-") as directory:
        root = Path(directory)
        environment = root / "venv"
        venv.EnvBuilder(with_pip=True).create(environment)
        python = environment / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
        subprocess.run(
            [str(python), "-m", "pip", "install", "--no-index", "--no-deps", str(wheel)],
            cwd=root,
            check=True,
        )
        subprocess.run(
            [
                str(python),
                "-I",
                "-c",
                "import importlib.metadata as m; import labgdd; "
                "import labgdd.domain.market_bar; import labgdd.config; "
                "import labgdd.domain.temporal; "
                "import labgdd.observability.logging; import labgdd.runners.check; "
                "assert all('extra ==' in r for r in (m.requires('labgdd-v2') or [])); "
                "assert 'site-packages' in labgdd.__file__; "
                "import labgdd.adapters.alpaca; import labgdd.adapters.bronze; "
                "import labgdd.adapters.calendar; import labgdd.processing.historical; "
                "import labgdd.runners.ingest; "
                "import labgdd.processing.expected_series_spark; "
                "import labgdd.processing.expected_gold; import labgdd.expected_gold_config; "
                "import labgdd.processing.gold_batch; "
                "import labgdd.processing.sector_publication; "
                "import labgdd.processing.sector_readiness; "
                "import labgdd.sector_policy; "
                "import labgdd.sector_publication_config; "
                "print('All package imports passed; zero mandatory runtime dependencies')",
            ],
            cwd=root,
            check=True,
        )
        subprocess.run(
            [str(python), "-I", "-m", "labgdd.runners.check", "--config", str(config)],
            cwd=root,
            check=True,
        )
        cli = environment / ("Scripts/labgdd.exe" if sys.platform == "win32" else "bin/labgdd")
        subprocess.run([str(cli), "--config", str(config)], cwd=root, check=True)
        ingest_cli = environment / (
            "Scripts/labgdd-ingest.exe" if sys.platform == "win32" else "bin/labgdd-ingest"
        )
        subprocess.run(
            [
                str(ingest_cli),
                "--check",
                "--config",
                str(config),
                "--request",
                str(config.parent / "historical.toml"),
            ],
            cwd=root,
            check=True,
        )
        subprocess.run([str(python), "-m", "pip", "check"], cwd=root, check=True)
    print("Clean wheel installation, imports, module CLI, console script and pip check passed")


if __name__ == "__main__":
    main()
