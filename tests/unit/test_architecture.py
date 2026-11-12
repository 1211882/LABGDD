import ast
import hashlib
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GIT = ["git", "-c", f"safe.directory={ROOT.as_posix()}"]


def test_domain_imports_only_standard_library() -> None:
    for source in (ROOT / "src/labgdd/domain").glob("*.py"):
        for node in ast.walk(ast.parse(source.read_text(encoding="utf-8"))):
            modules: list[str] = []
            if isinstance(node, ast.Import):
                modules = [alias.name.split(".")[0] for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                assert node.level == 0, "relative imports need explicit boundary review"
                if (node.module or "").startswith("labgdd.domain."):
                    continue
                modules = [(node.module or "").split(".")[0]]
            assert all(module in sys.stdlib_module_names for module in modules)


def test_reference_audit_unchanged() -> None:
    content = (ROOT / "docs/reference/PROJECT_AUDIT_AND_REBUILD_CONTEXT.md").read_bytes()
    assert hashlib.sha256(content).hexdigest() == (
        "553d34b93258fe35bd061956b616f30c72e3c8759419a4cfd849920d071eb069"
    )


def test_secret_and_runtime_paths_ignored() -> None:
    ignored = [".env", ".env.production", "config/.env", ".venv/a", "data/raw.parquet", "x.key"]
    result = subprocess.run(
        [*GIT, "check-ignore", "--no-index", "-z", "--stdin"],
        cwd=ROOT,
        input=("\0".join(ignored) + "\0").encode(),
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert set(result.stdout.decode().rstrip("\0").split("\0")) == set(ignored)
    template_result = subprocess.run(
        [*GIT, "check-ignore", "--no-index", ".env.example"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert template_result.returncode == 1


def test_no_secret_files_tracked() -> None:
    result = subprocess.run(
        [*GIT, "ls-files", "-z"],
        cwd=ROOT,
        capture_output=True,
        check=True,
    )
    paths = result.stdout.decode().split("\0")
    assert not any(
        Path(path).name.startswith(".env") and Path(path).name != ".env.example"
        for path in paths
        if path
    )


def test_credential_template_contains_no_values() -> None:
    for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            assert key in {"ALPACA_API_KEY", "ALPACA_API_SECRET"}
            assert not value.strip()
