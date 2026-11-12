"""Check Git candidates without displaying credential values or file contents."""

import argparse
import hashlib
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AUDIT = "docs/reference/PROJECT_AUDIT_AND_REBUILD_CONTEXT.md"
AUDIT_HASH = "553d34b93258fe35bd061956b616f30c72e3c8759419a4cfd849920d071eb069"
_NAME = (
    r"(?:ALPACA_API_KEY|ALPACA_API_SECRET|APCA_API_KEY_ID|APCA_API_SECRET_KEY|"
    r"AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|API_KEY|API_SECRET)"
)
ASSIGNMENT = re.compile(
    rf"^[ \t]*(?:(?:export[ \t]+)?{_NAME}[ \t]*=|"
    rf"[\"']{_NAME}[\"'][ \t]*:)[ \t]*(.*)$",
    re.IGNORECASE,
)
PRIVATE_KEY = re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")


def credential_lines(text: str) -> list[int]:
    """Line numbers only. Empty quoted values and comments are allowed."""
    findings = []
    for number, line in enumerate(text.splitlines(), 1):
        match = ASSIGNMENT.fullmatch(line)
        if match:
            value = match.group(1).strip()
            if value not in {"", "''", '""'} and not value.startswith("#"):
                findings.append(number)
        if PRIVATE_KEY.search(line):
            findings.append(number)
    return findings


def forbidden_path(name: str) -> bool:
    path = Path(name)
    excluded = {
        "data",
        "datasets",
        "models",
        "artifacts",
        "checkpoints",
        "dist",
        "build",
        "__pycache__",
        ".pytest_cache",
        ".ruff_cache",
        ".mypy_cache",
        ".aws",
        ".codex",
        ".agents",
        ".idea",
        ".vscode",
    }
    return (
        any(part in excluded or part.startswith(".venv") for part in path.parts)
        or (path.name.startswith(".env") and path.name != ".env.example")
        or path.name == "pyvenv.cfg"
        or path.suffix in {".pem", ".key", ".parquet", ".pkl", ".joblib", ".pyc", ".log"}
        or any(part.endswith(".egg-info") for part in path.parts)
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--staged", action="store_true")
    args = parser.parse_args()
    git = ["git", "-c", f"safe.directory={ROOT.as_posix()}"]
    flags = ["--cached"] if args.staged else ["--cached", "--others", "--exclude-standard"]
    names = subprocess.check_output([*git, "ls-files", *flags, "-z"], cwd=ROOT).decode().split("\0")
    problems: list[str] = []
    count = 0
    for name in sorted(set(filter(None, names))):
        count += 1
        path = ROOT / name
        if forbidden_path(name) or path.is_symlink():
            problems.append(f"{name}: forbidden candidate path")
            continue
        # In staged mode inspect the exact index bytes, not a possibly different worktree.
        content = (
            subprocess.check_output([*git, "show", f":{name}"], cwd=ROOT)
            if args.staged
            else path.read_bytes()
        )
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            problems.append(f"{name}: unexpected binary file")
            continue
        for number in credential_lines(text):
            problems.append(f"{name}:{number}: nonempty credential assignment or private key")
        if name == ".env.example":
            fields = [
                line.partition("=") for line in text.splitlines() if line.startswith("ALPACA_API_")
            ]
            if len(fields) != 2 or any(value.strip() for _, _, value in fields):
                problems.append(f"{name}: credential template must have two empty fields")
        if name == AUDIT and hashlib.sha256(content).hexdigest() != AUDIT_HASH:
            problems.append(f"{name}: reference audit checksum changed")
    if not args.staged and hashlib.sha256((ROOT / AUDIT).read_bytes()).hexdigest() != AUDIT_HASH:
        problems.append("reference audit: worktree checksum changed")
    for problem in problems:
        print(problem)
    if problems:
        return 1
    print(f"Repository checks passed: {count} {'index' if args.staged else 'candidate'} files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
