# ADR 002: Python 3.11, locked tooling, TOML and environment secrets

Date: 2026-11-02. Status: accepted for Phase 0.

Decision: target CPython 3.11 (initial verified patch 3.11.9), setuptools build
backend, src layout and zero runtime dependencies. TOML uses standard-library
tomllib; frozen settings validate strict keys/values and relative owned storage
paths. Secrets are environment-only when adapters need them. No automatic dotenv
loading or credentials required for a local foundation check.

Development tools: pytest 9.0.2, Ruff 0.14.3, mypy 1.18.2, build 1.3.0,
setuptools 80.9.0 and wheel 0.45.1. These deliberate stable exact pins support
Python 3.11; they are not a claim to be the newest available versions.
pytest 9 requires Python >=3.10. Their official release metadata was consulted:
[pytest](https://pypi.org/project/pytest/9.0.2/),
[Ruff](https://pypi.org/project/ruff/0.14.3/),
[mypy](https://pypi.org/project/mypy/1.18.2/),
[build](https://pypi.org/project/build/1.3.0/),
[setuptools](https://pypi.org/project/setuptools/80.9.0/).

`requirements-dev.lock` pins transitive tooling and hashes release artifacts.
Its target is CPython 3.11 on Windows/Linux; CI verifies both, while the initial
local check report records only the platform actually executed. Refresh explicitly
with the documented lock script, review the diff and rerun all checks. Dependency
updates are separate changes; do not float versions or reuse the old environment.
Python's patch is recorded rather than downloading another interpreter implicitly.
Future supported patch upgrades require revalidation; compiled future dependencies
may narrow the matrix. Locking cannot guarantee provider datasets remain unchanged.

Consequences: no configuration framework is needed; missing files fail explicitly.
Build dependencies are pinned in pyproject and lock, and the wheel smoke environment
has no third-party runtime packages. Large distributed/ML stacks remain unselected.

Phase 1 update (2026-11-03): base installation retains zero mandatory dependencies;
optional `ingestion` installs pinned calendar/Parquet libraries. The development
lock now includes this extra and its transitive hashes. See PHASE1_INGESTION.md
for the compatibility matrix actually exercised; Phase 0's tag remains unchanged.
