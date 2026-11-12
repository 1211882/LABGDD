# Phase 0 validation and handoff

Work date: 2026-11-02 (Europe/Lisbon). Platform actually exercised: Windows,
CPython 3.11.9, isolated project environments. Status: implemented, ready for
architecture review. Phase 1 has not started.

## Files created

```text
.env.example
.github/workflows/checks.yml
.gitignore
.python-version
README.md
pyproject.toml
requirements-dev.lock
config/project.toml
docs/architecture/ARCHITECTURE.md
docs/architecture/DATA_CONTRACTS.md
docs/architecture/MEASUREMENTS.md
docs/architecture/IMPLEMENTATION_PLAN.md
docs/architecture/PHASE0_VALIDATION.md
docs/decisions/001-boundaries-and-greenfield.md
docs/decisions/002-environment-and-configuration.md
docs/decisions/003-event-time-and-revisions.md
docs/decisions/004-measurements-and-experiments.md
scripts/lock_tools.py
scripts/smoke_wheel.py
src/labgdd/__init__.py
src/labgdd/config.py
src/labgdd/py.typed
src/labgdd/domain/__init__.py
src/labgdd/domain/market_bar.py
src/labgdd/observability/__init__.py
src/labgdd/observability/logging.py
src/labgdd/runners/__init__.py
src/labgdd/runners/check.py
tests/unit/test_architecture.py
tests/unit/test_config.py
tests/unit/test_logging_and_runner.py
tests/unit/test_market_bar.py
```

The pre-existing reference audit is preserved, not newly created. Generated local
state: new `.git/` (main, no commits/remotes), `.venv/`, `.venv-verify/`, build
metadata/caches, and `dist/labgdd_v2-0.1.0.{tar.gz,whl}` (wheel filename includes
`-py3-none-any`). Environments/build artifacts are ignored. Wheel smoke testing
creates and cleans a separate temporary environment outside the source tree.
No datasets, credentials, deployment services or legacy artifacts were imported.
No source files have been staged or committed.

## Decisions delivered

Four ADRs establish independent package boundaries; Python/TOML and hashed tooling;
immutable observations with explicit correction/time semantics; and versioned
financial estimands plus recoverable experiment stages. The domain is standard
library only. Prices are Decimal, volumes int64-compatible, bars immutable, UTC
and minute alignment enforced. Variable-sized sectors have no five-member rule.
No speculative adapter interfaces or empty future engine packages were created.

## Executed checks

Commands below used `.venv/Scripts/python`, unless otherwise stated.

| Check | Command/evidence | Result |
|---|---|---|
| Editable installation | `-m pip install --no-build-isolation --no-deps -e .` | Passed |
| Formatting | `-m ruff format --check .` | Passed, 14 Python files |
| Lint | `-m ruff check .` | Passed |
| Strict typing | `-m mypy` | Passed, 14 source/test/script files |
| Unit/architecture tests | `-m pytest` | 78 passed; no skips |
| Dependency consistency | `-m pip check` | No broken requirements |
| Distribution build | `-m build --no-isolation` | Built sdist and wheel from that sdist |
| Clean wheel installation | `scripts/smoke_wheel.py dist/labgdd_v2-0.1.0-py3-none-any.whl --config config/project.toml` | Passed offline in a temporary clean venv |
| Installed imports | Smoke script imports every functional package module with `-I`, outside repository | Passed; installation resolves to site-packages; zero runtime dependencies |
| Entry points | Smoke script invokes module and installed console script with explicit config | Both return 0 and JSON configuration-valid events |
| Domain independence | AST import guard in pytest | Only standard-library imports |
| Tracking prevention | Git ignore tests for `.env`, `.env.production`, nested `.env`, keys, environment and data paths; example exception; tracked-path test | Passed |
| Credential template/log handling | Template has blank credential fields; payload/exception/args/extra suppression tests | Passed |
| Audit preservation | SHA-256 recomputed and asserted by test | Unchanged: `553d34b93258fe35bd061956b616f30c72e3c8759419a4cfd849920d071eb069` |

Tool installation initially failed due to restricted DNS/socket access. A scoped
network-enabled retry succeeded. The lock was generated from exact direct pins
and resolved transitive versions, with non-yanked release artifact SHA-256 hashes
from official PyPI metadata. A second fresh environment `.venv-verify/` installed
the lock using `--require-hashes` and the built wheel using `--no-index --no-deps`.
The fresh environment then passed Ruff formatting (14 files), Ruff lint, strict
mypy (14 files), all 78 tests (0.80 seconds; no skips), `pip check`, and the installed
module's configuration-check CLI. This validates the lock through a fresh install,
not merely an already-satisfied dependency check. The separate clean wheel smoke
environment also passed its module and console entry points outside the repository.

Initial checks exposed test-harness issues: mypy correctly rejected deliberately
invalid typed fixture calls; the negative tests now use narrow documented
suppression codes. Windows Git ownership and CRLF stdin behavior required a
command-local safe-directory argument and NUL-delimited ignore tests. No global
Git configuration was changed. Final passing results supersede initial failures.

## Limits, risks and next milestone

Linux CI is configured but has not been executed here; no remote repository or
CI run is claimed. No live provider probe, engine parity, exchange calendar,
financial calculation, ML experiment or infrastructure check is claimed. Phase 0
has no implementation requiring those checks. Secret checks verify known path
prevention, the credential-free template and logging behavior; they are not an
independent full-history secret-scanner certification. This new Git repository
has no tracked files/history yet.

Review the calendar and raw/adjusted price policy, historical applicability of the
study cohort, source correction evidence, proposed ten-minute lateness, financial
eligibility rules and emergence thresholds. The current Python patch is the local
baseline, not a declaration of the newest security patch. Validate future Python
patches and distributed dependency compatibility before expanding the stack.

Next milestone after Phase 0 review: Phase 1 historical ingestion, beginning with
calendar/membership/feed gates, mocked pagination/retry tests and immutable Bronze
Parquet/manifest persistence. Do not automatically import legacy datasets or
implement later phases during this review.
