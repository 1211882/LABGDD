# LABGDD V2 – Development Handoff

**Phase:** Repository migration and canonicalization
**Task:** Replace the tracked LABGDD project tree with validated LABGDD V2
**Status:** Completed
**Date:** 2026-11-13
**Git branch and commit:** `main`, replacement commit `dc16d1843a8e22cc765f7862ffdce28918b8cab5`

## 1. Objective

Make `C:\Users\Deus\Documents\LABGDD` the canonical LABGDD V2 repository while preserving its existing Git ancestry and remote, retaining recoverable legacy history, and leaving the independent source repository `C:\Users\Deus\Documents\LABGDD_NEW` unchanged as the original V2 provenance repository.

## 2. Changes Implemented

The destination tracked tree was replaced from the source HEAD using the source Git tracked-file inventory rather than a directory-wide copy. All 168 source-indexed files, their Git blob identities, and their file modes were reproduced in the destination index. Obsolete destination-only tracked content was removed. Source `.git`, `.env`, virtual environments, caches, local data, Parquet outputs, build outputs, and other ignored machine-local artifacts were not copied.

Before replacement, two local recovery references were created at the original destination HEAD:

- Branch `backup/pre-v2-sync-20261112-11c02a88`
- Annotated tag `backup-pre-v2-sync-20261112-11c02a88` (tag object `dc5085e10a93a45ede1356220ff9ff43b3d8f42d`)

Both resolve to `11c02a88cbdf5f7100226590d48e047ddd840b0e`. The destination `.git` directory, object database, existing ancestry, and remote configuration were preserved. No history rewrite, squash, force-push, tag recreation, or source-repository mutation occurred.

The exact source-equivalent tree was committed as normal descendant `dc16d1843a8e22cc765f7862ffdce28918b8cab5` with message `chore: synchronize LABGDD with validated LABGDD V2`. A fresh fetch showed `origin/main` still at `223ec8fa7e68e6c2f0b678032e206ca45ee84684`; it was an ancestor of the replacement commit. The normal fast-forward push succeeded, and remote `main` was verified at the replacement commit.

## 3. Files Created or Modified

The replacement commit changed 31 paths relative to the prior destination HEAD: 3,102 insertions and 191 deletions. It introduced the current publication registries and governance evidence, Sector Gold architecture and ADRs, Milestone 7/8 handoffs, Sector Gold scalar and Spark implementations, and their tests. It updated existing architecture, measurement, registry, governance, and validation modules. It removed obsolete destination-only `docs/handoffs/TASK_V2_REPOSITORY_REPLACEMENT_HANDOFF.md` because that file was not present in source HEAD.

This document, `docs/handoffs/TASK_LABGDD_V2_REPOSITORY_MIGRATION_HANDOFF.md`, is a destination migration record added after the exact-equivalence replacement commit. It is intentionally not represented as part of source HEAD.

## 4. Architecture and Design Decisions

The destination retains its own Git identity and ancestry; the source and destination histories remain unrelated. The replacement commit contains the source HEAD tree but does not contain source commit ancestry. The source identity is:

- Repository: `C:\Users\Deus\Documents\LABGDD_NEW`
- Branch: `main`
- Commit: `7a3c8ed1bdb37a6af73419b9669740ac8bb8bf9b`
- Subject: `feat(sector-gold): implement sector measurement foundations`
- Remote: none

The destination identity before migration was commit `11c02a88cbdf5f7100226590d48e047ddd840b0e`; its remote remains exactly `https://github.com/1211882/LABGDD.git`.

The source's annotated V2 tags were recorded, not recreated in the destination:

- `phase0-approved-v1` -> `81be7794ffa4c197e866f3fc89d147287abb3731`
- `phase2-silver-approved-v1` -> `0d1584bd4b159f0af3a7b780b46a2472419581d3`
- `phase2-individual-gold-approved-v1` -> `0525c84a8ab72b24cf6340eaac0a898cf1db6eab`
- `phase2-research-universe-approved-v1` -> `453e86be6b07b09342a4e473f15b512cc8b2a236`
- `phase2-expected-series-spark-approved-v1` -> `e51c2d00dba881cf74efa9b8f23d93a42db5be5b`
- `phase2-expected-gold-publication-approved-v1` -> `c085197e8762190501ffac6d898414d96563ace7`
- `phase2-multisymbol-pilot-approved-v1` -> `e54e7261c1ffe5af63f4ef8814e18ce804f0d0ae`
- `phase2-multisymbol-publications-approved-v1` -> `94c8a769cb92a50cb20da2bc4de6cb65797934a0`

## 5. Tests and Validation

Passed against the destination replacement tree:

- `python -m ruff format --check .`: 78 files already formatted.
- `python -m ruff check .`: all checks passed.
- `python -m mypy --cache-dir <temporary>`: no issues in 78 source files.
- `python -m pip check`: no broken requirements.
- `python -m pytest -p no:cacheprovider`: 285 passed, 2 skipped in 1,318.75 seconds. The two skips were the expected Linux-only authoritative Spark Parquet publication tests. Pytest exited zero; Windows emitted a post-run `ERROR: Access denied` cleanup message, so Linux tests were independently executed in Docker.
- Network-isolated Linux Docker selection: `tests/integration/test_spark_sector_gold.py` and `tests/integration/test_expected_gold_publication.py`: 9 passed in 120.78 seconds, with no skips.
- `python -m build --no-isolation`: sdist and `labgdd_v2-0.4.0-py3-none-any.whl` built successfully from a secret-free temporary copy containing exactly the 168 staged files.
- `scripts/smoke_wheel.py`: clean wheel installation, imports, module CLI, console script, configuration checks, and dependency consistency passed.
- `scripts/check_repository.py`: passed for 168 candidate files.
- Git equivalence: source entries 168; destination entries 168; missing 0; extra 0; mode/blob mismatches 0; working-tree SHA-256 mismatches 0.
- Forbidden staged artifacts: 0. No LFS pointers were present. Tracked `.env.example` contains blank placeholders; local `.env` files remained ignored and were neither read nor staged.

## 6. Issues and Limitations

The migration deliberately preserves source HEAD exactly, including Milestone 8's unresolved review findings. ADR 014 remains Proposed. Known review concerns include constant nonzero-return correlation behavior at longer horizons due to exact floating-point comparison, omission of the strict-historical eligibility policy from output identity, and inconsistent acceptance of extra threshold keys between scalar Python and Spark validation. These are not migration defects, but they require a separate reviewed remediation before ADR 014 acceptance or Sector Gold publication.

The local backup branch and tag are not pushed. The independent source repository remains necessary for authentic V2 commit/tag ancestry. The migration does not claim that destination history contains that ancestry.

## 7. Scientific and Data Integrity

No financial formula, temporal contract, dataset, registry authority, or scientific result was changed by the migration. The operation transferred committed source bytes and modes exactly. No generated dataset was copied or regenerated, and no publication was created or activated.

## 8. Current Project Status

GitHub `origin/main` and the canonical destination contain the validated V2 source tree at replacement commit `dc16d1843a8e22cc765f7862ffdce28918b8cab5`. The destination retains recoverable legacy ancestry and local recovery references. Runtime, packaging, repository security, and Linux Spark regression gates passed. Sector Gold remains an unapproved foundation pending correction and architecture review; Milestone 9 is not started.

## 9. Recommended Next Step

Use `C:\Users\Deus\Documents\LABGDD` for future commits. Preserve `LABGDD_NEW` without modification as historical provenance. The next separately authorized task should remediate and re-review the concrete ADR 014 correctness findings; it must not publish Sector Gold implicitly.

## 10. Review Questions

None for the repository migration. ADR 014 acceptance remains a separate architecture decision.

**Review status:** READY FOR REVIEW
**Next phase:** NOT STARTED
