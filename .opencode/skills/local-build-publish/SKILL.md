---
name: local-build-publish
description: Use when working on local build and publish for pyrex-rocksdb, especially scripts/build_script.sh, PYREX_VERSION, ROCKSDB_VERSION, cibuildwheel, twine, PyPI, or TestPyPI release tasks.
---

# Local Build And Publish

Use this skill for pyrex-rocksdb local wheel builds and PyPI/TestPyPI publishing.

## Key Files

- `setup.py`: requires `PYREX_VERSION` and `ROCKSDB_VERSION`; builds RocksDB from source or cache before building `pyrex._pyrex`.
- `pyproject.toml`: main package metadata and cibuildwheel config for release-like builds.
- `pyproject_dev.toml`: local/dev cibuildwheel config, currently narrower than the main config.
- `.github/workflows/build_wheels.yml`: CI source of truth for current release defaults.
- `scripts/build_script.sh`: local build-and-publish helper.
- `scripts/runpath_guard.sh`: requires scripts to run from the repository root.

## Current Defaults

- Package version: `PYREX_VERSION=0.4.1`
- RocksDB version: `ROCKSDB_VERSION=10.7.5`
- Local version naming: `LOCAL_VERSION_NAMING=false` for PyPI-compatible public releases.
- Local build config: `PYREX_BUILD_CONFIG=pyproject_dev.toml`
- Output directory: `wheelhouse-RocksDB${ROCKSDB_VERSION}`
- Build tooling venv: `.venv-build-publish`

Before changing defaults, check both `setup.py` and `.github/workflows/build_wheels.yml` so local behavior does not drift from CI unintentionally.

## Safe Workflow

1. Inspect `git status --short` before changing release/build files.
2. Check version values in `.github/workflows/build_wheels.yml`, `docs/source/conf.py`, and any script defaults.
3. Run `bash -n scripts/build_script.sh` after editing shell code.
4. Run `./scripts/build_script.sh --help` to verify the script still starts from the repo root.
5. Do not publish unless the user explicitly asks for publishing or passes equivalent intent.
6. Never commit tokens; keep `rocksdb-pypi-token.txt` or other token files untracked.

## Local Build Commands

Build with local/dev defaults:

```bash
./scripts/build_script.sh
```

Build using the main cibuildwheel config:

```bash
PYREX_BUILD_CONFIG=pyproject.toml ./scripts/build_script.sh
```

Override versions explicitly:

```bash
PYREX_VERSION=0.4.1 ROCKSDB_VERSION=10.7.5 ./scripts/build_script.sh
```

Skip local wheel install tests when the host Python cannot install the built wheel tag:

```bash
./scripts/build_script.sh --skip-tests
```

## Publish Commands

Publish to PyPI only after explicit user approval:

```bash
TWINE_PASSWORD="$PYPI_TOKEN" ./scripts/build_script.sh --publish
```

Publish using a local token file:

```bash
PYREX_PYPI_TOKEN_FILE=rocksdb-pypi-token.txt ./scripts/build_script.sh --publish
```

Publish to TestPyPI:

```bash
TWINE_PASSWORD="$TEST_PYPI_TOKEN" ./scripts/build_script.sh --publish --testpypi
```

## Important Details

- `setup.py` raises immediately if `PYREX_VERSION` or `ROCKSDB_VERSION` is missing.
- `LOCAL_VERSION_NAMING=true` produces versions like `0.4.1+rocksdb1075`; keep it `false` for normal PyPI releases.
- Linux builds may require Docker because cibuildwheel uses containerized manylinux builds.
- RocksDB build caching uses `HOST_CACHE_DIR`; default local cache is `$HOME/.cache/cibuildwheel/pyrex_builds`.
- macOS builds need Homebrew dependencies from the CI config: `cmake`, `snappy`, `lz4`, `zstd`, `zlib`, and `bzip2`.
- Windows builds depend on `VCPKG_ROOT` and `ROCKSDB_VERSION_VSPKG` handling in `setup.py`.
- `twine check` should pass before upload.

## Verification

Minimum script validation after edits:

```bash
bash -n scripts/build_script.sh
./scripts/build_script.sh --help
git diff --check
```

For build verification, prefer a dev/narrow build first because full RocksDB/cibuildwheel builds can take a long time.
