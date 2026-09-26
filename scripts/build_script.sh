#!/usr/bin/env bash

source "$(dirname "${BASH_SOURCE[0]}")/runpath_guard.sh"

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: ./scripts/build_script.sh [--publish] [--testpypi] [--skip-tests]

Build local pyrex-rocksdb wheels with cibuildwheel. Publishing is opt-in.

Environment overrides:
  PYREX_VERSION              Package version. Defaults to 0.4.1.
  ROCKSDB_VERSION            RocksDB source version. Defaults to 10.7.5.
  LOCAL_VERSION_NAMING       Set true for local versions like +rocksdb1075. Defaults to false.
  PYREX_BUILD_CONFIG         cibuildwheel config file. Defaults to pyproject_dev.toml.
  PYREX_OUTPUT_DIR           Wheel output directory. Defaults to wheelhouse-RocksDB${ROCKSDB_VERSION}.
  PYREX_JOBS                 cibuildwheel jobs. Defaults to 1.
  PYREX_BUILD_PYTHON         Python used to create the build venv. Defaults to python3.
  HOST_CACHE_DIR             RocksDB build cache. Defaults to $HOME/.cache/cibuildwheel/pyrex_builds.
  TWINE_USERNAME             Twine username. Defaults to __token__ when publishing.
  TWINE_PASSWORD             Twine token/password. Required for publish unless PYREX_PYPI_TOKEN_FILE is set.
  PYREX_PYPI_TOKEN_FILE      Optional local file containing the Twine token/password.

Examples:
  ./scripts/build_script.sh
  PYREX_BUILD_CONFIG=pyproject.toml ./scripts/build_script.sh
  TWINE_PASSWORD="$PYPI_TOKEN" ./scripts/build_script.sh --publish
  PYREX_PYPI_TOKEN_FILE=rocksdb-pypi-token.txt ./scripts/build_script.sh --publish
  TWINE_PASSWORD="$TEST_PYPI_TOKEN" ./scripts/build_script.sh --publish --testpypi
USAGE
}

publish=false
skip_tests=false
repository_url="https://upload.pypi.org/legacy/"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --publish)
      publish=true
      ;;
    --testpypi)
      repository_url="https://test.pypi.org/legacy/"
      ;;
    --skip-tests)
      skip_tests=true
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

export ROCKSDB_VERSION="${ROCKSDB_VERSION:-10.7.5}"
export PYREX_VERSION="${PYREX_VERSION:-0.4.1}"
export LOCAL_VERSION_NAMING="${LOCAL_VERSION_NAMING:-false}"
export HOST_CACHE_DIR="${HOST_CACHE_DIR:-$HOME/.cache/cibuildwheel/pyrex_builds}"

config_file="${PYREX_BUILD_CONFIG:-pyproject_dev.toml}"
output_dir="${PYREX_OUTPUT_DIR:-wheelhouse-RocksDB${ROCKSDB_VERSION}}"

if [[ ! -f "$config_file" ]]; then
  echo "Build config not found: $config_file" >&2
  exit 1
fi

mkdir -p "$HOST_CACHE_DIR" "$output_dir"

build_python="${PYREX_BUILD_PYTHON:-python3}"
build_venv=".venv-build-publish"
if [[ ! -x "$build_venv/bin/python" ]]; then
  "$build_python" -m venv "$build_venv"
fi
python_cmd="$build_venv/bin/python"

echo "Building pyrex-rocksdb ${PYREX_VERSION} against RocksDB ${ROCKSDB_VERSION}"
echo "Config: $config_file"
echo "Output: $output_dir"
echo "Cache:  $HOST_CACHE_DIR"

"$python_cmd" -m pip install --upgrade pip cibuildwheel twine

export CIBW_ENVIRONMENT_PASS="${CIBW_ENVIRONMENT_PASS:-ROCKSDB_VERSION PYREX_VERSION HOST_CACHE_DIR MACOS_HOST_CACHE_DIR VCPKG_ROOT}"

"$python_cmd" -m cibuildwheel \
  --jobs "${PYREX_JOBS:-1}" \
  --platform linux \
  --config-file "$config_file" \
  --output-dir "$output_dir"

"$python_cmd" -m twine check "$output_dir"/*.whl

if [[ "$skip_tests" != true ]]; then
  wheel_count=$("$python_cmd" - <<PY
from pathlib import Path
print(len(list(Path("$output_dir").glob("*.whl"))))
PY
)
  current_tag=$("$python_cmd" - <<'PY'
import sys
print(f"cp{sys.version_info.major}{sys.version_info.minor}")
PY
)
  if [[ "$wheel_count" -eq 1 ]]; then
    wheel_path=$("$python_cmd" - <<PY
from pathlib import Path
print(next(Path("$output_dir").glob("*.whl")))
PY
)
    if [[ "$wheel_path" == *"-$current_tag-"* ]]; then
      tmp_venv="$(mktemp -d)"
      "$python_cmd" -m venv "$tmp_venv/venv"
      "$tmp_venv/venv/bin/python" -m pip install --upgrade pip pytest
      "$tmp_venv/venv/bin/python" -m pip install --force-reinstall "$wheel_path"
      "$tmp_venv/venv/bin/python" -m pytest tests/
      rm -rf "$tmp_venv"
    else
      echo "Skipping install tests because $wheel_path is not compatible with host Python tag $current_tag."
    fi
  else
    echo "Skipping install tests because $wheel_count wheels were built. Use a single-wheel dev config to test locally."
  fi
fi

if [[ "$publish" != true ]]; then
  echo "Build complete. Re-run with --publish to upload wheels."
  exit 0
fi

export TWINE_USERNAME="${TWINE_USERNAME:-__token__}"
if [[ -z "${TWINE_PASSWORD:-}" && -n "${PYREX_PYPI_TOKEN_FILE:-}" ]]; then
  if [[ ! -f "$PYREX_PYPI_TOKEN_FILE" ]]; then
    echo "PYREX_PYPI_TOKEN_FILE does not exist: $PYREX_PYPI_TOKEN_FILE" >&2
    exit 1
  fi
  TWINE_PASSWORD="$(tr -d '\n\r' < "$PYREX_PYPI_TOKEN_FILE")"
  export TWINE_PASSWORD
fi

if [[ -z "${TWINE_PASSWORD:-}" ]]; then
  echo "TWINE_PASSWORD or PYREX_PYPI_TOKEN_FILE is required for publishing." >&2
  exit 1
fi

"$python_cmd" -m twine upload \
  --non-interactive \
  --repository-url "$repository_url" \
  "$output_dir"/*.whl
