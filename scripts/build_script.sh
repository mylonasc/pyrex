#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]}")/runpath_guard.sh"
# Script for local builds of the library.

# vim pyproject.toml
# export ROCKSDB_VERSION='10.4.2'
export ROCKSDB_VERSION='9.11.2'
export PYREX_VERSION='0.1.3a'
export HOST_CACHE_DIR="$HOME/.cache/cibuildwheel/pyrex_builds"

export LOCAL_VERSION_NAMING='false' # This needs to be false for pypi deployments.
export TOML_FILE='pyproject_dev.toml'
# export CIBW_VOLUME="/home/charilaos/Workspace/pyrex/cibw_cache/pyrex_builds:/tmp/cache/pyrex_builds"

echo "cache dir: $HOST_CACHE_DIR"
mkdir -p $HOST_CACHE_DIR

cibuildwheel \
	--platform linux \
	--config-file $TOML_FILE \
	--output-dir "wheelhouse-RocksDB${ROCKSDB_VERSION}"
