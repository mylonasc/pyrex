#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]}")/runpath_guard.sh"
# Script for local builds of the library.
mkdir -p cibw_cache
# vim pyproject.toml
# export ROCKSDB_VERSION='10.4.2'
export ROCKSDB_VERSION='9.11.2'
export PYREX_VERSION='0.1.3a'
export LOCAL_VERSION_NAMING='false' # This needs to be false for pypi deployments.
export TOML_FILE='pyproject_dev.toml'

cibuildwheel \
	--platform linux \
       	--config-file $TOML_FILE \
	--output-dir "wheelhouse-RocksDB${ROCKSDB_VERSION}"
