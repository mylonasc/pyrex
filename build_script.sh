#!/bin/bash

# Script for local builds of the library.
mkdir cibw_cache
# vim pyproject.toml
export ROCKSDB_VERSION='9.11.2'
export PYREX_VERSION='0.1.2'
export LOCAL_VERSION_NAMING='false' # This needs to be false for pypi deployments.
cibuildwheel \
	--platform linux \
       	--config-file pyproject.toml \
	--output-dir "wheelhouse-RocksDB${ROCKSDB_VERSION}"
