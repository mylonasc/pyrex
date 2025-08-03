#!/bin/bash
#
# Creates stubs automatically for more readable and statically analyzable code.
# This is included in the built wheels assets from the following configuration option:
#
# [tool.setuptools.package-data]
# pyrex = ["*.pyi", "py.typed"]
#

source "$(dirname "${BASH_SOURCE[0]}")/runpath_guard.sh"

pybind11-stubgen \
	pyrex \
	--output-dir pyrex-stubs \
       	--root-suffix ""

# This is to flag the package as typed:
touch py.typed
