# This makes functions and classes like `PyRocksDB` directly available as `pyrex.PyRocksDB`
from ._pyrex import *

# Version information (highly recommended)
try:
    from importlib.metadata import version, PackageNotFoundError
except ImportError:  # Python < 3.8
    from importlib_metadata import version, PackageNotFoundError # pip install importlib_metadata for older Pythons

try:
    __version__ = version("pyrex-rocksdb") # Use the 'name' from pyproject.toml
except PackageNotFoundError:
    # Package is not installed (e.g., running tests in dev mode without editable install)
    __version__ = "unknown"


# Package Docstring 
"""
A fast RocksDB wrapper for Python using pybind11.

This package provides high-performance bindings to the RocksDB key-value store,
allowing seamless interaction from Python applications.
"""
