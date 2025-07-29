[![Build Status](https://github.com/mylonasc/pyrex/actions/workflows/build_wheels.yml/badge.svg)](https://github.com/mylonasc/pyrex/actions/workflows/build_wheels.yml)

# pyrex
a python rocksdb wrapper

## Motivation
rocksdb python wrappers are broken. This is yet another attempt to create a working python wrapper for rocksdb.

## Example usage:
Check the `test.py` file.

## Installation

On Linux/macOS: Open your terminal, navigate to the parent directory of my_rocksdb_wrapper, and run:


Build and Use the Wrapper:
After saving the files, follow these steps to build and use your Python wrapper:

### Prerequisites:

* RocksDB C++ Library Installed (headers and libraries accessible). (in Ubuntu `sudo apt-get install librocksdb` may suffice)
* C++11 compatible compiler (e.g., g++ or clang++).
* Python 3.7+ and its development headers.    

* Python pybind11 package: `pip install pybind11`

Python setuptools package: `pip install --upgrade setuptools`

### Adjust setup.py (if needed):

Open setup.py and verify that `include_dirs` and `library_dirs` correctly point to your RocksDB installation paths. 
If RocksDB is not in `/usr/local/include` or `/usr/local/lib`, update these paths.

If RocksDB was built with specific compression libraries (like Snappy, Zlib, LZ4, Zstandard), add their corresponding names (e.g., 'snappy', 'z') to the libraries list.

Compile the Wrapper:
Navigate to the directory containing rocksdb_wrapper.cpp and setup.py in your terminal, and run:

```Bash
# build with pyproject.toml (uses setup.py for some programmatic parts)
pip install build
python -m build
```

## For PyPi compatible build
Either rely on the CICD/Actions to work, or use a local `.toml`:

```
cibuildwheel --platform linux --config-file pyproject_localbuild.toml 
```
