# setup.py
import os
import sys
import setuptools
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext

# A small helper to find pybind11 include directory
class get_pybind_include(object):
    def __init__(self, user=False):
        self.user = user

    def __str__(self):
        import pybind11
        return pybind11.get_include(self.user)

# Define the C++ extension module
ext_modules = [
    Extension(
        'pyrex', # Name of the Python module that will be imported (e.g., `import pyrocksdb`)
        ['pyrex/rocksdb_wrapper.cpp'], # List of C++ source files
        # IMPORTANT: These paths and libraries depend on your RocksDB installation!
        # You might need to adjust them.
        # Common include paths for RocksDB
        include_dirs=[
            # Path to RocksDB headers (e.g., where rocksdb/db.h is found)
            '/usr/local/include', # Default install location for RocksDB headers
            # Path to pybind11 headers
            get_pybind_include(),
            get_pybind_include(user=True),
        ],
        # Common library paths for RocksDB
        library_dirs=[
            # Path to RocksDB libraries (e.g., where librocksdb.so or librocksdb.a is found)
            '/usr/local/lib', # Default install location for RocksDB libraries
        ],
        # Libraries to link against
        libraries=[
            'rocksdb', # Link against librocksdb
            'pthread', # RocksDB is multi-threaded
            # Add other compression libraries if RocksDB was built with them
            # e.g., 'snappy', 'bz2', 'z', 'lz4', 'zstd'
        ],
        # Compiler arguments
        extra_compile_args=['-std=c++11', '-fPIC'], # -fPIC is crucial for shared libraries
        # Linker arguments
        extra_link_args=[],
        language='c++',
    ),
]

# Setuptools setup function
setup(
    name='pyrex', # Name of your Python package
    version='0.1.0',
    author='Charilaos Mylonas',
    author_email='mylonas.charilaos@gmail.com',
    description='A simple Python wrapper for RocksDB using pybind11',
    long_description=open('README.md').read(),
    ext_modules=ext_modules,
    # Use pybind11's custom build_ext command to ensure correct compilation flags
    cmdclass={'build_ext': build_ext},
    zip_safe=False, # Important for C++ extensions
    install_requires=['pybind11>=2.6.0'], # Specify pybind11 as a dependency
    python_requires='>=3.7',
    packages=setuptools.find_packages(), # Automatically find packages (now will find 'pyrex')
)


