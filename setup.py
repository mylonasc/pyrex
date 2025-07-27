# setup.py
import os
import sys
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext as _build_ext

# You might need to adjust these paths based on where RocksDB is installed on your build system.
# For cross-platform building, these will likely be managed by your CI system.

# Example for common Linux/macOS paths
# These should ideally be determined at build time (e.g., by CI or vcpkg)
ROCKSDB_INCLUDE_DIRS = [
    '/usr/local/include/',
    '/usr/include/']
    #'/usr/local/include/rocksdb/', # Common for homebrew on macOS or manual installs
    #'/usr/include/rocksdb/',       # Common for apt on Debian/Ubuntu

ROCKSDB_LIBRARY_DIRS = [
    '/usr/local/lib/',             # Common for homebrew on macOS or manual installs
    '/usr/lib/',                   # Common for apt on Debian/Ubuntu
]
ROCKSDB_LIBRARIES = ['rocksdb'] # The library name to link against

# Helper to find rocksdb on systems where it might be in a non-standard location
class build_ext(_build_ext):
    def build_extensions(self):
        # On Windows, you might need vcpkg integration or explicit paths
        if sys.platform == 'win32':
            # Example for vcpkg. Adjust as needed.
            # You might need to set VCPKG_ROOT environment variable
            vcpkg_root = os.environ.get('VCPKG_ROOT')
            if vcpkg_root:
                # Assuming rocksdb is installed for x64-windows
                rocksdb_vcpkg_include = os.path.join(vcpkg_root, 'installed', 'x64-windows', 'include')
                rocksdb_vcpkg_lib = os.path.join(vcpkg_root, 'installed', 'x64-windows', 'lib')
                if os.path.exists(rocksdb_vcpkg_include):
                    if rocksdb_vcpkg_include not in ROCKSDB_INCLUDE_DIRS:
                        ROCKSDB_INCLUDE_DIRS.append(rocksdb_vcpkg_include)
                    if rocksdb_vcpkg_lib not in ROCKSDB_LIBRARY_DIRS:
                        ROCKSDB_LIBRARY_DIRS.append(rocksdb_vcpkg_lib)
            # Add specific Windows rocksdb paths if known
            # ROCKSDB_INCLUDE_DIRS.append("C:/path/to/rocksdb/include")
            # ROCKSDB_LIBRARY_DIRS.append("C:/path/to/rocksdb/lib")
            # ROCKSDB_LIBRARIES = ['rocksdb'] # Or 'rocksdb_static'

        # This part ensures that pybind11's include path is found.
        import pybind11
        self.compiler.add_include_dir(pybind11.get_include())

        # For the extension, we need to ensure RocksDB's headers are available
        for ext in self.extensions:
            ext.include_dirs.extend(ROCKSDB_INCLUDE_DIRS)
            ext.library_dirs.extend(ROCKSDB_LIBRARY_DIRS)
            ext.libraries.extend(ROCKSDB_LIBRARIES)
            if sys.platform.startswith('win'):
                ext.extra_compile_args = ['/std:c++17']
            else:
                ext.extra_compile_args = ['-std=c++17'] # Ensure C++17 or later for modern C++ features

        _build_ext.build_extensions(self)


# Define the pybind11 extension module
pyrex_module = Extension(
    'pyrex._pyrex', # This creates pyrex/_pyrex.so (or .pyd)
    sources=['src/pyrex/_pyrex.cpp'],
    language='c++',
    # These are handled by the custom build_ext command
    include_dirs=[],
    library_dirs=[],
    libraries=[],
)

setup(
    ext_modules=[pyrex_module],
    cmdclass={'build_ext': build_ext},
    # All other metadata (name, version, etc.) is now in pyproject.toml
    # Do NOT set `packages` or `install_requires` here if they are in pyproject.toml
    zip_safe=False, # Important for C extensions
)
