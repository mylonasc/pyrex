# setup.py
import os
import sys
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext as _build_ext

# Define platform-specific build parameters
ROCKSDB_INCLUDE_DIRS = []
ROCKSDB_LIBRARY_DIRS = []
ROCKSDB_LIBRARIES = []
extra_compile_args = []

if sys.platform.startswith('darwin'):  # macOS
    BREW_PREFIX = os.environ.get('BREW_PREFIX', '/opt/homebrew')
    ROCKSDB_INCLUDE_DIRS.append(os.path.join(BREW_PREFIX, 'include'))
    ROCKSDB_LIBRARY_DIRS.append(os.path.join(BREW_PREFIX, 'lib'))
    # ROCKSDB_LIBRARIES.extend(['rocksdb', 'snappy', 'lz4', 'z', 'bz2'])
    ROCKSDB_LIBRARIES.extend(['rocksdb'])
    extra_compile_args.append('-std=c++17')

elif sys.platform.startswith('linux'):
    ROCKSDB_INCLUDE_DIRS.extend(['/usr/local/include/', '/usr/include/'])
    ROCKSDB_LIBRARY_DIRS.extend(['/usr/local/lib/', '/usr/lib/'])
    # ROCKSDB_LIBRARIES.extend(['rocksdb', 'snappy', 'lz4', 'z', 'bz2'])
    ROCKSDB_LIBRARIES.extend(['rocksdb'])
    extra_compile_args.append('-std=c++17')

elif sys.platform.startswith('win'):
    vcpkg_root = os.environ.get('VCPKG_ROOT')
    if vcpkg_root:
        ROCKSDB_INCLUDE_DIRS.append(os.path.join(vcpkg_root, 'installed', 'x64-windows', 'include'))
        ROCKSDB_LIBRARY_DIRS.append(os.path.join(vcpkg_root, 'installed', 'x64-windows', 'lib'))
    ROCKSDB_LIBRARIES.extend(['rocksdb', 'shlwapi', 'rpcrt4', 'zlib'])
    extra_compile_args.append('/std:c++17')


class build_ext(_build_ext):
    def build_extensions(self):
        import pybind11
        self.compiler.add_include_dir(pybind11.get_include())

        for ext in self.extensions:
            ext.include_dirs.extend(ROCKSDB_INCLUDE_DIRS)
            ext.library_dirs.extend(ROCKSDB_LIBRARY_DIRS)
            ext.libraries.extend(ROCKSDB_LIBRARIES)
            ext.extra_compile_args.extend(extra_compile_args)

        _build_ext.build_extensions(self)


pyrex_module = Extension(
    'pyrex._pyrex',
    sources=['src/pyrex/_pyrex.cpp'],
    language='c++',
    include_dirs=[],
    library_dirs=[],
    libraries=[],
    extra_compile_args=[],
    extra_link_args=[],
)

setup(
    ext_modules=[pyrex_module],
    cmdclass={'build_ext': build_ext},
    zip_safe=False,
)
