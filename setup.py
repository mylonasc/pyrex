# setup.py
import os
import sys
import subprocess
from pathlib import Path
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext as _build_ext

# Path to the RocksDB submodule
ROCKSDB_SRC_DIR = Path(__file__).parent / "third_party" / "rocksdb"

class CMakeRocksDBExtension(Extension):
    """A placeholder for our CMake-built RocksDB library."""
    def __init__(self, name, sourcedir=''):
        Extension.__init__(self, name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)

class build_ext(_build_ext):
    """
    Custom build_ext command to compile RocksDB using CMake
    and then build the pyrex extension.
    """
    def run(self):
        # Ensure CMake is available
        try:
            subprocess.check_output(['cmake', '--version'])
        except OSError:
            raise RuntimeError("CMake must be installed to build the following extensions: " +
                             ", ".join(e.name for e in self.extensions))

        # Separate CMake extensions from regular extensions
        cmake_extensions = []
        regular_extensions = []
        for ext in self.extensions:
            if isinstance(ext, CMakeRocksDBExtension):
                cmake_extensions.append(ext)
            else:
                regular_extensions.append(ext)

        # Build all CMake-based extensions (our static RocksDB library)
        for ext in cmake_extensions:
            self.build_cmake_extension(ext)

        self.extensions = regular_extensions

        super().run()

    def build_cmake_extension(self, ext):
        # The project's root directory
        project_root = Path(__file__).parent.resolve()

        # Define an absolute path for the RocksDB build directory
        build_temp = project_root / self.build_temp / "rocksdb"
        build_temp.mkdir(parents=True, exist_ok=True)

        # Define a single, absolute path for where RocksDB will be installed
        install_dir = build_temp / "install"

        # C++ flags from the first fix
        cxx_flags = "-std=c++17 -include cstdint -include system_error"

        # CMake configuration arguments using the absolute install path
        cmake_args = [
            f'-DCMAKE_INSTALL_PREFIX={install_dir}',
            '-DCMAKE_BUILD_TYPE=Release',
            '-DROCKSDB_BUILD_SHARED=OFF',
            '-DWITH_SNAPPY=ON',
            f'-DCMAKE_CXX_FLAGS={cxx_flags}',
            '-DCMAKE_POSITION_INDEPENDENT_CODE=ON',
            '-DWITH_LZ4=ON',
            '-DWITH_ZLIB=ON',
            '-DWITH_BZ2=ON',
            '-DFAIL_ON_WARNINGS=OFF',
        ]

        # Build arguments
        build_args = ['--config', 'Release', '--', 'VERBOSE=1']

        # 1. Configure CMake
        subprocess.check_call(['cmake', str(ROCKSDB_SRC_DIR)] + cmake_args, cwd=build_temp)

        # 2. Build and install RocksDB
        subprocess.check_call(['cmake', '--build', '.', '--target', 'install'] + build_args, cwd=build_temp)

        # --- Update the pyrex extension with the correct, absolute paths ---
        pyrex_ext = self.get_pyrex_extension()
        if pyrex_ext:
            rocksdb_include_dir = install_dir / "include"
            
            # Detect lib64 vs lib and use the correct one
            rocksdb_library_dir = install_dir / "lib"
            if not rocksdb_library_dir.exists() and (install_dir / "lib64").exists():
                rocksdb_library_dir = install_dir / "lib64"

            print(f"Updating pyrex extension with RocksDB paths:")
            print(f"  Includes: {rocksdb_include_dir}")
            print(f"  Libraries: {rocksdb_library_dir}")

            pyrex_ext.include_dirs.append(str(rocksdb_include_dir))
            pyrex_ext.library_dirs.append(str(rocksdb_library_dir))

            # Static library names are different across platforms
            if sys.platform == "win32":
                 pyrex_ext.libraries.extend(['rocksdb', 'shlwapi', 'rpcrt4', 'zlibstatic', 'snappy', 'lz4', 'bz2'])
            else:
                 pyrex_ext.libraries.extend(['rocksdb', 'snappy', 'lz4', 'z', 'bz2'])


                 
    def get_pyrex_extension(self):
        for ext in self.extensions:
            if ext.name == 'pyrex._pyrex':
                return ext
        return None


# The main Python extension module
pyrex_module = Extension(
    'pyrex._pyrex',
    sources=['src/pyrex/_pyrex.cpp'],
    language='c++',
    # We will populate these dynamically in our custom build_ext
    include_dirs=[], 
    library_dirs=[],
    libraries=[],
    extra_compile_args=['-std=c++17', '-include', 'cstdint'] if sys.platform != 'win32' else ['/std:c++17', '/FIcstdint'],
    # extra_compile_args=['-std=c++17'] if sys.platform != 'win32' else ['/std:c++17'],
    extra_link_args=[],
)

setup(
    # Our extensions list now includes the placeholder for RocksDB
    ext_modules=[CMakeRocksDBExtension('rocksdb'), pyrex_module],
    cmdclass={'build_ext': build_ext},
    zip_safe=False,
)

