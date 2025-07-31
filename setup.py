# setup.py
import os
import io
import sys
import subprocess
import tarfile
import urllib.request
from pathlib import Path
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext as _build_ext


# --- Project Configuration ---

PROJECT_DIR = Path(__file__).parent.resolve()

# The default version of RocksDB to build if the env var is not set.

# --- Versioning Logic ---
# Read the base package version from an environment variable.
BASE_VERSION = os.environ.get("PYREX_VERSION")
if not BASE_VERSION:
    raise RuntimeError("PYREX_VERSION environment variable must be set (e.g., '0.1.2').")

# Read the RocksDB version from an environment variable.
rocksdb_version = os.environ.get("ROCKSDB_VERSION")
if not rocksdb_version:
    raise RuntimeError("ROCKSDB_VERSION environment variable must be set (e.g., '10.2.1').")

# The version of RocksDB that will be built.
# It can be overridden by the ROCKSDB_VERSION environment variable.
rocksdb_version = os.environ.get("ROCKSDB_VERSION")

# Construct a PEP 440-compliant version string for the final Python package.
# This appends a local version identifier (e.g., +rocksdb1021) to the base version.
sanitized_rocksdb_version = rocksdb_version.replace('.', '')

LOCAL_VERSION_NAMING = os.environ.get('LOCAL_VERSION_NAMING','false')
if LOCAL_VERSION_NAMING == 'false':
    final_version = BASE_VERSION
else:
    final_version = f"{BASE_VERSION}+rocksdb{sanitized_rocksdb_version}"

# --- Custom Build Logic ---
class CMakeRocksDBExtension(Extension):
    """A placeholder to signal a CMake-based dependency."""
    def __init__(self, name):
        super().__init__(name, sources=[])

class build_ext(_build_ext):
    """
    Custom 'build_ext' command to download and build RocksDB before
    building the actual Python extension.
    """
    def run(self):
        self._ensure_cmake_is_available()

        # 1. Download and build RocksDB for the specified version.
        rocksdb_install_path = self._get_and_build_rocksdb()


        # 2. 
        pyrex_ext = self._get_pyrex_extension()
        if not pyrex_ext:
            raise RuntimeError("Could not find the 'pyrex._pyrex' extension.")
        self._configure_pyrex_extension(pyrex_ext, rocksdb_install_path)

        self.extensions = [pyrex_ext]
        # 3. Call the parent run() to build the pyrex extension.
        super().run()

    def _get_pyrex_extension(self) -> Extension | None:
        """Finds the 'pyrex._pyrex' extension object from the list."""
        for ext in self.extensions:
            if ext.name == 'pyrex._pyrex':
                return ext
        return None

    def _ensure_cmake_is_available(self):
        """Check if CMake is installed and available."""
        try:
            subprocess.check_output(['cmake', '--version'])
        except OSError:
            raise RuntimeError("CMake must be installed to build this project.")

    def _get_and_build_rocksdb(self):
        """
        Downloads, extracts, and builds a specific version of RocksDB.
        Implements a simple caching mechanism to avoid rebuilding.
        """

        build_dir = Path(self.build_temp)
        # Create a version-specific installation directory for caching.
        install_dir = build_dir / f"rocksdb_install_{rocksdb_version}"

        # If the library for this version already exists, use the cache.
        if (install_dir / "lib" / "librocksdb.a").exists() or \
           (install_dir / "lib64" / "librocksdb.a").exists():
            print(f"--- Using cached RocksDB v{rocksdb_version} from {install_dir} ---")
            return install_dir

        build_dir.mkdir(parents=True, exist_ok=True)

        # 1. Download and Extract RocksDB Source
        url = f"https://github.com/facebook/rocksdb/archive/refs/tags/v{rocksdb_version}.tar.gz"
        print(f"--- Downloading RocksDB v{rocksdb_version} from {url} ---")
        with urllib.request.urlopen(url) as response:
            download_content = response.read()

        with io.BytesIO(download_content) as buffer:
            with tarfile.open(fileobj=buffer, mode="r:gz") as tar:
                top_level_dir = Path(tar.getmembers()[0].name).parts[0]
                # ** FIX: Add filter='data' to address the DeprecationWarning **
                tar.extractall(path=build_dir, filter='data')

        source_dir = build_dir / top_level_dir

        # 2. Configure and Build with CMake
        cmake_build_dir = source_dir / "build"
        cmake_build_dir.mkdir(exist_ok=True)


        # C++ flags (rocksdb version 6.x)
        cxx_flags = "-std=c++17 -include cstdint -include system_error"


        ## The following flags were found to work with rocksdb 6:
        cmake_args = [
            f'-DCMAKE_INSTALL_PREFIX={install_dir.resolve()}',
            '-DCMAKE_BUILD_TYPE=Release',
            '-DCMAKE_POSITION_INDEPENDENT_CODE=ON',
            f'-DCMAKE_CXX_FLAGS={cxx_flags}',
            '-DROCKSDB_BUILD_SHARED=OFF',
            '-DFAIL_ON_WARNINGS=OFF',
            '-DWITH_SNAPPY=ON', '-DWITH_LZ4=ON', '-DWITH_ZLIB=ON', 
            '-DWITH_BZ2=ON', '-DWITH_ZSTD=ON',
        ]

        if int(rocksdb_version.split('.')[0]) >= 7:
            ## Fix for version 7:
            # This version contains some stress testing tools that are not necessary. 
            # We skip them by adding the following flags:
            cmake_args.extend(
                    ['-DWITH_TOOLS=OFF','-DWITH_TESTS=OFF']
            )


        
        print(f"--- Configuring RocksDB v{rocksdb_version} ---")
        subprocess.check_call(['cmake', '..'] + cmake_args, cwd=cmake_build_dir)
        
        print(f"--- Building and installing RocksDB v{rocksdb_version} ---")
        subprocess.check_call(['cmake', '--build', '.', '--target', 'install'], cwd=cmake_build_dir)
            
        return install_dir

    def _configure_pyrex_extension(self, ext, rocksdb_install_path):
        """Updates the pyrex extension with the correct paths and libraries."""
        ext.include_dirs.append(str(rocksdb_install_path / "include"))
        

        """Updates a Python extension with the necessary RocksDB paths and libraries."""
        ext.include_dirs.append(str(rocksdb_install_path / "include"))

        lib_dir = rocksdb_install_path / ("lib64" if (rocksdb_install_path / "lib64").exists() else "lib")
        ext.library_dirs.append(str(lib_dir))


        if sys.platform == "win32":
            ext.libraries.extend(['rocksdb', 'shlwapi', 'rpcrt4', 'zlibstatic', 'snappy', 'lz4', 'bz2','zstd'])
        else:
            ext.libraries.extend(['rocksdb', 'snappy', 'lz4', 'z', 'bz2','zstd'])
        
        print(f"--- Configured pyrex extension with RocksDB paths ---")

# --- Extension Definitions ---

pyrex_module = Extension(
    name='pyrex._pyrex',
    sources=['src/pyrex/_pyrex.cpp'],
    language='c++',
    include_dirs=[], 
    library_dirs=[],
    libraries=[],
    extra_compile_args=['-std=c++17'] if sys.platform != 'win32' else ['/std:c++17'],
)

# --- Main Setup Call ---

setup(
    version=final_version,
    ext_modules=[CMakeRocksDBExtension('rocksdb'), pyrex_module],
    cmdclass={'build_ext': build_ext},
    zip_safe=False,
)
