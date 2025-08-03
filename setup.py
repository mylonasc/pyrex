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
import platform
import hashlib

# --- Project Configuration ---

PROJECT_DIR = Path(__file__).parent.resolve()

# --- Versioning Logic ---
# Read the base package version from an environment variable.
BASE_VERSION = os.environ.get("PYREX_VERSION")
if not BASE_VERSION:
    raise RuntimeError("PYREX_VERSION environment variable must be set ('xx.yy.zz').")

if sys.platform=='win32':
    # This is executed for the VSPKG build on windows.
    # It is required because the VSPKG does not support all versions of rocksdb.
    if 'ROCKSDB_VERSION_VSPKG' in os.environ:
        os.environ['ROCKSDB_VERSION'] = os.environ['ROCKSDB_VERSION_VSPKG']

# Read the RocksDB version from an environment variable.
rocksdb_version = os.environ.get("ROCKSDB_VERSION")
if not rocksdb_version:
    raise RuntimeError("ROCKSDB_VERSION environment variable must be set ('xx.yy.zz').")

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

def _detect_libc():
    """
    Detects the C library on Linux. Returns 'gnu' for glibc or 'musl' for musl-libc.
    Returns None for non-Linux platforms.
    """
    import ctypes
    if not sys.platform.startswith('linux'):
        return 'none'
    
    try:
        # ctypes.CDLL(None) gives a handle to the main program, which is linked to libc.
        libc = ctypes.CDLL(None)
        # Check for the existence of a glibc-specific function.
        libc.gnu_get_libc_version
        return 'gnu'
    except AttributeError:
        # If the function doesn't exist, we assume it's musl.
        return 'musl'


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
        if sys.platform != 'win32': # MacOS and Linux
            rocksdb_install_path = self._get_and_build_rocksdb()
        else:
            # using vspkg to manage the rocksdb dependency.
            # assuming that VSPKG_ROOT is set by the build env.
            vcpkg_root = os.environ.get('VCPKG_ROOT')
            if not vcpkg_root:
                raise RuntimeError("VCPKG_ROOT environment variable must be set on Windows.")
            rocksdb_install_path = Path(vcpkg_root) / 'installed' / 'x64-windows'

        pyrex_ext = self._get_pyrex_extension()
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

        def _get_cmake_args():
            """Returns all cmake flags apart from DCMAKE_INSTALL_PREFIX=...
            The cmake params are used to build a cache key and avoid
            re-builds or rocksdb.
            """
            # C++ flags (rocksdb version 6.x)
            cxx_flags = "-std=c++17 -include cstdint -include system_error"

            ## The following flags were found to work with rocksdb 6:
            cmake_args = [
                '-DCMAKE_BUILD_TYPE=Release',
                '-DCMAKE_POSITION_INDEPENDENT_CODE=ON',
                f'-DCMAKE_CXX_FLAGS={cxx_flags}',
                '-DROCKSDB_BUILD_SHARED=OFF',
                '-DFAIL_ON_WARNINGS=OFF',
                '-DWITH_TESTS=OFF',
                '-DWITH_SNAPPY=ON', '-DWITH_LZ4=ON', '-DWITH_ZLIB=ON', 
                '-DWITH_BZ2=ON', '-DWITH_ZSTD=ON',
            ]
            
            # liburing-enabled builds are linux only
            if sys.platform.startswith('linux'):
                cmake_args.append('-DWITH_LIBURING=ON')
            
            if int(rocksdb_version.split('.')[0]) >= 7:
                ## Fix for version 7 and later (these do not exist 
                # in earlier versions)
                cmake_args.extend(
                        ['-DWITH_TOOLS=OFF','-DWITH_TESTS=OFF']
                )
            return cmake_args
        
        # Creating a hash key for identifying the 
        # rocksdb build uniquely (and avoid re-building
        # it when its available)
        cmake_args = _get_cmake_args()
        libc_type = _detect_libc()        
        config_str = "".join([
            rocksdb_version,
            platform.machine(),
            platform.system(),
            libc_type,
            *sorted(cmake_args)
        ])
        
        config_hash = hashlib.sha256(config_str.encode('utf-8')).hexdigest()[:16]
        
        # Cache path management for linux:
        if 'HOST_CACHE_DIR' in os.environ:
            cache_root =  Path('/host' + os.environ.get('HOST_CACHE_DIR'))
        else:
            print("Did not find the cache root of the host!")
            cache_root = Path('/host/tmp')
        
        # Cache path management for MacOS:
        if sys.platform == 'darwin': # MacOS (does not run in container)
            cache_root = Path(os.environ.get('MACOS_HOST_CACHE_DIR',"/Users/runner/cibw-build-cache"))
            

        build_dir = self.build_temp
        
        # Create a version-specific installation directory for caching.
        install_dir = cache_root / f"rocksdb_install-{rocksdb_version}-{config_hash}"

        # If the library for this version already exists, use the cache.
        if (install_dir / "lib" / "librocksdb.a").exists() or \
            (install_dir / "lib64" / "librocksdb.a").exists():
            print(f"--- Using cached RocksDB v{rocksdb_version} from {install_dir} ---")
            return install_dir
        
        print(f"📦 --- No cache found. Starting new RocksDB v{rocksdb_version} build ---")
    
        # Temporary directory for the source download and build
        build_dir = Path(self.build_temp)
        build_dir.mkdir(parents=True, exist_ok=True)
        install_dir.mkdir(parents=True, exist_ok=True)

        ## Building rocksdb:
        # 1. Download and Extract RocksDB Source
        url = f"https://github.com/facebook/rocksdb/archive/refs/tags/v{rocksdb_version}.tar.gz"
        print(f"--- Downloading RocksDB v{rocksdb_version} from {url} ---")
        with urllib.request.urlopen(url) as response:
            download_content = response.read()

        # Assuming 'download_content' and 'build_dir' are defined
        with io.BytesIO(download_content) as buffer:
            with tarfile.open(fileobj=buffer, mode="r:gz") as tar:
                top_level_dir = Path(tar.getmembers()[0].name).parts[0]
                # Conditionally use the 'filter' argument based on Python version
                if sys.version_info >= (3, 12):
                    tar.extractall(path=build_dir, filter='data')
                else:
                    tar.extractall(path=build_dir)

        source_dir = build_dir / top_level_dir

        # 2. Configure and Build with CMake
        cmake_build_dir = source_dir / "build"
        cmake_build_dir.mkdir(exist_ok=True)
        
        # add the INSTALL_PREFIX argument in cmake args.
        cmake_args = [f'-DCMAKE_INSTALL_PREFIX={install_dir.resolve()}'] + cmake_args

        
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


        # libs_only_win = ['shlwapi','rpcrt4','zlibstatic']
        libs_only_win = ['shlwapi','rpcrt4','zlib']
        libs_only_linux_macos = ['z', 'snappy','lz4','bz2','zstd']
        if sys.platform.startswith('linux'):
            # may have perf. benefits. It is required
            # when -DWITH_LIBURING=ON. 
            libs_only_linux_macos += ['uring']
            
        libs_both = ['rocksdb']
        
        # libs_both = ['rocksdb','lz4','bz2','zstd']
        if sys.platform == "win32":
            _win_ext_libs = libs_both + libs_only_win
            ext.libraries.extend(libs_both + libs_only_win)
        else:
            ext.libraries.extend(libs_both + libs_only_linux_macos)
        
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
