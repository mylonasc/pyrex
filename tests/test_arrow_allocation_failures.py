"""Fail every schema/output allocation without adding production test hooks."""
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys

import pytest


@pytest.mark.skipif(sys.platform != "linux" or platform.libc_ver()[0] != "glibc",
                    reason="malloc interposition test requires Linux/glibc")
@pytest.mark.skipif("asan" in os.environ.get("LD_PRELOAD", ""),
                    reason="run glibc allocation interposition separately from ASan")
def test_allocation_failures_cleanup_without_deadlock(tmp_path):
    compiler = shutil.which("g++")
    if compiler is None:
        pytest.skip("g++ is required to build the allocation failure interposer")
    root = Path(__file__).resolve().parents[1]
    library = tmp_path / "allocation_failure.so"
    subprocess.run([compiler, "-shared", "-fPIC", "-std=c++17", "-I", str(root / "src/pyrex"),
                    str(root / "tests/native/allocation_failure.cpp"), "-o", str(library)], check=True)
    code = """
import ctypes, os, tempfile, pyrex
helper = ctypes.CDLL(os.environ['PYREX_ALLOC_HELPER'])
for name in ('fail_schema', 'fail_batch'):
    fn = getattr(helper, name)
    fn.argtypes = [ctypes.c_void_p, ctypes.c_int]
    fn.restype = ctypes.c_int
get_pointer = ctypes.pythonapi.PyCapsule_GetPointer
get_pointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
get_pointer.restype = ctypes.c_void_p
with pyrex.PyRocksDB(tempfile.mkdtemp()) as db:
    db.put(b'k', b'v')
    for callback, count in ((helper.fail_schema, 9), (helper.fail_batch, 15)):
        for allocation in range(1, count + 1):
            scan = db.scan_arrow(batch_size=1)
            capsule = scan.__arrow_c_stream__()
            ptr = get_pointer(capsule, b'arrow_array_stream')
            status = callback(ptr, allocation)
            assert status != 0, (callback, allocation)
            scan.close()
            del capsule, scan
"""
    env = dict(os.environ, PYREX_ALLOC_HELPER=str(library))
    # Sanitizer interceptors must load first when running an ASan build.
    env["LD_PRELOAD"] = ":".join(filter(None, [env.get("LD_PRELOAD"), str(library)]))
    subprocess.run([sys.executable, "-c", code], env=env, check=True, timeout=30)
