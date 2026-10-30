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
helper.fail_point_bitmap.argtypes = [ctypes.c_void_p]
helper.fail_point_bitmap.restype = ctypes.c_int
with pyrex.PyRocksDB(tempfile.mkdtemp()) as db:
    db.put(b'k', b'v')
    points = db.multi_get_arrow([b'k'] * 10000, collect_metrics=True)
    capsule = points.__arrow_c_stream__()
    assert helper.fail_point_bitmap(get_pointer(capsule, b'arrow_array_stream')) != 0
    assert points.stats['rows_emitted'] == points.stats['live_buffer_bytes'] == 0
    points.close()
    del capsule, points
    for collect_metrics in (False, True):
        for callback, count in ((helper.fail_schema, 9), (helper.fail_batch, 15)):
            for allocation in range(1, count + 1):
                scan = db.scan_arrow(batch_size=1, collect_metrics=collect_metrics)
                capsule = scan.__arrow_c_stream__()
                ptr = get_pointer(capsule, b'arrow_array_stream')
                status = callback(ptr, allocation)
                assert status != 0, (callback, allocation)
                scan.close()
                if collect_metrics:
                    assert scan.stats['rows_emitted'] == 0
                    assert scan.stats['live_buffer_bytes'] == 0
                    assert scan.stats['buffer_capacity_bytes_emitted'] == 0
                else:
                    assert scan.stats is None
                del capsule, scan
    # A failure after an earlier successful publication must retain its
    # cumulative counters, without counting the failed second batch.
    db.put(b'z', b'w')
    for allocation in range(1, 16):
        scan = db.scan_arrow(batch_size=1, collect_metrics=True)
        capsule = scan.__arrow_c_stream__()
        ptr = get_pointer(capsule, b'arrow_array_stream')
        assert helper.fail_batch(ptr, 0) == 0
        before = scan.stats
        assert before['rows_emitted'] == 1 and before['live_buffer_bytes'] == 0
        assert helper.fail_batch(ptr, allocation) != 0
        after = scan.stats
        assert after['rows_emitted'] == before['rows_emitted']
        assert after['output_bytes_emitted'] == before['output_bytes_emitted']
        assert after['buffer_capacity_bytes_emitted'] == before['buffer_capacity_bytes_emitted']
        assert after['live_buffer_bytes'] == 0
        scan.close()
        del capsule, scan
"""
    env = dict(os.environ, PYREX_ALLOC_HELPER=str(library))
    # Sanitizer interceptors must load first when running an ASan build.
    env["LD_PRELOAD"] = ":".join(filter(None, [env.get("LD_PRELOAD"), str(library)]))
    subprocess.run([sys.executable, "-c", code], env=env, check=True, timeout=30)
