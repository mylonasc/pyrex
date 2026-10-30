"""ABR-03: accepted cancellation is an error, never successful exhaustion."""
import ctypes as ct
import errno
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys

import pytest
import pyrex

from test_arrow_scan_metrics import ArrowArray, ArrowStream, NativeStream, release_array


def last_error(stream):
    return ct.CFUNCTYPE(ct.c_char_p, ct.POINTER(ArrowStream))(
        stream.pointer.contents.get_last_error)(stream.pointer)


@pytest.fixture(params=[pyrex.PyRocksDB, pyrex.PyRocksDBExtended, pyrex.TransactionDB])
def db(request, tmp_path):
    handle = request.param(str(tmp_path / "db"))
    yield handle
    handle.close()


@pytest.mark.parametrize("metrics", [False, True])
def test_cancel_before_export_is_stable_c_stream_error(db, metrics):
    db.put(b"k", b"v")
    scan = db.scan_arrow(collect_metrics=metrics)
    assert not scan.cancel_requested
    assert scan.cancel() is True
    assert scan.cancel_requested
    assert scan.cancel() is False
    with NativeStream(scan) as stream:
        for call in range(1, 3):
            status, batch = stream.next()
            assert status == errno.ECANCELED and not batch.release
            assert last_error(stream) == b"scan_arrow: cancelled"
            if metrics:
                stats = scan.stats
                assert stats["cancelled_calls"] == stats["get_next_calls"] == call
                assert stats["rows_gathered"] == stats["rows_emitted"] == 0
                assert stats["buffer_capacity_bytes_emitted"] == stats["live_buffer_bytes"] == 0
            else:
                assert scan.stats is None
    scan.close()
    assert scan.cancel() is False and scan.cancel_requested and scan.is_closed


@pytest.mark.parametrize("batch_size", [1, 2])
def test_cancel_between_batches_preserves_already_exported_output(db, batch_size):
    db.put(b"a", b"v")
    db.put(b"b", b"w")
    scan = db.scan_arrow(batch_size=batch_size, collect_metrics=True)
    with NativeStream(scan) as stream:
        status, first = stream.next()
        assert status == 0 and first.length == batch_size
        try:
            before = scan.stats
            # Even if this was the last data batch, cancellation is accepted
            # until clean end-of-stream is actually committed by get_next.
            assert scan.cancel() is True
            status, missing = stream.next()
            assert status == errno.ECANCELED and not missing.release
            db.close()
            after = scan.stats
            assert after["rows_emitted"] == before["rows_emitted"] == batch_size
            assert after["live_buffer_bytes"] == before["live_buffer_bytes"]
            assert after["cancelled_calls"] == 1
            assert ct.string_at(first.children[0].contents.buffers[2], 1) == b"a"
        finally:
            release_array(first)
    assert scan.stats["live_buffer_bytes"] == 0


def test_cancel_after_clean_eof_or_close_is_noop(db):
    scan = db.scan_arrow(collect_metrics=True)
    with NativeStream(scan) as stream:
        status, empty = stream.next()
        assert status == 0 and not empty.release
        assert scan.cancel() is False and not scan.cancel_requested
        status, empty = stream.next()
        assert status == 0 and not empty.release
        assert scan.stats["cancelled_calls"] == 0
    closed = db.scan_arrow()
    closed.close()
    assert closed.cancel() is False and not closed.cancel_requested
    with pytest.raises(AttributeError):
        closed.cancel_requested = True


def test_cancel_after_native_failure_does_not_relabel_the_failure(db):
    db.put(b"k", b"v" * 100)
    scan = db.scan_arrow(max_batch_bytes=1, collect_metrics=True)
    with NativeStream(scan) as stream:
        status, batch = stream.next()
        assert status != 0 and status != errno.ECANCELED and not batch.release
        assert b"max_batch_bytes" in last_error(stream)
        assert scan.cancel() is False and not scan.cancel_requested
        assert b"max_batch_bytes" in last_error(stream)
        assert scan.stats["cancelled_calls"] == 0


def test_arrow_consumer_receives_error_not_empty_success(db):
    pa = pytest.importorskip("pyarrow")
    db.put(b"k", b"v")
    scan = db.scan_arrow()
    reader = pa.RecordBatchReader.from_stream(scan)
    assert scan.cancel()
    try:
        with pytest.raises((pa.ArrowException, OSError), match="cancelled"):
            reader.read_all()
    finally:
        reader.close()


@pytest.mark.parametrize("kind", [pyrex.PyRocksDBExtended, pyrex.TransactionDB])
def test_cancel_closes_only_its_own_cf_and_shared_snapshot_resources(kind, tmp_path):
    with kind(str(tmp_path / "cf")) as db:
        cf = db.create_column_family("cancel")
        db.put_cf(cf, b"a", b"v")
        db.put(b"default", b"w")
        snapshot = db.create_snapshot()
        scan = db.scan_arrow_cf(cf, snapshot=snapshot, collect_metrics=True)
        other = db.scan_arrow(snapshot=snapshot)
        assert scan.cancel()
        # A request alone does not free idle native resources. Consume the
        # cancellation or call close before dropping the CF.
        with pytest.raises(pyrex.RocksDBException, match="active Arrow scan"):
            db.drop_column_family(cf)
        with NativeStream(scan) as stream:
            assert stream.next()[0] == errno.ECANCELED
        db.drop_column_family(cf)
        assert snapshot.is_valid() and not other.cancel_requested
        with NativeStream(other) as stream:
            status, batch = stream.next()
            assert status == 0 and batch.length == 1
            release_array(batch)
        snapshot.release()


@pytest.mark.parametrize("kind", ["PyRocksDB", "PyRocksDBExtended", "TransactionDB"])
def test_cancel_racing_eof_has_one_unambiguous_winner(kind, tmp_path):
    code = r'''
import errno, threading
from concurrent.futures import ThreadPoolExecutor
import pyrex
from test_arrow_scan_metrics import NativeStream, release_array
db = getattr(pyrex, KIND)(PATH)
db.put(b'k', b'v')
for _ in range(30):
    scan = db.scan_arrow(batch_size=1, collect_metrics=True)
    with NativeStream(scan) as stream:
        status, first = stream.next()
        assert status == 0 and first.length == 1
        release_array(first)
        gate = threading.Barrier(2)
        def end():
            gate.wait()
            return stream.next()
        def cancel():
            gate.wait()
            return scan.cancel()
        with ThreadPoolExecutor(max_workers=2) as pool:
            end_job = pool.submit(end)
            cancel_job = pool.submit(cancel)
            status, out = end_job.result()
            accepted = cancel_job.result()
        assert not out.release
        assert status == (errno.ECANCELED if accepted else 0)
        assert scan.cancel_requested is accepted
        assert scan.stats['cancelled_calls'] == int(accepted)
        assert scan.stats['rows_emitted'] == 1
db.close()
'''
    prefix = f"import sys\nsys.path.insert(0, {str(Path(__file__).parent)!r})\nKIND={kind!r}\nPATH={str(tmp_path / 'eof')!r}\n"
    subprocess.run([sys.executable, "-c", prefix + code], check=True, timeout=30)


@pytest.mark.parametrize("kind", ["PyRocksDB", "PyRocksDBExtended", "TransactionDB"])
@pytest.mark.parametrize("action", ["close", "drop"])
def test_cancel_racing_db_close_or_cf_drop(kind, action, tmp_path):
    if kind == "PyRocksDB" and action == "drop":
        pytest.skip("plain DB has no CF API")
    code = r'''
import errno, threading
from concurrent.futures import ThreadPoolExecutor
import pyrex
from test_arrow_scan_metrics import NativeStream, release_array
for repeat in range(10):
    db = getattr(pyrex, KIND)(PATH + str(repeat))
    cf = db.create_column_family('race') if ACTION == 'drop' else None
    for i in range(100):
        key = f'k{i:06d}'.encode()
        if cf is None: db.put(key, b'v')
        else: db.put_cf(cf, key, b'v')
    scan = (db.scan_arrow(collect_metrics=True) if cf is None else
            db.scan_arrow_cf(cf, collect_metrics=True))
    with NativeStream(scan) as stream:
        gate = threading.Barrier(3)
        def pull():
            gate.wait()
            return stream.next()
        def cancel():
            gate.wait()
            return scan.cancel()
        def mutate():
            gate.wait()
            if ACTION == 'close': db.close()
            else:
                try: db.drop_column_family(cf)
                except pyrex.RocksDBException as exc:
                    assert 'active Arrow scan' in str(exc)
        with ThreadPoolExecutor(max_workers=3) as pool:
            pulling, cancelling, mutation = [pool.submit(fn) for fn in (pull, cancel, mutate)]
            status, out = pulling.result()
            accepted = cancelling.result()
            mutation.result()
        try:
            if status == 0:
                # A publication decision may precede a racing request/close,
                # but a full data batch is the only successful possibility.
                assert out.release and out.length == 100
            else:
                assert not out.release and status in (errno.ECANCELED, errno.EIO)
            assert scan.stats['rows_emitted'] in (0, 100)
            assert scan.stats['rows_emitted'] == (100 if status == 0 else 0)
        finally:
            release_array(out)
        scan.close()
    if cf is not None and cf.is_valid(): db.drop_column_family(cf)
    db.close()
    assert scan.stats['live_buffer_bytes'] == 0
'''
    prefix = (f"import sys\nsys.path.insert(0, {str(Path(__file__).parent)!r})\n"
              f"KIND={kind!r}\nACTION={action!r}\nPATH={str(tmp_path / 'race')!r}\n")
    subprocess.run([sys.executable, "-c", prefix + code], check=True, timeout=30)


@pytest.fixture(scope="module")
def allocation_latch(tmp_path_factory):
    if sys.platform != "linux" or platform.libc_ver()[0] != "glibc":
        pytest.skip("controlled malloc latch requires Linux/glibc")
    if "asan" in os.environ.get("LD_PRELOAD", ""):
        pytest.skip("run malloc latch separately from ASan")
    compiler = shutil.which("g++")
    if compiler is None:
        pytest.skip("g++ is required for the test-only allocation latch")
    root = Path(__file__).resolve().parents[1]
    library = tmp_path_factory.mktemp("cancel-latch") / "cancel_latch.so"
    subprocess.run([compiler, "-shared", "-fPIC", "-pthread", "-std=c++17",
                    "-I", str(root / "src/pyrex"), str(root / "tests/native/arrow_cancellation.cpp"),
                    "-o", str(library)], check=True)
    return library


@pytest.mark.parametrize("kind", ["PyRocksDB", "PyRocksDBExtended", "TransactionDB"])
@pytest.mark.parametrize("phase", ["gather", "build"])
def test_in_flight_cancel_never_waits_for_gather_lock(allocation_latch, kind, phase, tmp_path):
    code = r'''
import ctypes as ct, errno, json, os, time
from concurrent.futures import ThreadPoolExecutor
import pyrex
from test_arrow_scan_metrics import ArrowArray, NativeStream, release_array
helper = ct.CDLL(os.environ['PYREX_CANCEL_LATCH'])
helper.paused_get_next.argtypes = [ct.c_void_p, ct.c_int, ct.c_size_t, ct.POINTER(ArrowArray)]
helper.paused_get_next.restype = ct.c_int
helper.allocation_paused.restype = ct.c_bool
db = getattr(pyrex, KIND)(PATH)
batch = pyrex.PyWriteBatch()
for i in range(10000): batch.put(f'k{i:018d}'.encode(), b'v' * 64)
db.write(batch)
scan = db.scan_arrow(batch_size=10000, collect_metrics=True)
with NativeStream(scan) as stream:
    out = ArrowArray()
    def consume():
        return helper.paused_get_next(stream.pointer, 700 if PHASE == 'gather' else -1,
                                     190000 if PHASE == 'build' else 0, ct.byref(out))
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(consume)
        deadline = time.monotonic() + 10
        while not helper.allocation_paused():
            assert not future.done(), 'native allocation latch was not reached'
            assert time.monotonic() < deadline, 'allocation latch timeout'
            time.sleep(0.001)
        before = scan.stats['rows_gathered']
        start = time.perf_counter_ns()
        # If cancel waited for the gather mutex, this process would deadlock
        # here: the allocation is resumed only after cancel returns.
        accepted = scan.cancel()
        request_ns = time.perf_counter_ns() - start
        assert accepted and not scan.cancel()
        resume = time.perf_counter_ns()
        helper.resume_allocation()
        status = future.result()
        completion_ns = time.perf_counter_ns() - resume
    try:
        assert status == errno.ECANCELED and not out.release
        stats = scan.stats
        assert stats['rows_emitted'] == stats['batches_emitted'] == 0
        assert stats['buffer_capacity_bytes_emitted'] == stats['live_buffer_bytes'] == 0
        assert stats['cancelled_calls'] == 1
        if PHASE == 'gather':
            assert 0 < before < 10000
            assert before <= stats['rows_gathered'] <= before + 256
        else:
            assert before == stats['rows_gathered'] == 10000
        print(json.dumps({'phase': PHASE, 'kind': KIND, 'request_ns': request_ns,
                          'completion_after_resume_ns': completion_ns,
                          'gathered_at_request': before, 'gathered_after': stats['rows_gathered']}))
    finally:
        release_array(out)
db.close()
'''
    prefix = f"import sys\nsys.path.insert(0, {str(Path(__file__).parent)!r})\nKIND={kind!r}\nPHASE={phase!r}\nPATH={str(tmp_path / 'db')!r}\n"
    env = dict(os.environ, PYREX_CANCEL_LATCH=str(allocation_latch))
    env["LD_PRELOAD"] = ":".join(filter(None, [env.get("LD_PRELOAD"), str(allocation_latch)]))
    result = subprocess.run([sys.executable, "-c", prefix + code], env=env,
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr + result.stdout
    measured = json.loads(result.stdout)
    (tmp_path / "controlled-cancellation.json").write_text(json.dumps(measured, indent=2))
