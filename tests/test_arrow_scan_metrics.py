"""ABR-01: output accounting follows Arrow ownership, not stream lifetime."""
import ctypes as ct
import gc
import subprocess
import sys

import pytest
import pyrex


class ArrowArray(ct.Structure):
    pass


ArrowArray._fields_ = [
    ("length", ct.c_int64), ("null_count", ct.c_int64),
    ("offset", ct.c_int64), ("n_buffers", ct.c_int64),
    ("n_children", ct.c_int64), ("buffers", ct.POINTER(ct.c_void_p)),
    ("children", ct.POINTER(ct.POINTER(ArrowArray))),
    ("dictionary", ct.POINTER(ArrowArray)),
    ("release", ct.c_void_p), ("private_data", ct.c_void_p),
]


class ArrowStream(ct.Structure):
    _fields_ = [(name, ct.c_void_p) for name in
                ("get_schema", "get_next", "get_last_error", "release", "private_data")]


def release_array(array):
    if array.release:
        ct.CFUNCTYPE(None, ct.POINTER(ArrowArray))(array.release)(ct.byref(array))


class NativeStream:
    def __init__(self, scan):
        self.capsule = scan.__arrow_c_stream__()
        get_pointer = ct.pythonapi.PyCapsule_GetPointer
        get_pointer.argtypes = [ct.py_object, ct.c_char_p]
        get_pointer.restype = ct.c_void_p
        self.pointer = ct.cast(get_pointer(self.capsule, b"arrow_array_stream"), ct.POINTER(ArrowStream))

    def next(self):
        array = ArrowArray()
        callback = ct.CFUNCTYPE(ct.c_int, ct.POINTER(ArrowStream), ct.POINTER(ArrowArray))(
            self.pointer.contents.get_next)
        status = callback(self.pointer, ct.byref(array))
        return status, array

    def close(self):
        if self.pointer.contents.release:
            ct.CFUNCTYPE(None, ct.POINTER(ArrowStream))(
                self.pointer.contents.release)(self.pointer)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


@pytest.fixture(params=[pyrex.PyRocksDB, pyrex.PyRocksDBExtended, pyrex.TransactionDB])
def db(request, tmp_path):
    handle = request.param(str(tmp_path / "db"))
    yield handle
    handle.close()


def test_metrics_opt_in_and_snapshot_semantics(db):
    ordinary = db.scan_arrow()
    assert ordinary.stats is None
    ordinary.close()
    assert ordinary.stats is None
    scan = db.scan_arrow(collect_metrics=True)
    original = scan.stats
    assert original and all(value == 0 for value in original.values())
    original["rows_emitted"] = 999
    assert scan.stats["rows_emitted"] == 0
    with pytest.raises(AttributeError):
        scan.stats = {}
    with NativeStream(scan) as stream:
        status, batch = stream.next()
        assert status == 0 and not batch.release
        stats = scan.stats
        assert stats["get_next_calls"] == 1
        assert stats["rows_emitted"] == stats["batches_emitted"] == 0
        assert stats["output_bytes_emitted"] == stats["live_buffer_bytes"] == 0
        assert stats["build_time_ns"] == 0
    assert scan.stats["get_next_calls"] == 1


@pytest.mark.parametrize("batch_size", [1, 2, 10])
def test_exact_batch_output_bytes_and_capacities(db, batch_size):
    pairs = [(b"", b""), (b"a", b""), (b"b\x00\xff", b"value")]
    for key, value in pairs:
        db.put(key, value)
    scan = db.scan_arrow(batch_size=batch_size, collect_metrics=True)
    batches = rows = keys = values = offsets = capacity = max_capacity = 0
    with NativeStream(scan) as stream:
        while True:
            status, batch = stream.next()
            assert status == 0
            if not batch.release:
                break
            selected = pairs[rows:rows + batch.length]
            key_bytes = sum(len(k) for k, _ in selected)
            value_bytes = sum(len(v) for _, v in selected)
            used_offsets = 8 * (batch.length + 1)
            batch_capacity = max(1, key_bytes) + max(1, value_bytes) + used_offsets
            rows += batch.length
            batches += 1
            keys += key_bytes
            values += value_bytes
            offsets += used_offsets
            capacity += batch_capacity
            max_capacity = max(max_capacity, batch_capacity)
            stats = scan.stats
            assert stats["rows_emitted"] == rows
            assert stats["batches_emitted"] == batches
            assert stats["key_bytes_emitted"] == keys
            assert stats["value_bytes_emitted"] == values
            assert stats["offset_bytes_emitted"] == offsets
            assert stats["output_bytes_emitted"] == keys + values + offsets
            assert stats["buffer_capacity_bytes_emitted"] == capacity
            assert stats["allocation_slack_bytes_emitted"] == capacity - keys - values - offsets
            assert stats["live_buffer_bytes"] == batch_capacity
            assert stats["peak_live_buffer_bytes"] == max_capacity
            release_array(batch)
            assert scan.stats["live_buffer_bytes"] == 0
    assert scan.stats["get_next_calls"] == batches + 1
    assert rows == len(pairs)
    assert scan.stats["peak_staged_payload_bytes"] == max(
        sum(len(k) + len(v) for k, v in pairs[i:i + batch_size])
        for i in range(0, len(pairs), batch_size))


def test_buffer_lifetime_after_detaching_child_and_closing_db(db):
    db.put(b"k", b"vv")
    scan = db.scan_arrow(collect_metrics=True)
    with NativeStream(scan) as stream:
        status, batch = stream.next()
        assert status == 0 and batch.release
        assert scan.stats["live_buffer_bytes"] == 19  # 16 offsets + 3 payload
        child = batch.children[0]
        detached = ArrowArray()
        ct.memmove(ct.byref(detached), child, ct.sizeof(ArrowArray))
        child.contents.release = None  # Arrow C Data move semantics
        try:
            release_array(batch)  # frees only the value buffers
            assert scan.stats["live_buffer_bytes"] == 9
            scan.close()
            db.close()
            assert scan.stats["live_buffer_bytes"] == 9
            assert ct.string_at(detached.buffers[2], 1) == b"k"
        finally:
            release_array(detached)
            release_array(batch)  # already released: idempotent
        assert scan.stats["live_buffer_bytes"] == 0
        assert scan.stats["peak_live_buffer_bytes"] == 19


@pytest.mark.parametrize("overflow", [False, True])
def test_oversized_record_has_no_partial_output(db, overflow):
    db.put(b"k", b"x" * 10)
    scan = db.scan_arrow(max_batch_bytes=18, allow_overflow=overflow, collect_metrics=True)
    with NativeStream(scan) as stream:
        status, batch = stream.next()
        try:
            assert (status == 0) is overflow
            assert bool(batch.release) is overflow
            stats = scan.stats
            assert stats["rows_emitted"] == stats["batches_emitted"] == int(overflow)
            assert stats["output_bytes_emitted"] == (27 if overflow else 0)
            assert stats["live_buffer_bytes"] == (27 if overflow else 0)
        finally:
            release_array(batch)
    assert scan.stats["live_buffer_bytes"] == 0


def test_byte_limit_prefix_bounds_and_snapshot(db):
    for key in (b"p/a", b"p/b", b"p/c", b"q/a"):
        db.put(key, b"x")
    snapshot = db.create_snapshot()
    scan = db.scan_arrow(prefix=b"p/", start=b"p/b", stop=b"p/d", batch_size=100,
                         max_batch_bytes=20, snapshot=snapshot, collect_metrics=True)
    snapshot.release()
    with NativeStream(scan) as stream:
        for _ in range(2):
            status, batch = stream.next()
            assert status == 0 and batch.length == 1
            release_array(batch)
        status, batch = stream.next()
        assert status == 0 and not batch.release
    stats = scan.stats
    assert stats["rows_emitted"] == stats["batches_emitted"] == 2
    assert stats["output_bytes_emitted"] == stats["buffer_capacity_bytes_emitted"] == 40


@pytest.mark.parametrize("kind", [pyrex.PyRocksDBExtended, pyrex.TransactionDB])
def test_cf_metrics(kind, tmp_path):
    with kind(str(tmp_path / "cf")) as db:
        cf = db.create_column_family("metrics")
        db.put_cf(cf, b"cf", b"v")
        scan = db.scan_arrow_cf(cf, collect_metrics=True)
        with NativeStream(scan) as stream:
            status, batch = stream.next()
            assert status == 0 and batch.length == 1
            assert scan.stats["output_bytes_emitted"] == 19
            release_array(batch)
        db.drop_column_family(cf)
        assert scan.stats["rows_emitted"] == 1


def test_pyarrow_retains_output_after_reader_and_db_close(db):
    pa = pytest.importorskip("pyarrow")
    db.put(b"a", b"v")
    db.put(b"b", b"w")
    scan = db.scan_arrow(batch_size=1, collect_metrics=True)
    reader = pa.RecordBatchReader.from_stream(scan)
    table = reader.read_all()
    reader.close()
    db.close()
    assert table.to_pydict() == {"key": [b"a", b"b"], "value": [b"v", b"w"]}
    assert scan.stats["live_buffer_bytes"] == 36
    assert scan.stats["rows_emitted"] == 2
    del table
    gc.collect()
    assert scan.stats["live_buffer_bytes"] == 0


def test_owned_batch_can_outlive_scan_object_and_db(db):
    db.put(b"k", b"v")
    scan = db.scan_arrow(collect_metrics=True)
    stream = NativeStream(scan)
    status, batch = stream.next()
    assert status == 0 and batch.release
    stream.close()
    db.close()
    del scan, stream
    gc.collect()
    try:
        assert ct.string_at(batch.children[0].contents.buffers[2], 1) == b"k"
        assert ct.string_at(batch.children[1].contents.buffers[2], 1) == b"v"
    finally:
        release_array(batch)


def test_read_after_close_does_not_count_failed_output(db):
    db.put(b"k", b"v")
    scan = db.scan_arrow(collect_metrics=True)
    with NativeStream(scan) as stream:
        db.close()
        status, batch = stream.next()
        assert status != 0 and not batch.release
        stats = scan.stats
        assert stats["get_next_calls"] == 1
        assert stats["rows_emitted"] == stats["batches_emitted"] == 0
        assert stats["live_buffer_bytes"] == stats["buffer_capacity_bytes_emitted"] == 0


def test_concurrent_stats_reads_release_and_close(tmp_path):
    pytest.importorskip("pyarrow")
    code = r'''
import gc, threading, pyrex, pyarrow as pa
from concurrent.futures import ThreadPoolExecutor
db = pyrex.PyRocksDB(PATH)
for i in range(500): db.put(str(i).encode(), b'x' * 100)
scan = db.scan_arrow(batch_size=3, collect_metrics=True)
start = threading.Barrier(2)
def consume():
    start.wait()
    reader = pa.RecordBatchReader.from_stream(scan)
    for batch in reader:
        assert batch.num_rows > 0
    del batch
    reader.close()
def inspect():
    start.wait()
    previous = 0
    for _ in range(3000):
        s = scan.stats
        assert s['rows_emitted'] >= previous
        previous = s['rows_emitted']
        assert s['output_bytes_emitted'] == s['key_bytes_emitted'] + s['value_bytes_emitted'] + s['offset_bytes_emitted']
        assert s['buffer_capacity_bytes_emitted'] == s['output_bytes_emitted'] + s['allocation_slack_bytes_emitted']
        assert s['live_buffer_bytes'] <= s['peak_live_buffer_bytes'] <= s['buffer_capacity_bytes_emitted']
with ThreadPoolExecutor(max_workers=2) as pool:
    jobs = [pool.submit(consume), pool.submit(inspect)]
    for job in jobs: job.result()
gc.collect()
db.close()
assert scan.stats['rows_emitted'] == 500
assert scan.stats['live_buffer_bytes'] == 0
'''
    prefix = f"PATH={str(tmp_path / 'concurrent')!r}\n"
    subprocess.run([sys.executable, "-c", prefix + code], check=True, timeout=30)
