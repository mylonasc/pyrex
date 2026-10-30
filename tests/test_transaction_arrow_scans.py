"""ABR-08: stable transaction-local Arrow views, not DB committed scans."""
import errno
import os
from pathlib import Path
import subprocess
import sys

import pytest
import pyrex

from test_arrow_scan_metrics import NativeStream, release_array
from test_arrow_scan_cancellation import allocation_latch

pa = pytest.importorskip("pyarrow")


def consume(scan):
    with pa.RecordBatchReader.from_stream(scan) as reader:
        return reader.read_all()


@pytest.mark.parametrize("cf_mode", [False, True])
@pytest.mark.parametrize("batch_size", [1, 3, 65536])
def test_own_writes_deletes_and_other_uncommitted_invisibility(tmp_path, cf_mode, batch_size):
    with pyrex.TransactionDB(str(tmp_path / "db")) as db:
        cf = db.create_column_family("local") if cf_mode else None
        put = (lambda k, v: db.put_cf(cf, k, v)) if cf else db.put
        put(b"a", b"committed")
        put(b"deleted", b"committed")
        txn = db.begin_transaction()
        other = db.begin_transaction()
        write = (lambda k, v: txn.put_cf(cf, k, v)) if cf else txn.put
        delete = (lambda k: txn.delete_cf(cf, k)) if cf else txn.delete
        write(b"a", b"private")
        write(b"", b"")
        write(b"\xff\x00", b"binary")
        delete(b"deleted")
        if cf:
            other.put_cf(cf, b"other-private", b"invisible")
            scan = txn.scan_arrow_cf(cf, batch_size=batch_size, collect_metrics=True)
        else:
            other.put(b"other-private", b"invisible")
            scan = txn.scan_arrow(batch_size=batch_size, collect_metrics=True)
        assert scan.configuration["source"] == "transaction_scan"
        result = consume(scan)
        assert result.to_pylist() == [dict(key=b"", value=b""), dict(key=b"a", value=b"private"),
                                      dict(key=b"\xff\x00", value=b"binary")]
        assert scan.stats["rows_emitted"] == 3 and scan.stats["value_bytes_copied_to_output"] == 0
        txn.commit()
        other.rollback()
        db.close()
        assert result.column("value").to_pylist() == [b"", b"private", b"binary"]


@pytest.mark.parametrize("action", ["put", "delete", "write", "set_snapshot", "commit", "rollback", "get_for_update"])
def test_mutation_rejected_until_all_native_scan_resources_are_released(tmp_path, action):
    with pyrex.TransactionDB(str(tmp_path / "db")) as db:
        txn = db.begin_transaction()
        txn.put(b"k", b"v")
        first, second = txn.scan_arrow(), txn.scan_arrow()
        batch = pyrex.PyWriteBatch()
        batch.put(b"x", b"y")
        args = {"put": (b"x", b"y"), "delete": (b"k",), "write": (batch,),
                "get_for_update": (b"k",)}.get(action, ())
        first.close()
        with pytest.raises(pyrex.RocksDBException, match="active Arrow scans"):
            getattr(txn, action)(*args)
        assert txn.get(b"k") == b"v"
        second.close()
        getattr(txn, action)(*args)
        if txn.is_active:
            txn.rollback()


def test_cancel_request_alone_does_not_unfreeze_transaction(tmp_path):
    with pyrex.TransactionDB(str(tmp_path / "db")) as db:
        txn = db.begin_transaction()
        scan = txn.scan_arrow()
        assert scan.cancel()
        with pytest.raises(pyrex.RocksDBException, match="active Arrow scans"):
            txn.commit()
        with NativeStream(scan) as raw:
            assert raw.next()[0] == errno.ECANCELED
            txn.commit()
            assert raw.next()[0] == errno.ECANCELED


@pytest.mark.parametrize("set_snapshot", [False, True])
def test_committed_horizon_is_stable_across_batches(tmp_path, set_snapshot):
    with pyrex.TransactionDB(str(tmp_path / "db")) as db:
        db.put(b"a", b"before")
        txn = db.begin_transaction()
        if set_snapshot:
            txn.set_snapshot()
        db.put(b"b", b"later-than-txn-snapshot")
        scan = txn.scan_arrow(batch_size=1)
        if set_snapshot:
            assert scan.sequence_number == txn.snapshot_sequence_number
        db.put(b"c", b"later-than-scan")
        expected = [dict(key=b"a", value=b"before")]
        if not set_snapshot:
            expected.append(dict(key=b"b", value=b"later-than-txn-snapshot"))
        assert consume(scan).to_pylist() == expected
        txn.rollback()


def test_raw_eof_then_commit_preserves_eof_and_retained_buffers(tmp_path):
    with pyrex.TransactionDB(str(tmp_path / "db")) as db:
        txn = db.begin_transaction()
        txn.put(b"k", b"v")
        scan = txn.scan_arrow(collect_metrics=True)
        with NativeStream(scan) as raw:
            status, retained = raw.next()
            assert status == 0 and retained.length == 1
            status, empty = raw.next()
            assert status == 0 and not empty.release
            txn.commit()
            assert raw.next()[0] == 0
            db.close()
            assert retained.length == 1
            release_array(retained)
        assert scan.stats["live_buffer_bytes"] == 0


def test_boundaries_empty_and_native_error_unfreeze(tmp_path):
    with pyrex.TransactionDB(str(tmp_path / "db")) as db:
        txn = db.begin_transaction()
        txn.put(b"a:1", b"v" * 64)
        txn.put(b"b:1", b"ignored")
        assert consume(txn.scan_arrow(prefix=b"a:", start=b"a:", stop=b"b:")).num_rows == 1
        assert consume(txn.scan_arrow(start=b"x", stop=b"x")).num_rows == 0
        scan = txn.scan_arrow(prefix=b"a:", max_batch_bytes=1)
        with NativeStream(scan) as raw:
            status, empty = raw.next()
            assert status != 0 and not empty.release
        txn.put(b"after-error", b"allowed")
        txn.rollback()


def test_cf_identity_and_mutation_policy(tmp_path):
    with pyrex.TransactionDB(str(tmp_path / "one")) as db, pyrex.TransactionDB(str(tmp_path / "two")) as other:
        cf = db.create_column_family("local")
        foreign = other.create_column_family("local")
        txn = db.begin_transaction()
        txn.put_cf(cf, b"k", b"v")
        with pytest.raises(pyrex.RocksDBException, match="different database"):
            txn.scan_arrow_cf(foreign)
        scan = txn.scan_arrow_cf(cf)
        for method, args in [(txn.put_cf, (cf, b"x", b"y")), (txn.delete_cf, (cf, b"k"))]:
            with pytest.raises(pyrex.RocksDBException, match="active Arrow scans"):
                method(*args)
        with pytest.raises(pyrex.RocksDBException):
            db.drop_column_family(cf)
        assert consume(scan).to_pylist() == [dict(key=b"k", value=b"v")]
        txn.rollback()
        db.drop_column_family(cf)


def test_transaction_capabilities_and_invalid_requests(tmp_path):
    with pyrex.TransactionDB(str(tmp_path / "db")) as db:
        txn = db.begin_transaction()
        caps = txn.arrow_capabilities()
        assert caps["snapshot_scope"] == "transaction_local_frozen_writes"
        for method in ("scan_arrow", "scan_arrow_cf", "multi_get_arrow", "scan_arrow_ranges"):
            assert caps[method] is hasattr(txn, method)
        for kwargs in [{"prefix": "text"}, {"start": b"z", "stop": b"a"},
                       {"batch_size": 0}, {"max_batch_bytes": 0}, {"snapshot": db.create_snapshot()}]:
            with pytest.raises((TypeError, ValueError)):
                txn.scan_arrow(**kwargs)
        txn.rollback()
        with pytest.raises(pyrex.RocksDBException, match="no longer active"):
            txn.scan_arrow()


@pytest.mark.parametrize("action", ["commit", "rollback", "close"])
def test_native_scan_racing_completion_is_bounded(allocation_latch, tmp_path, action):
    code = r'''
import ctypes as ct, errno, os, time
from concurrent.futures import ThreadPoolExecutor
import pyrex
from test_arrow_scan_metrics import ArrowArray, NativeStream, release_array
helper = ct.CDLL(os.environ['PYREX_CANCEL_LATCH'])
helper.paused_get_next.argtypes = [ct.c_void_p, ct.c_int, ct.c_size_t, ct.POINTER(ArrowArray)]
helper.paused_get_next.restype = ct.c_int
helper.allocation_paused.restype = ct.c_bool
db = pyrex.TransactionDB(PATH)
txn = db.begin_transaction()
for i in range(10000): txn.put(f'k{i:018d}'.encode(), b'v' * 64)
scan = txn.scan_arrow(collect_metrics=True)
with NativeStream(scan) as stream, ThreadPoolExecutor(max_workers=2) as pool:
    out = ArrowArray()
    pulling = pool.submit(helper.paused_get_next, stream.pointer, 700, 0, ct.byref(out))
    deadline = time.monotonic() + 10
    while not helper.allocation_paused():
        assert not pulling.done(), 'allocation latch not reached'
        assert time.monotonic() < deadline
        time.sleep(.001)
    def finish():
        try:
            (db.close if ACTION == 'close' else getattr(txn, ACTION))()
            return True
        except pyrex.RocksDBException:
            return False
    finishing = pool.submit(finish)
    assert scan.cancel()  # never waits for transaction/gather locks
    helper.resume_allocation()
    status = pulling.result()
    assert status == errno.ECANCELED and not out.release
    finishing.result()
    assert scan.stats['rows_emitted'] == scan.stats['live_buffer_bytes'] == 0
if txn.is_active: txn.rollback()
db.close()
'''
    prefix = (f"import sys\nsys.path.insert(0, {str(Path(__file__).parent)!r})\n"
              f"PATH={str(tmp_path / 'race')!r}\nACTION={action!r}\n")
    env = dict(os.environ, PYREX_CANCEL_LATCH=str(allocation_latch))
    env["LD_PRELOAD"] = ":".join(filter(None, [env.get("LD_PRELOAD"), str(allocation_latch)]))
    subprocess.run([sys.executable, "-c", prefix + code], env=env, check=True, timeout=30)


def test_db_close_before_export_invalidates_transaction_iterator(tmp_path):
    db = pyrex.TransactionDB(str(tmp_path / "db"))
    txn = db.begin_transaction()
    txn.put(b"k", b"v")
    scan = txn.scan_arrow()
    db.close()
    assert scan.is_closed and not txn.is_active
    scan.close()


def test_transaction_benchmark_smoke(tmp_path):
    import json
    script = Path(__file__).resolve().parents[1] / "benchmarks/bench_transaction_arrow.py"
    result = subprocess.run([sys.executable, str(script), "--rows", "100", "--repeat", "2",
                             "--work-dir", str(tmp_path)], capture_output=True, text=True, check=True, timeout=30)
    report = json.loads(result.stdout)
    assert len(report["ordered_digest"]) == 64
    assert all(len(samples) == 2 for samples in report["samples_ns"].values())
