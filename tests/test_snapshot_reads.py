"""One storage horizon from index scan through fact materialization."""
import gc
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
import pyrex

pa = pytest.importorskip("pyarrow")


@pytest.fixture(params=[pyrex.PyRocksDB, pyrex.PyRocksDBExtended, pyrex.TransactionDB])
def db(request, tmp_path):
    handle = request.param(str(tmp_path / "db"))
    yield handle
    handle.close()


def records(scan):
    return pa.RecordBatchReader.from_stream(scan).read_all().to_pylist()


def test_scan_and_point_materialization_share_horizon(db):
    db.put(b"index/a", b"version/1")
    db.put(b"version/1", b"old")
    snap = db.create_snapshot()
    scan = db.scan_arrow(prefix=b"index/", snapshot=snap)
    db.put(b"version/1", b"new")
    db.put(b"index/b", b"version/2")
    db.put(b"version/2", b"later")
    locators = [row["value"] for row in records(scan)]
    assert locators == [b"version/1"]
    assert db.get(locators[0], snapshot=snap) == b"old"
    assert db.multi_get(locators + [b"missing", b"version/2", locators[0]], snapshot=snap) == [b"old", None, None, b"old"]
    assert db.get(b"version/1") == b"new"
    assert db.multi_get([b"version/2"]) == [b"later"]
    snap.release()


def test_binary_empty_duplicate_missing_and_read_options(db):
    db.put(b"", b"")
    db.put(b"\x00\xff", b"value")
    opts = pyrex.ReadOptions()
    opts.fill_cache = False
    opts.verify_checksums = True
    snap = db.create_snapshot()
    assert db.get(b"", opts, snap) == b""
    assert db.multi_get([b"", b"\x00\xff", b"", b"missing"], opts, snap) == [b"", b"value", b"", None]
    assert db.multi_get([], snapshot=snap) == []
    with pytest.raises(TypeError):
        db.multi_get([b"valid", "not-bytes"])
    with pytest.raises(ValueError):
        db.multi_get([b"x"] * 65537)
    snap.release()


def test_released_foreign_and_closed_snapshots_fail(db, tmp_path):
    other = pyrex.PyRocksDB(str(tmp_path / "other"))
    foreign = other.create_snapshot()
    try:
        for read in (lambda s: db.get(b"k", snapshot=s),
                     lambda s: db.multi_get([], snapshot=s),
                     lambda s: db.multi_get([b"k"], snapshot=s)):
            with pytest.raises(pyrex.RocksDBException, match="different database"):
                read(foreign)
        own = db.create_snapshot()
        own.release()
        with pytest.raises(pyrex.RocksDBException, match="released"):
            db.multi_get([b"k"], snapshot=own)
        snap = db.create_snapshot()
        db.close()
        assert not snap.is_valid()
        with pytest.raises(pyrex.RocksDBException):
            db.get(b"k", snapshot=snap)
    finally:
        foreign.release()
        other.close()


@pytest.mark.parametrize("kind", ["PyRocksDB", "PyRocksDBExtended", "TransactionDB"])
def test_temporary_snapshot_no_deadlock(kind, tmp_path):
    # A subprocess timeout makes a lock-order regression fail, not hang CI.
    code = f"""
import pyrex, pyarrow as pa, gc
db = pyrex.{kind}({str(tmp_path / kind)!r})
db.put(b'a', b'1')
for mode in ('close', 'exhaust', 'db-close'):
    scan = db.scan_arrow(snapshot=db.create_snapshot())
    if mode == 'close': scan.close()
    elif mode == 'exhaust': pa.RecordBatchReader.from_stream(scan).read_all()
    else: db.close()
    del scan
    gc.collect()
db.close()
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=20)


def test_concurrent_scan_creation_release_and_close(db):
    db.put(b"k", b"v")
    def scan_worker():
        for _ in range(50):
            try:
                snap = db.create_snapshot()
                scan = db.scan_arrow(snapshot=snap, batch_size=1)
                db.multi_get([b"k"], snapshot=snap)
                snap.release()
                scan.close()
            except pyrex.RocksDBException:
                return
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(scan_worker) for _ in range(3)]
        futures.append(pool.submit(db.close))
        for future in futures:
            future.result(timeout=15)
    gc.collect()


def test_real_output_byte_cap_and_oversized_policy(db):
    db.put(b"a", b"b")
    with pytest.raises(OSError, match="max_batch_bytes"):
        records(db.scan_arrow(max_batch_bytes=10))
    for cap in (18, 19, 64):
        reader = pa.RecordBatchReader.from_stream(db.scan_arrow(max_batch_bytes=cap))
        for batch in reader:
            assert sum(buf.size for col in batch.columns for buf in col.buffers() if buf is not None) <= cap
    assert records(db.scan_arrow(max_batch_bytes=1, allow_overflow=True)) == [{"key": b"a", "value": b"b"}]


@pytest.mark.parametrize("kind", [pyrex.PyRocksDBExtended, pyrex.TransactionDB])
def test_reader_release_releases_cf_resources(kind, tmp_path):
    with kind(str(tmp_path / "db")) as handle:
        cf = handle.create_column_family("facts")
        handle.put_cf(cf, b"k", b"v")
        scan = handle.scan_arrow_cf(cf)
        reader = pa.RecordBatchReader.from_stream(scan)
        reader.close()
        assert scan.is_closed
        handle.drop_column_family(cf)
        assert not cf.is_valid()


@pytest.mark.parametrize("kind", [pyrex.PyRocksDBExtended, pyrex.TransactionDB])
def test_cross_cf_snapshot_and_transactional_reopen(kind, tmp_path):
    path = str(tmp_path / "db")
    with kind(path) as handle:
        index = handle.create_column_family("index")
        facts = handle.create_column_family("facts")
        handle.put_cf(index, b"p/a", b"v1")
        handle.put_cf(facts, b"v1", b"old")
        snap = handle.create_snapshot()
        handle.put_cf(facts, b"v1", b"new")
        handle.put_cf(index, b"p/b", b"v2")
        rows = records(handle.scan_arrow_cf(index, prefix=b"p/", snapshot=snap))
        keys = [row["value"] for row in rows]
        assert keys == [b"v1"]
        assert handle.get_cf(facts, b"v1", snapshot=snap) == b"old"
        assert handle.multi_get_cf(facts, keys + [b"missing"], snapshot=snap) == [b"old", None]
        snap.release()
    with kind(path) as reopened:
        assert set(reopened.list_column_families()) == {"default", "index", "facts"}
        facts = reopened.get_column_family("facts")
        assert reopened.multi_get_cf(facts, [b"v1"]) == [b"new"]
        reopened.delete_cf(facts, b"v1")
        assert reopened.get_cf(facts, b"v1") is None


def test_atomic_cf_transactions_and_read_your_writes(tmp_path):
    with pyrex.TransactionDB(str(tmp_path / "db")) as handle:
        index = handle.create_column_family("index")
        facts = handle.create_column_family("facts")
        before = handle.create_snapshot()
        with handle.transaction() as txn:
            txn.put_cf(index, b"p/a", b"v1")
            txn.put_cf(facts, b"v1", b"value")
            assert txn.get_cf(facts, b"v1") == b"value"
            assert records(handle.scan_arrow_cf(index)) == []
            with pytest.raises(pyrex.RocksDBException, match="transactions"):
                handle.drop_column_family(facts)
            txn.commit()
        assert handle.get_cf(facts, b"v1", snapshot=before) is None
        assert records(handle.scan_arrow_cf(index, snapshot=before)) == []
        after = handle.create_snapshot()
        assert records(handle.scan_arrow_cf(index, snapshot=after)) == [{"key": b"p/a", "value": b"v1"}]
        assert handle.multi_get_cf(facts, [b"v1"], snapshot=after) == [b"value"]
        with handle.transaction() as txn:
            txn.delete_cf(facts, b"v1")
            assert txn.get_cf(facts, b"v1") is None
            # automatic rollback
        assert handle.get_cf(facts, b"v1") == b"value"
        batch = pyrex.PyWriteBatch()
        batch.put_cf(facts, b"v2", b"batch")
        batch.delete_cf(index, b"p/a")
        with handle.transaction() as txn:
            txn.write(batch)
            txn.commit()
        assert handle.get_cf(facts, b"v2") == b"batch"
        assert handle.get_cf(index, b"p/a") is None
        before.release()
        after.release()


def test_transaction_snapshot_applies_to_default_and_cf_reads(tmp_path):
    with pyrex.TransactionDB(str(tmp_path / "db")) as handle:
        cf = handle.create_column_family("facts")
        handle.put(b"k", b"old")
        handle.put_cf(cf, b"k", b"old")
        opts = pyrex.TransactionOptions()
        opts.set_snapshot = True
        with handle.transaction(transaction_options=opts) as txn:
            handle.put(b"k", b"new")
            handle.put_cf(cf, b"k", b"new")
            assert txn.get(b"k") == b"old"
            assert txn.get_cf(cf, b"k") == b"old"


@pytest.mark.parametrize("kind", [pyrex.PyRocksDBExtended, pyrex.TransactionDB])
def test_foreign_cf_identity_checked(kind, tmp_path):
    with kind(str(tmp_path / "a")) as a, kind(str(tmp_path / "b")) as b:
        a.create_column_family("facts")
        foreign = b.create_column_family("facts")
        operations = [lambda: a.scan_arrow_cf(foreign), lambda: a.get_cf(foreign, b"k"),
                      lambda: a.multi_get_cf(foreign, []), lambda: a.put_cf(foreign, b"k", b"v"),
                      lambda: a.delete_cf(foreign, b"k"), lambda: a.drop_column_family(foreign)]
        for operation in operations:
            with pytest.raises(pyrex.RocksDBException, match="different database"):
                operation()
        batch = pyrex.PyWriteBatch()
        batch.put_cf(foreign, b"k", b"v")
        with pytest.raises(pyrex.RocksDBException, match="different database"):
            a.write(batch)
        if kind is pyrex.TransactionDB:
            with a.transaction() as txn:
                with pytest.raises(pyrex.RocksDBException, match="different database"):
                    txn.write(batch)
                for operation in (lambda: txn.get_cf(foreign, b"k"), lambda: txn.put_cf(foreign, b"k", b"v"), lambda: txn.delete_cf(foreign, b"k")):
                    with pytest.raises(pyrex.RocksDBException, match="different database"):
                        operation()


@pytest.mark.parametrize("use_cf", [False, True])
def test_waiting_writer_does_not_prevent_other_transaction_commit(use_cf, tmp_path):
    # A lifecycle mutex held across RocksDB key-lock waits would make the
    # owner unable to commit and force the waiting writer to time out.
    with pyrex.TransactionDB(str(tmp_path / "db")) as handle:
        cf = handle.create_column_family("facts") if use_cf else None
        owner = handle.begin_transaction()
        waiter = handle.begin_transaction()
        if use_cf: owner.put_cf(cf, b"k", b"owner")
        else: owner.put(b"k", b"owner")
        started = threading.Event()
        def write():
            started.set()
            if use_cf: waiter.put_cf(cf, b"k", b"waiter")
            else: waiter.put(b"k", b"waiter")
            waiter.commit()
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(write)
            assert started.wait(5)
            time.sleep(0.05)
            owner.commit()
            pending.result(timeout=5)
        assert (handle.get_cf(cf, b"k") if use_cf else handle.get(b"k")) == b"waiter"


def test_cf_batches_outlive_reader_and_database(tmp_path):
    handle = pyrex.TransactionDB(str(tmp_path / "db"))
    cf = handle.create_column_family("facts")
    handle.put_cf(cf, b"\x00key", b"value")
    scan = handle.scan_arrow_cf(cf)
    reader = pa.RecordBatchReader.from_stream(scan)
    batch = reader.read_next_batch()
    reader.close()
    handle.close()
    del scan, reader, handle
    gc.collect()
    assert batch.to_pylist() == [{"key": b"\x00key", "value": b"value"}]


def test_cf_point_read_snapshot_rejection(tmp_path):
    with pyrex.TransactionDB(str(tmp_path / "a")) as a, pyrex.TransactionDB(str(tmp_path / "b")) as b:
        cf = a.create_column_family("facts")
        foreign = b.create_snapshot()
        for read in (lambda s: a.get_cf(cf, b"k", snapshot=s),
                     lambda s: a.multi_get_cf(cf, [], snapshot=s)):
            with pytest.raises(pyrex.RocksDBException, match="different database"):
                read(foreign)
        own = a.create_snapshot()
        own.release()
        with pytest.raises(pyrex.RocksDBException, match="released"):
            a.get_cf(cf, b"k", snapshot=own)
        foreign.release()


def test_transactional_cf_polars_consumer(tmp_path):
    pl = pytest.importorskip("polars")
    with pyrex.TransactionDB(str(tmp_path / "db")) as handle:
        cf = handle.create_column_family("facts")
        handle.put_cf(cf, b"a", b"1")
        handle.put_cf(cf, b"b", b"2")
        scan = handle.scan_arrow_cf(cf, batch_size=1)
        frame = pl.from_arrow(pa.RecordBatchReader.from_stream(scan))
        assert frame.schema == {"key": pl.Binary, "value": pl.Binary}
        assert frame.rows() == [(b"a", b"1"), (b"b", b"2")]
