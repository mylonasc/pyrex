"""ABR-04: ordered, nullable, owned Arrow point materialization."""
import ctypes as ct
import errno
import gc
from concurrent.futures import ThreadPoolExecutor

import pytest
import pyrex

from test_arrow_scan_metrics import NativeStream, release_array
from test_arrow_scan_cancellation import last_error

pa = pytest.importorskip("pyarrow")


@pytest.fixture(params=[pyrex.PyRocksDB, pyrex.PyRocksDBExtended, pyrex.TransactionDB])
def db(request, tmp_path):
    with request.param(str(tmp_path / "db")) as handle:
        yield handle


def table(stream):
    with pa.RecordBatchReader.from_stream(stream) as reader:
        return reader.read_all()


@pytest.mark.parametrize("input_type", ["list", "tuple", "binary", "large_binary"])
@pytest.mark.parametrize("batch_size", [1, 3, 256])
def test_order_duplicates_missing_and_empty(db, input_type, batch_size):
    db.put(b"", b"")
    db.put(b"a\x00", b"one")
    db.put(b"\xff", b"two")
    keys = [b"\xff", b"missing", b"", b"a\x00", b"\xff", b"missing", b""]
    input_keys = keys if input_type == "list" else tuple(keys) if input_type == "tuple" else pa.array(
        [b"ignored"] + keys + [b"ignored"], type=getattr(pa, input_type)()).slice(1, len(keys))
    scan = db.multi_get_arrow(input_keys, batch_size=batch_size, collect_metrics=True)
    result = table(scan)
    assert result.schema == pa.schema([pa.field("key", pa.binary(), nullable=False),
                                       pa.field("value", pa.binary(), nullable=True)])
    assert result.column("key").to_pylist() == keys
    assert result.column("value").to_pylist() == db.multi_get(keys)
    stats = scan.stats
    assert stats["rows_gathered"] == stats["rows_emitted"] == len(keys)
    assert stats["point_lookup_calls"] > 0
    assert stats["validity_bytes_emitted"] == sum((len(chunk) + 7) // 8 for chunk in result.column("key").chunks)
    assert stats["output_bytes_emitted"] == (stats["key_bytes_emitted"] + stats["value_bytes_emitted"]
                                           + stats["offset_bytes_emitted"] + stats["validity_bytes_emitted"])
    assert stats["live_buffer_bytes"] > 0
    del result
    gc.collect()
    assert scan.stats["live_buffer_bytes"] == 0


@pytest.mark.parametrize("keys", [[], (), pa.array([], type=pa.binary())])
def test_empty_still_has_nullable_schema_and_no_native_calls(db, keys):
    scan = db.multi_get_arrow(keys, collect_metrics=True)
    assert table(scan).num_rows == 0
    assert scan.stats["point_lookup_calls"] == 0
    assert not scan.cancel()


@pytest.mark.parametrize("keys", [[None], ["text"], pa.array([None], type=pa.binary()),
                                  pa.array(["text"]), pa.array([1]), [b"x"] * 65537])
def test_invalid_inputs_fail_before_export(db, keys):
    with pytest.raises((TypeError, ValueError)):
        db.multi_get_arrow(keys)


@pytest.mark.parametrize("kwargs", [{"batch_size": 0}, {"max_batch_bytes": 0},
                                   {"batch_size": 16777217}, {"max_batch_bytes": 4294967297}])
def test_invalid_limits(db, kwargs):
    with pytest.raises(ValueError):
        db.multi_get_arrow([], **kwargs)


def test_pinned_at_creation_and_shared_snapshot_release(db):
    db.put(b"k", b"before")
    snapshot = db.create_snapshot()
    first = db.multi_get_arrow([b"k"] * 513, batch_size=1, snapshot=snapshot)
    second = db.multi_get_arrow([b"k"], snapshot=snapshot)
    own = db.multi_get_arrow([b"k"])
    db.put(b"k", b"after")
    snapshot.release()
    assert not snapshot.is_valid()
    with pytest.raises(pyrex.RocksDBException, match="released"):
        db.multi_get_arrow([b"k"], snapshot=snapshot)
    assert table(first).column("value").to_pylist() == [b"before"] * 513
    assert table(second).column("value").to_pylist() == [b"before"]
    assert table(own).column("value").to_pylist() == [b"before"]
    assert db.get(b"k") == b"after"


def test_foreign_snapshot_and_closed_db(db, tmp_path):
    with pyrex.PyRocksDB(str(tmp_path / "other")) as other:
        snapshot = other.create_snapshot()
        with pytest.raises(pyrex.RocksDBException, match="different database"):
            db.multi_get_arrow([], snapshot=snapshot)
    db.close()
    with pytest.raises(pyrex.RocksDBException, match="closed"):
        db.multi_get_arrow([])


def test_byte_boundaries_and_singleton_overflow(db):
    db.put(b"k", b"v" * 20)
    keys = [b"k", b"missing", b"k"] * 100
    result = table(db.multi_get_arrow(keys, max_batch_bytes=38))
    assert result.num_rows == len(keys)
    assert all(len(chunk) == 1 for chunk in result.column("key").chunks)
    scan = db.multi_get_arrow([b"k"], max_batch_bytes=37)
    with NativeStream(scan) as raw:
        status, out = raw.next()
        assert status != 0 and not out.release
        assert b"max_batch_bytes" in last_error(raw)
        assert not scan.cancel()
    assert table(db.multi_get_arrow([b"k"], max_batch_bytes=1, allow_overflow=True)).num_rows == 1


def test_input_and_db_lifetimes_and_detached_child(db):
    db.put(b"k", b"v")
    keys = pa.array([b"k", b"missing", b""], type=pa.binary())
    scan = db.multi_get_arrow(keys, collect_metrics=True)
    del keys
    gc.collect()
    with NativeStream(scan) as raw:
        status, batch = raw.next()
        assert status == 0 and batch.length == 3
        child = type(batch).from_buffer_copy(batch.children[1].contents)
        batch.children[1].contents.release = 0
        release_array(batch)
        assert scan.stats["live_buffer_bytes"] > 0
        db.close()
        assert ct.string_at(child.buffers[2], 1) == b"v"
        assert ct.string_at(child.buffers[0], 1) == b"\x01"
        release_array(child)
        assert scan.stats["live_buffer_bytes"] == 0


def test_cancel_before_export_and_between_batches(db):
    db.put(b"k", b"v")
    pre = db.multi_get_arrow([b"k"], collect_metrics=True)
    assert pre.cancel()
    with NativeStream(pre) as raw:
        assert raw.next()[0] == errno.ECANCELED
    assert pre.stats["point_lookup_calls"] == 0
    scan = db.multi_get_arrow([b"k"] * 1000, batch_size=2, collect_metrics=True)
    with NativeStream(scan) as raw:
        status, batch = raw.next()
        assert status == 0 and batch.length == 2
        assert scan.cancel()
        for _ in range(2):
            status, empty = raw.next()
            assert status == errno.ECANCELED and not empty.release
            assert last_error(raw) == b"scan_arrow: cancelled"
        assert scan.stats["rows_emitted"] == 2
        assert ct.string_at(batch.children[1].contents.buffers[2], 2) == b"vv"
        release_array(batch)


@pytest.mark.parametrize("kind", [pyrex.PyRocksDBExtended, pyrex.TransactionDB])
def test_cf_identity_drop_and_snapshot(kind, tmp_path):
    with kind(str(tmp_path / "one")) as db, kind(str(tmp_path / "two")) as other:
        cf = db.create_column_family("points")
        foreign = other.create_column_family("points")
        db.put_cf(cf, b"k", b"cf")
        db.put(b"k", b"default")
        snapshot = db.create_snapshot()
        scan = db.multi_get_arrow_cf(cf, [b"k", b"absent"], snapshot=snapshot)
        with pytest.raises(pyrex.RocksDBException, match="different database"):
            db.multi_get_arrow_cf(foreign, [])
        with pytest.raises(pyrex.RocksDBException, match="active Arrow scan"):
            db.drop_column_family(cf)
        db.put_cf(cf, b"k", b"later")
        result = table(scan)
        db.drop_column_family(cf)
        assert result.column("value").to_pylist() == [b"cf", None]
        with pytest.raises(pyrex.RocksDBException):
            db.multi_get_arrow_cf(cf, [])


def test_transaction_db_points_are_committed_only(tmp_path):
    with pyrex.TransactionDB(str(tmp_path / "txn")) as db:
        db.put(b"k", b"committed")
        txn = db.begin_transaction()
        txn.put(b"k", b"private")
        assert table(db.multi_get_arrow([b"k"])).column("value").to_pylist() == [b"committed"]
        txn.rollback()


def test_point_pull_close_races_are_bounded(tmp_path):
    # Subprocess deadline bounds deadlock regressions; no concurrent pulls.
    import subprocess
    import sys
    code = r'''
import pyrex, pyarrow as pa
from concurrent.futures import ThreadPoolExecutor
for i in range(20):
    db = pyrex.PyRocksDB(PATH + str(i))
    db.put(b'k', b'v')
    snapshot = db.create_snapshot()
    scan = db.multi_get_arrow([b'k'] * 1000, batch_size=16, snapshot=snapshot)
    snapshot.release()
    reader = pa.RecordBatchReader.from_stream(scan)
    def pull():
        try:
            result = reader.read_all()
            assert result.num_rows == 1000
        except (OSError, pa.ArrowException):
            pass
        finally:
            reader.close()
    with ThreadPoolExecutor(max_workers=2) as pool:
        a = pool.submit(pull)
        b = pool.submit(db.close)
        a.result(); b.result()
'''
    subprocess.run([sys.executable, "-c", f"PATH={str(tmp_path / 'race')!r}\n" + code],
                   check=True, timeout=30)
