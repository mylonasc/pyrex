"""Point-read fast path vs preserved safe path (issue #36).

The ordinary point read (default column family, default read options, no
explicit snapshot) is dispatched to a fast path that borrows the key buffer
and holds the lifecycle mutex in shared mode. Every other read keeps the
previous safe implementation. These tests pin the contract:

- fast and safe reads agree on hits, misses, empty/binary keys and values;
- explicit read options and explicit snapshots still observe their horizon;
- concurrent readers return correct values without errors;
- closed-DB reads fail on both paths;
- invalid column-family handles are still rejected.
"""
import threading
import subprocess
import sys

import pytest
import pyrex


@pytest.fixture(params=[pyrex.PyRocksDB, pyrex.PyRocksDBExtended, pyrex.TransactionDB])
def db(request, tmp_path):
    handle = request.param(str(tmp_path / "db"))
    yield handle
    handle.close()


def seed(db):
    pairs = {
        b"alpha": b"1",
        b"": b"empty-key",
        b"\x00\xffbinary": b"\x00\xffvalue",
        b"large": b"x" * 100000,
    }
    for k, v in pairs.items():
        db.put(k, v)
    return pairs


def test_fast_and_safe_paths_agree(db):
    pairs = seed(db)
    opts = pyrex.ReadOptions()
    for key, value in pairs.items():
        assert db.get(key) == value  # fast path
        assert db.get(key, opts) == value  # safe path: explicit options
        assert db.get(key, snapshot=None) == value  # fast path: explicit None
    assert db.get(b"missing") is None
    assert db.get(b"missing", opts) is None


def test_explicit_snapshot_still_pinned(db):
    db.put(b"k", b"old")
    snap = db.create_snapshot()
    db.put(b"k", b"new")
    try:
        assert db.get(b"k") == b"new"  # fast path observes latest
        assert db.get(b"k", snapshot=snap) == b"old"  # safe path pinned
    finally:
        snap.release()
    assert db.get(b"k") == b"new"


def test_concurrent_fast_readers(db):
    expected = seed(db)
    expected[b"missing"] = None
    keys = [b"alpha", b"", b"\x00\xffbinary", b"large", b"missing"]
    errors = []

    def reader():
        try:
            for _ in range(200):
                for k in keys:
                    assert db.get(k) == expected[k]
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=reader) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors


def test_reads_after_close_fail(db):
    db.put(b"k", b"v")
    db.close()
    with pytest.raises(pyrex.RocksDBException):
        db.get(b"k")
    with pytest.raises(pyrex.RocksDBException):
        db.get(b"k", pyrex.ReadOptions())


def test_invalid_cf_handle_rejected(tmp_path):
    first = pyrex.PyRocksDBExtended(str(tmp_path / "a"))
    second = pyrex.PyRocksDBExtended(str(tmp_path / "b"))
    try:
        first.put(b"k", b"v")
        foreign = second.default_cf
        with pytest.raises(pyrex.RocksDBException, match="different database"):
            first.get_cf(foreign, b"k")
    finally:
        first.close()
        second.close()


def test_binding_dispatch_compatibility(db):
    db.put(b"k", b"v")
    # Specialized/general paths and positional/keyword/None call shapes.
    assert db.get(key=b"k") == b"v"
    assert db.get(b"k", None) == b"v"
    assert db.get(b"k", None, None) == b"v"
    assert db.get(key=b"k", read_options=None, snapshot=None) == b"v"
    for fill_cache in (False, True):
        for verify_checksums in (False, True):
            opts = pyrex.ReadOptions()
            opts.fill_cache = fill_cache
            opts.verify_checksums = verify_checksums
            assert opts.fill_cache is fill_cache
            assert opts.verify_checksums is verify_checksums
            db.default_read_options = opts
            assert db.default_read_options is opts
            assert db.get(b"k") == db.get(b"k", opts) == b"v"
    with pytest.raises(TypeError):
        db.get(b"k", unknown_option=True)
    with pytest.raises(TypeError):
        db.get(b"k", read_options=object())
    with pytest.raises(TypeError):
        db.get(b"k", snapshot=object())


@pytest.mark.parametrize("kind", ["PyRocksDB", "PyRocksDBExtended", "TransactionDB"])
@pytest.mark.parametrize("race", ["close", "options", "snapshot", "drop", "writer"])
def test_read_lifetime_races(kind, race, tmp_path):
    if race == "drop" and kind == "PyRocksDB":
        pytest.skip("plain DB has no CF API")
    # Native lifetime bugs can crash or deadlock the interpreter. Bound the
    # entire child process, rather than relying on Future.result's timeout
    # (executor shutdown would still wait for a deadlocked worker).
    code = r'''
import threading
from concurrent.futures import ThreadPoolExecutor
import pyrex
db = getattr(pyrex, KIND)(PATH)
value = b'v' * 4096
db.put(b'key', value)
options = pyrex.ReadOptions()
db.default_read_options = options
snapshot = db.create_snapshot()
cf = db.create_column_family('race') if RACE == 'drop' else None
if cf is not None:
    db.put_cf(cf, b'key', value)
start = threading.Barrier(4)
ready = threading.Event()

def reader():
    start.wait()
    for _ in range(2000):
        # Always exercise the specialized ordinary read, even when the race
        # also needs an explicit-snapshot/CF/options read.
        try:
            assert db.get(b'key') in (value, b'new')
        except pyrex.RocksDBException as exc:
            assert RACE == 'close' and 'closed' in str(exc)
            return
        ready.set()
        try:
            if RACE == 'snapshot':
                assert db.get(b'key', snapshot=snapshot) == value
            elif RACE == 'drop':
                assert db.get_cf(cf, b'key') == value
            elif RACE == 'options':
                assert db.get(b'key', options) == value
                assert db.multi_get([b'key'], options) == [value]
        except pyrex.RocksDBException as exc:
            assert ((RACE == 'snapshot' and 'released' in str(exc)) or
                    (RACE == 'drop' and 'invalid' in str(exc)))
            return

def mutate():
    start.wait()
    assert ready.wait(5)
    if RACE == 'close':
        db.close()
    elif RACE == 'snapshot':
        snapshot.release()
    elif RACE == 'drop':
        db.drop_column_family(cf)
    elif RACE == 'options':
        for i in range(4000):
            options.fill_cache = bool(i & 1)
            options.verify_checksums = bool(i & 2)
            db.default_read_options = options if i & 1 else pyrex.ReadOptions()
            assert isinstance(db.default_read_options, pyrex.ReadOptions)
    else:
        for _ in range(1000):
            db.put(b'key', b'new')
            db.put(b'key', value)

try:
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(reader) for _ in range(3)] + [pool.submit(mutate)]
        for future in futures:
            future.result()
finally:
    snapshot.release()
    db.close()
'''
    preamble = f"KIND={kind!r}\nRACE={race!r}\nPATH={str(tmp_path / 'race-db')!r}\n"
    subprocess.run([sys.executable, "-c", preamble + code], check=True, timeout=30)
