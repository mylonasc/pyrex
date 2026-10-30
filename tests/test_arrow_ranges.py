"""ABR-05: normalized ordered unions, one snapshot and lazy native seeks."""
import errno
import random

import pytest
import pyrex

from test_arrow_scan_metrics import NativeStream, release_array

pa = pytest.importorskip("pyarrow")


@pytest.fixture(params=[pyrex.PyRocksDB, pyrex.PyRocksDBExtended, pyrex.TransactionDB])
def db(request, tmp_path):
    with request.param(str(tmp_path / "db")) as db:
        for key in [b""] + [bytes([i]) for i in range(256)] + [b"a\x00", b"\xff\xff"]:
            db.put(key, b"v" + key)
        yield db


def rows(scan):
    with pa.RecordBatchReader.from_stream(scan) as reader:
        return reader.read_all().to_pylist()


@pytest.mark.parametrize("selectors", [
    {"ranges": []}, {"prefixes": []}, {"ranges": [(b"a", b"a")]},
    {"ranges": [(b"c", b"f"), (b"a", b"d"), (b"c", b"f"), (b"f", b"g")]},
    {"ranges": [(None, b"a"), (b"\xff", None)]},
    {"prefixes": [b"a", b"\xff", b"a"]}, {"prefixes": [b""]},
    {"ranges": [(b"x", b"z")], "prefixes": [b"a", b"\xff\xff"]},
    {"ranges": [(None, None), (b"a", b"b")]},
    {"ranges": [(b"no", b"np"), (b"zz", b"zzz")]},
])
@pytest.mark.parametrize("batch_size", [1, 7, 65536])
def test_ordered_union_exactly_once(db, selectors, batch_size):
    all_keys = sorted([b""] + [bytes([i]) for i in range(256)] + [b"a\x00", b"\xff\xff"])
    def selected(key):
        return (any((lo is None or key >= lo) and (hi is None or key < hi)
                    for lo, hi in selectors.get("ranges", [])) or
                any(key.startswith(prefix) for prefix in selectors.get("prefixes", [])))
    expected = [dict(key=k, value=b"v" + k) for k in all_keys if selected(k)]
    scan = db.scan_arrow_ranges(**selectors, batch_size=batch_size, collect_metrics=True)
    assert rows(scan) == expected
    assert scan.stats["rows_gathered"] == scan.stats["rows_emitted"] == len(expected)
    assert scan.stats["live_buffer_bytes"] == 0
    assert not scan.cancel()


def test_random_overlapping_intervals(db):
    rng = random.Random(705)
    ranges = [tuple(sorted([bytes([rng.randrange(256)]), bytes([rng.randrange(256)])])) for _ in range(50)]
    expected = [k for k in sorted([b""] + [bytes([i]) for i in range(256)] + [b"a\x00", b"\xff\xff"])
                if any(lo <= k < hi for lo, hi in ranges)]
    assert [r["key"] for r in rows(db.scan_arrow_ranges(ranges, batch_size=3, max_batch_bytes=40))] == expected


@pytest.mark.parametrize("selectors", [{}, {"ranges": b"bad"}, {"prefixes": [None]},
                                      {"prefixes": ["text"]}, {"ranges": [(b"z", b"a")]},
                                      {"ranges": [(b"a",)]}, {"ranges": [("a", b"b")]},
                                      {"ranges": [(None, None)] * 65537}])
def test_invalid_selectors_before_iteration(db, selectors):
    with pytest.raises((TypeError, ValueError)):
        db.scan_arrow_ranges(**selectors)


def test_snapshot_and_selector_capture_across_transitions(db):
    selectors = [(b"a", b"b"), (b"\xff", None)]
    snap = db.create_snapshot()
    scan = db.scan_arrow_ranges(selectors, snapshot=snap, batch_size=1)
    selectors.clear()
    db.put(b"\xff", b"new")
    db.put(b"\xffx", b"new-key")
    assert scan.sequence_number == snap.sequence_number
    assert rows(scan) == [dict(key=k, value=b"v" + k) for k in [b"a", b"a\x00", b"\xff", b"\xff\xff"]]


def test_cancel_between_ranges_and_retained_batch(db):
    scan = db.scan_arrow_ranges([(b"a", b"b"), (b"x", b"z")], batch_size=1, collect_metrics=True)
    with NativeStream(scan) as raw:
        status, batch = raw.next()
        assert status == 0 and batch.length == 1
        assert scan.cancel()
        status, empty = raw.next()
        assert status == errno.ECANCELED and not empty.release
        db.close()
        assert batch.length == 1
        release_array(batch)
    assert scan.stats["live_buffer_bytes"] == 0


@pytest.mark.parametrize("kind", [pyrex.PyRocksDBExtended, pyrex.TransactionDB])
def test_cf_snapshot_drop_and_foreign_handles(kind, tmp_path):
    with kind(str(tmp_path / "one")) as db, kind(str(tmp_path / "two")) as other:
        cf = db.create_column_family("union")
        foreign = other.create_column_family("union")
        db.put_cf(cf, b"a:1", b"old")
        snap = db.create_snapshot()
        scan = db.scan_arrow_ranges_cf(cf, prefixes=[b"a:", b"a"], snapshot=snap)
        db.put_cf(cf, b"a:1", b"new")
        with pytest.raises(pyrex.RocksDBException, match="active Arrow scan"):
            db.drop_column_family(cf)
        with pytest.raises(pyrex.RocksDBException, match="different database"):
            db.scan_arrow_ranges_cf(foreign, prefixes=[])
        with pytest.raises(pyrex.RocksDBException, match="different database"):
            other.scan_arrow_ranges(prefixes=[], snapshot=snap)
        assert rows(scan) == [dict(key=b"a:1", value=b"old")]
        db.drop_column_family(cf)


def test_range_benchmark_smoke(tmp_path):
    import json
    from pathlib import Path
    import subprocess
    import sys
    result = subprocess.run([sys.executable, str(Path(__file__).resolve().parents[1] / "benchmarks/bench_arrow_ranges.py"),
                             "--rows", "1000", "--ranges", "10", "--repeat", "2", "--work-dir", str(tmp_path)],
                            capture_output=True, text=True, check=True, timeout=30)
    report = json.loads(result.stdout)
    assert len(report["ordered_digest"]) == 64
    assert all(len(values) == 2 for values in report["samples_ns"].values())
