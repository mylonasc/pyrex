import os
import shutil
import tempfile
import unittest

import pyrex

try:
    import pyarrow as pa
except ImportError:
    pa = None

try:
    import polars as pl
except ImportError:
    pl = None


@unittest.skipIf(pa is None, "pyarrow is required for Arrow scan tests")
class TestArrowScan(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="pyrex-arrow-scan-")
        self.dbs = []

    def tearDown(self):
        for db in self.dbs:
            try:
                db.close()
            except Exception:
                pass
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- helpers ---------------------------------------------------------
    def open_db(self, name="db", extended=False):
        path = os.path.join(self.tmp, name)
        db = pyrex.PyRocksDBExtended(path) if extended else pyrex.PyRocksDB(path)
        self.dbs.append(db)
        return db

    def write_kv(self, db, pairs):
        for k, v in pairs:
            db.put(k, v)

    def iterator_pairs(self, db):
        it = db.new_iterator()
        it.seek_to_first()
        out = []
        while it.valid():
            out.append((it.key(), it.value()))
            it.next()
        it.check_status()
        return out

    def collect(self, scan):
        return pa.RecordBatchReader.from_stream(scan).read_all()

    def pairs_of(self, table):
        keys = table.column("key").to_pylist()
        values = table.column("value").to_pylist()
        return list(zip(keys, values))

    # -- schema contract -------------------------------------------------
    def test_schema_contract(self):
        db = self.open_db()
        self.write_kv(db, [(b"a", b"1"), (b"b", b""), (b"", b"empty-key")])
        table = self.collect(db.scan_arrow(batch_size=1))
        self.assertEqual(table.schema.names, ["key", "value"])
        self.assertEqual(table.schema.field("key").type, pa.binary())
        self.assertEqual(table.schema.field("value").type, pa.binary())
        self.assertFalse(table.schema.field("key").nullable)
        self.assertFalse(table.schema.field("value").nullable)
        self.assertEqual(table.column("key").null_count, 0)
        self.assertEqual(table.column("value").null_count, 0)

    def test_schema_stable_across_batches(self):
        db = self.open_db()
        self.write_kv(db, [(f"k{i:04d}".encode(), b"v") for i in range(50)])
        reader = pa.RecordBatchReader.from_stream(db.scan_arrow(batch_size=7))
        first_schema = reader.schema
        count = 0
        for batch in reader:
            self.assertEqual(batch.schema, first_schema)
            count += batch.num_rows
        self.assertEqual(count, 50)

    # -- equivalence with iterators --------------------------------------
    def test_equivalence_with_iterator(self):
        db = self.open_db()
        pairs = []
        x = 123456789
        for i in range(2000):
            # deterministic xorshift for reproducible pseudo-random payloads
            x ^= (x << 13) & 0xFFFFFFFFFFFFFFFF
            x ^= x >> 7
            x ^= (x << 17) & 0xFFFFFFFFFFFFFFFF
            k = f"k{i:05d}-{(x & 0xFFFF):04x}".encode()
            v = f"value-{(x >> 16) & 0xFFFFFF:06x}-".encode() + bytes([(x >> (8 * (i % 4))) & 0xFF]) * (i % 17)
            pairs.append((k, v))
        self.write_kv(db, pairs)
        expected = sorted(pairs)
        got = self.pairs_of(self.collect(db.scan_arrow(batch_size=129)))
        self.assertEqual(got, expected)
        self.assertEqual(self.iterator_pairs(db), expected)

    # -- bounds semantics --------------------------------------------------
    def test_full_prefix_range(self):
        db = self.open_db()
        pairs = [(b"a/1", b"1"), (b"a/2", b"2"), (b"b/1", b"3"), (b"b/2", b"4"), (b"c", b"5")]
        self.write_kv(db, pairs)
        self.assertEqual(len(self.pairs_of(self.collect(db.scan_arrow()))), 5)
        self.assertEqual(
            self.pairs_of(self.collect(db.scan_arrow(prefix=b"a/"))),
            [(b"a/1", b"1"), (b"a/2", b"2")],
        )
        self.assertEqual(
            self.pairs_of(self.collect(db.scan_arrow(start=b"a/2", stop=b"b/2"))),
            [(b"a/2", b"2"), (b"b/1", b"3")],
        )
        # prefix intersects [start, stop)
        self.assertEqual(
            self.pairs_of(self.collect(db.scan_arrow(prefix=b"a/", start=b"a/2", stop=b"zzz"))),
            [(b"a/2", b"2")],
        )
        # disjoint prefix/range intersection is empty but keeps schema
        empty = self.collect(db.scan_arrow(prefix=b"a/", start=b"b/1", stop=b"b/2"))
        self.assertEqual(empty.num_rows, 0)
        self.assertEqual(empty.schema.names, ["key", "value"])

    def test_range_semantics(self):
        db = self.open_db()
        self.write_kv(db, [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")])
        # start inclusive, stop exclusive
        self.assertEqual(self.pairs_of(self.collect(db.scan_arrow(start=b"b", stop=b"c"))), [(b"b", b"2")])
        # equal bounds -> empty scan
        self.assertEqual(self.collect(db.scan_arrow(start=b"b", stop=b"b")).num_rows, 0)
        # invalid bounds fail before iteration
        with self.assertRaises(ValueError):
            db.scan_arrow(start=b"c", stop=b"b")

    def test_binary_edge_cases(self):
        db = self.open_db()
        pairs = [
            (b"", b"empty-key"),
            (b"\x00", b"nul"),
            (b"\x00\x01", b"nul-2"),
            (b"\xff", b"ff"),
            (b"\xff\xff", b"ff-ff"),
            (b"\xff\x00", b"ff-nul"),
            (b"a\x00b", b"embedded"),
            (b"b", b""),
        ]
        self.write_kv(db, pairs)
        self.assertEqual(self.pairs_of(self.collect(db.scan_arrow())), sorted(pairs))
        # prefix of all-0xff is unbounded above within its range but still filters
        self.assertEqual(
            self.pairs_of(self.collect(db.scan_arrow(prefix=b"\xff\xff"))),
            [(b"\xff\xff", b"ff-ff")],
        )
        self.assertEqual(
            self.pairs_of(self.collect(db.scan_arrow(prefix=b"\xff"))),
            [(b"\xff", b"ff"), (b"\xff\x00", b"ff-nul"), (b"\xff\xff", b"ff-ff")],
        )
        # prefix ending in 0xff
        self.assertEqual(
            self.pairs_of(self.collect(db.scan_arrow(prefix=b"\x00"))),
            [(b"\x00", b"nul"), (b"\x00\x01", b"nul-2")],
        )
        # empty prefix selects all keys
        self.assertEqual(self.collect(db.scan_arrow(prefix=b"")).num_rows, len(pairs))
        # range over binary keys incl. empty start
        self.assertEqual(
            self.pairs_of(self.collect(db.scan_arrow(start=b"", stop=b"\x00\x01"))),
            [(b"", b"empty-key"), (b"\x00", b"nul")],
        )

    # -- batch sizing ------------------------------------------------------
    def test_batch_sizing_row_cap(self):
        db = self.open_db()
        self.write_kv(db, [(f"k{i:03d}".encode(), b"v") for i in range(100)])
        reader = pa.RecordBatchReader.from_stream(db.scan_arrow(batch_size=7))
        sizes = [b.num_rows for b in reader]
        self.assertEqual(sum(sizes), 100)
        self.assertTrue(all(s <= 7 for s in sizes))
        self.assertEqual(len(sizes), 15)  # 14 x 7 + 1 x 2

    def test_batch_sizing_byte_cap(self):
        db = self.open_db()
        pairs = [(f"k{i:04d}".encode(), bytes(500)) for i in range(40)]
        self.write_kv(db, pairs)
        table = self.collect(db.scan_arrow(batch_size=100000, max_batch_bytes=2048))
        self.assertEqual(self.pairs_of(table), sorted(pairs))
        reader = pa.RecordBatchReader.from_stream(
            db.scan_arrow(batch_size=100000, max_batch_bytes=2048)
        )
        sizes = [b.num_rows for b in reader]
        self.assertGreater(len(sizes), 1)
        self.assertTrue(all(s >= 1 for s in sizes))

    def test_oversized_record_policy(self):
        db = self.open_db()
        db.put(b"big", bytes(10000))
        db.put(b"small", b"1")
        # default policy: clear error, no partial successful batch. Native
        # stream errors surface through the Arrow consumer as OSError
        # carrying the native message (errno channel of ArrowArrayStream).
        with self.assertRaises(OSError) as ctx:
            self.collect(db.scan_arrow(max_batch_bytes=1024))
        self.assertIn("max_batch_bytes", str(ctx.exception))
        # explicit opt-in: emit the oversized record alone, scan continues
        table = self.collect(db.scan_arrow(max_batch_bytes=1024, allow_overflow=True))
        self.assertEqual(self.pairs_of(table), [(b"big", bytes(10000)), (b"small", b"1")])

    def test_large_values(self):
        db = self.open_db()
        big = bytes(2 * 1024 * 1024)
        db.put(b"k", big)
        table = self.collect(db.scan_arrow())
        self.assertEqual(table.column("value")[0].as_py(), big)

    def test_invalid_limits(self):
        db = self.open_db()
        with self.assertRaises(ValueError):
            db.scan_arrow(batch_size=0)
        with self.assertRaises(ValueError):
            db.scan_arrow(max_batch_bytes=0)
        with self.assertRaises(TypeError):
            db.scan_arrow(prefix="str-not-bytes")
        with self.assertRaises(TypeError):
            db.scan_arrow(start=123)

    # -- snapshots -----------------------------------------------------------
    def test_snapshot_isolation(self):
        db = self.open_db()
        self.write_kv(db, [(f"k{i:03d}".encode(), b"v") for i in range(10)])
        snap = db.create_snapshot()
        self.assertTrue(snap.is_valid())
        seq = snap.sequence_number
        self.assertIsInstance(seq, int)
        self.write_kv(db, [(f"k{i:03d}".encode(), b"v") for i in range(10, 20)])
        db.put(b"extra", b"1")
        frozen = self.collect(db.scan_arrow(snapshot=snap))
        fresh = self.collect(db.scan_arrow())
        self.assertEqual(frozen.num_rows, 10)
        self.assertEqual(fresh.num_rows, 21)
        self.assertEqual(snap.sequence_number, seq)
        snap.release()
        self.assertFalse(snap.is_valid())

    def test_shared_snapshot_same_horizon(self):
        db = self.open_db()
        self.write_kv(db, [(b"a/1", b"1"), (b"b/1", b"2")])
        snap = db.create_snapshot()
        s1 = db.scan_arrow(prefix=b"a/", snapshot=snap)
        s2 = db.scan_arrow(prefix=b"b/", snapshot=snap)
        db.put(b"a/2", b"new")
        db.put(b"b/2", b"new")
        self.assertEqual(self.pairs_of(self.collect(s1)), [(b"a/1", b"1")])
        self.assertEqual(self.pairs_of(self.collect(s2)), [(b"b/1", b"2")])
        self.assertEqual(s1.sequence_number, s2.sequence_number)
        self.assertEqual(s1.sequence_number, snap.sequence_number)
        snap.release()

    def test_foreign_and_released_snapshot_rejected(self):
        db1 = self.open_db("db1")
        db2 = self.open_db("db2")
        db1.put(b"k", b"v")
        snap = db1.create_snapshot()
        with self.assertRaises(pyrex.RocksDBException):
            db2.scan_arrow(snapshot=snap)
        snap.release()
        with self.assertRaises(pyrex.RocksDBException):
            db1.scan_arrow(snapshot=snap)

    def test_scan_without_snapshot_pins_view_at_creation(self):
        db = self.open_db()
        db.put(b"k1", b"v1")
        scan = db.scan_arrow()
        db.put(b"k2", b"v2")  # concurrent write after establishment
        got = self.pairs_of(self.collect(scan))
        self.assertEqual(got, [(b"k1", b"v1")])

    # -- column families -----------------------------------------------------
    def test_cf_scan(self):
        db = self.open_db(extended=True)
        cf = db.create_column_family("analytics")
        db.put(b"default/1", b"d1")
        db.put_cf(cf, b"cf/1", b"c1")
        db.put_cf(cf, b"cf/2", b"c2")
        default_rows = self.pairs_of(self.collect(db.scan_arrow()))
        self.assertEqual(default_rows, [(b"default/1", b"d1")])
        cf_rows = self.pairs_of(self.collect(db.scan_arrow_cf(cf)))
        self.assertEqual(cf_rows, [(b"cf/1", b"c1"), (b"cf/2", b"c2")])

    def test_cf_shared_snapshot_horizon(self):
        db = self.open_db(extended=True)
        cf = db.create_column_family("analytics")
        db.put(b"d/1", b"1")
        db.put_cf(cf, b"c/1", b"1")
        snap = db.create_snapshot()
        db.put(b"d/2", b"2")
        db.put_cf(cf, b"c/2", b"2")
        d_rows = self.pairs_of(self.collect(db.scan_arrow(snapshot=snap)))
        c_rows = self.pairs_of(self.collect(db.scan_arrow_cf(cf, snapshot=snap)))
        self.assertEqual(d_rows, [(b"d/1", b"1")])
        self.assertEqual(c_rows, [(b"c/1", b"1")])
        snap.release()

    def test_drop_cf_with_active_scan_rejected(self):
        db = self.open_db(extended=True)
        cf = db.create_column_family("temp")
        db.put_cf(cf, b"k", b"v")
        scan = db.scan_arrow_cf(cf)
        with self.assertRaises(pyrex.RocksDBException):
            db.drop_column_family(cf)
        scan.close()
        db.drop_column_family(cf)
        self.assertIsNone(db.get_column_family("temp"))

    # -- lifecycle ------------------------------------------------------------
    def test_single_consumer_export(self):
        db = self.open_db()
        db.put(b"k", b"v")
        scan = db.scan_arrow()
        cap1 = scan.__arrow_c_stream__()
        self.assertIsNotNone(cap1)
        with self.assertRaises(Exception):
            scan.__arrow_c_stream__()
        # the first capsule still imports fine
        reader = pa.RecordBatchReader._import_from_c_capsule(cap1)
        self.assertEqual(reader.read_all().num_rows, 1)

    def test_early_close(self):
        db = self.open_db()
        self.write_kv(db, [(f"k{i:03d}".encode(), b"v") for i in range(100)])
        scan = db.scan_arrow(batch_size=10)
        reader = pa.RecordBatchReader.from_stream(scan)
        first = reader.read_next_batch()
        self.assertEqual(first.num_rows, 10)
        scan.close()
        self.assertTrue(scan.is_closed)
        with self.assertRaises(Exception):
            reader.read_next_batch()

    def test_export_without_import_is_safe(self):
        import gc

        db = self.open_db()
        db.put(b"k", b"v")
        for _ in range(50):
            scan = db.scan_arrow()
            cap = scan.__arrow_c_stream__()
            self.assertIsNotNone(cap)
            del cap
            del scan
            gc.collect()
        # db fully usable afterwards
        self.assertEqual(self.collect(db.scan_arrow()).num_rows, 1)

    def test_close_without_export(self):
        db = self.open_db()
        db.put(b"k", b"v")
        scan = db.scan_arrow()
        scan.close()
        self.assertTrue(scan.is_closed)
        self.assertEqual(self.collect(db.scan_arrow()).num_rows, 1)

    def test_context_manager(self):
        db = self.open_db()
        db.put(b"k", b"v")
        with db.scan_arrow() as scan:
            table = self.collect(scan)
        self.assertEqual(table.num_rows, 1)
        self.assertTrue(scan.is_closed)

    def test_exported_batches_survive_db_close(self):
        db = self.open_db()
        self.write_kv(db, [(f"k{i:03d}".encode(), b"v") for i in range(20)])
        reader = pa.RecordBatchReader.from_stream(db.scan_arrow(batch_size=5))
        first = reader.read_next_batch()
        self.assertEqual(first.num_rows, 5)
        db.close()
        # already exported batch stays valid after DB closure
        self.assertEqual(first.column("key")[0].as_py(), b"k000")
        self.assertEqual(first.num_rows, 5)

    def test_db_close_invalidates_active_scan(self):
        db = self.open_db()
        self.write_kv(db, [(f"k{i:03d}".encode(), b"v") for i in range(20)])
        scan = db.scan_arrow()
        reader = pa.RecordBatchReader.from_stream(scan)
        db.close()
        with self.assertRaises(Exception):
            reader.read_next_batch()

    def test_lifecycle_stress(self):
        import gc

        db = self.open_db()
        self.write_kv(db, [(f"k{i:05d}".encode(), b"v" * 64) for i in range(500)])
        for i in range(200):
            scan = db.scan_arrow(batch_size=37)
            reader = pa.RecordBatchReader.from_stream(scan)
            total = 0
            for batch in reader:
                total += batch.num_rows
                if i % 3 == 0:
                    break  # abandon mid-stream regularly
                if i % 3 == 1 and total >= 100:
                    scan.close()  # early termination
                    break
            self.assertIn(total, [37, 74, 111, 148, 185, 222, 259, 296, 333, 370, 407, 444, 481, 500])
            if i % 5 == 0:
                snap = db.create_snapshot()
                snap.release()
            gc.collect()
        # db still fully usable afterwards
        self.assertEqual(self.collect(db.scan_arrow()).num_rows, 500)

    def test_empty_scan_schema(self):
        db = self.open_db()
        table = self.collect(db.scan_arrow(prefix=b"missing/"))
        self.assertEqual(table.num_rows, 0)
        self.assertEqual(table.schema.names, ["key", "value"])

    def test_read_options_passthrough(self):
        db = self.open_db()
        db.put(b"k", b"v")
        opts = pyrex.ReadOptions()
        opts.fill_cache = False
        opts.verify_checksums = True
        table = self.collect(db.scan_arrow(read_options=opts))
        self.assertEqual(table.num_rows, 1)

    @unittest.skipIf(pl is None, "polars not installed")
    def test_polars_interop(self):
        db = self.open_db()
        self.write_kv(db, [(f"k{i:03d}".encode(), f"v{i}".encode()) for i in range(100)])
        df = pl.from_arrow(pa.RecordBatchReader.from_stream(db.scan_arrow(batch_size=16)))
        self.assertEqual(df.height, 100)
        self.assertEqual(df.columns, ["key", "value"])
        self.assertEqual(df.filter(pl.col("key") == b"k042").height, 1)


if __name__ == "__main__":
    unittest.main()
