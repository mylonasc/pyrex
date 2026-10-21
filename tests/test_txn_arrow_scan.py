import os
import shutil
import tempfile
import unittest

import pyrex

try:
    import pyarrow as pa
except ImportError:
    pa = None


@unittest.skipIf(pa is None, "pyarrow is required for TransactionDB Arrow scan tests")
class TestTransactionDBArrowScan(unittest.TestCase):
    """Native Arrow scans on TransactionDB (follow-up to pyrex#31).

    Contract mirrors PyRocksDB.scan_arrow: fixed
    struct<key: binary (non-null), value: binary (non-null)> schema, lazy
    bounded-memory streaming, [start, stop) bounds with prefix intersection,
    snapshot-horizon isolation. Scans observe *committed* state at the pinned
    snapshot; uncommitted writes of open transactions are not visible.
    """

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="pyrex-txn-arrow-scan-")
        self.dbs = []

    def tearDown(self):
        for db in self.dbs:
            try:
                db.close()
            except Exception:
                pass
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- helpers ---------------------------------------------------------
    def open_db(self, name="db"):
        path = os.path.join(self.tmp, name)
        db = pyrex.TransactionDB(path)
        self.dbs.append(db)
        return db

    def collect(self, scan):
        return pa.RecordBatchReader.from_stream(scan).read_all()

    def pairs_of(self, table):
        return list(zip(table.column("key").to_pylist(), table.column("value").to_pylist()))

    def txn_iterator_pairs(self, db):
        with db.transaction() as txn:
            it = txn.new_iterator()
            it.seek_to_first()
            out = []
            while it.valid():
                out.append((it.key(), it.value()))
                it.next()
            it.check_status()
            txn.commit()
        return out

    # -- schema contract -------------------------------------------------
    def test_schema_contract(self):
        db = self.open_db()
        db.put(b"a", b"1")
        db.put(b"b", b"")
        db.put(b"", b"empty-key")
        table = self.collect(db.scan_arrow(batch_size=1))
        self.assertEqual(table.schema.names, ["key", "value"])
        self.assertEqual(table.schema.field("key").type, pa.binary())
        self.assertEqual(table.schema.field("value").type, pa.binary())
        self.assertFalse(table.schema.field("key").nullable)
        self.assertFalse(table.schema.field("value").nullable)
        self.assertEqual(table.column("key").null_count, 0)
        self.assertEqual(table.column("value").null_count, 0)

    # -- equivalence -----------------------------------------------------
    def test_scan_matches_committed_state(self):
        db = self.open_db()
        pairs = [(f"k{i:04d}".encode(), f"v{i}".encode()) for i in range(100)]
        for k, v in pairs:
            db.put(k, v)
        table = self.collect(db.scan_arrow(batch_size=7))
        self.assertEqual(self.pairs_of(table), sorted(pairs))
        # Matches the transaction-iterator view of committed state.
        self.assertEqual(self.pairs_of(table), self.txn_iterator_pairs(db))

    def test_new_iterator_over_committed_state(self):
        db = self.open_db()
        db.put(b"a", b"1")
        db.put(b"b", b"2")
        it = db.new_iterator()
        it.seek_to_first()
        out = []
        while it.valid():
            out.append((it.key(), it.value()))
            it.next()
        it.check_status()
        self.assertEqual(out, [(b"a", b"1"), (b"b", b"2")])

    # -- bounds ----------------------------------------------------------
    def test_prefix_and_range(self):
        db = self.open_db()
        for k in [b"a/1", b"a/2", b"b/1", b"\x00bin", b"\xff\xff"]:
            db.put(k, b"v")
        self.assertEqual(
            sorted(k for k, _ in self.pairs_of(self.collect(db.scan_arrow(prefix=b"a/")))),
            [b"a/1", b"a/2"],
        )
        table = self.collect(db.scan_arrow(start=b"a/2", stop=b"b/2"))
        self.assertEqual([k for k, _ in self.pairs_of(table)], [b"a/2", b"b/1"])
        # prefix intersected with [start, stop)
        table = self.collect(db.scan_arrow(prefix=b"a/", start=b"a/2", stop=b"z"))
        self.assertEqual([k for k, _ in self.pairs_of(table)], [b"a/2"])
        # invalid bounds fail before iteration
        with self.assertRaises(ValueError):
            db.scan_arrow(start=b"z", stop=b"a")
        # equal bounds: empty scan, schema still exposed
        table = self.collect(db.scan_arrow(start=b"m", stop=b"m"))
        self.assertEqual(table.num_rows, 0)
        self.assertEqual(table.schema.names, ["key", "value"])

    def test_binary_edge_cases(self):
        db = self.open_db()
        pairs = [(b"", b"empty-key"), (b"\x00\x01\x02", b"nulls"), (b"\xff\xff", b"ff"), (b"a\x00b", b"mid")]
        for k, v in pairs:
            db.put(k, v)
        table = self.collect(db.scan_arrow())
        self.assertEqual(self.pairs_of(table), sorted(pairs))

    # -- batch sizing ----------------------------------------------------
    def test_batch_sizing_and_oversized_policy(self):
        db = self.open_db()
        pairs = [(f"k{i:04d}".encode(), bytes(500)) for i in range(40)]
        for k, v in pairs:
            db.put(k, v)
        table = self.collect(db.scan_arrow(batch_size=100000, max_batch_bytes=2048))
        self.assertEqual(self.pairs_of(table), sorted(pairs))
        reader = pa.RecordBatchReader.from_stream(db.scan_arrow(batch_size=100000, max_batch_bytes=2048))
        sizes = [b.num_rows for b in reader]
        self.assertGreater(len(sizes), 1)
        self.assertTrue(all(s >= 1 for s in sizes))
        # default policy: clear error, no partial successful batch
        db.put(b"big", bytes(10000))
        with self.assertRaises(OSError) as ctx:
            self.collect(db.scan_arrow(max_batch_bytes=1024))
        self.assertIn("max_batch_bytes", str(ctx.exception))
        table = self.collect(db.scan_arrow(max_batch_bytes=1024, allow_overflow=True))
        self.assertIn((b"big", bytes(10000)), self.pairs_of(table))

    # -- snapshots -------------------------------------------------------
    def test_shared_snapshot_horizon(self):
        db = self.open_db()
        db.put(b"a", b"1")
        snap = db.create_snapshot()
        self.assertTrue(snap.is_valid())
        seq = snap.sequence_number
        db.put(b"b", b"2")
        s1 = self.collect(db.scan_arrow(snapshot=snap))
        s2 = self.collect(db.scan_arrow(snapshot=snap))
        self.assertEqual(self.pairs_of(s1), [(b"a", b"1")])
        self.assertEqual(self.pairs_of(s2), [(b"a", b"1")])
        # fresh scan sees later data
        s3 = self.collect(db.scan_arrow())
        self.assertEqual(self.pairs_of(s3), [(b"a", b"1"), (b"b", b"2")])
        self.assertTrue(snap.is_valid())
        snap.release()
        self.assertFalse(snap.is_valid())
        snap.release()  # idempotent

    def test_foreign_snapshot_rejected(self):
        db = self.open_db()
        other = pyrex.PyRocksDB(os.path.join(self.tmp, "plain"))
        self.dbs.append(other)
        db.put(b"a", b"1")
        other.put(b"a", b"1")
        with self.assertRaises(Exception):
            db.scan_arrow(snapshot=other.create_snapshot())
        with self.assertRaises(Exception):
            other.scan_arrow(snapshot=db.create_snapshot())
        with self.assertRaises(Exception):
            db.scan_arrow(snapshot=other.create_snapshot())

    # -- transaction isolation -------------------------------------------
    def test_uncommitted_writes_invisible(self):
        db = self.open_db()
        db.put(b"a", b"1")
        txn = db.begin_transaction()
        try:
            txn.put(b"b", b"2")
            # Read-your-own-writes still works through the transaction handle.
            self.assertEqual(txn.get(b"b"), b"2")
            # But native scans observe committed state only.
            table = self.collect(db.scan_arrow())
            self.assertEqual(self.pairs_of(table), [(b"a", b"1")])
            it = db.new_iterator()
            it.seek_to_first()
            seen = []
            while it.valid():
                seen.append((it.key(), it.value()))
                it.next()
            self.assertEqual(seen, [(b"a", b"1")])
        finally:
            txn.commit()
        table = self.collect(db.scan_arrow())
        self.assertEqual(self.pairs_of(table), [(b"a", b"1"), (b"b", b"2")])

    def test_transactions_still_work_after_scans(self):
        db = self.open_db()
        db.put(b"k", b"v0")
        table = self.collect(db.scan_arrow())
        self.assertEqual(self.pairs_of(table), [(b"k", b"v0")])
        with db.transaction() as txn:
            txn.put(b"k", b"v1")
            self.assertEqual(txn.get(b"k"), b"v1")
            txn.commit()
        self.assertEqual(db.get(b"k"), b"v1")
        with db.transaction() as txn:
            txn.put(b"tmp", b"x")
            # no commit -> automatic rollback
        self.assertIsNone(db.get(b"tmp"))

    # -- lifecycle ---------------------------------------------------------
    def test_close_lifecycle(self):
        db = self.open_db()
        db.put(b"a", b"1")
        scan = db.scan_arrow()
        scan.close()
        self.assertTrue(scan.is_closed)
        reader = pa.RecordBatchReader.from_stream(db.scan_arrow())
        db.close()
        with self.assertRaises(Exception):
            reader.read_next_batch()


if __name__ == "__main__":
    unittest.main()
