"""Atomic cross-CF publication and one-horizon scan/lookup materialization."""
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyrex


def main():
    with TemporaryDirectory(prefix="pyrex-snapshot-cf-") as path:
        with pyrex.TransactionDB(path) as db:
            index = db.create_column_family("index")
            facts = db.create_column_family("facts")
            with db.transaction() as txn:
                txn.put_cf(index, b"p/a", b"version/1")
                txn.put_cf(facts, b"version/1", b"old")
                txn.commit()

            snapshot = db.create_snapshot()
            try:
                # A concurrent publication must not change materialization.
                db.put_cf(facts, b"version/1", b"new")
                with db.scan_arrow_cf(index, prefix=b"p/", snapshot=snapshot) as scan:
                    reader = pa.RecordBatchReader.from_stream(scan)
                    for batch in reader:
                        keys = batch.column("value").to_pylist()
                        assert db.multi_get_cf(facts, keys, snapshot=snapshot) == [b"old"]
                        print("snapshot values:", db.multi_get_cf(facts, keys, snapshot=snapshot))
                assert db.get_cf(facts, b"version/1") == b"new"
            finally:
                snapshot.release()


if __name__ == "__main__":
    main()
