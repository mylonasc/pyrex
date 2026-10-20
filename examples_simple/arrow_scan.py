"""Minimal end-to-end example for native batched Arrow scans (feature #31)."""

import shutil
import tempfile

import pyrex

try:
    import pyarrow as pa
except ImportError:
    raise SystemExit("pyarrow is required for this example")


def main():
    path = tempfile.mkdtemp(prefix="pyrex-arrow-example-")
    try:
        db = pyrex.PyRocksDB(path)
        try:
            for i in range(1000):
                db.put(f"sensor/{i:04d}".encode(), f"reading-{i}".encode())
            db.put(b"other/1", b"noise")

            # Full scan consumed as Arrow without per-row Python objects.
            table = pa.RecordBatchReader.from_stream(db.scan_arrow(batch_size=256)).read_all()
            print(f"full scan: {table.num_rows} rows, schema={table.schema}")

            # Prefix scan with a shared snapshot: both streams see one horizon.
            snap = db.create_snapshot()
            try:
                temps = pa.RecordBatchReader.from_stream(
                    db.scan_arrow(prefix=b"sensor/", snapshot=snap)
                ).read_all()
                print(f"prefix scan: {temps.num_rows} rows @ seq {snap.sequence_number}")
            finally:
                snap.release()
        finally:
            db.close()
    finally:
        shutil.rmtree(path, ignore_errors=True)


if __name__ == "__main__":
    main()
