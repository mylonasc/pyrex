import argparse
import shutil
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyrex


def make_data(rows: int):
    width = max(8, len(str(rows)))
    keys = [f"k{i:0{width}d}".encode() for i in range(rows)]
    values = [f"value-{i:0{width}d}".encode() for i in range(rows)]
    return keys, values, pa.array(keys, type=pa.binary()), pa.array(values, type=pa.binary())


def run_once(name, rows, disable_wal, fn):
    path = Path(tempfile.mkdtemp(prefix=f"pyrex-bench-{name}-"))
    opts = pyrex.WriteOptions()
    opts.disable_wal = disable_wal
    try:
        with pyrex.PyRocksDB(str(path)) as db:
            start = time.perf_counter()
            fn(db, opts)
            elapsed = time.perf_counter() - start

            if rows:
                last_key = f"k{rows - 1:0{max(8, len(str(rows)))}d}".encode()
                expected = f"value-{rows - 1:0{max(8, len(str(rows)))}d}".encode()
                actual = db.get(last_key)
                if actual != expected:
                    raise RuntimeError(f"verification failed for {name}: {actual!r} != {expected!r}")
    finally:
        shutil.rmtree(path, ignore_errors=True)

    return elapsed


def main():
    parser = argparse.ArgumentParser(description="Compare Python WriteBatch loop against native Arrow columnar ingestion.")
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--disable-wal", action="store_true")
    parser.add_argument("--repeat", type=int, default=3)
    args = parser.parse_args()

    keys, values, arrow_keys, arrow_values = make_data(args.rows)

    def python_write_batch(db, opts):
        batch = pyrex.PyWriteBatch()
        for key, value in zip(keys, values):
            batch.put(key, value)
        db.write(batch, opts)

    def native_columnar_batch(db, opts):
        db.write_columnar_batch(arrow_keys, arrow_values, write_options=opts)

    modes = [
        ("python_write_batch", python_write_batch),
        ("native_columnar_batch_arrow_binary", native_columnar_batch),
    ]

    print(f"rows={args.rows}")
    print(f"disable_wal={args.disable_wal}")
    print(f"repeat={args.repeat}")

    results = {}
    for name, fn in modes:
        timings = [run_once(name, args.rows, args.disable_wal, fn) for _ in range(args.repeat)]
        best = min(timings)
        results[name] = best
        print(f"mode={name}")
        print(f"best_elapsed={best:.6f}")
        print(f"writes_per_second={args.rows / best:.2f}")

    baseline = results["python_write_batch"]
    native = results["native_columnar_batch_arrow_binary"]
    print(f"speedup_vs_python_write_batch={baseline / native:.2f}x")


if __name__ == "__main__":
    main()
