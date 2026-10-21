"""Reproducible benchmark for native batched Arrow scans (feature #31).

Compares, on identical ordered records (digest-validated):
  1. Python iterator collecting key/value objects.
  2. Python iterator followed by Arrow array construction.
  3. Native batched Arrow scan, materialized via PyArrow.
  4. Native scan consumed by an Arrow-compute operation (no per-row Python).

Covers full scans and selective prefix scans, short and variable-length
values, and batch-size sensitivity. Reports wall time, throughput, table
bytes and cumulative process high-water RSS changes. This is not a
per-mode isolated RSS or native allocation-capacity measurement.

Streaming consumption and full materialization are labeled separately: a
``collect()`` of the whole stream is NOT a bounded-memory scan benchmark.
"""

import argparse
import gc
import hashlib
import os
import resource
import shutil
import sys
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyrex


def make_keys(n, prefix="k"):
    width = len(str(n))
    return [f"{prefix}{i:0{width}d}".encode() for i in range(n)]


def seed_db(path, rows, value_size, variable):
    if os.path.exists(path):
        shutil.rmtree(path)
    db = pyrex.PyRocksDB(path)
    batch = 5000
    for start in range(0, rows, batch):
        width = len(str(rows))
        keys = [f"sensor/{i:0{width}d}".encode() for i in range(start, min(rows, start + batch))]
        if variable:
            values = [bytes(((i * 7) % value_size) + 1) for i in range(start, start + len(keys))]
        else:
            values = [bytes(value_size) for _ in keys]
        wb = pyrex.PyWriteBatch()
        for k, v in zip(keys, values):
            wb.put(k, v)
        db.write(wb)
    # a decoy prefix for selective scans
    for i in range(max(1, rows // 10)):
        db.put(f"other/{i:08d}".encode(), b"x")
    return db


def digest_pairs(pairs):
    h = hashlib.sha256()
    for k, v in pairs:
        h.update(len(k).to_bytes(4, "little"))
        h.update(k)
        h.update(len(v).to_bytes(4, "little"))
        h.update(v)
    return h.hexdigest()


def digest_table(table):
    # content digest (independent of chunking/offsets layout)
    h = hashlib.sha256()
    keys = table.column("key").to_pylist()
    values = table.column("value").to_pylist()
    for k, v in zip(keys, values):
        h.update(len(k).to_bytes(4, "little"))
        h.update(k)
        h.update(len(v).to_bytes(4, "little"))
        h.update(v)
    return h.hexdigest()


def rss_mb():
    divisor = 1024.0 * 1024 if sys.platform == "darwin" else 1024.0
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / divisor


def time_fn(fn, repeat):
    best = None
    for _ in range(repeat):
        gc.collect()
        start = time.perf_counter()
        out = fn()
        elapsed = time.perf_counter() - start
        best = elapsed if best is None or elapsed < best else best
    return best, out


def bench_python_iter(db, prefix=None):
    def run():
        it = db.new_iterator()
        if prefix is None: it.seek_to_first()
        else: it.seek(prefix)
        pairs = []
        while it.valid():
            key = it.key()
            if prefix is not None and not key.startswith(prefix): break
            pairs.append((key, it.value()))
            it.next()
        it.check_status()
        return pairs

    return run


def bench_python_iter_arrow(db, prefix=None):
    def run():
        it = db.new_iterator()
        if prefix is None: it.seek_to_first()
        else: it.seek(prefix)
        keys, values = [], []
        while it.valid():
            key = it.key()
            if prefix is not None and not key.startswith(prefix): break
            keys.append(key)
            values.append(it.value())
            it.next()
        it.check_status()
        return pa.table({"key": pa.array(keys, type=pa.binary()), "value": pa.array(values, type=pa.binary())})

    return run


def bench_native_scan(db, batch_size, max_batch_bytes, prefix=None):
    def run():
        reader = pa.RecordBatchReader.from_stream(
            db.scan_arrow(prefix=prefix, batch_size=batch_size, max_batch_bytes=max_batch_bytes)
        )
        return reader.read_all()

    return run


def bench_native_scan_compute(db, batch_size, max_batch_bytes, prefix=None):
    # streaming: per-batch Arrow compute, no full materialization
    def run():
        reader = pa.RecordBatchReader.from_stream(
            db.scan_arrow(prefix=prefix, batch_size=batch_size, max_batch_bytes=max_batch_bytes)
        )
        total_len = 0
        batches = 0
        rows = 0
        for batch in reader:
            total_len += pc.sum(pc.binary_length(batch.column("value"))).as_py()
            batches += 1
            rows += batch.num_rows
        return total_len, batches, rows

    return run


def main():
    parser = argparse.ArgumentParser(description="Benchmark native batched Arrow scans vs Python iterators.")
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--value-size", type=int, default=64)
    parser.add_argument("--variable", action="store_true", help="variable-length values")
    parser.add_argument("--batch-size", type=int, default=4096)
    parser.add_argument("--max-batch-bytes", type=int, default=8 * 1024 * 1024)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--prefix", type=str, default=None, help="selective prefix scan, e.g. sensor/")
    args = parser.parse_args()

    path = str(Path(tempfile.mkdtemp(prefix="pyrex-arrow-bench-")))
    db = seed_db(path, args.rows, args.value_size, args.variable)
    prefix = args.prefix.encode() if args.prefix else None
    try:
        # reference digest from the Python iterator (full scan only for digest)
        ref_pairs = bench_python_iter(db, prefix)()
        ref_digest = digest_pairs(ref_pairs)
        ref_count = len(ref_pairs)
        print(f"dataset: rows={ref_count} value_size~{args.value_size} variable={args.variable}")
        print(f"reference sha256: {ref_digest}")

        modes = [
            ("python_iter_collect", bench_python_iter(db, prefix)),
            ("python_iter_then_arrow", bench_python_iter_arrow(db, prefix)),
            ("native_scan_materialize", bench_native_scan(db, args.batch_size, args.max_batch_bytes, prefix)),
            ("native_scan_compute", bench_native_scan_compute(db, args.batch_size, args.max_batch_bytes, prefix)),
        ]
        for name, fn in modes:
            rss_before = rss_mb()
            elapsed, out = time_fn(fn, args.repeat)
            rss_after = rss_mb()
            if isinstance(out, list):
                assert digest_pairs(out) == ref_digest, f"{name}: digest mismatch"
                rows = len(out)
                extra = ""
            elif isinstance(out, tuple):
                assert out[0] == sum(len(v) for _, v in ref_pairs) and out[2] == ref_count
                rows = out[2]
                extra = f" value_bytes={out[0]} batches={out[1]}"
            else:
                assert digest_table(out) == ref_digest, f"{name}: digest mismatch"
                rows = out.num_rows
                extra = f" table_bytes={out.nbytes}"
            rps = f"{(ref_count / elapsed):,.0f} rows/s" if isinstance(rows, int) else ""
            print(
                f"{name:28s} best_of_{args.repeat}={elapsed * 1000:9.1f} ms  {rps:>16s}  "
                f"rows={rows}{extra}  cumulative_process_peak_rss_delta={rss_after - rss_before:.1f} MB"
            )
        # batch-size sensitivity on the native scan
        print("batch-size sensitivity (native_scan_materialize):")
        for bs in (256, 1024, 4096, 65536):
            elapsed, out = time_fn(bench_native_scan(db, bs, args.max_batch_bytes, prefix), args.repeat)
            print(f"  batch_size={bs:6d}  {elapsed * 1000:9.1f} ms")
        print(f"versions: pyrex-scan stream schema struct<key: binary, value: binary>; "
              f"pyarrow={pa.__version__}; rows={args.rows}")
    finally:
        db.close()
        shutil.rmtree(path, ignore_errors=True)


if __name__ == "__main__":
    main()
