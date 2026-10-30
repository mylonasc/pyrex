"""ABR-07 same-toolchain before/after worker (select native build via PYTHONPATH).

Full Arrow materialization, not streaming RSS. Each warmed timing includes
stream establishment and read_all; ordered digest validation is outside timing.
"""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import statistics
import tempfile
import time

import pyarrow as pa
import pyrex


def update(digest, key, value):
    for item in (key, value):
        digest.update(len(item).to_bytes(8, "little") + item)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--value-size", type=int, default=64)
    parser.add_argument("--batch-size", type=int, default=4096)
    parser.add_argument("--repeat", type=int, default=7)
    parser.add_argument("--variable", action="store_true")
    parser.add_argument("--metrics", action="store_true")
    parser.add_argument("--work-dir", type=Path)
    args = parser.parse_args()
    if min(args.rows, args.value_size, args.batch_size, args.repeat) < 1:
        parser.error("counts/sizes must be positive")
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    with tempfile.TemporaryDirectory(dir=args.work_dir) as path, pyrex.PyRocksDB(path + "/db") as db:
        expected = hashlib.sha256()
        batch = pyrex.PyWriteBatch()
        for i in range(args.rows):
            key = f"k{i:018d}".encode()
            size = (0 if i % 8 == 0 else args.value_size * (1 + i % 9) // 5) if args.variable else args.value_size
            value = bytes([i % 256]) * size
            batch.put(key, value)
            update(expected, key, value)
            if (i + 1) % 5000 == 0:
                db.write(batch)
                batch = pyrex.PyWriteBatch()
        if args.rows % 5000:
            db.write(batch)
        samples = []
        stats = None
        for repeat in range(args.repeat + 1):
            start = time.perf_counter_ns()
            scan = db.scan_arrow(batch_size=args.batch_size, collect_metrics=args.metrics)
            with pa.RecordBatchReader.from_stream(scan) as reader:
                result = reader.read_all()
            elapsed = time.perf_counter_ns() - start
            observed = hashlib.sha256()
            for row in result.to_pylist():
                update(observed, row["key"], row["value"])
            assert observed.digest() == expected.digest()
            if repeat:
                samples.append(elapsed)
            stats = scan.stats
            del result
        print(json.dumps({"parameters": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                          "python": platform.python_version(), "pyarrow": pa.__version__,
                          "extension": pyrex._pyrex.__file__, "ordered_digest": expected.hexdigest(),
                          "samples_ns": samples, "median_ns": statistics.median(samples),
                          "native_stats_last_repeat_while_retained": stats,
                          "cache": "warm, full materialization; setup included; digest outside timing"}, indent=2))


if __name__ == "__main__":
    main()
