"""ABR-03 no-request overhead probe, runnable on ABR-01 and ABR-03 builds.

Use PYTHONPATH to select SAME-toolchain native artifacts; alternate fresh
processes/build order. This probe issues NO cancel requests: it measures
polling overhead, separately from controlled cancellation latency tests.
It checks row/value-byte totals, not a full ordered digest (use ABR-02 for
that). Native metrics are a last-repeat snapshot, not median phase times.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import statistics
import tempfile
import time

import pyarrow as pa
import pyrex

from bench_arrow_scan import bench_native_scan_compute


def measure(args):
    results = []
    with tempfile.TemporaryDirectory(prefix="pyrex-cancel-overhead-", dir=args.work_dir) as temporary:
        with pyrex.PyRocksDB(str(Path(temporary) / "db")) as db:
            batch = pyrex.PyWriteBatch()
            for index in range(args.rows):
                batch.put(f"k{index:018d}".encode(), b"v" * args.value_size)
                if (index + 1) % 5000 == 0:
                    db.write(batch)
                    batch = pyrex.PyWriteBatch()
            if args.rows % 5000:
                db.write(batch)
            for batch_size in args.batch_sizes:
                for metrics in (False, True):
                    operation = bench_native_scan_compute(db, batch_size, args.max_batch_bytes,
                                                         collect_metrics=metrics)
                    operation()  # same worker's warm prepass; no cold claim
                    samples = []
                    for _ in range(args.repeat):
                        start = time.perf_counter_ns()
                        output = operation()
                        samples.append(time.perf_counter_ns() - start)
                        assert output[0] == args.rows * args.value_size and output[2] == args.rows
                    results.append({
                        "batch_size": batch_size, "collect_metrics": metrics,
                        "samples_ns": samples, "median_ns": statistics.median(samples),
                        "min_ns": min(samples), "max_ns": max(samples),
                        "native_stats_last_repeat": operation.last_scan.stats,
                    })
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--value-size", type=int, default=64)
    parser.add_argument("--batch-sizes", type=int, nargs="+", default=[256, 4096, 65536])
    parser.add_argument("--max-batch-bytes", type=int, default=8 * 1024 * 1024)
    parser.add_argument("--repeat", type=int, default=7)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--label", default="unspecified native artifact")
    args = parser.parse_args()
    if min([args.rows, args.value_size, args.max_batch_bytes, args.repeat] + args.batch_sizes) < 1:
        parser.error("counts and sizes must be positive")
    if args.cpu is not None:
        if not hasattr(os, "sched_getaffinity") or args.cpu not in os.sched_getaffinity(0):
            parser.error("requested CPU is not available")
        os.sched_setaffinity(0, {args.cpu})
    if args.work_dir is not None:
        args.work_dir.mkdir(parents=True, exist_ok=True)
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    print(json.dumps({
        "parameters": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
        "python": platform.python_version(), "pyarrow": pa.__version__,
        "extension": pyrex._pyrex.__file__,
        "affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
        "cancellation_requests": "none; polling-overhead probe only",
        "results": measure(args),
    }, indent=2))


if __name__ == "__main__":
    main()
