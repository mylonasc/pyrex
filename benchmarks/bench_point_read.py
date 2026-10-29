"""Warmed binding, decode-only and decoded-read probes for issue #36.

Use the SAME interpreter/toolchain/RocksDB for source-revision comparisons.
Select each build via PYTHONPATH and alternate fresh-process invocations.
Every timed mode uses a one-argument Python wrapper around a cached callable;
explicit-options/snapshot costs are not confounded with an extra lambda.
Samples rotate mode order and include their full distribution. No timing
thresholds are asserted. This is not the GestaltDB end-to-end benchmark.

Example: python benchmarks/bench_point_read.py --db-kind all --cpu 2
"""
import argparse
import importlib.metadata
import json
import os
import platform
import statistics
import tempfile
import time
from pathlib import Path

import pyrex


def measure(operation, keys, warm):
    for i in range(warm):
        operation(keys[i % len(keys)])
    start = time.perf_counter_ns()
    for key in keys:
        operation(key)
    return (time.perf_counter_ns() - start) / len(keys)


def distribution(samples):
    return {
        "median_ns": statistics.median(samples),
        "min_ns": min(samples),
        "max_ns": max(samples),
        "samples_ns": samples,
    }


def benchmark(kind, args):
    constructor = {
        "plain": pyrex.PyRocksDB,
        "extended": pyrex.PyRocksDBExtended,
        "txn": pyrex.TransactionDB,
    }[kind]
    with tempfile.TemporaryDirectory(prefix="pyrex-point-read-") as tmp:
        db = constructor(str(Path(tmp) / "db"))
        snapshot = None
        try:
            # JSON string of exactly vsize bytes; supports a decode control
            # without modifying the stored payload between modes/builds.
            value = b'"' + b'v' * (args.vsize - 2) + b'"'
            keys = [f"{i:016x}".encode().ljust(args.key_size, b"k")
                    for i in range(args.nkeys)]
            for key in keys:
                db.put(key, value)
            ids = [keys[i % len(keys)] for i in range(args.nreads)]
            if args.missing:
                ids = [b"z" + key[1:] for key in ids]
            get = db.get
            options = pyrex.ReadOptions()
            operations = {
                "ordinary": lambda key: get(key),
                "explicit_none": lambda key: get(key, None),
                "options": lambda key: get(key, options),
            }
            skipped = []
            if hasattr(db, "create_snapshot"):
                snapshot = db.create_snapshot()
                operations["snapshot"] = lambda key: get(key, snapshot=snapshot)
            else:
                skipped.append("snapshot (not supported by this build)")
            if hasattr(db, "get_cf"):
                get_cf = db.get_cf
                default_cf = db.default_cf
                operations["default_cf"] = lambda key: get_cf(default_cf, key)
            if not args.missing:
                decode = json.loads
                operations["decode_only"] = lambda key: decode(value)
                operations["decoded_read"] = lambda key: decode(get(key))
            # Correctness checks are outside the timed region, equally for
            # every path, including long/binary-ish keys and misses.
            for name, operation in operations.items():
                expected = None if args.missing else value
                if name in ("decode_only", "decoded_read"):
                    expected = json.loads(value)
                assert operation(ids[0]) == expected, name
            samples = {name: [] for name in operations}
            names = list(operations)
            for round_index in range(args.trials):
                order = names[round_index % len(names):] + names[:round_index % len(names)]
                if round_index % 2:
                    order.reverse()
                for name in order:
                    samples[name].append(measure(operations[name], ids, args.warm))
            return {
                "modes": {name: distribution(values) for name, values in samples.items()},
                "skipped": skipped,
            }
        finally:
            if snapshot is not None:
                snapshot.release()
            db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-kind", nargs="+", default=["plain"],
                        choices=["plain", "extended", "txn", "all"])
    parser.add_argument("--nkeys", type=int, default=5000)
    parser.add_argument("--key-size", type=int, default=16)
    parser.add_argument("--vsize", type=int, default=100)
    parser.add_argument("--nreads", type=int, default=100000)
    parser.add_argument("--warm", type=int, default=5000)
    parser.add_argument("--trials", type=int, default=9)
    parser.add_argument("--missing", action="store_true")
    parser.add_argument("--cpu", type=int, help="Pin this process (Linux sched_setaffinity)")
    parser.add_argument("--label", default="unspecified-build")
    args = parser.parse_args()
    if min(args.nkeys, args.nreads, args.trials) < 1 or args.key_size < 16 or args.vsize < 2 or args.warm < 0:
        parser.error("counts must be positive; key-size >= 16, vsize >= 2, warm >= 0")
    if args.cpu is not None:
        if not hasattr(os, "sched_setaffinity"):
            parser.error("CPU pinning is unavailable on this platform")
        os.sched_setaffinity(0, {args.cpu})
    kinds = ["plain", "extended", "txn"] if "all" in args.db_kind else args.db_kind
    # Package metadata can describe an installed wheel even with a source
    # build selected via PYTHONPATH; the extension path is authoritative.
    try:
        installed_version = importlib.metadata.version("pyrex-rocksdb")
    except importlib.metadata.PackageNotFoundError:
        installed_version = None
    print(json.dumps({
        "parameters": vars(args),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "module": pyrex.__file__,
        "extension": pyrex._pyrex.__file__,
        "installed_distribution_version": installed_version,
        "affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
        "results": {kind: benchmark(kind, args) for kind in kinds},
    }, indent=2))


if __name__ == "__main__":
    main()
