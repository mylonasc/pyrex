"""ABR-04: ordered point-read materialization vs MultiGet + Arrow rebuilding.

Same prepared DB, explicit shared snapshot, warmed repeats; setup of each
point stream and Arrow consumption is timed. No cold-cache or RSS claim.
"""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import tempfile
import time

import pyarrow as pa
import pyrex


def digest(table):
    result = hashlib.sha256()
    for row in table.to_pylist():
        for field in ("key", "value"):
            value = row[field]
            result.update(b"N" if value is None else b"V" + len(value).to_bytes(8, "little") + value)
    return result.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--repeat", type=int, default=7)
    parser.add_argument("--batch-size", type=int, default=4096)
    parser.add_argument("--value-size", type=int, default=64)
    parser.add_argument("--work-dir", type=Path)
    args = parser.parse_args()
    if not 1 <= args.rows <= 65536 or min(args.repeat, args.batch_size, args.value_size) < 1:
        parser.error("rows must be 1..65536; other counts must be positive")
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    with tempfile.TemporaryDirectory(dir=args.work_dir) as path, pyrex.PyRocksDB(path + "/db") as db:
        batch = pyrex.PyWriteBatch()
        for i in range(args.rows):
            batch.put(f"k{i:08d}".encode(), b"v" * args.value_size)
        db.write(batch)
        # Duplicates, reverse order and 10% misses; both paths use identical keys.
        keys = [f"{'m' if i % 10 == 0 else 'k'}{(args.rows - i - 1) // 2:08d}".encode()
                for i in range(args.rows)]
        arrow_keys = pa.array(keys, type=pa.binary())
        snapshot = db.create_snapshot()
        schema = pa.schema([pa.field("key", pa.binary(), nullable=False), pa.field("value", pa.binary())])
        def baseline():
            return pa.Table.from_arrays([arrow_keys, pa.array(db.multi_get(keys, snapshot=snapshot), type=pa.binary())],
                                        schema=schema)
        def native():
            scan = db.multi_get_arrow(arrow_keys, snapshot=snapshot, batch_size=args.batch_size)
            with pa.RecordBatchReader.from_stream(scan) as reader:
                return reader.read_all()
        expected = digest(baseline())
        assert digest(native()) == expected
        samples = {"multi_get_then_arrow": [], "multi_get_arrow": []}
        for repeat in range(args.repeat):
            order = [("multi_get_then_arrow", baseline), ("multi_get_arrow", native)]
            if repeat % 2:
                order.reverse()
            for name, operation in order:
                start = time.perf_counter_ns()
                result = operation()
                samples[name].append(time.perf_counter_ns() - start)
                assert digest(result) == expected
                del result
        print(json.dumps({"rows": args.rows, "value_size": args.value_size, "batch_size": args.batch_size,
                          "pyarrow": pa.__version__, "ordered_digest": expected,
                          "cache": "warm; same DB/shared snapshot; alternating order",
                          "samples_ns": samples,
                          "median_ns": {k: statistics.median(v) for k, v in samples.items()}}, indent=2))


if __name__ == "__main__":
    main()
