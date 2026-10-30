"""ABR-05: union stream vs separate scans sharing one explicit snapshot."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import tempfile
import time

import pyarrow as pa
import pyrex


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--ranges", type=int, default=100)
    parser.add_argument("--repeat", type=int, default=7)
    parser.add_argument("--work-dir", type=Path)
    args = parser.parse_args()
    if min(args.rows, args.ranges, args.repeat) < 1 or args.ranges > args.rows:
        parser.error("positive counts required; ranges <= rows")
    key = lambda i: f"k{i:012d}".encode()
    selectors = [(key(i * args.rows // args.ranges), key((i * args.rows // args.ranges) + max(1, args.rows // args.ranges // 10)))
                 for i in range(args.ranges)]
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    with tempfile.TemporaryDirectory(dir=args.work_dir) as path, pyrex.PyRocksDB(path + "/db") as db:
        batch = pyrex.PyWriteBatch()
        for i in range(args.rows):
            batch.put(key(i), b"v" * 64)
        db.write(batch)
        snap = db.create_snapshot()
        def consume(scan):
            with pa.RecordBatchReader.from_stream(scan) as reader:
                return reader.read_all()
        def separate():
            return pa.concat_tables([consume(db.scan_arrow(start=lo, stop=hi, snapshot=snap)) for lo, hi in selectors])
        def union():
            return consume(db.scan_arrow_ranges(selectors, snapshot=snap))
        def digest(table):
            h = hashlib.sha256()
            for row in table.to_pylist():
                for name in ("key", "value"):
                    value = row[name]
                    h.update(len(value).to_bytes(8, "little") + value)
            return h.hexdigest()
        expected = digest(separate())
        assert digest(union()) == expected
        samples = {"separate_scans": [], "union_stream": []}
        for repeat in range(args.repeat):
            order = [("separate_scans", separate), ("union_stream", union)]
            if repeat % 2:
                order.reverse()
            for name, operation in order:
                start = time.perf_counter_ns()
                result = operation()
                samples[name].append(time.perf_counter_ns() - start)
                assert digest(result) == expected
                del result
        print(json.dumps({"rows": args.rows, "ranges": args.ranges, "ordered_digest": expected,
                          "cache": "warm; same explicit snapshot; disjoint 10%-width intervals",
                          "samples_ns": samples, "median_ns": {k: statistics.median(v) for k, v in samples.items()}}, indent=2))


if __name__ == "__main__":
    main()
