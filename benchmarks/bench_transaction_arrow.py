"""ABR-08: transaction-local iterator + Arrow vs transaction-local native stream.

Never compare private-write reads against committed-state TransactionDB scans.
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
        for name in ("key", "value"):
            value = row[name]
            result.update(len(value).to_bytes(8, "little") + value)
    return result.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--repeat", type=int, default=7)
    parser.add_argument("--work-dir", type=Path)
    args = parser.parse_args()
    if min(args.rows, args.repeat) < 1:
        parser.error("counts must be positive")
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    with tempfile.TemporaryDirectory(dir=args.work_dir) as path, pyrex.TransactionDB(path + "/db") as db:
        batch = pyrex.PyWriteBatch()
        for i in range(args.rows):
            batch.put(f"k{i:012d}".encode(), b"committed")
        db.write(batch)
        txn = db.begin_transaction()
        txn.set_snapshot()
        for i in range(args.rows):
            if i % 10 == 0:
                txn.delete(f"k{i:012d}".encode())
            elif i % 3 == 0:
                txn.put(f"k{i:012d}".encode(), b"private" * 9)
        txn.put(b"private-only", b"v" * 64)
        schema = pa.schema([pa.field("key", pa.binary(), nullable=False), pa.field("value", pa.binary(), nullable=False)])
        def iterator():
            keys, values = [], []
            it = txn.new_iterator()
            it.seek_to_first()
            while it.valid():
                keys.append(it.key())
                values.append(it.value())
                it.next()
            it.check_status()
            del it
            return pa.Table.from_arrays([pa.array(keys, type=pa.binary()), pa.array(values, type=pa.binary())], schema=schema)
        def native():
            with pa.RecordBatchReader.from_stream(txn.scan_arrow(batch_size=4096)) as reader:
                return reader.read_all()
        expected = digest(iterator())
        assert digest(native()) == expected
        samples = {"transaction_iterator_then_arrow": [], "transaction_arrow": []}
        for repeat in range(args.repeat):
            order = [("transaction_iterator_then_arrow", iterator), ("transaction_arrow", native)]
            if repeat % 2:
                order.reverse()
            for name, operation in order:
                start = time.perf_counter_ns()
                result = operation()
                samples[name].append(time.perf_counter_ns() - start)
                assert digest(result) == expected
                del result
        txn.rollback()
        print(json.dumps({"rows_prepared": args.rows, "ordered_digest": expected,
                          "cache": "warm, same transaction snapshot/private writes; alternating order",
                          "samples_ns": samples, "median_ns": {k: statistics.median(v) for k, v in samples.items()}}, indent=2))


if __name__ == "__main__":
    main()
