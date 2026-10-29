"""Native sequence-accessor overhead and ordinary-operation regression probe.

Run the same command against baseline and feature builds (via PYTHONPATH).
The baseline may lack the new properties; ordinary operations still run.
No timing thresholds are asserted: compare medians on an otherwise idle host.
"""

import argparse
import json
from pathlib import Path
import statistics
import tempfile
import timeit

import pyrex


def measure(operation, iterations, repeats):
    samples = timeit.repeat(operation, number=iterations, repeat=repeats)
    return {"median_ns": statistics.median(samples) * 1e9 / iterations}


def benchmark(rows, iterations, repeats):
    with tempfile.TemporaryDirectory(prefix="pyrex-sequences-") as tmp:
        with pyrex.TransactionDB(str(Path(tmp) / "db")) as db:
            batch = pyrex.PyWriteBatch()
            for i in range(rows):
                batch.put(f"key:{i:08d}".encode(), b"value")
            db.write(batch)
            options = pyrex.TransactionOptions()
            options.set_snapshot = True
            with db.transaction(transaction_options=options) as txn:
                def read():
                    return txn.get(b"key:00000000")

                def transaction():
                    with db.transaction() as ordinary:
                        ordinary.get(b"key:00000000")
                        ordinary.commit()

                def write_transaction():
                    with db.transaction() as writer:
                        writer.put(b"write-key", b"value")
                        writer.commit()

                operations = {
                    "transaction_get": read,
                    "read_transaction": transaction,
                    "write_transaction": write_transaction,
                }
                if hasattr(type(txn), "snapshot_sequence_number"):
                    operations["snapshot_sequence_number"] = lambda: txn.snapshot_sequence_number
                if hasattr(type(db), "latest_sequence_number"):
                    operations["latest_sequence_number"] = lambda: db.latest_sequence_number
                return {
                    name: measure(operation, iterations, repeats)
                    for name, operation in operations.items()
                }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", nargs="+", type=int, default=[1, 100000])
    parser.add_argument("--iterations", type=int, default=10000)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    if min(args.rows) < 1 or args.iterations < 1 or args.repeats < 1:
        parser.error("rows, iterations and repeats must be positive")
    print(json.dumps({
        "module": pyrex.__file__,
        "iterations": args.iterations,
        "repeats": args.repeats,
        "results_by_rows": {
            str(rows): benchmark(rows, args.iterations, args.repeats)
            for rows in args.rows
        },
    }, indent=2))


if __name__ == "__main__":
    main()
