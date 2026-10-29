# Transaction sequence benchmark

`bench_transaction_sequences.py` measures the Python-to-native accessor cost,
pinned point reads, ordinary read transactions, and write transactions. Run it
against native builds of both `main` and the feature branch with identical Python,
compiler, RocksDB, and optimization settings on an otherwise idle host:

```bash
PYTHONPATH=/path/to/baseline/lib python benchmarks/bench_transaction_sequences.py
PYTHONPATH=/path/to/feature/lib python benchmarks/bench_transaction_sequences.py
```

Each run reports median nanoseconds per operation over five repeats, at one row
and 100,000 rows. Baseline builds without the new properties skip only the two
accessor measurements. Override `--rows`, `--iterations`, and `--repeats` for
longer runs. The write-transaction benchmark intentionally writes the same key,
so it measures ordinary transaction overhead rather than growing the key space.

Compare ordinary-operation medians between builds to look for regressions; compare
accessor medians across row counts to check size independence. This is a local
microbenchmark, not a timing assertion suitable for CI. No full-state hashing is
performed by these properties or by ordinary reads/transactions.

## Native validation

The feature was tested against a locally compiled RocksDB 10.7.5 extension on
Linux x86-64 with Python 3.14.2 (free-threading build, with the GIL automatically
enabled when loading PyRex). All 19 sequence-specific tests passed, including
synchronized concurrent commits and accessor/completion races. The full suite
passed with 134 tests and three skips for unavailable Polars integration.

## Local comparison

On an Intel Core i7-13700F, identical local release builds of `main` and the
feature branch (GCC, RocksDB 10.7.5, Python as above) produced the following
medians with the default 10,000 iterations and five repeats. Builds and tests
were finished before running benchmarks sequentially. Times include Python
property/method dispatch and native lifetime locking.

| Operation | Initial rows | Main (ns/op) | Feature (ns/op) |
| --- | ---: | ---: | ---: |
| Pinned transaction get | 1 | 516 | 496 |
| Read transaction | 1 | 3,085 | 2,974 |
| Write transaction | 1 | 3,529 | 3,405 |
| Snapshot sequence | 1 | unavailable | 136 |
| Latest sequence | 1 | unavailable | 130 |
| Pinned transaction get | 100,000 | 584 | 601 |
| Read transaction | 100,000 | 3,079 | 3,030 |
| Write transaction | 100,000 | 3,685 | 3,542 |
| Snapshot sequence | 100,000 | unavailable | 143 |
| Latest sequence | 100,000 | unavailable | 131 |

Accessor costs were approximately 0.13–0.14 microseconds across database sizes.
Ordinary reads/transactions showed no material regression in this local run
(the largest slowdown was about 3% for the larger pinned point-read case).
These noisy microbenchmarks are a sanity check, not a cross-platform performance
guarantee. The implementation leaves existing read/write operation paths unchanged.
