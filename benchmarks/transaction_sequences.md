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
