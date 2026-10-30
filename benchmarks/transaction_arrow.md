# ABR-08: transaction-local Arrow view

`Transaction.scan_arrow` / `scan_arrow_cf` read private writes/deletes using
native transaction iterators, unlike committed-state TransactionDB scans.
Existing transaction snapshot (if set) supplies the committed horizon;
otherwise the Arrow view pins one at creation. Native Arrow resources freeze
transaction mutation, snapshot reset, get-for-update, commit and rollback.
Ordinary reads stay allowed; close/exhaust all streams before completion.
DB teardown deletes Arrow iterators before their transaction write batches.
See the transaction guide for the full contract and context nesting.

## Ordered transaction-local benchmark

```sh
PYTHONPATH=/path/to/module python benchmarks/bench_transaction_arrow.py \
  --rows 10000 --repeat 7 > transaction-10k.json
```

The baseline is **transaction-local iterator + Arrow reconstruction**, not a
committed DB scan. Both paths use the same transaction snapshot, private
updates (30% of prepared rows), deletes (10%) and one private-only key. The
prepared DB uses 13-byte keys and 9-byte committed values; private updates
have 63-byte values. Each path warms and then records seven alternating-order
repeats. Timings include iterator/stream setup and full Arrow materialization;
ordered length-prefixed key/value digests are checked outside every timing.
Arrow CPU/IO controls are one. CPython 3.12.3, PyArrow 26, GCC 13.3, portable
RocksDB 10.7.5; native batch rows 4096.

Milliseconds, median (min–max), from one warm local process per size:

| Prepared rows | Transaction iterator + Arrow | Transaction Arrow stream |
|---:|---:|---:|
| 10000 | 8.951 (8.920–9.092) | 4.014 (3.987–4.052) |
| 100000 | 95.269 (94.336–96.405) | 43.191 (43.070–43.261) |

This is not cold-cache/concurrent throughput or an RSS bound. Digests:
10K `eb09446e67decab7ac02be659b4d4c40da7d3f4589cbf86fd1388f63d88564d6`;
100K `e83daf994fa7123b9d7519f061f0af177e7a81eae4f89f543068dee7a15dc3fc`.
Raw local JSON: `/tmp/opencode/abr-08/transaction-{10k,100k}.json`.

## Validation

Full suite: **561 passed, 2 skipped**. Extension-ASan: **549 passed, 14 skipped**,
no errors. RocksDB archive uninstrumented, leak detection disabled
(`detect_leaks=0:halt_on_error=1`); allocator-interposition tests skipped.
Generic pull/commit/rollback/DB-close races still run under ASan in bounded
subprocesses. Native allocation latches separately exercise an actual gather
while racing completion/close and requesting lock-independent cancellation.
Every actual transaction-output allocation is fault-injected separately;
failed attempts do not leave mutation frozen. Coverage also includes CF
identity/drop protection, own writes/deletes, other uncommitted invisibility,
snapshot/no-snapshot horizons, multiple active streams, errors, repeated EOF/
cancellation after completion and retained output after transaction/DB close.
