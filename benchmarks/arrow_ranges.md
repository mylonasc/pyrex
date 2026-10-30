# ABR-05: ordered range union

The union API sorts/merges bytewise ranges and prefixes, emitting matching
records once in key order at one creation-time snapshot. Native seeks skip
gaps; output remains lazy and bounded by row/used-byte limits. Selector input
is owned, and retained output keeps independent Arrow ownership.

```sh
PYTHONPATH=/path/to/module python benchmarks/bench_arrow_ranges.py \
  --rows 100000 --ranges 100 --repeat 7 > ranges.json
```

Local warm example (CPython 3.12.3, PyArrow 26, GCC 13.3, portable RocksDB
10.7.5): 100K prepared 13-byte keys / 64-byte values, 100 disjoint intervals,
each selecting 10% of its partition; same explicit snapshot for both paths.
Seven alternating-order repeats, materializing a full Arrow table:

| Path | Median ms | Min–max ms |
|---|---:|---:|
| Separate scans | 2.070 | 2.052–2.103 |
| Union stream | 1.438 | 1.417–1.528 |

Every repeat checks the same ordered length-prefixed key/value digest outside
timing: `1da484d9b35d77f79915e70a09547a7d7156ac4db67439c866a151c8df7ade78`.
This measures one warm materialization workload, not cold-cache performance,
RSS or universal speedup. Raw local data: `/tmp/opencode/abr-05/ranges.json`.

Full inherited/new suite: **473 passed, 2 skipped**. Warning-as-error Sphinx
HTML passes. Union-specific coverage includes all three DB types, randomized
overlaps, gaps, empty/unbounded/binary selectors, row/byte boundaries, selector
capture, shared snapshots, CF identity/drop protection and cancellation with
retained output. Existing native allocation-failure/lifecycle tests exercise
the shared Arrow builder.
