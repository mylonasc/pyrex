# Cumulative ABR-01–08 integration checkpoint

All indexed features have implementation/review PRs (#46–#53); this is not a
claim that they are merged/released. ABR-02 (#47) remains an independent draft
stack on ABR-01 until that base merges. The later source stack ends at ABR-08.
No automatic merge, version bump or package publication was performed.

## Native validation

Final cumulative full suite: **561 passed, 2 skipped**. Extension-ASan full
suite: **549 passed, 14 skipped**, no ASan errors. GCC ASan instruments the
extension, not the portable RocksDB archive; leak detection is disabled.
Allocator-interposition tests run separately, not under ASan; generic bounded
completion/close races do run under ASan. Sphinx HTML passes with warnings as
errors. Pre-existing untracked artifacts are not included in commits.

Feature evidence/limits are in `arrow_cancellation.md`, `arrow_points.md`,
`arrow_ranges.md`, `arrow_builder.md` and `transaction_arrow.md`. They cover
ordered/null semantics, ownership, checked limits, snapshots/CFs, cancellation,
allocation faults, immutable controls and transaction-local completion policy.

## Integration with the independent ABR-02 harness

The ABR-02 isolated runner was executed from its own branch/worktree against
the cumulative ABR-08 native artifact (CPython 3.12.3, PyArrow 26, GCC 13.3,
portable RocksDB 10.7.5, CPU 2 pinned, Arrow CPU/IO controls one, warm selected
iterator prepass). This verifies compatibility with the independent benchmark
dependency without merging or copying that PR into this stack.

- **216 fresh measurement workers**: 10K/100K scales × fixed/variable values ×
  full/prefix/range scopes × six iterator/Arrow/Polars modes × three repeats.
- **18 fresh measurement workers**: 1M fixed/full × six modes × three repeats.
- Both manifests report `status=ok`, no skipped modes; all actual ordered
  output digests/counts match reference data in every sample.
- No 10M or cold-cache claim. Raw local JSON is retained at
  `/tmp/opencode/abr-08/isolated-{matrix,1m}.json` (not public downloads).

1M observations: median extraction and complete validation-inclusive operation
wall time in milliseconds, and per-worker peak RSS in MiB:

| Mode | Extraction ms | Operation wall ms | Worker peak MiB |
|---|---:|---:|---:|
| Python iterator collect | 634.348 | 859.415 | 383.6 |
| Python iterator → Arrow | 656.688 | 717.871 | 498.7 |
| Native materialization | 160.780 | 207.342 | 239.7 |
| Native streaming compute | 144.344 | 190.497 | 152.4 |
| Polars materialization | 175.247 | 252.484 | 401.8 |
| Polars streaming compute | 166.139 | 217.419 | 191.5 |

These are different consumer workloads, not interchangeable operations.
Streaming extraction uses segmented clocks around validation pauses, not a
continuous-throughput certification. Worker RSS includes imports/open/warmup/
validation/cleanup, not net producer allocations. Full materialization retains
the full output. Neither a batch cap nor these examples establishes a universal
memory bound, cold-IO speedup or decoded-application performance.

```sh
PYTHONPATH=/path/to/cumulative/module python /path/to/ABR-02/benchmarks/bench_arrow_scan.py \
  --isolated --scales 10000 100000 --value-shapes fixed variable \
  --workloads full prefix range --include-polars --repeat 3 --cpu 2 \
  --build-label ABR-08-cumulative --output matrix.json
PYTHONPATH=/path/to/cumulative/module python /path/to/ABR-02/benchmarks/bench_arrow_scan.py \
  --isolated --scales 1000000 --value-shapes fixed --workloads full \
  --include-polars --repeat 3 --cpu 2 --build-label ABR-08-cumulative --output 1m.json
```
