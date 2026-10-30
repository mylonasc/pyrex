# ABR-04: Arrow point reads

`multi_get_arrow` and `multi_get_arrow_cf` preserve key order/duplicates and
use value validity to distinguish missing from stored empty values. Binary
Arrow input (including slices) is validated and copied natively; existing
list-returning MultiGet APIs are unchanged. See the Arrow scan guide for
snapshot, cancellation, limits and lifetime contracts.

## Reproduction

```sh
PYTHONPATH=/path/to/module python benchmarks/bench_arrow_points.py \
  --rows 10000 --repeat 7 --batch-size 4096 > points-10k.json
PYTHONPATH=/path/to/module python benchmarks/bench_arrow_points.py \
  --rows 65536 --repeat 7 --batch-size 4096 > points-65k.json
```

Both paths materialize a full Arrow table from the same prepared DB and shared
snapshot. Keys are reverse ordered with duplicates and 10% missing; values
are 64 bytes. An ordered SHA-256 digest includes lengths, null markers, keys
and values and is checked outside every timed operation. Native timings include
input capture, snapshot/stream establishment, lookup and Arrow consumption;
baseline timings include list-returning MultiGet and Arrow reconstruction.
Inputs are prebuilt for both paths. Each path warms before seven repeats,
with order alternating each repeat. Arrow CPU/IO thread controls are one.

## Local observations

CPython 3.12.3, PyArrow 26.0.0, GCC 13.3, portable RocksDB 10.7.5; batch rows
4096. Milliseconds for full table materialization, median (min–max):

| Input rows | MultiGet + Arrow | Arrow-native point stream |
|---:|---:|---:|
| 10000 | 6.329 (5.651–6.551) | 5.428 (5.188–5.849) |
| 65536 | 45.764 (42.220–47.741) | 38.140 (36.800–38.647) |

These are warm single-process examples, not cold-cache/concurrent throughput
or RSS results. Chunked native MultiGet amortizes calls but still stages up to
256 values whose sizes are unknown before lookup. Output limits are not an
input/prefetch-memory cap; point streams accept at most 65536 keys. No claim
of zero-copy RocksDB storage or universally faster lookup is made.

Ordered digests: 10K `1f8fdea1f6d9c64b07623eb1950ee24645492eb42850506b191e97ffd65818b2`;
65536 `c1a07977c04494ed42e616db9ecf4e124b2dbc230e450918b4e27fa63f71f4a0`.
Raw local JSON: `/tmp/opencode/abr-04/points-{10k,65k}.json` (not public downloads).

Tests cover all three DB kinds, sliced binary/large-binary inputs, malformed
offsets, invalid/null inputs, row/byte boundaries, missing/empty values, shared
snapshot release, CF identity/drop, committed-state TransactionDB semantics,
detached bitmap/data ownership, cancellation during native gather/build,
bitmap allocation failure and bounded pull/DB-close races.

Validation: **347 passed, 2 skipped** normally; **338 passed, 11 skipped** with
the extension instrumented using GCC ASan, no ASan errors. Allocator-latch/
failure tests skip under ASan. RocksDB itself is uninstrumented and leak
detection is disabled (`detect_leaks=0:halt_on_error=1`); this is not full-stack
or leak certification. Sphinx HTML passes with warnings treated as errors.
