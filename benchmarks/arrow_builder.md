# ABR-07: contiguous value adoption

Scan/range-union values no longer allocate a payload string per record and
then explicitly memcpy those payloads into output. Instead they append into
one geometrically grown malloc/realloc buffer and Arrow adopts it after an
exact-size shrink. Keys remain string-staged; point-prefetched values retain
their existing path. No buffer is reused after publication. This is **not**
zero-copy RocksDB storage: gathering copies once, and realloc can move/copy
internally. Int32/used-byte/singleton checks still precede append/allocation.

## Same-toolchain comparison

`bench_arrow_builder.py` selects artifacts through PYTHONPATH and validates
an ordered SHA-256 digest outside every timed operation. Timings include
stream establishment and full Arrow table materialization, not streaming RSS.
The prepared DB has 19-byte keys and fixed or deterministic variable values
(including empty values). Arrow CPU/IO controls are one. Every process warms
then records seven repeats. Three fresh processes/build are interleaved,
reversing build order in the middle round. CPython 3.12.3, PyArrow 26.0.0,
GCC 13.3 `-O2`, same portable RocksDB 10.7.5 archive; batch rows 4096.

Median of process medians in milliseconds (range of those medians):

| Rows / values | Metrics | Before ABR-06 | After ABR-07 |
|---|---|---:|---:|
| 100K / 64 bytes | Off | 13.618 (13.602–13.741) | 10.915 (10.873–11.125) |
| 100K / 64 bytes | On | 13.610 (13.603–13.769) | 11.431 (10.841–11.833) |
| 100K / variable, nominal 1024 | Off | 75.601 (74.634–76.397) | 67.982 (67.824–69.047) |
| 100K / variable, nominal 1024 | On | 75.868 (75.567–78.433) | 67.198 (66.967–69.382) |
| 10K / 4096 bytes | Off | 7.156 (6.973–7.267) | 4.740 (4.636–4.864) |
| 10K / 4096 bytes | On | 7.174 (6.835–7.549) | 4.641 (4.607–4.833) |

No regression was observed in these warm examples. These are not confidence
intervals or claims about cold IO, concurrent throughput or every value size.

## Copies, staging and requested capacity

| Workload | Value payload bytes | Requested output capacity before/after | Output slack before/after | Peak staged payload before/after |
|---|---:|---:|---:|---:|
| 100K / 64 | 6400000 | 9100200 | 0 | 339968 |
| 100K / variable | 89559677 | 92259877 | 0 | 3747681 |
| 10K / 4096 | 40960000 | 41230040 | 0 | 8369910 |

Before: each emitted value payload is explicitly copied from strings to Arrow.
After: `value_bytes_copied_to_output=0` for scans, because Arrow adopts the
gathered value allocation. Point streams still report their explicit copy.
This metric excludes realloc-internal copying and failed attempts. Payload
staging bytes intentionally stay the same; removed per-row string metadata,
spare contiguous growth capacity and allocator overhead are not measured RSS.
The builder no longer creates a vector of per-value strings for scan batches.

## Validation / reproduction

Full suite: **533 passed, 2 skipped**. Extension-ASan: **524 passed, 11 skipped**,
no errors; RocksDB archive uninstrumented, leak detection disabled, allocator
interposition tests skipped. ASan options: `detect_leaks=0:halt_on_error=1`.
Fault injection now counts and fails every actual schema/output allocation,
including realloc growth/shrink, without production hooks. Existing cancellation
latches still exercise real gather/build callbacks. Retained tables, empty/
variable/large payloads, byte limits and detached child releases remain covered.

```sh
PYTHONPATH=/path/to/before-or-after/module python benchmarks/bench_arrow_builder.py \
  --rows 100000 --value-size 1024 --variable --repeat 7 --metrics
```

Raw local repeats: `/tmp/opencode/abr-07/comparison/` (not public downloads).
ABR-02's broader isolated-worker scale/interoperability harness remains the
delivery dependency; this targeted comparison does not replace it.
