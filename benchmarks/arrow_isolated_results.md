# ABR-02: isolated runner validation

This validates the harness and demonstrates what it reports. It is not a
release-to-release speedup claim, a cold-cache result or a hard memory bound.
The existing quick benchmark remains available without `--isolated`.

## Execution coverage

- A small 72-worker run: all six modes, full/prefix/range scans, fixed and
  variable values, two repeats. Actual ordered output and compute counts
  matched generator references in every sample.
- A **216-worker matrix**: 10K and 100K rows, fixed/variable 64-byte-maximum
  values, all three scopes, all six modes, three independent repeats.
- An **18-worker profile**: 1M rows, fixed 64-byte values, full scans, all six
  modes, three independent repeats.
- 10M and the full 1M multi-scope/variable matrix were **not executed**.
  Their opt-in execution/resource plan is in `docs/source/arrow_benchmarks.rst`.

Each dataset was created in a separate process. Each measured mode/repeat
opened it read-only in a fresh process; jobs ran sequentially. The worker
performed its own selected-scope iterator prepass, then scan setup, operation,
ordered validation and cleanup. This is the `warm` protocol, not a claim that
all records fit in cache. No global caches were dropped.

Host: i7-13700F, Linux 7.0.0-38-generic x86_64/glibc 2.39; CPython 3.12.3,
PyArrow 26.0.0, Polars 2.1.0. Read workers were pinned to CPU 2, with Arrow
CPU/IO and Polars controls set to one thread (RocksDB background defaults
unchanged). The selected local extension implements ABR-01; declared native
build provenance is GCC 13.3 `-O2`, portable RocksDB 10.7.5. Installed package
metadata alone does not identify this PYTHONPATH-selected source extension.

Settings: seed 1729; batch size 4096; 8 MiB output cap; fill_cache and checksum
verification enabled; no singleton overflow. The 1M profile contains 100K
`other/` records and 900K `sensor/` records. Reference:

- Key bytes: **18,900,000**; value bytes: **64,000,000**.
- Digest scheme: `ABR-02/v1/four-sha256-streams-native-u64` (little-endian lengths).
- Ordered digest: `ac46aa7ac757ababef86e93071d6e990599f9a78fb250ba4f3c803e804a1387c`.

## 1M full scan results

Median of three fresh workers. Parentheses show min–max, not confidence
intervals. RSS is each worker's high-water through cleanup, including
imports, DB open/warmup, operation, validation and retained output—not
incremental producer allocations.

| Mode | Extraction/compute ms | Operation wall ms (includes validation) | Worker peak MiB |
|---|---:|---:|---:|
| Python iterator collect | 629.05 (628.31–636.18) | 854.70 (853.96–862.92) | 383.4 |
| Python iterator → Arrow | 660.80 (660.19–661.04) | 722.93 (722.26–723.95) | 498.8 |
| Native → PyArrow materialize | 206.62 (205.72–207.28) | 253.04 (252.20–253.79) | 243.2 |
| Native → Arrow streaming compute | 195.72 (194.36–199.64) | 245.55 (244.26–249.53) | 152.7 |
| Native → Polars materialize | 222.39 (222.14–222.50) | 300.62 (300.15–301.29) | 405.5 |
| Native → Polars streaming compute | 211.09 (210.71–218.40) | 280.20 (279.72–288.36) | 192.1 |

**Streaming extraction is segmented**: native pull/compute is timed, then
paused to hash the actual consumed buffers before releasing the batch.
Operation wall includes validation pauses and harness work. These are not
uninterrupted-throughput measurements or identical consumer workloads.
Python-only validation also has a different cost from buffer-based hashing;
report both phase and validation-inclusive times rather than mixing them.

## Output ownership evidence

All native modes emitted **90,901,960 requested data/offset bytes**, with no
empty-column slack in this profile. Materialized PyArrow and Polars results
retained that exported capacity at the end of the operation. Streaming modes
peaked at **372,744 live exported bytes** and ended the operation at zero.
Every native mode reached zero exported bytes after consumer release.

The streaming worker peaks were still 152.7/192.1 MiB, much greater than
372,744 bytes. That is expected: native output counters do not measure
interpreter/library overhead, DB/cache state, all staging capacity, consumer
copies or allocator overhead. A full table retaining all output is not a
bounded-memory streaming measurement.

Native gather/build clocks, scan setup, validation, GC-inclusive consumer
release and DB/native close are stored separately in the JSON; they are not
interchangeable with the total wall time.

## Reproduction

```sh
PYTHONPATH=/path/to/ABR-01-or-later/module python benchmarks/bench_arrow_scan.py \
  --isolated --scales 10000 100000 --value-shapes fixed variable \
  --workloads full prefix range --include-polars --value-size 64 \
  --batch-size 4096 --repeat 3 --cpu 2 --threads 1 \
  --build-label 'record actual compiler/flags/RocksDB provenance here' \
  --work-dir /large/scratch --output matrix.json

PYTHONPATH=/path/to/ABR-01-or-later/module python benchmarks/bench_arrow_scan.py \
  --isolated --scales 1000000 --value-shapes fixed --workloads full \
  --include-polars --value-size 64 --batch-size 4096 --repeat 3 \
  --cpu 2 --threads 1 --timeout 600 --work-dir /large/scratch \
  --output million.json
```

The default quick benchmark and isolated `--no-metrics` mode were also
smoke-tested with the historical 0.7.0 source extension. Without native
metrics, the corresponding fields are null, not inferred capacities.

Raw JSON is saved in `/tmp/opencode/abr02-scale/report.json` and
`/tmp/opencode/abr02-million/report.json` in the investigation environment
(not public downloads). The recorded method, inputs, digest and distributions
above make the validation independent of access to those temporary paths.
