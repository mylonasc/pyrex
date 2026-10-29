# Point-read regression: 0.7.1 follow-up (#36)

## Outcome

The ordinary **hit** workload recovered the 0.4.1 baseline without removing
lifecycle protection, retaining the GIL during IO, or removing snapshot/CF/
options APIs. The earlier claim that recovery required unsafe operations
was incorrect. Optional-argument binding conversion was a substantial cost
in this configuration, not an unavoidable consequence of safe lifetimes.

These are Python-to-native binding measurements, not native C++-only or
GestaltDB `GraphDB.get_node()` measurements. The historical end-to-end
regression was not rerun here. No universal latency/throughput guarantee is
inferred from the warmed microbenchmark.

## Method and environment

- Linux 7.0.0-38-generic, x86_64/glibc 2.39; Intel i7-13700F.
- CPython 3.12.3, pybind11 3.1.0, GCC/G++ 13.3.0, `-O2 -g -DNDEBUG`,
  `-fPIC -std=c++20`. Same interpreter and build/link settings for **source
  builds** of tags v0.4.1, v0.7.0, the candidate, and the ablation below.
- Same portable RocksDB 10.7.5 static archive and system compression/uring
  libraries for every build. Archive SHA256:
  `f9b2c8b7d50a4e6acdbb94bd9e1e78f3d5570250d2d639e907fc03ba7e3c715e`.
- CPU affinity fixed to logical CPU 2. Three fresh-process invocations per
  build; build order reversed on the second round. No concurrent builds,
  benchmarks or test jobs during measurements. This does not eliminate
  background OS activity or CPU-frequency variation.
- Fresh DB per kind/process; 5,000 precomputed byte keys (16 bytes), 100-byte
  JSON-string payloads. 1,000 warmups, 40,000 reads per sample, seven samples
  per mode; mode order rotated/reversed. No explicit flush: this small
  dataset mainly exercises warm in-memory reads, not cold SST IO.
- All timed modes use a single Python wrapper around cached callables;
  results verified before timing. No extra lambda/attribute-lookup penalty
  assigned only to the options/snapshot modes. Separate decode-only and
  decoded-read modes are synthetic controls, not graph entity construction.
- `bench_point_read.py` records every sample, selected module/extension
  path and process affinity in JSON. Installed package metadata is recorded
  separately: with PYTHONPATH it need not describe the selected extension.

Example (repeat in alternating build order with independent output files):

```sh
PYTHONPATH=/path/to/source-build/module python benchmarks/bench_point_read.py \
  --label candidate --db-kind all --cpu 2 --nkeys 5000 --key-size 16 \
  --vsize 100 --nreads 40000 --warm 1000 --trials 7 > candidate.json
```

Archived revisions were compiled against the **same** already-built RocksDB
archive, rather than allowing old build scripts/cache keys to select other
native dependencies. Runtime extension paths and compiler/linker commands
were verified for each build. Do not substitute a PyPI wheel for the
historical source build and call that an identical-toolchain comparison.

## Ordinary get: microseconds/read

Median of three process medians; parentheses show their min–max, **not** a
confidence interval. Each process median summarizes seven timed samples.

| DB | v0.4.1 source | v0.7.0 source | Candidate (0.7.1) |
|---|---:|---:|---:|
| Plain | 0.631 (0.616–0.633) | 0.780 (0.772–0.788) | **0.536 (0.523–0.548)** |
| Extended | 0.646 (0.641–0.653) | 0.791 (0.771–0.819) | **0.549 (0.540–0.569)** |
| TransactionDB | 0.583 (0.580–0.590) | 0.775 (0.754–0.833) | **0.540 (0.520–0.589)** |

Candidate ordinary hits were about 30–31% faster than pristine 0.7.0 here,
while retaining a shared lifecycle lock across each native Get. Historical
TransactionDB reads held the GIL; candidate reads still release it.

## General paths and decode controls

Microseconds/read, median of process medians:

| Mode | Plain 0.7 → candidate | Extended 0.7 → candidate | TransactionDB 0.7 → candidate |
|---|---:|---:|---:|
| Explicit None | 0.781 → 0.536 | 0.792 → 0.548 | 0.773 → 0.540 |
| Explicit ReadOptions | 0.734 → 0.620 | 0.748 → 0.633 | 0.731 → 0.622 |
| Explicit snapshot | 0.895 → 0.771 | 0.900 → 0.796 | 0.889 → 0.782 |
| Explicit default CF | n/a | 0.818 → 0.801 | 0.803 → 0.793 |
| Synthetic decoded read | 1.786 → 1.541 | 1.799 → 1.563 | 1.768 → 1.567 |
| Decode only | 0.908 → 0.909 | 0.911 → 0.929 | 0.915 → 0.916 |

The general implementation remains in use for actual options/snapshot/CF
objects, with owned key storage, copied options and validation. Not all of
these paths match 0.4.1: for example, plain explicit-options reads were
0.552 µs in the historical source build versus 0.620 in the candidate.
Snapshot reads were unavailable in 0.4.1. CF figures are not evidence of
a meaningful improvement; their distributions overlap substantially.

## Binding experiments

- **No-dispatch ablation:** same candidate native code/options/lifetime
  design, but bound directly through the general typed shared-pointer
  signature. Ordinary reads: plain **0.796**, extended **0.757**,
  transactional **0.727** µs. Replacing only the binding dispatch with the
  candidate helper gave the ordinary-read figures above. This isolates a
  substantial binding contribution; it does not measure mutex cost.
- **Rejected one-key overload:** an earlier experiment made ordinary hits
  fast, but placed a failed-overload dispatch penalty on explicit calls.
  Plain explicit-options/snapshot medians were approximately **0.891/1.110**
  µs in that experiment. It is **not shipped**. The final single dispatcher
  checks borrowed optional handles for None and converts real objects to
  shared ownership before calling the retained general implementation.

No locking scheme was removed to obtain the candidate results. None of
these experiments establishes a lower bound on safe performance.

## Additional ordinary-read profiles

Two fresh processes/build, five samples/process, 20,000 reads/sample,
otherwise the same setup. Values are medians of two process medians (µs).

| Profile / DB | v0.4.1 source | v0.7.0 source | Candidate |
|---|---:|---:|---:|
| 256-byte key / plain | 0.690 | 0.838 | 0.592 |
| 256-byte key / extended | 0.668 | 0.836 | 0.614 |
| 256-byte key / transactional | 0.648 | 0.872 | 0.583 |
| 4096-byte value / plain | 0.797 | 1.037 | 0.746 |
| 4096-byte value / extended | 0.817 | 1.006 | 0.716 |
| 4096-byte value / transactional | 0.764 | 0.963 | 0.763 |
| Missing / plain | 0.509 | 0.765 | 0.607 |
| Missing / extended | 0.660 | 0.681 | 0.496 |
| Missing / transactional | 0.515 | 0.765 | 0.478 |

Misses were notably variable: plain candidate process medians were
0.567–0.648 µs versus 0.455–0.563 in 0.4.1. **Do not claim full recovery
for every workload.** These miss/size probes are exploratory, not statistical
certification or disk/cache/concurrent-reader throughput measurements.

## Correctness and remaining work

The final implementation keeps the key owner alive, checks DB pointers under
the lifecycle lock and publishes immutable read configurations via atomic
flags. General paths still validate snapshot/CF identity and lifetime.
New bounded subprocess tests cover reads racing close, snapshot release,
CF drop, mutable/replaced options, and writers, across all three DB kinds.
Tests verify returned values and supported positional/keyword/None call
forms, including TypeError for invalid options/snapshots.

CPython 3.12: 167 tests passed with Arrow/Polars installed; one skipped case is the plain
DB CF-drop race (plain DB has no CF API). Passing these stress tests is not
a proof of every possible interleaving. ThreadSanitizer and true
free-threaded Python support were not validated.

The local CPython 3.14t environment was rebuilt too, with the extension
enabling the GIL: 164 passed, four skipped (the CF case above and three
environment-dependent existing skips). That is not a GIL-disabled test.

`PinnableSlice`, operation leases/deferred reclamation, and broader
concurrent/disk-backed measurements remain separate candidates. They are
not necessary for the ordinary-hit recovery shown here and have not been
shipped as unproved lifetime shortcuts.

Full local JSON/build logs are in `/tmp/opencode/point-read-review/` in the
investigation environment (not a public download); the method, environment,
process distributions and results above do not rely on access to that path.
