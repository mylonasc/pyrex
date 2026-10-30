# Changelog

All notable changes to `pyrex-rocksdb` are documented here. This changelog
is maintained incrementally from this point onward; earlier history is
available via `git log`.

## [Unreleased]

### Added

- ABR-07 (#44): scan/range-union values now gather directly into a contiguous
  native buffer adopted by owned Arrow output, removing per-value payload
  strings and the explicit second value copy. Keys and point prefetch retain
  their compatible staging paths. Requested output capacity remains exact;
  realloc growth/shrink failures use tested terminal cleanup. New published
  `value_bytes_copied_to_output` metrics distinguish adoption from copying.

- ABR-06 (#43): versioned per-DB `arrow_capabilities()` and per-stream
  `configuration` snapshots describe actual schemas, limits, ordering,
  snapshot scope, cancellation and controls. `ReadOptions.readahead_size`
  adds an atomic iterator-only hint captured at creation, without changing
  the ordinary point-read fast path. Arrow iterators require total-order
  seeking; unsupported prefix/tailing/raw-bound options remain rejected.

- ABR-05 (#42): `scan_arrow_ranges()` / `scan_arrow_ranges_cf()` stream an
  ordered union of bytewise ranges and prefixes at one pinned snapshot.
  Overlaps/duplicates are normalized; native seeks skip gaps without
  full-scan materialization. Existing row/byte limits, cancellation, metrics
  and owned-buffer lifetimes apply across range transitions.

- ABR-04 (#41): `multi_get_arrow()` / `multi_get_arrow_cf()` provide lazy,
  owned Arrow point-read streams in input order, preserving duplicates and
  representing missing keys as nullable values. Native MultiGet groups are
  capped at 256 keys; row/byte limits, snapshots, cancellation and opt-in
  metrics reuse the scan lifecycle. Existing list-returning APIs are unchanged.
  Binary Arrow inputs are copied without per-key Python conversion; sliced
  offsets are validated (also fixing sliced columnar writes).

- ABR-03 (#40): atomic `ArrowScan.cancel()` requests with explicit admission
  results and a read-only `cancel_requested` flag. Native gather and buffer
  copying poll every 256 rows; observed cancellation discards unpublished
  output and returns C Stream `ECANCELED` with a stable diagnostic, never
  false clean exhaustion. Existing `close()` remains the deterministic
  resource-release API. Opt-in stats add gathered progress and cancelled
  callback counts, with test-only native allocation latches validating
  in-flight cancellation without production test hooks.

- ABR-01 (#38): opt-in `collect_metrics=True` for native default/CF Arrow
  scans, including TransactionDB. `ArrowScan.stats` reports coherent output
  counters, requested buffer capacity/slack, retained-buffer high-water
  marks, staged payload size and native gather/build time. Independent
  Arrow child releases update accounting even after stream/DB closure.
  Default scans do not allocate metrics state or collect phase timings.

## [0.7.1] - 2026-10-29

### Added

- Lightweight optional-argument dispatch and specialized default-CF read path
  for #36, retaining the general options/snapshot/CF implementations and all
  existing Python call forms. Ordinary reads borrow immutable key/options
  storage and use the cached default CF under shared lifetime protection.
- Distribution-reporting point-read benchmark with equal Python wrapper
  overhead, rotated mode order, CPU pinning, hit/miss/size controls and
  synthetic decode probes. See `benchmarks/point_reads.md` for controlled
  same-toolchain measurements and `docs/source/point_reads.rst` for limits.
  Warmed plain-DB ordinary hits measured 0.631 µs (0.4.1 source), 0.780 µs
  (0.7.0 source) and 0.536 µs (candidate), retaining lifetime locking. This
  recovery is workload-specific; general paths and misses are reported too.

### Fixed

- Remove the pre-lock raw DB-pointer check from the specialized read path.
- Publish mutable ReadOptions flags atomically using immutable native
  configurations, preventing concurrent flag changes from racing native
  reads. Synchronize retrieval/replacement of the default options handle.
- Ordinary DB reads/writes share lifetime protection, relying on RocksDB's
  documented data-operation synchronization. Teardown, CF mutation and
  snapshot release retain exclusive protection. Added bounded subprocess
  race tests for close, CF drop, snapshot release, options and writers.
- Correct earlier unsupported claims that restoring performance requires
  unsafe operations, and that explicit `None` selects the general native
  read path. Full recovery is a measurement question, not a proven safety
  limit; historical PyPI-wheel versus local-build results did not isolate it.

## [0.7.0] - 2026-10-29

### Added

- Read-only `Transaction.snapshot_sequence_number` (`int | None`) and
  `TransactionDB.latest_sequence_number` (`int`) for snapshot provenance (#34).
  Both are constant-time native lookups with lifetime/operation locking;
  reading them does not write, scan, hash, or change the pinned snapshot.

## [0.6.0] - 2026-10-21

### Added

- `TransactionDB.scan_arrow()` / `scan_arrow_cf()`, `create_snapshot()`
  and `new_iterator()` over committed state. Shared snapshots span default
  and named CFs; uncommitted transaction writes are deliberately excluded.
- TransactionDB CF management, persisted-CF reopen, direct CF reads/writes,
  and transactional `put_cf` / `get_cf` / `delete_cf` / CF WriteBatch writes.
  CF drops are rejected while transactions or relevant Arrow scans are active.
- Snapshot-consistent `get(..., snapshot=...)` / `get_cf(...)` and native
  `multi_get` / `multi_get_cf` on plain, extended and transactional DBs.
  MultiGet accepts up to 65536 byte keys, preserves order/duplicates and
  returns `None` for missing values. Index scans and fact materialization
  can now share exactly one storage horizon.
- Cross-CF snapshot/materialization example and documented GestaltDB
  integration opportunities/limitations; regression and allocation-failure tests.
- Mandatory Arrow/Polars-enabled PR tests and release-wheel tests.

### Fixed

- Shared-snapshot destruction deadlocks, scan creation/close/drop races,
  allocation-failure cleanup/deadlocks and deterministic Arrow consumer release.
- Batch byte caps now include both initial binary offsets (eight previously
  omitted bytes). Foreign CF handles are rejected by identity, not name.
- Owned CF handles are destroyed before their DB; native DB/iterator and
  transaction operations are serialized with explicit closure.
- Transaction snapshots are applied to default and CF reads/iterators.
- Selective benchmark baselines now use identical bounds, validate digests
  and use the selected row count for throughput.
- RocksDB cache keys include libc, compiler, flags and liburing headers,
  preventing cross-environment ABI-incompatible cache reuse.

## [0.5.0] - 2026-10-21

### Added

- Native batched Arrow scans complementing `write_columnar_batch()`
  (feature request #31):
  - `PyRocksDB.scan_arrow()` and `PyRocksDBExtended.scan_arrow_cf()`
    iterate RocksDB in C++ and pack raw key/value bytes into Arrow
    `binary` columns, exported through the Arrow C Stream interface
    (`__arrow_c_stream__`, PyCapsule `arrow_array_stream`) with a fixed
    `struct<key: binary (non-null), value: binary (non-null)>` schema.
    Consumable by PyArrow, Polars, DataFusion and other Arrow C Stream
    clients without per-row Python objects.
  - Lazy, bounded-memory streaming with configurable `batch_size` and
    `max_batch_bytes`, plus an explicit oversized-record policy
    (`allow_overflow`).
  - Full scans, prefix scans and explicit `[start, stop)` key-range scans
    (start inclusive, stop exclusive); prefix/range intersections and
    binary-safe keys (empty keys, embedded nulls, `0xff` bytes).
  - Bounds pushed down to RocksDB via `iterate_upper_bound` where
    possible; custom (non-bytewise) comparators are rejected explicitly.
  - `PyRocksDB.create_snapshot()` pins a consistent read view shareable
    by multiple scans (including across column families), with a
    `sequence_number` read-view identity. Scans without an explicit
    snapshot pin their own view at creation.
  - Safe lifecycle: single-consumer streams, cooperative early close
    (`close()` / context manager), exported batches remain valid after
    stream/DB closure, column-family drops under active scans are
    rejected, and iterator errors propagate instead of masking as clean
    end-of-stream.
- Tests: `tests/test_arrow_scan.py` (equivalence with iterators, bounds
  semantics, binary edge cases, batch sizing, snapshots, column
  families, lifecycle stress).
- Benchmark: `benchmarks/bench_arrow_scan.py` comparing Python iterators,
  Python iterators + Arrow construction, and native scans (materialized
  and streaming compute), with digest validation.
- Docs: `docs/source/arrow_scans.rst`, API reference entries, and
  `examples_simple/arrow_scan.py`.
