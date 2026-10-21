# Changelog

All notable changes to `pyrex-rocksdb` are documented here. This changelog
is maintained incrementally from this point onward; earlier history is
available via `git log`.

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
