# Changelog

All notable changes to `pyrex-rocksdb` are documented here. This changelog
is maintained incrementally from this point onward; earlier history is
available via `git log`.

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
