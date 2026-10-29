#pragma once

// Native batched Arrow scans (feature request #31).
//
// A scan pins a consistent RocksDB read view when it is established, iterates
// natively in C++ and packs raw key/value bytes into Arrow binary columns.
// Batches are exported through the Arrow C Stream interface so PyArrow,
// Polars, DataFusion, ... can consume them without per-row Python objects.
//
// Schema contract (fixed for the whole stream):
//   struct<key: binary (non-null), value: binary (non-null)>
// Empty bytes are valid values, never nulls. Offset width is int32 for the
// whole stream; a record that would overflow int32 offsets fails explicitly.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <pybind11/pybind11.h>

#include "arrow_c.h"
#include "column_family.hpp"

namespace py = pybind11;

class PyRocksDB;
class PyReadOptions;
struct ArrowScanMetricsState;

// ABR-01: coherent, by-value snapshot. Buffer capacities are requested Arrow
// data/offset allocation sizes, not malloc usable sizes or whole-process RSS.
struct ArrowScanStatistics {
    uint64_t get_next_calls = 0;
    uint64_t batches_emitted = 0;
    uint64_t rows_emitted = 0;
    uint64_t key_bytes_emitted = 0;
    uint64_t value_bytes_emitted = 0;
    uint64_t offset_bytes_emitted = 0;
    uint64_t output_bytes_emitted = 0;
    uint64_t buffer_capacity_bytes_emitted = 0;
    uint64_t allocation_slack_bytes_emitted = 0;
    uint64_t live_buffer_bytes = 0;
    uint64_t peak_live_buffer_bytes = 0;
    uint64_t peak_staged_payload_bytes = 0;
    uint64_t gather_time_ns = 0;
    uint64_t build_time_ns = 0;
};

namespace rocksdb {
class DB;
class Iterator;
class Snapshot;
}  // namespace rocksdb

// ---------------------------------------------------------------------------
// Bytewise helpers (the scan assumes RocksDB's default bytewise comparator).
// ---------------------------------------------------------------------------

// Returns true and fills `successor` with the smallest key that is strictly
// greater than every key with `prefix` as a prefix. Returns false when no
// such key exists (prefix is empty-handled by callers, or all bytes are
// 0xff and the prefix range is unbounded above).
bool prefix_successor(const std::string& prefix, std::string& successor);

// ---------------------------------------------------------------------------
// PySnapshot: an explicitly pinned, shareable RocksDB read view.
// ---------------------------------------------------------------------------

class PySnapshot {
public:
    PySnapshot(std::shared_ptr<PyRocksDB> parent, const rocksdb::Snapshot* snapshot);
    ~PySnapshot();

    PySnapshot(const PySnapshot&) = delete;
    PySnapshot& operator=(const PySnapshot&) = delete;

    // Releases the snapshot early. Idempotent; also runs at destruction.
    void release();
    bool is_valid() const;
    // Stable read-view identity for integration checks (RocksDB sequence
    // number of the pinned view). Throws once released.
    uint64_t sequence_number() const;

private:
    friend class PyRocksDB;
    friend class PyArrowScan;

    // Called by PyRocksDB::close() with the still-valid DB pointer so the
    // snapshot is released before the DB is destroyed. Never calls back
    // into the parent (close() already holds the parent arrow mutex).
    void invalidate_for_db_close(rocksdb::DB* db_raw);

    std::shared_ptr<PyRocksDB> parent_;
    const rocksdb::Snapshot* snapshot_ = nullptr;
    mutable std::mutex mutex_;
    bool released_ = false;

    void release_locked();
};

// ---------------------------------------------------------------------------
// PyArrowScan: lazy, bounded-memory native scan exported as Arrow stream.
// ---------------------------------------------------------------------------

class PyArrowScan : public std::enable_shared_from_this<PyArrowScan> {
public:
    struct Config {
        std::string prefix;
        bool has_prefix = false;
        std::string start;
        bool has_start = false;
        std::string stop;
        bool has_stop = false;
        size_t batch_size = 65536;
        size_t max_batch_bytes = 8 * 1024 * 1024;
        bool allow_overflow = false;
        bool collect_metrics = false;
    };

    // Factory path only: native initialization and parent registration are
    // performed atomically under the DB lifecycle mutex.
    PyArrowScan(std::shared_ptr<PyRocksDB> parent, std::shared_ptr<PyColumnFamilyHandle> cf_handle, const Config& config,
                std::shared_ptr<PySnapshot> shared_snapshot, std::shared_ptr<PyReadOptions> read_options);
    ~PyArrowScan();

    PyArrowScan(const PyArrowScan&) = delete;
    PyArrowScan& operator=(const PyArrowScan&) = delete;

    // Arrow PyCapsule interface: exports the stream exactly once and returns
    // a PyCapsule named "arrow_array_stream" (per the Arrow PyCapsule
    // standard). Single-consumer: a second export raises. `requested_schema`
    // is accepted for protocol compatibility and ignored (this stream has
    // one fixed schema).
    py::object export_stream(py::object requested_schema);

    // Cooperative early termination: releases the iterator and any owned
    // snapshot. Idempotent. Already exported batches stay valid (owned
    // buffers) but further get_next calls fail.
    void close();
    bool is_closed() const;
    uint64_t sequence_number() const;
    // None when collection was not requested. Does not touch native DB/CF
    // resources and remains readable after close, error and exhaustion.
    std::optional<ArrowScanStatistics> statistics() const;

    const std::string& cf_name() const { return cf_name_; }

    // True while the scan still holds iterator/snapshot resources on the
    // named column family (used to reject dropping a CF under an active
    // scan). Takes the scan mutex; callers must not hold it already.
    bool uses_cf_active(const std::string& name) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return !closed_ && !exhausted_ && cf_name_ == name;
    }

private:
    friend class PyRocksDB;
    // ArrowArrayStream C trampolines (defined in arrow_scan.cpp).
    friend int stream_get_schema(ArrowArrayStream* stream, ArrowSchema* out);
    friend int stream_get_next(ArrowArrayStream* stream, ArrowArray* out);
    friend const char* stream_get_last_error(ArrowArrayStream* stream);

    // Called by PyRocksDB::close() with the still-valid DB pointer. Deletes
    // the iterator and releases an owned snapshot before the DB is
    // destroyed; afterwards the scan reports "database closed" instead of
    // touching freed memory. Never calls back into the parent.
    void invalidate_for_db_close(rocksdb::DB* db_raw);

    // ArrowArrayStream callbacks (static trampolines below).
    int fill_schema(ArrowSchema* out);
    int fill_next(ArrowArray* out);
    const char* last_error_cstr();

    // Gathers the next batch under the scan mutex with the GIL released by
    // the caller. Returns true with `keys`/`values` filled when a batch is
    // ready, false at clean end-of-stream. Throws RocksDBException on I/O,
    // overflow or lifecycle errors.
    bool gather_next_batch(std::vector<std::string>& keys, std::vector<std::string>& values);
    // Releases iterator + owned snapshot. Caller holds mutex_ (via the
    // parent detach helper or invalidate_for_db_close); db_raw may be
    // null when the parent DB is already gone (then the snapshot was already
    // released by close() and only local state is cleared).
    std::shared_ptr<PySnapshot> finish_locked(rocksdb::DB* db_raw);

    std::shared_ptr<PyRocksDB> parent_;
    std::shared_ptr<PyColumnFamilyHandle> cf_handle_;
    std::string cf_name_;

    // Effective (intersected) scan bounds: [eff_start, eff_stop).
    std::string eff_start_;
    bool has_eff_start_ = false;
    std::string eff_stop_;
    bool has_eff_stop_ = false;
    // Storage backing ReadOptions.iterate_upper_bound for push-down.
    std::string upper_bound_storage_;
    rocksdb::Slice upper_bound_slice_;

    size_t batch_size_;
    size_t max_batch_bytes_;
    bool allow_overflow_;

    rocksdb::Iterator* iter_ = nullptr;
    const rocksdb::Snapshot* owned_snapshot_ = nullptr;
    std::shared_ptr<PySnapshot> shared_snapshot_;
    uint64_t snapshot_seq_ = 0;

    mutable std::mutex mutex_;
    bool closed_ = false;
    bool exhausted_ = false;
    bool exported_ = false;
    bool started_ = false;
    // Set when the iterator reaches the effective stop bound; the current
    // batch is still emitted, the *next* get_next ends the stream.
    bool stop_reached_ = false;
    std::string last_error_;
    // Shared with exported child buffers, so release accounting outlives the
    // stream and the DB. No parent/scan lock is taken by buffer callbacks.
    std::shared_ptr<ArrowScanMetricsState> metrics_;
};
