#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <vector>

#include <pybind11/pybind11.h>

#include "column_family.hpp"
#include "options.hpp"
#include "rocksdb/db.h"

namespace py = pybind11;

class PyRocksDBIterator;
class PyWriteBatch;
class PySnapshot;
class PyArrowScan;

class PyRocksDB : public std::enable_shared_from_this<PyRocksDB> {
protected:
    rocksdb::DB* db_ = nullptr;
    rocksdb::ColumnFamilyHandle* default_cf_handle_ = nullptr;
    PyOptions opened_options_;
    std::string path_;
    std::map<std::string, std::shared_ptr<PyColumnFamilyHandle>> cf_handles_;
    bool owns_cf_handles_ = false;
    std::atomic<bool> is_closed_{false};
    std::atomic<bool> is_read_only_{false};
    std::mutex active_iterators_mutex_;
    std::set<rocksdb::Iterator*> active_rocksdb_iterators_;
    std::shared_ptr<PyReadOptions> default_read_options_;
    std::shared_ptr<PyWriteOptions> default_write_options_;

    // Guards db_ lifetime for snapshot/scan paths, plus the two sets below.
    // Lock order is always arrow_mutex_ -> scan/snapshot mutex_, never the
    // reverse (scan helpers never take arrow_mutex_ while holding their own).
    //
    // Reader-shared protocol (issue #36): reads and ordinary writes take
    // shared access; RocksDB synchronizes the underlying data operations.
    // Close/CF create/drop, snapshot/scan bookkeeping and default-options
    // replacement retain exclusive access. Keep the lock through Get/Write:
    // raw pointer lookup alone does not protect resource lifetimes.
    // See docs/source/point_reads.rst for the fast-path/safe-path contract.
    mutable std::shared_mutex arrow_mutex_;
    std::set<PyArrowScan*> active_arrow_scans_;
    std::set<PySnapshot*> active_snapshots_;

    friend class PyRocksDBIterator;
    friend class PySnapshot;
    friend class PyArrowScan;
    friend class PyTransaction;
    friend class PyTransactionIterator;

    void check_db_open() const;
    virtual void before_close_locked() {}
    virtual void check_cf_drop_locked() {}
    void check_read_only() const;
    rocksdb::ColumnFamilyHandle* get_default_cf_handle() const;
    // Caller holds arrow_mutex_ (shared or exclusive). Checks identity, not just the CF name.
    std::shared_ptr<PyColumnFamilyHandle> require_cf(PyColumnFamilyHandle& cf) const;
    rocksdb::ReadOptions snapshot_read_options(std::shared_ptr<PyReadOptions> options,
                                             const std::shared_ptr<PySnapshot>& snapshot) const;
    // Previous point-read implementation, preserved for every read that is
    // not eligible for the fast path (explicit snapshot, explicit read
    // options, or a named column family). Keeps the key/ReadOptions copies,
    // snapshot/CF validation and map lookup of the 0.6.0/0.7.0 path.
    py::object get_safe_impl(PyColumnFamilyHandle* cf, const py::bytes& key,
                             std::shared_ptr<PyReadOptions> options,
                             std::shared_ptr<PySnapshot> snapshot);
    // Fast path for the common ordinary read: default column family, default
    // read options, no explicit snapshot. Borrows the Python key buffer and
    // uses the cached default handle/options under a shared lock, so it pays
    // none of the per-read copies or map lookups of the safe path.
    py::object get_fast_impl(const py::bytes& key);
    py::object get_impl(PyColumnFamilyHandle* cf, const py::bytes& key,
                        std::shared_ptr<PyReadOptions> options, std::shared_ptr<PySnapshot> snapshot);
    py::list multi_get_impl(PyColumnFamilyHandle* cf, const py::sequence& keys,
                           std::shared_ptr<PyReadOptions> options, std::shared_ptr<PySnapshot> snapshot);

public:
    PyRocksDB();
    PyRocksDB(const std::string& path, PyOptions* py_options, bool read_only = false);
    virtual ~PyRocksDB();

    virtual void close();
    virtual void put(const py::bytes& key, const py::bytes& value, std::shared_ptr<PyWriteOptions> write_options = nullptr);
    // Ordinary-read binding entry point, alongside the general get API.
    py::object get_default(const py::bytes& key) { return get_fast_impl(key); }
    virtual py::object get(const py::bytes& key, std::shared_ptr<PyReadOptions> read_options = nullptr,
                           std::shared_ptr<PySnapshot> snapshot = nullptr);
    py::list multi_get(const py::sequence& keys, std::shared_ptr<PyReadOptions> read_options = nullptr,
                       std::shared_ptr<PySnapshot> snapshot = nullptr);
    virtual void del(const py::bytes& key, std::shared_ptr<PyWriteOptions> write_options = nullptr);
    virtual void write(PyWriteBatch& batch, std::shared_ptr<PyWriteOptions> write_options = nullptr);
    void write_columnar_batch(const py::object& keys, const py::object& values, std::shared_ptr<PyWriteOptions> write_options = nullptr, const std::string& on_null = "error");
    std::shared_ptr<PyRocksDBIterator> new_iterator(std::shared_ptr<PyReadOptions> read_options = nullptr);
    // Pins a consistent read view shared by one or more scan_arrow streams.
    std::shared_ptr<PySnapshot> create_snapshot();
    // Native batched Arrow scan of the default column family. Bounds are
    // raw bytes (None = unbounded); start inclusive, stop exclusive; prefix
    // intersects with [start, stop). See docs/arrow_scan.md for the contract.
    std::shared_ptr<PyArrowScan> scan_arrow(py::object prefix = py::none(), py::object start = py::none(), py::object stop = py::none(),
                                            size_t batch_size = 65536, size_t max_batch_bytes = 8 * 1024 * 1024,
                                            std::shared_ptr<PyReadOptions> read_options = nullptr,
                                            std::shared_ptr<PySnapshot> snapshot = nullptr, bool allow_overflow = false,
                                            bool collect_metrics = false);
    PyOptions get_options() const;
    std::shared_ptr<PyArrowScan> multi_get_arrow(py::object keys, size_t batch_size = 65536,
        size_t max_batch_bytes = 8 * 1024 * 1024, std::shared_ptr<PyReadOptions> read_options = nullptr,
        std::shared_ptr<PySnapshot> snapshot = nullptr, bool allow_overflow = false, bool collect_metrics = false);
    std::shared_ptr<PyReadOptions> get_default_read_options();
    void set_default_read_options(std::shared_ptr<PyReadOptions> opts);
    std::shared_ptr<PyWriteOptions> get_default_write_options();
    void set_default_write_options(std::shared_ptr<PyWriteOptions> opts);

    // Internal lifetime helpers for PySnapshot / PyArrowScan. detach_* take
    // arrow_mutex_ exclusively first, then the object mutex (consistent with
    // close(), which holds arrow_mutex_ exclusively while invalidating).
    void unregister_scan(PyArrowScan* scan);
    void detach_scan_resources(PyArrowScan* scan, bool mark_exhausted);
    void detach_snapshot(PySnapshot* snapshot);
};

class PyRocksDBExtended : public PyRocksDB {
protected:
    PyRocksDBExtended() = default;
public:
    PyRocksDBExtended(const std::string& path, PyOptions* py_options, bool read_only = false);

    void put_cf(PyColumnFamilyHandle& cf, const py::bytes& key, const py::bytes& value, std::shared_ptr<PyWriteOptions> write_options = nullptr);
    py::object get_cf(PyColumnFamilyHandle& cf, const py::bytes& key, std::shared_ptr<PyReadOptions> read_options = nullptr,
                      std::shared_ptr<PySnapshot> snapshot = nullptr);
    py::list multi_get_cf(PyColumnFamilyHandle& cf, const py::sequence& keys,
                         std::shared_ptr<PyReadOptions> read_options = nullptr,
                         std::shared_ptr<PySnapshot> snapshot = nullptr);
    void del_cf(PyColumnFamilyHandle& cf, const py::bytes& key, std::shared_ptr<PyWriteOptions> write_options = nullptr);
    std::vector<std::string> list_column_families();
    std::shared_ptr<PyColumnFamilyHandle> create_column_family(const std::string& name, PyOptions* cf_py_options = nullptr);
    void drop_column_family(PyColumnFamilyHandle& cf_handle);
    std::shared_ptr<PyColumnFamilyHandle> get_column_family(const std::string& name);
    std::shared_ptr<PyColumnFamilyHandle> get_default_cf();
    std::shared_ptr<PyArrowScan> multi_get_arrow_cf(std::shared_ptr<PyColumnFamilyHandle> cf, py::object keys,
        size_t batch_size = 65536, size_t max_batch_bytes = 8 * 1024 * 1024,
        std::shared_ptr<PyReadOptions> read_options = nullptr, std::shared_ptr<PySnapshot> snapshot = nullptr,
        bool allow_overflow = false, bool collect_metrics = false);
    std::shared_ptr<PyRocksDBIterator> new_cf_iterator(PyColumnFamilyHandle& cf_handle, std::shared_ptr<PyReadOptions> read_options = nullptr);
    // Native batched Arrow scan of a named column family (same contract as
    // scan_arrow; shares the DB snapshot when one is supplied).
    std::shared_ptr<PyArrowScan> scan_arrow_cf(PyColumnFamilyHandle& cf_handle, py::object prefix = py::none(), py::object start = py::none(),
                                               py::object stop = py::none(), size_t batch_size = 65536,
                                               size_t max_batch_bytes = 8 * 1024 * 1024,
                                               std::shared_ptr<PyReadOptions> read_options = nullptr,
                                                std::shared_ptr<PySnapshot> snapshot = nullptr, bool allow_overflow = false,
                                                bool collect_metrics = false);
};
