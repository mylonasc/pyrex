#pragma once

#include <cstdint>
#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <shared_mutex>

#include <pybind11/pybind11.h>

#include "options.hpp"
#include "rocksdb/iterator.h"
#include "rocksdb/utilities/transaction.h"

namespace py = pybind11;

class PyTransactionDB;
class PyWriteBatch;
class PyTransactionIterator;
class PyColumnFamilyHandle;
class PyArrowScan;

class PyTransaction : public std::enable_shared_from_this<PyTransaction> {
private:
    rocksdb::Transaction* txn_ = nullptr;
    std::shared_ptr<PyTransactionDB> parent_db_;
    bool active_ = true;
    std::mutex active_iterators_mutex_;
    std::set<PyTransactionIterator*> active_iterators_;
    mutable std::mutex operation_mutex_;
    std::atomic<size_t> active_arrow_scans_{0};
    struct OperationGuard {
        std::shared_lock<std::shared_mutex> db_lock;
        std::lock_guard<std::mutex> txn_lock;
        explicit OperationGuard(const PyTransaction& txn);
    };
    void check_cf(PyColumnFamilyHandle& cf) const;

    friend class PyTransactionDB;
    friend class PyTransactionIterator;
    friend class PyArrowScan;

    PyTransaction(rocksdb::Transaction* txn, std::shared_ptr<PyTransactionDB> parent_db);
    void check_active() const;
    void check_mutable() const;
    std::shared_ptr<PyArrowScan> scan_arrow_impl(std::shared_ptr<PyColumnFamilyHandle> cf,
        py::object prefix, py::object start, py::object stop, size_t batch_size, size_t max_batch_bytes,
        std::shared_ptr<PyReadOptions> read_options, bool allow_overflow, bool collect_metrics);
    void invalidate_from_parent_close();
    void register_iterator(PyTransactionIterator* it);
    void unregister_iterator(PyTransactionIterator* it);
    void invalidate_iterators();

public:
    ~PyTransaction();

    void put(const py::bytes& key, const py::bytes& value);
    void put_cf(PyColumnFamilyHandle& cf, const py::bytes& key, const py::bytes& value);
    py::object get_cf(PyColumnFamilyHandle& cf, const py::bytes& key, std::shared_ptr<PyReadOptions> read_options = nullptr);
    void del_cf(PyColumnFamilyHandle& cf, const py::bytes& key);
    py::object get(const py::bytes& key, std::shared_ptr<PyReadOptions> read_options = nullptr);
    py::object get_for_update(const py::bytes& key, std::shared_ptr<PyReadOptions> read_options = nullptr, bool exclusive = true, bool do_validate = true, bool read_value = true);
    void del(const py::bytes& key);
    void write(PyWriteBatch& batch);
    void commit(std::shared_ptr<PyWriteOptions> write_options = nullptr);
    void rollback();
    void set_snapshot();
    std::optional<uint64_t> snapshot_sequence_number() const;
    std::shared_ptr<PyTransactionIterator> new_iterator(std::shared_ptr<PyReadOptions> read_options = nullptr);
    bool is_active() const;
    std::shared_ptr<PyArrowScan> scan_arrow(py::object prefix = py::none(), py::object start = py::none(),
        py::object stop = py::none(), size_t batch_size = 65536, size_t max_batch_bytes = 8 * 1024 * 1024,
        std::shared_ptr<PyReadOptions> read_options = nullptr, bool allow_overflow = false, bool collect_metrics = false);
    std::shared_ptr<PyArrowScan> scan_arrow_cf(std::shared_ptr<PyColumnFamilyHandle> cf,
        py::object prefix = py::none(), py::object start = py::none(), py::object stop = py::none(),
        size_t batch_size = 65536, size_t max_batch_bytes = 8 * 1024 * 1024,
        std::shared_ptr<PyReadOptions> read_options = nullptr, bool allow_overflow = false, bool collect_metrics = false);
    py::dict arrow_capabilities() const;
};

class PyTransactionIterator {
private:
    friend class PyTransaction;

    rocksdb::Iterator* it_raw_ptr_ = nullptr;
    std::shared_ptr<PyTransaction> parent_txn_;
    void check_parent_transaction_is_active() const;
    void invalidate_from_parent_completion();

public:
    PyTransactionIterator(rocksdb::Iterator* it, std::shared_ptr<PyTransaction> parent_txn);
    ~PyTransactionIterator();
    bool valid();
    void seek_to_first();
    void seek_to_last();
    void seek(const py::bytes& key);
    void next();
    void prev();
    py::object key();
    py::object value();
    void check_status();
};
