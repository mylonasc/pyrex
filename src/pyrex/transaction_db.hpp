#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>

#include <pybind11/pybind11.h>

#include "db.hpp"
#include "options.hpp"
#include "rocksdb/utilities/transaction_db.h"

namespace py = pybind11;

class PyTransaction;
class PyWriteBatch;

// PyTransactionDB reuses the PyRocksDB scan/snapshot machinery (create_snapshot,
// scan_arrow, new_iterator) by inheriting from PyRocksDB. A TransactionDB IS-A
// rocksdb::DB (via StackableDB), so GetSnapshot/ReleaseSnapshot/NewIterator and
// the default-column-family handle work through the base-class pointer.
//
// Column-family management is inherited from PyRocksDBExtended.
// Documented semantics: scans observe the committed state at the pinned
// snapshot. They do NOT include uncommitted writes of open transactions.
// Transaction-local read-your-writes remains a separate follow-up.
class PyTransactionDB : public PyRocksDBExtended {
private:
    // Non-owning alias of PyRocksDB::db_ (which owns the TransactionDB).
    // Used for BeginTransaction; nulled on close alongside db_.
    rocksdb::TransactionDB* txn_db_ = nullptr;
    PyTransactionDBOptions opened_transaction_db_options_;
    std::mutex active_transactions_mutex_;
    std::set<PyTransaction*> active_transactions_;
    // Shared native operations allow one transaction to commit while
    // another waits for its key locks. Explicit close takes exclusive access.
    mutable std::shared_mutex transaction_operations_mutex_;

    friend class PyTransaction;
    friend class PyTransactionIterator;

    void register_transaction(PyTransaction* txn);
    void unregister_transaction(PyTransaction* txn);
    rocksdb::WriteOptions copy_write_options(std::shared_ptr<PyWriteOptions> options);

protected:
    void before_close_locked() override;
    void check_cf_drop_locked() override;

public:
    PyTransactionDB(const std::string& path, PyOptions* py_options = nullptr, PyTransactionDBOptions* txn_db_options = nullptr);
    ~PyTransactionDB() override;

    void close() override;
    uint64_t latest_sequence_number() const;
    void put(const py::bytes& key, const py::bytes& value, std::shared_ptr<PyWriteOptions> write_options = nullptr) override;
    void del(const py::bytes& key, std::shared_ptr<PyWriteOptions> write_options = nullptr) override;
    void write(PyWriteBatch& batch, std::shared_ptr<PyWriteOptions> write_options = nullptr) override;
    void put_cf(PyColumnFamilyHandle& cf, const py::bytes& key, const py::bytes& value,
                std::shared_ptr<PyWriteOptions> write_options = nullptr);
    void del_cf(PyColumnFamilyHandle& cf, const py::bytes& key, std::shared_ptr<PyWriteOptions> write_options = nullptr);
    void drop_column_family(PyColumnFamilyHandle& cf);
    std::shared_ptr<PyTransaction> begin_transaction(std::shared_ptr<PyWriteOptions> write_options = nullptr, std::shared_ptr<PyTransactionOptions> txn_options = nullptr);
    std::shared_ptr<PyTransaction> transaction(std::shared_ptr<PyWriteOptions> write_options = nullptr, std::shared_ptr<PyTransactionOptions> txn_options = nullptr);
    PyTransactionDBOptions get_transaction_db_options() const;
};
