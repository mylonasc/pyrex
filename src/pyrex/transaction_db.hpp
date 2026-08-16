#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <set>
#include <string>

#include <pybind11/pybind11.h>

#include "options.hpp"
#include "rocksdb/utilities/transaction_db.h"

namespace py = pybind11;

class PyTransaction;
class PyWriteBatch;

class PyTransactionDB : public std::enable_shared_from_this<PyTransactionDB> {
private:
    rocksdb::TransactionDB* db_ = nullptr;
    PyOptions opened_options_;
    PyTransactionDBOptions opened_transaction_db_options_;
    std::string path_;
    std::atomic<bool> is_closed_{false};
    std::shared_ptr<PyReadOptions> default_read_options_;
    std::shared_ptr<PyWriteOptions> default_write_options_;
    std::mutex active_transactions_mutex_;
    std::set<PyTransaction*> active_transactions_;

    friend class PyTransaction;

    void check_db_open() const;
    void register_transaction(PyTransaction* txn);
    void unregister_transaction(PyTransaction* txn);

public:
    PyTransactionDB(const std::string& path, PyOptions* py_options = nullptr, PyTransactionDBOptions* txn_db_options = nullptr);
    ~PyTransactionDB();

    void close();
    void put(const py::bytes& key, const py::bytes& value, std::shared_ptr<PyWriteOptions> write_options = nullptr);
    py::object get(const py::bytes& key, std::shared_ptr<PyReadOptions> read_options = nullptr);
    void del(const py::bytes& key, std::shared_ptr<PyWriteOptions> write_options = nullptr);
    void write(PyWriteBatch& batch, std::shared_ptr<PyWriteOptions> write_options = nullptr);
    std::shared_ptr<PyTransaction> begin_transaction(std::shared_ptr<PyWriteOptions> write_options = nullptr, std::shared_ptr<PyTransactionOptions> txn_options = nullptr);
    std::shared_ptr<PyTransaction> transaction(std::shared_ptr<PyWriteOptions> write_options = nullptr, std::shared_ptr<PyTransactionOptions> txn_options = nullptr);
    PyOptions get_options() const;
    PyTransactionDBOptions get_transaction_db_options() const;
    std::shared_ptr<PyReadOptions> get_default_read_options();
    void set_default_read_options(std::shared_ptr<PyReadOptions> opts);
    std::shared_ptr<PyWriteOptions> get_default_write_options();
    void set_default_write_options(std::shared_ptr<PyWriteOptions> opts);
};
