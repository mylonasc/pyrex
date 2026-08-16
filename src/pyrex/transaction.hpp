#pragma once

#include <memory>

#include <pybind11/pybind11.h>

#include "options.hpp"
#include "rocksdb/iterator.h"
#include "rocksdb/utilities/transaction.h"

namespace py = pybind11;

class PyTransactionDB;
class PyWriteBatch;

class PyTransaction : public std::enable_shared_from_this<PyTransaction> {
private:
    rocksdb::Transaction* txn_ = nullptr;
    std::shared_ptr<PyTransactionDB> parent_db_;
    bool active_ = true;

    friend class PyTransactionDB;

    PyTransaction(rocksdb::Transaction* txn, std::shared_ptr<PyTransactionDB> parent_db);
    void check_active() const;
    void invalidate_from_parent_close();

public:
    ~PyTransaction();

    void put(const py::bytes& key, const py::bytes& value);
    py::object get(const py::bytes& key, std::shared_ptr<PyReadOptions> read_options = nullptr);
    void del(const py::bytes& key);
    void write(PyWriteBatch& batch);
    void commit(std::shared_ptr<PyWriteOptions> write_options = nullptr);
    void rollback();
    void set_snapshot();
    std::shared_ptr<class PyTransactionIterator> new_iterator(std::shared_ptr<PyReadOptions> read_options = nullptr);
    bool is_active() const;
};

class PyTransactionIterator {
private:
    rocksdb::Iterator* it_raw_ptr_ = nullptr;
    std::shared_ptr<PyTransaction> parent_txn_;
    void check_parent_transaction_is_active() const;

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
