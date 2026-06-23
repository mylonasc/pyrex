#pragma once

#include <memory>

#include <pybind11/pybind11.h>

#include "rocksdb/iterator.h"

namespace py = pybind11;

class PyRocksDB;

class PyRocksDBIterator {
private:
    rocksdb::Iterator* it_raw_ptr_;
    std::shared_ptr<PyRocksDB> parent_db_ptr_;
    void check_parent_db_is_open() const;

public:
    explicit PyRocksDBIterator(rocksdb::Iterator* it, std::shared_ptr<PyRocksDB> parent_db);
    ~PyRocksDBIterator();
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
