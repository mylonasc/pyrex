#pragma once

#include <pybind11/pybind11.h>

#include "column_family.hpp"
#include "rocksdb/write_batch.h"

namespace py = pybind11;

class PyWriteBatch {
public:
    rocksdb::WriteBatch wb_;

    PyWriteBatch() = default;
    // This API batches RocksDB writes but still pays one Python-to-C++ call per put.
    // Arrow-backed serialized batch ingestion can avoid that overhead for columnar chunks.
    void put(const py::bytes& key, const py::bytes& value);
    void put_cf(PyColumnFamilyHandle& cf, const py::bytes& key, const py::bytes& value);
    void del(const py::bytes& key);
    void del_cf(PyColumnFamilyHandle& cf, const py::bytes& key);
    void merge(const py::bytes& key, const py::bytes& value);
    void merge_cf(PyColumnFamilyHandle& cf, const py::bytes& key, const py::bytes& value);
    void clear();
};
