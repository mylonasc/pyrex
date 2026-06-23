#include "write_batch.hpp"

#include <string>

#include "exceptions.hpp"
#include "rocksdb/slice.h"

void PyWriteBatch::put(const py::bytes& key, const py::bytes& value) {
    wb_.Put(static_cast<std::string>(key), static_cast<std::string>(value));
}

void PyWriteBatch::put_cf(PyColumnFamilyHandle& cf, const py::bytes& key, const py::bytes& value) {
    if (!cf.is_valid()) throw RocksDBException("ColumnFamilyHandle is invalid.");
    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    rocksdb::Slice value_slice(static_cast<std::string_view>(value));
    wb_.Put(cf.cf_handle_, key_slice, value_slice);
}

void PyWriteBatch::del(const py::bytes& key) {
    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    wb_.Delete(key_slice);
}

void PyWriteBatch::del_cf(PyColumnFamilyHandle& cf, const py::bytes& key) {
    if (!cf.is_valid()) throw RocksDBException("ColumnFamilyHandle is invalid.");
    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    wb_.Delete(cf.cf_handle_, key_slice);
}

void PyWriteBatch::merge(const py::bytes& key, const py::bytes& value) {
    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    rocksdb::Slice value_slice(static_cast<std::string_view>(value));
    wb_.Merge(key_slice, value_slice);
}

void PyWriteBatch::merge_cf(PyColumnFamilyHandle& cf, const py::bytes& key, const py::bytes& value) {
    if (!cf.is_valid()) throw RocksDBException("ColumnFamilyHandle is invalid.");
    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    rocksdb::Slice value_slice(static_cast<std::string_view>(value));
    wb_.Merge(cf.cf_handle_, key_slice, value_slice);
}

void PyWriteBatch::clear() { wb_.Clear(); }
