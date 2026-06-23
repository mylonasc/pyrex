#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include <pybind11/pybind11.h>

#include "column_family.hpp"
#include "options.hpp"
#include "rocksdb/db.h"

namespace py = pybind11;

class PyRocksDBIterator;
class PyWriteBatch;

class PyRocksDB : public std::enable_shared_from_this<PyRocksDB> {
protected:
    rocksdb::DB* db_ = nullptr;
    rocksdb::ColumnFamilyHandle* default_cf_handle_ = nullptr;
    PyOptions opened_options_;
    std::string path_;
    std::map<std::string, std::shared_ptr<PyColumnFamilyHandle>> cf_handles_;
    std::atomic<bool> is_closed_{false};
    std::atomic<bool> is_read_only_{false};
    std::mutex active_iterators_mutex_;
    std::set<rocksdb::Iterator*> active_rocksdb_iterators_;
    std::shared_ptr<PyReadOptions> default_read_options_;
    std::shared_ptr<PyWriteOptions> default_write_options_;

    friend class PyRocksDBIterator;

    void check_db_open() const;
    void check_read_only() const;
    rocksdb::ColumnFamilyHandle* get_default_cf_handle() const;

public:
    PyRocksDB();
    PyRocksDB(const std::string& path, PyOptions* py_options, bool read_only = false);
    virtual ~PyRocksDB();

    void close();
    void put(const py::bytes& key, const py::bytes& value, std::shared_ptr<PyWriteOptions> write_options = nullptr);
    py::object get(const py::bytes& key, std::shared_ptr<PyReadOptions> read_options = nullptr);
    void del(const py::bytes& key, std::shared_ptr<PyWriteOptions> write_options = nullptr);
    void write(PyWriteBatch& batch, std::shared_ptr<PyWriteOptions> write_options = nullptr);
    void write_columnar_batch(const py::object& keys, const py::object& values, std::shared_ptr<PyWriteOptions> write_options = nullptr, const std::string& on_null = "error");
    std::shared_ptr<PyRocksDBIterator> new_iterator(std::shared_ptr<PyReadOptions> read_options = nullptr);
    PyOptions get_options() const;
    std::shared_ptr<PyReadOptions> get_default_read_options();
    void set_default_read_options(std::shared_ptr<PyReadOptions> opts);
    std::shared_ptr<PyWriteOptions> get_default_write_options();
    void set_default_write_options(std::shared_ptr<PyWriteOptions> opts);
};

class PyRocksDBExtended : public PyRocksDB {
public:
    PyRocksDBExtended(const std::string& path, PyOptions* py_options, bool read_only = false);

    void put_cf(PyColumnFamilyHandle& cf, const py::bytes& key, const py::bytes& value, std::shared_ptr<PyWriteOptions> write_options = nullptr);
    py::object get_cf(PyColumnFamilyHandle& cf, const py::bytes& key, std::shared_ptr<PyReadOptions> read_options = nullptr);
    void del_cf(PyColumnFamilyHandle& cf, const py::bytes& key, std::shared_ptr<PyWriteOptions> write_options = nullptr);
    std::vector<std::string> list_column_families();
    std::shared_ptr<PyColumnFamilyHandle> create_column_family(const std::string& name, PyOptions* cf_py_options = nullptr);
    void drop_column_family(PyColumnFamilyHandle& cf_handle);
    std::shared_ptr<PyColumnFamilyHandle> get_column_family(const std::string& name);
    std::shared_ptr<PyColumnFamilyHandle> get_default_cf();
    std::shared_ptr<PyRocksDBIterator> new_cf_iterator(PyColumnFamilyHandle& cf_handle, std::shared_ptr<PyReadOptions> read_options = nullptr);
};
