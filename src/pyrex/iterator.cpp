#include "iterator.hpp"

#include <string>

#include "db.hpp"
#include "exceptions.hpp"
#include "rocksdb/status.h"

PyRocksDBIterator::PyRocksDBIterator(rocksdb::Iterator* it, std::shared_ptr<PyRocksDB> parent_db)
    : it_raw_ptr_(it), parent_db_ptr_(std::move(parent_db)) {
    if (!it_raw_ptr_) {
        throw RocksDBException("Failed to create iterator: null pointer received.");
    }
}

PyRocksDBIterator::~PyRocksDBIterator() {
    if (parent_db_ptr_) {
        std::lock_guard<std::mutex> lock(parent_db_ptr_->active_iterators_mutex_);
        if (it_raw_ptr_ && parent_db_ptr_->active_rocksdb_iterators_.count(it_raw_ptr_)) {
            parent_db_ptr_->active_rocksdb_iterators_.erase(it_raw_ptr_);
            delete it_raw_ptr_;
        }
    }
    it_raw_ptr_ = nullptr;
}

void PyRocksDBIterator::check_parent_db_is_open() const {
    if (!parent_db_ptr_ || parent_db_ptr_->is_closed_.load()) {
        throw RocksDBException("Database is closed.");
    }
}

bool PyRocksDBIterator::valid() { check_parent_db_is_open(); return it_raw_ptr_->Valid(); }
void PyRocksDBIterator::seek_to_first() { check_parent_db_is_open(); it_raw_ptr_->SeekToFirst(); }
void PyRocksDBIterator::seek_to_last() { check_parent_db_is_open(); it_raw_ptr_->SeekToLast(); }
void PyRocksDBIterator::seek(const py::bytes& key) { check_parent_db_is_open(); it_raw_ptr_->Seek(static_cast<std::string>(key)); }
void PyRocksDBIterator::next() { check_parent_db_is_open(); it_raw_ptr_->Next(); }
void PyRocksDBIterator::prev() { check_parent_db_is_open(); it_raw_ptr_->Prev(); }

py::object PyRocksDBIterator::key() {
    check_parent_db_is_open();
    if (it_raw_ptr_ && it_raw_ptr_->Valid()) {
        return py::bytes(it_raw_ptr_->key().ToString());
    }
    return py::none();
}

py::object PyRocksDBIterator::value() {
    check_parent_db_is_open();
    if (it_raw_ptr_ && it_raw_ptr_->Valid()) {
        return py::bytes(it_raw_ptr_->value().ToString());
    }
    return py::none();
}

void PyRocksDBIterator::check_status() {
    check_parent_db_is_open();
    if (it_raw_ptr_) {
        rocksdb::Status s = it_raw_ptr_->status();
        if (!s.ok()) throw RocksDBException("Iterator error: " + s.ToString());
    }
}
