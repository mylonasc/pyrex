// rocksdb_wrapper.cpp
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

// NEW: Include for std::unique_ptr
#include <memory>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/iterator.h"

#include <iostream>
#include <string>
#include <cstring>

namespace py = pybind11;

// Define a custom exception for RocksDB errors
class RocksDBException : public std::runtime_error {
public:
    explicit RocksDBException(const std::string& msg) : std::runtime_error(msg) {}
};

// --- PyOptions class to wrap rocksdb::Options ---
class PyOptions {
public:
    rocksdb::Options options_;

    PyOptions() : options_() {}

    bool get_create_if_missing() const { return options_.create_if_missing; }
    void set_create_if_missing(bool value) { options_.create_if_missing = value; }

    bool get_error_if_exists() const { return options_.error_if_exists; }
    void set_error_if_exists(bool value) { options_.error_if_exists = value; }

    int get_max_open_files() const { return options_.max_open_files; }
    void set_max_open_files(int value) { options_.max_open_files = value; }

    size_t get_write_buffer_size() const { return options_.write_buffer_size; }
    void set_write_buffer_size(size_t value) { options_.write_buffer_size = value; }

    rocksdb::CompressionType get_compression() const { return options_.compression; }
    void set_compression(rocksdb::CompressionType value) { options_.compression = value; }

    int get_max_background_jobs() const { return options_.max_background_jobs; }
    void set_max_background_jobs(int value) { options_.max_background_jobs = value; }

    void increase_parallelism(int total_threads) {
        options_.IncreaseParallelism(total_threads);
    }

    void optimize_for_small_db() {
        options_.OptimizeForSmallDb();
    }

    void use_block_based_bloom_filter(double bits_per_key = 10.0) {
        if (options_.table_factory == nullptr ||
            std::strcmp(options_.table_factory->Name(), "BlockBasedTable") != 0) {
            options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory());
        }

        rocksdb::BlockBasedTableOptions table_options;
        // NOTE: This part assumes we are creating a new policy, not modifying an existing one.
        // This is a reasonable simplification for a wrapper.
        
        // Create a new bloom filter policy
        table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bits_per_key));
        options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    }
};

// --- PyWriteBatch class to wrap rocksdb::WriteBatch ---
class PyWriteBatch {
public:
    rocksdb::WriteBatch wb_;

    PyWriteBatch() : wb_() {}

    void put(const py::bytes& key_bytes, const py::bytes& value_bytes) {
        wb_.Put(static_cast<std::string>(key_bytes), static_cast<std::string>(value_bytes));
    }

    void del(const py::bytes& key_bytes) {
        wb_.Delete(static_cast<std::string>(key_bytes));
    }

    void clear() {
        wb_.Clear();
    }
};

// --- PyRocksDBIterator class to wrap rocksdb::Iterator ---
class PyRocksDBIterator {
public:
    // Raw pointer to the RocksDB Iterator.
    // The lifetime of this pointer is managed by this class's constructor/destructor.
    rocksdb::Iterator* it_;

    explicit PyRocksDBIterator(rocksdb::Iterator* it) : it_(it) {
        if (!it_) {
            throw RocksDBException("Failed to create RocksDB iterator: null pointer received.");
        }
        // std::cout << "DEBUG: Creating RocksDB iterator." << std::endl; // Optional: for debugging
    }

    // Destructor: Ensures the C++ iterator is deleted when this object is destroyed.
    ~PyRocksDBIterator() {
        if (it_ != nullptr) {
            std::cout << "DEBUG: Deleting RocksDB iterator." << std::endl;
            delete it_;
            it_ = nullptr;
        }
    }

    bool valid() const {
        return it_->Valid();
    }

    void seek_to_first() {
        it_->SeekToFirst();
    }

    void seek_to_last() {
        it_->SeekToLast();
    }

    void seek(const py::bytes& key_bytes) {
        it_->Seek(static_cast<std::string>(key_bytes));
    }

    void next() {
        it_->Next();
    }

    void prev() {
        it_->Prev();
    }

    py::object key() {
        if (it_->Valid()) {
            return py::bytes(it_->key().ToString());
        }
        return py::none();
    }

    py::object value() {
        if (it_->Valid()) {
            return py::bytes(it_->value().ToString());
        }
        return py::none();
    }

    void check_status() {
        rocksdb::Status status = it_->status();
        if (!status.ok()) {
            throw RocksDBException("RocksDB Iterator error: " + status.ToString());
        }
    }
};

// --- PyRocksDB class to wrap rocksdb::DB ---
class PyRocksDB {
public:
    rocksdb::DB* db_;
    PyOptions opened_options_; // Store the options used to open the DB
    std::string path_;         // IMPROVEMENT: Store the path for accurate debug messages

    PyRocksDB(const std::string& path, PyOptions* py_options = nullptr) : db_(nullptr), path_(path) {
        rocksdb::Options actual_options;

        if (py_options != nullptr) {
            actual_options = py_options->options_;
        } else {
            actual_options.create_if_missing = true;
        }

        opened_options_.options_ = actual_options;

        rocksdb::Status status = rocksdb::DB::Open(actual_options, path_, &db_);

        if (!status.ok()) {
            throw RocksDBException("Failed to open RocksDB at " + path_ + ": " + status.ToString());
        }

        std::cout << "RocksDB opened successfully at: " << path_ << std::endl;
    }

    ~PyRocksDB() {
        if (db_ != nullptr) {
            // IMPROVEMENT: Use the stored path_ member for a reliable close message.
            std::cout << "DEBUG: Closing RocksDB database at " << path_ << std::endl;
            delete db_;
            db_ = nullptr;
        }
    }

    void put(const py::bytes& key_bytes, const py::bytes& value_bytes) {
        rocksdb::Status status = db_->Put(rocksdb::WriteOptions(),
                                         static_cast<std::string>(key_bytes),
                                         static_cast<std::string>(value_bytes));
        if (!status.ok()) {
            throw RocksDBException("Failed to put key-value pair: " + status.ToString());
        }
    }

    py::object get(const py::bytes& key_bytes) {
        std::string value_str;
        rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), static_cast<std::string>(key_bytes), &value_str);

        if (status.ok()) {
            return py::bytes(value_str);
        } else if (status.IsNotFound()) {
            return py::none();
        } else {
            throw RocksDBException("Failed to get value for key: " + status.ToString());
        }
    }

    // This method is safe to return by value, as PyOptions is a simple
    // wrapper around a copyable rocksdb::Options object.
    PyOptions get_options() const {
        return opened_options_;
    }

    void write(PyWriteBatch& py_write_batch) {
        rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &py_write_batch.wb_);
        if (!status.ok()) {
            throw RocksDBException("Failed to write batch: " + status.ToString());
        }
    }

    // **FIXED**: Return a unique_ptr to transfer ownership to pybind11.
    // This prevents the temporary iterator object from being destroyed prematurely.
    std::unique_ptr<PyRocksDBIterator> new_iterator() {
        rocksdb::ReadOptions read_options;
        return std::make_unique<PyRocksDBIterator>(db_->NewIterator(read_options));
    }
};

// --- PYBIND11 MODULE DEFINITION ---
PYBIND11_MODULE(_pyrex, m) {
    m.doc() = "pybind11 RocksDB wrapper";

    py::register_exception<RocksDBException>(m, "RocksDBException");

    py::enum_<rocksdb::CompressionType>(m, "CompressionType")
        .value("kNoCompression", rocksdb::kNoCompression)
        .value("kSnappyCompression", rocksdb::kSnappyCompression)
        .value("kBZip2Compression", rocksdb::kBZip2Compression)
        .value("kLZ4Compression", rocksdb::kLZ4Compression)
        .value("kLZ4HCCompression", rocksdb::kLZ4HCCompression)
        .value("kXpressCompression", rocksdb::kXpressCompression)
        .value("kZSTD", rocksdb::kZSTD)
        .value("kDisableCompressionOption", rocksdb::kDisableCompressionOption)
        .export_values();

    py::class_<PyOptions>(m, "PyOptions")
        .def(py::init<>())
        .def_property("create_if_missing", &PyOptions::get_create_if_missing, &PyOptions::set_create_if_missing)
        .def_property("error_if_exists", &PyOptions::get_error_if_exists, &PyOptions::set_error_if_exists)
        .def_property("max_open_files", &PyOptions::get_max_open_files, &PyOptions::set_max_open_files)
        .def_property("write_buffer_size", &PyOptions::get_write_buffer_size, &PyOptions::set_write_buffer_size)
        .def_property("compression", &PyOptions::get_compression, &PyOptions::set_compression)
        .def_property("max_background_jobs", &PyOptions::get_max_background_jobs, &PyOptions::set_max_background_jobs)
        .def("increase_parallelism", &PyOptions::increase_parallelism, py::arg("total_threads"))
        .def("optimize_for_small_db", &PyOptions::optimize_for_small_db)
        .def("use_block_based_bloom_filter", &PyOptions::use_block_based_bloom_filter, py::arg("bits_per_key") = 10.0);

    py::class_<PyWriteBatch>(m, "PyWriteBatch")
        .def(py::init<>())
        .def("put", &PyWriteBatch::put, py::arg("key"), py::arg("value"))
        .def("delete", &PyWriteBatch::del, py::arg("key"))
        .def("clear", &PyWriteBatch::clear);

    py::class_<PyRocksDBIterator>(m, "PyRocksDBIterator")
        .def("valid", &PyRocksDBIterator::valid)
        .def("seek_to_first", &PyRocksDBIterator::seek_to_first)
        .def("seek_to_last", &PyRocksDBIterator::seek_to_last)
        .def("seek", &PyRocksDBIterator::seek, py::arg("key"))
        .def("next", &PyRocksDBIterator::next)
        .def("prev", &PyRocksDBIterator::prev)
        .def("key", &PyRocksDBIterator::key)
        .def("value", &PyRocksDBIterator::value)
        .def("check_status", &PyRocksDBIterator::check_status);

    py::class_<PyRocksDB>(m, "PyRocksDB")
        .def(py::init<const std::string&, PyOptions*>(), py::arg("path"), py::arg("options") = nullptr)
        .def("put", &PyRocksDB::put, py::arg("key"), py::arg("value"))
        .def("get", &PyRocksDB::get, py::arg("key"))
        .def("get_options", &PyRocksDB::get_options)
        .def("write", &PyRocksDB::write, py::arg("write_batch"))
        .def("new_iterator", &PyRocksDB::new_iterator,
             "Creates and returns a new RocksDB iterator.",
             py::keep_alive<0, 1>()
        ); 
}
