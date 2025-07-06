// rocksdb_wrapper.cpp
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/write_batch.h" // NEW: For WriteBatch functionality
#include "rocksdb/iterator.h"    // NEW: For Iterator functionality

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
        table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bits_per_key));
        options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    }
};

// --- PyWriteBatch class to wrap rocksdb::WriteBatch ---
class PyWriteBatch {
public:
    rocksdb::WriteBatch wb_; // The actual RocksDB WriteBatch object

    PyWriteBatch() : wb_() {}

    // Add a Put operation to the batch
    void put(const py::bytes& key_bytes, const py::bytes& value_bytes) {
        std::string key_str = static_cast<std::string>(key_bytes);
        std::string value_str = static_cast<std::string>(value_bytes);
        wb_.Put(key_str, value_str);
    }

    // Add a Delete operation to the batch
    void del(const py::bytes& key_bytes) { // Using 'del' as 'delete' is a keyword in Python
        std::string key_str = static_cast<std::string>(key_bytes);
        wb_.Delete(key_str);
    }

    // Clear all operations from the batch
    void clear() {
        wb_.Clear();
    }
};

// --- PyRocksDBIterator class to wrap rocksdb::Iterator ---
class PyRocksDBIterator {
public:
    // Raw pointer to the RocksDB Iterator.
    // Ownership is transferred from PyRocksDB::new_iterator to this object.
    rocksdb::Iterator* it_;

    // Constructor takes a raw pointer to rocksdb::Iterator.
    // It's marked explicit to prevent accidental conversions.
    explicit PyRocksDBIterator(rocksdb::Iterator* it) : it_(it) {
        if (!it_) {
            throw RocksDBException("Failed to create RocksDB iterator: null pointer received.");
        }
    }

    // Destructor: Ensures the C++ iterator is deleted when PyRocksDBIterator is garbage collected
    ~PyRocksDBIterator() {
        if (it_ != nullptr) {
            delete it_;
            it_ = nullptr;
        }
    }

    // Check if the iterator is valid (pointing to a key-value pair)
    bool valid() const {
        return it_->Valid();
    }

    // Position at the first key in the database
    void seek_to_first() {
        it_->SeekToFirst();
    }

    // Position at the last key in the database
    void seek_to_last() {
        it_->SeekToLast();
    }

    // Position at or after the given key
    void seek(const py::bytes& key_bytes) {
        std::string key_str = static_cast<std::string>(key_bytes);
        it_->Seek(key_str);
    }

    // Move to the next key
    void next() {
        it_->Next();
    }

    // Move to the previous key
    void prev() {
        it_->Prev();
    }

    // Get the current key (returns Python bytes or None if not valid)
    py::object key() {
        if (it_->Valid()) {
            return py::bytes(it_->key().ToString());
        }
        return py::none();
    }

    // Get the current value (returns Python bytes or None if not valid)
    py::object value() {
        if (it_->Valid()) {
            return py::bytes(it_->value().ToString());
        }
        return py::none();
    }

    // Check iterator status for errors
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

    PyRocksDB(const std::string& path, PyOptions* py_options = nullptr) : db_(nullptr) {
        rocksdb::Options actual_options;

        if (py_options != nullptr) {
            actual_options = py_options->options_;
        } else {
            PyOptions default_py_options;
            actual_options = default_py_options.options_;
            actual_options.create_if_missing = true;
        }

        opened_options_.options_ = actual_options;

        rocksdb::Status status = rocksdb::DB::Open(actual_options, path, &db_);

        if (!status.ok()) {
            throw RocksDBException("Failed to open RocksDB at " + path + ": " + status.ToString());
        }

        std::cout << "RocksDB opened successfully at: " << path << std::endl;
    }

    ~PyRocksDB() {
        if (db_ != nullptr) {
            std::cout << "Closing RocksDB database." << std::endl;
            delete db_;
            db_ = nullptr;
        }
    }

    void put(const py::bytes& key_bytes, const py::bytes& value_bytes) {
        std::string key_str = static_cast<std::string>(key_bytes);
        std::string value_str = static_cast<std::string>(value_bytes);

        rocksdb::WriteOptions write_options;
        rocksdb::Status status = db_->Put(write_options, key_str, value_str);

        if (!status.ok()) {
            throw RocksDBException("Failed to put key-value pair: " + status.ToString());
        }
    }

    py::object get(const py::bytes& key_bytes) {
        std::string key_str = static_cast<std::string>(key_bytes);
        std::string value_str;

        rocksdb::ReadOptions read_options;
        rocksdb::Status status = db_->Get(read_options, key_str, &value_str);

        if (status.ok()) {
            return py::bytes(value_str);
        } else if (status.IsNotFound()) {
            return py::none();
        } else {
            throw RocksDBException("Failed to get value for key: " + status.ToString());
        }
    }

    PyOptions get_options() const {
        return opened_options_;
    }

    // NEW: Method to write a batch of operations
    void write(PyWriteBatch& py_write_batch) {
        rocksdb::WriteOptions write_options; // Default write options for batch
        rocksdb::Status status = db_->Write(write_options, &py_write_batch.wb_);
        if (!status.ok()) {
            throw RocksDBException("Failed to write batch: " + status.ToString());
        }
    }

    // NEW: Method to create and return an iterator
    PyRocksDBIterator new_iterator() {
        rocksdb::ReadOptions read_options; // Default read options for iterator
        // The iterator is created on the heap and its ownership is transferred to PyRocksDBIterator
        return PyRocksDBIterator(db_->NewIterator(read_options));
    }
};

// PYBIND11_MODULE macro creates the Python module
PYBIND11_MODULE(pyrex, m) {
    m.doc() = "pybind11 RocksDB wrapper";

    py::register_exception<RocksDBException>(m, "RocksDBException");

    py::enum_<rocksdb::CompressionType>(m, "CompressionType")
        .value("kNoCompression", rocksdb::kNoCompression)
        .value("kSnappyCompression", rocksdb::kSnappyCompression)
        .value("kZlibCompression", rocksdb::kZlibCompression)
        .value("kBZip2Compression", rocksdb::kBZip2Compression)
        .value("kLZ4Compression", rocksdb::kLZ4Compression)
        .value("kLZ4HCCompression", rocksdb::kLZ4HCCompression)
        .value("kXpressCompression", rocksdb::kXpressCompression)
        .value("kZSTD", rocksdb::kZSTD)
        .value("kDisableCompressionOption", rocksdb::kDisableCompressionOption)
        .export_values();

    py::class_<PyOptions>(m, "PyOptions")
        .def(py::init<>(), "Initializes RocksDB options with default values.")
        .def_property("create_if_missing", &PyOptions::get_create_if_missing, &PyOptions::set_create_if_missing,
                      "If true, the database will be created if it is missing.")
        .def_property("error_if_exists", &PyOptions::get_error_if_exists, &PyOptions::set_error_if_exists,
                      "If true, an error will be thrown during open if the database already exists.")
        .def_property("max_open_files", &PyOptions::get_max_open_files, &PyOptions::set_max_open_files,
                      "Number of open files that can be used by the DB.")
        .def_property("write_buffer_size", &PyOptions::get_write_buffer_size, &PyOptions::set_write_buffer_size,
                      "The maximum size of a write buffer that is used as a memtable.")
        .def_property("compression", &PyOptions::get_compression, &PyOptions::set_compression,
                      "The compression algorithm used for all SST files.")
        .def_property("max_background_jobs", &PyOptions::get_max_background_jobs, &PyOptions::set_max_background_jobs,
                      "Maximum number of concurrent background jobs (both flushes and compactions combined).")
        .def("increase_parallelism", &PyOptions::increase_parallelism, py::arg("total_threads"),
             "Increase parallelism by setting the number of background threads.")
        .def("optimize_for_small_db", &PyOptions::optimize_for_small_db,
             "Optimizes RocksDB for small databases (less than a few GB).")
        .def("use_block_based_bloom_filter", &PyOptions::use_block_based_bloom_filter,
             py::arg("bits_per_key") = 10.0,
             "Enables a bloom filter for block-based tables to speed up lookups.");

    // NEW: Bind PyWriteBatch class
    py::class_<PyWriteBatch>(m, "PyWriteBatch")
        .def(py::init<>(), "Creates an empty RocksDB write batch.")
        .def("put", &PyWriteBatch::put, py::arg("key"), py::arg("value"),
             "Adds a put operation to the batch. Keys and values must be bytes.")
        .def("delete", &PyWriteBatch::del, py::arg("key"), // Renamed to 'del' for Python keyword compatibility
             "Adds a delete operation to the batch. Key must be bytes.")
        .def("clear", &PyWriteBatch::clear, "Clears all operations from the batch.");

    // NEW: Bind PyRocksDBIterator class
    py::class_<PyRocksDBIterator>(m, "PyRocksDBIterator")
        .def("valid", &PyRocksDBIterator::valid, "Returns true if the iterator is currently positioned at a valid key-value pair.")
        .def("seek_to_first", &PyRocksDBIterator::seek_to_first, "Positions the iterator at the first key in the database.")
        .def("seek_to_last", &PyRocksDBIterator::seek_to_last, "Positions the iterator at the last key in the database.")
        .def("seek", &PyRocksDBIterator::seek, py::arg("key"), "Positions the iterator at or after the given key.")
        .def("next", &PyRocksDBIterator::next, "Moves the iterator to the next key-value pair.")
        .def("prev", &PyRocksDBIterator::prev, "Moves the iterator to the previous key-value pair.")
        .def("key", &PyRocksDBIterator::key, "Returns the current key as bytes, or None if the iterator is not valid.")
        .def("value", &PyRocksDBIterator::value, "Returns the current value as bytes, or None if the iterator is not valid.")
        .def("check_status", &PyRocksDBIterator::check_status, "Checks the status of the iterator for errors.");


    py::class_<PyRocksDB>(m, "PyRocksDB")
        .def(py::init<const std::string&, PyOptions*>(), py::arg("path"), py::arg("options") = nullptr,
             "Initializes and opens a RocksDB database at the given path with optional PyOptions.")
        .def("put", &PyRocksDB::put, py::arg("key"), py::arg("value"),
             "Puts a key-value pair into the database. Keys and values must be bytes.")
        .def("get", &PyRocksDB::get, py::arg("key"),
             "Retrieves a value by key. Returns bytes or None if key not found. Key must be bytes.")
        .def("get_options", &PyRocksDB::get_options,
             "Retrieves the PyOptions object used to open this database instance.")
        // NEW: Expose write batch method
        .def("write", &PyRocksDB::write, py::arg("write_batch"),
             "Applies a batch of write operations atomically to the database.")
        // NEW: Expose iterator creation method
        .def("new_iterator", &PyRocksDB::new_iterator,
             "Creates and returns a new RocksDB iterator.");
}

