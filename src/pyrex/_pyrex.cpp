// rocksdb_wrapper.cpp
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

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
    rocksdb::Iterator* it_;

    explicit PyRocksDBIterator(rocksdb::Iterator* it) : it_(it) {
        if (!it_) {
            throw RocksDBException("Failed to create RocksDB iterator: null pointer received.");
        }
    }

    ~PyRocksDBIterator() {
        if (it_ != nullptr) {
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
    PyOptions opened_options_;
    std::string path_;

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
    }

    ~PyRocksDB() {
        if (db_ != nullptr) {
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

    PyOptions get_options() const {
        return opened_options_;
    }

    void write(PyWriteBatch& py_write_batch) {
        rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &py_write_batch.wb_);
        if (!status.ok()) {
            throw RocksDBException("Failed to write batch: " + status.ToString());
        }
    }

    std::unique_ptr<PyRocksDBIterator> new_iterator() {
        rocksdb::ReadOptions read_options;
        return std::make_unique<PyRocksDBIterator>(db_->NewIterator(read_options));
    }
};

// --- PYBIND11 MODULE DEFINITION ---
PYBIND11_MODULE(_pyrex, m) {
    m.doc() = "A Python wrapper for RocksDB, providing key-value store functionality.";

    // CORRECTED LINE for exception docstring
    py::register_exception<RocksDBException>(m, "RocksDBException")
        .doc() = "Custom exception raised for RocksDB operational errors.";

    py::enum_<rocksdb::CompressionType>(m, "CompressionType", "Enum for different compression types supported by RocksDB.")
        .value("kNoCompression", rocksdb::kNoCompression, "No compression.")
        .value("kSnappyCompression", rocksdb::kSnappyCompression, "Snappy compression.")
        .value("kBZip2Compression", rocksdb::kBZip2Compression, "BZip2 compression.")
        .value("kLZ4Compression", rocksdb::kLZ4Compression, "LZ4 compression.")
        .value("kLZ4HCCompression", rocksdb::kLZ4HCCompression, "LZ4HC compression.")
        .value("kXpressCompression", rocksdb::kXpressCompression, "Xpress compression.")
        .value("kZSTD", rocksdb::kZSTD, "Zstandard compression.")
        .value("kDisableCompressionOption", rocksdb::kDisableCompressionOption, "Disable compression option.");

    py::class_<PyOptions>(m, "PyOptions", "Configuration options for opening and managing a RocksDB database.")
        .def(py::init<>(), "Constructs a new PyOptions object with default RocksDB options.")
        .def_property("create_if_missing", &PyOptions::get_create_if_missing, &PyOptions::set_create_if_missing,
                      "If true, the database will be created if it is missing. Default: True.")
        .def_property("error_if_exists", &PyOptions::get_error_if_exists, &PyOptions::set_error_if_exists,
                      "If true, an error is raised if the database already exists. Default: False.")
        .def_property("max_open_files", &PyOptions::get_max_open_files, &PyOptions::set_max_open_files,
                      "Number of open files that can be used by the DB. Default: 5000.")
        .def_property("write_buffer_size", &PyOptions::get_write_buffer_size, &PyOptions::set_write_buffer_size,
                      "Amount of data to build up in a memory buffer (MemTable) before compacting. Default: 64MB.")
        .def_property("compression", &PyOptions::get_compression, &PyOptions::set_compression,
                      "The compression type to use for sst files. Default: Snappy.")
        .def_property("max_background_jobs", &PyOptions::get_max_background_jobs, &PyOptions::set_max_background_jobs,
                      "Maximum number of concurrent background jobs (compactions and flushes). Default: 2.")
        .def("increase_parallelism", &PyOptions::increase_parallelism, py::arg("total_threads"),
             "Increases RocksDB's parallelism by increasing the number of background threads. "
             "Args:\n"
             "    total_threads (int): The total number of background threads to use.")
        .def("optimize_for_small_db", &PyOptions::optimize_for_small_db,
             "Optimizes RocksDB for small databases (less than 1GB) by reducing memory and CPU consumption.")
        .def("use_block_based_bloom_filter", &PyOptions::use_block_based_bloom_filter, py::arg("bits_per_key") = 10.0,
             "Enables a Bloom filter for block-based tables to speed up 'Get' operations.\n"
             "Args:\n"
             "    bits_per_key (float, optional): The number of bits to use per key for the Bloom filter. "
             "        Higher values reduce false positives but increase memory usage. Default: 10.0.");

    py::class_<PyWriteBatch>(m, "PyWriteBatch", "A batch of write operations (Put, Delete) that can be applied atomically to the database.")
        .def(py::init<>(), "Constructs an empty write batch.")
        .def("put", &PyWriteBatch::put, py::arg("key"), py::arg("value"),
             "Adds a key-value pair to the batch for insertion.\n"
             "Args:\n"
             "    key (bytes): The key to insert.\n"
             "    value (bytes): The value to associate with the key.")
        .def("delete", &PyWriteBatch::del, py::arg("key"),
             "Adds a key to the batch for deletion.\n"
             "Args:\n"
             "    key (bytes): The key to delete.")
        .def("clear", &PyWriteBatch::clear, "Clears all operations from the batch.");

    py::class_<PyRocksDBIterator>(m, "PyRocksDBIterator", "An iterator for traversing key-value pairs in a RocksDB database.")
        .def("valid", &PyRocksDBIterator::valid, "Checks if the iterator is currently pointing to a valid key-value pair.")
        .def("seek_to_first", &PyRocksDBIterator::seek_to_first, "Positions the iterator at the first key in the database.")
        .def("seek_to_last", &PyRocksDBIterator::seek_to_last, "Positions the iterator at the last key in the database.")
        .def("seek", &PyRocksDBIterator::seek, py::arg("key"),
             "Positions the iterator at the first key that is greater than or equal to the given key.\n"
             "Args:\n"
             "    key (bytes): The key to seek to.")
        .def("next", &PyRocksDBIterator::next, "Advances the iterator to the next key-value pair.")
        .def("prev", &PyRocksDBIterator::prev, "Moves the iterator to the previous key-value pair.")
        .def("key", &PyRocksDBIterator::key,
             "Returns the current key as bytes. Returns None if the iterator is not valid.")
        .def("value", &PyRocksDBIterator::value,
             "Returns the current value as bytes. Returns None if the iterator is not valid.")
        .def("check_status", &PyRocksDBIterator::check_status, "Checks the status of the iterator and raises a RocksDBException if an error occurred during iteration.");

    py::class_<PyRocksDB>(m, "PyRocksDB", "A Python wrapper for RocksDB, providing key-value storage functionality.")
        .def(py::init<const std::string&, PyOptions*>(), py::arg("path"), py::arg("options") = nullptr,
             "Opens a RocksDB database at the specified path. If the database does not exist, it will be created.\n"
             "Args:\n"
             "    path (str): The file system path to the RocksDB database.\n"
             "    options (PyOptions, optional): Custom options to configure the database. "
             "        If None, default options will be used (create_if_missing=True).")
        .def("put", &PyRocksDB::put, py::arg("key"), py::arg("value"),
             "Inserts a key-value pair into the database.\n"
             "Args:\n"
             "    key (bytes): The key to insert. Must be bytes.\n"
             "    value (bytes): The value to associate with the key. Must be bytes.\n"
             "Raises:\n"
             "    RocksDBException: If the put operation fails.")
        .def("get", &PyRocksDB::get, py::arg("key"),
             "Retrieves the value associated with a given key.\n"
             "Args:\n"
             "    key (bytes): The key to retrieve. Must be bytes.\n"
             "Returns:\n"
             "    bytes or None: The retrieved value as bytes, or None if the key is not found.\n"
             "Raises:\n"
             "    RocksDBException: If the get operation fails for reasons other than key not found.")
        .def("get_options", &PyRocksDB::get_options,
             "Returns the PyOptions object with which the database was opened.\n"
             "Returns:\n"
             "    PyOptions: The options used for this database instance.")
        .def("write", &PyRocksDB::write, py::arg("write_batch"),
             "Applies a batch of write operations (Put, Delete) atomically to the database.\n"
             "Args:\n"
             "    write_batch (PyWriteBatch): The batch of operations to apply.\n"
             "Raises:\n"
             "    RocksDBException: If the write operation fails.")
        .def("new_iterator", &PyRocksDB::new_iterator,
             "Creates and returns a new RocksDB iterator. This iterator allows sequential access to key-value pairs.\n"
             "Returns:\n"
             "    PyRocksDBIterator: A new iterator instance.",
             py::keep_alive<0, 1>()
        );
}
