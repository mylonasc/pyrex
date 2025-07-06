// rocksdb_wrapper.cpp
#include <pybind11/pybind11.h> // Core pybind11 header
#include <pybind11/stl.h>     // For converting C++ STL containers to Python lists/tuples/dicts

#include "rocksdb/db.h"       // RocksDB database main header
#include "rocksdb/options.h"  // RocksDB options for opening/creating DB
#include "rocksdb/status.h"   // RocksDB status object for error handling
#include "rocksdb/slice.h"    // RocksDB Slice for efficient byte handling
#include "rocksdb/table.h"    // For BlockBasedTableOptions (used for bloom filter)
#include "rocksdb/filter_policy.h" // Required for NewBloomFilterPolicy

#include <iostream>           // For C++ standard output/error (useful for debugging)
#include <string>             // For std::string
#include <cstring>            // Required for strcmp

namespace py = pybind11;

// Define a custom exception for RocksDB errors
// This allows us to raise specific Python exceptions from C++
class RocksDBException : public std::runtime_error {
public:
    explicit RocksDBException(const std::string& msg) : std::runtime_error(msg) {}
};

// --- PyOptions class to wrap rocksdb::Options ---
// This class will allow Python users to configure RocksDB options
class PyOptions {
public:
    rocksdb::Options options_; // The actual RocksDB Options object

    // Constructor: Initializes with default RocksDB options
    PyOptions() : options_() {
        // Default options are fine, RocksDB's default constructor sets sensible values.
        // We can override specific ones via setters below.
    }

    // --- Getters and Setters for various RocksDB options ---

    // create_if_missing
    bool get_create_if_missing() const { return options_.create_if_missing; }
    void set_create_if_missing(bool value) { options_.create_if_missing = value; }

    // error_if_exists
    bool get_error_if_exists() const { return options_.error_if_exists; }
    void set_error_if_exists(bool value) { options_.error_if_exists = value; }

    // max_open_files
    int get_max_open_files() const { return options_.max_open_files; }
    void set_max_open_files(int value) { options_.max_open_files = value; }

    // write_buffer_size
    size_t get_write_buffer_size() const { return options_.write_buffer_size; }
    void set_write_buffer_size(size_t value) { options_.write_buffer_size = value; }

    // compression
    rocksdb::CompressionType get_compression() const { return options_.compression; }
    void set_compression(rocksdb::CompressionType value) { options_.compression = value; }

    // max_background_jobs
    int get_max_background_jobs() const { return options_.max_background_jobs; }
    void set_max_background_jobs(int value) { options_.max_background_jobs = value; }

    // increase_parallelism (a method on Options, not a direct field)
    void increase_parallelism(int total_threads) {
        options_.IncreaseParallelism(total_threads);
    }

    // OptimizeForSmallDb (a helper method on Options)
    // This method optimizes RocksDB for small databases (less than a few GB)
    void optimize_for_small_db() {
        options_.OptimizeForSmallDb();
    }

    // Use a bloom filter for block-based tables
    // This requires casting the TableFactory to BlockBasedTableFactory
    // and setting the filter policy.
    void use_block_based_bloom_filter(double bits_per_key = 10.0) {
        // Ensure the table factory is BlockBasedTableFactory
        // If it's not already, set it.
        if (options_.table_factory == nullptr ||
            std::strcmp(options_.table_factory->Name(), "BlockBasedTable") != 0) {
            options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory());
        }

        rocksdb::BlockBasedTableOptions table_options;
        // Attempt to get existing table options if applicable
        if (options_.table_factory != nullptr) {
            // This is a bit tricky as RocksDB's C++ API doesn't easily expose
            // how to get the current BlockBasedTableOptions from a TableFactory.
            // For simplicity, we'll assume we're setting it fresh or overriding.
        }

        // Create a new bloom filter policy
        table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bits_per_key));
        options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    }
};


// --- PyRocksDB class to wrap rocksdb::DB ---
// Our C++ class that will be exposed to Python
class PyRocksDB {
public:
    // Pointer to the RocksDB database instance
    rocksdb::DB* db_;
    // Store the options used to open the DB
    PyOptions opened_options_;

    // Constructor: Opens the RocksDB database
    // This will be called when `PyRocksDB()` is created in Python
    PyRocksDB(const std::string& path, PyOptions* py_options = nullptr) : db_(nullptr) {
        rocksdb::Options actual_options;

        if (py_options != nullptr) {
            // If PyOptions object is provided, use its internal RocksDB options
            actual_options = py_options->options_;
        } else {
            // If no PyOptions object is provided, use a default PyOptions object's internal RocksDB options
            PyOptions default_py_options;
            actual_options = default_py_options.options_;
            actual_options.create_if_missing = true; // Ensure default behavior
        }

        // Store a copy of the options that were actually used to open the DB
        opened_options_.options_ = actual_options;

        // Attempt to open the database
        rocksdb::Status status = rocksdb::DB::Open(actual_options, path, &db_);

        // Check if the operation was successful
        if (!status.ok()) {
            // If not, throw our custom exception with the RocksDB error message
            throw RocksDBException("Failed to open RocksDB at " + path + ": " + status.ToString());
        }

        std::cout << "RocksDB opened successfully at: " << path << std::endl;
    }

    // Destructor: Closes the RocksDB database
    // This will be called when the Python `PyRocksDB` object is garbage collected
    ~PyRocksDB() {
        if (db_ != nullptr) {
            std::cout << "Closing RocksDB database." << std::endl;
            delete db_; // Release the database resources
            db_ = nullptr;
        }
    }

    // Put method: Writes a key-value pair to the database
    // Takes Python bytes objects for key and value
    void put(const py::bytes& key_bytes, const py::bytes& value_bytes) {
        // Convert Python bytes to C++ std::string (which RocksDB Slice can use)
        std::string key_str = static_cast<std::string>(key_bytes);
        std::string value_str = static_cast<std::string>(value_bytes);

        rocksdb::WriteOptions write_options; // Default write options

        // Perform the Put operation
        rocksdb::Status status = db_->Put(write_options, key_str, value_str);

        // Check for errors
        if (!status.ok()) {
            throw RocksDBException("Failed to put key-value pair: " + status.ToString());
        }
    }

    // Get method: Retrieves a value by key from the database
    // Takes a Python bytes object for the key
    // Returns Python bytes or None if key not found
    py::object get(const py::bytes& key_bytes) {
        std::string key_str = static_cast<std::string>(key_bytes);
        std::string value_str; // String to hold the retrieved value

        rocksdb::ReadOptions read_options; // Default read options

        // Perform the Get operation
        rocksdb::Status status = db_->Get(read_options, key_str, &value_str);

        if (status.ok()) {
            // If found, convert C++ string to Python bytes and return
            return py::bytes(value_str);
        } else if (status.IsNotFound()) {
            // If key not found, return Python None
            return py::none();
        } else {
            // For other errors, throw an exception
            throw RocksDBException("Failed to get value for key: " + status.ToString());
        }
    }

    // Method to retrieve the options used to open this database instance
    // Returns a PyOptions object
    PyOptions get_options() const {
        return opened_options_;
    }
};

// PYBIND11_MODULE macro creates the Python module
// The first argument is the module name (e.g., 'pyrex')
PYBIND11_MODULE(pyrex, m) { // Changed module name to 'pyrex'
    m.doc() = "pybind11 RocksDB wrapper"; // Optional: module docstring

    // Define our custom exception class in Python
    // This allows Python users to catch `pyrex.RocksDBException`
    py::register_exception<RocksDBException>(m, "RocksDBException");

    // Expose rocksdb::CompressionType enum to Python
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
        .export_values(); // Makes enum values directly accessible (e.g., `pyrex.CompressionType.kSnappyCompression`)

    // Bind the PyOptions C++ class to a Python class named 'PyOptions'
    py::class_<PyOptions>(m, "PyOptions")
        .def(py::init<>(), "Initializes RocksDB options with default values.")
        // Properties for options (getters and setters)
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
        // Methods for options
        .def("increase_parallelism", &PyOptions::increase_parallelism, py::arg("total_threads"),
             "Increase parallelism by setting the number of background threads.")
        .def("optimize_for_small_db", &PyOptions::optimize_for_small_db,
             "Optimizes RocksDB for small databases (less than a few GB).")
        .def("use_block_based_bloom_filter", &PyOptions::use_block_based_bloom_filter,
             py::arg("bits_per_key") = 10.0,
             "Enables a bloom filter for block-based tables to speed up lookups.");


    // Bind the PyRocksDB C++ class to a Python class named 'PyRocksDB'
    py::class_<PyRocksDB>(m, "PyRocksDB")
        // Bind the constructor, now accepting an optional PyOptions object
        .def(py::init<const std::string&, PyOptions*>(), py::arg("path"), py::arg("options") = nullptr,
             "Initializes and opens a RocksDB database at the given path with optional PyOptions.")
        // Bind the put method
        .def("put", &PyRocksDB::put, py::arg("key"), py::arg("value"),
             "Puts a key-value pair into the database. Keys and values must be bytes.")
        // Bind the get method
        .def("get", &PyRocksDB::get, py::arg("key"),
             "Retrieves a value by key. Returns bytes or None if key not found. Key must be bytes.")
        // Bind the get_options method
        .def("get_options", &PyRocksDB::get_options,
             "Retrieves the PyOptions object used to open this database instance.");
}


