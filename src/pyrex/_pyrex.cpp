// rocksdb_wrapper_extended.cpp
#include <pybind11/pybind11.h>
#include <pybind11/stl.h> // For std::vector, std::map, etc.

#include <memory>
#include <vector>
#include <map>
#include <string>
#include <cstring>
#include <atomic>     // For std::atomic
#include <iostream>   // For debug prints, can be removed in production

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/iterator.h"

// Platform-specific includes for directory creation and file existence check
#ifdef _WIN32
#include <direct.h>   // For _mkdir
#include <io.h>       // For _access
#include <windows.h>  // For FindFirstFileA, FindNextFileA, FindClose
#else
#include <sys/stat.h> // For mkdir, stat
#include <unistd.h>   // For access
#endif

namespace py = pybind11;

// Define a custom exception for RocksDB errors
class RocksDBException : public std::runtime_error {
public:
    explicit RocksDBException(const std::string& msg) : std::runtime_error(msg) {}
};

// Forward declaration for PyColumnFamilyHandle and PyRocksDB
class PyColumnFamilyHandle;
class PyRocksDB;

// --- PyOptions class to wrap rocksdb::Options ---
// Includes ColumnFamilyOptions for default behavior
class PyOptions {
public:
    rocksdb::Options options_;
    rocksdb::ColumnFamilyOptions cf_options_; // Default CF options

    PyOptions() : options_(), cf_options_() {
        // Ensure default compression is Snappy for both global and CF options
        options_.compression = rocksdb::kSnappyCompression;
        cf_options_.compression = rocksdb::kSnappyCompression;
    }

    // Methods for rocksdb::Options
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
        rocksdb::BlockBasedTableOptions table_options;
        table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bits_per_key));
        options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
        // Also apply to default CF options so newly created CFs get it by default
        cf_options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    }

    // Methods for rocksdb::ColumnFamilyOptions (default CF options)
    // These affect only the `cf_options_` member within PyOptions
    size_t get_cf_write_buffer_size() const { return cf_options_.write_buffer_size; }
    void set_cf_write_buffer_size(size_t value) { cf_options_.write_buffer_size = value; }

    rocksdb::CompressionType get_cf_compression() const { return cf_options_.compression; }
    void set_cf_compression(rocksdb::CompressionType value) { cf_options_.compression = value; }
};

// --- PyColumnFamilyHandle class to wrap rocksdb::ColumnFamilyHandle ---
class PyColumnFamilyHandle {
public:
    rocksdb::ColumnFamilyHandle* cf_handle_;
    std::string name_;

    // Private constructor to enforce creation only via PyRocksDBExtended
    PyColumnFamilyHandle(rocksdb::ColumnFamilyHandle* handle, const std::string& name)
        : cf_handle_(handle), name_(name) {
        if (!cf_handle_) {
            throw RocksDBException("Invalid ColumnFamilyHandle received during construction.");
        }
    }

    // No destructor needed, ownership is by PyRocksDBExtended
    const std::string& get_name() const {
        return name_;
    }

    // Explicit check for validity (e.g., if dropped)
    bool is_valid() const {
        return cf_handle_ != nullptr;
    }
};


// --- PyWriteBatch class to wrap rocksdb::WriteBatch ---
class PyWriteBatch {
public:
    rocksdb::WriteBatch wb_;

    PyWriteBatch() : wb_() {}

    void put(const py::bytes& key_bytes, const py::bytes& value_bytes) {
        // This will now explicitly use kDefaultColumnFamilyName
        wb_.Put(static_cast<std::string>(key_bytes), static_cast<std::string>(value_bytes));
    }

    // NEW: put_cf method
    void put_cf(PyColumnFamilyHandle& cf_handle, const py::bytes& key_bytes, const py::bytes& value_bytes) {
        if (!cf_handle.is_valid()) {
            throw RocksDBException("ColumnFamilyHandle is invalid (might be dropped) for put_cf.");
        }
        wb_.Put(cf_handle.cf_handle_, static_cast<std::string>(key_bytes), static_cast<std::string>(value_bytes));
    }

    void del(const py::bytes& key_bytes) {
        // This will now explicitly use kDefaultColumnFamilyName
        wb_.Delete(static_cast<std::string>(key_bytes));
    }

    // NEW: del_cf method
    void del_cf(PyColumnFamilyHandle& cf_handle, const py::bytes& key_bytes) {
        if (!cf_handle.is_valid()) {
            throw RocksDBException("ColumnFamilyHandle is invalid (might be dropped) for del_cf.");
        }
        wb_.Delete(cf_handle.cf_handle_, static_cast<std::string>(key_bytes));
    }

    void merge(const py::bytes& key_bytes, const py::bytes& value_bytes) {
        // This will now explicitly use kDefaultColumnFamilyName
        wb_.Merge(static_cast<std::string>(key_bytes), static_cast<std::string>(value_bytes));
    }

    // NEW: merge_cf method
    void merge_cf(PyColumnFamilyHandle& cf_handle, const py::bytes& key_bytes, const py::bytes& value_bytes) {
        if (!cf_handle.is_valid()) {
            throw RocksDBException("ColumnFamilyHandle is invalid (might be dropped) for merge_cf.");
        }
        wb_.Merge(cf_handle.cf_handle_, static_cast<std::string>(key_bytes), static_cast<std::string>(value_bytes));
    }

    void clear() {
        wb_.Clear();
    }
};

// --- PyRocksDBIterator class to wrap rocksdb::Iterator ---
class PyRocksDBIterator {
public:
    rocksdb::Iterator* it_;
    PyRocksDB* parent_db_; // <<< CHANGED: Pointer to parent to check its state

    explicit PyRocksDBIterator(rocksdb::Iterator* it, PyRocksDB* parent_db) 
        : it_(it), parent_db_(parent_db) { // <<< CHANGED: Constructor
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

    // <<< HELPER: Check if parent DB is closed
    void check_parent_db_is_open() const;

    bool valid() const {
        check_parent_db_is_open();
        return it_->Valid();
    }

    void seek_to_first() {
        check_parent_db_is_open();
        it_->SeekToFirst();
    }

    void seek_to_last() {
        check_parent_db_is_open();
        it_->SeekToLast();
    }

    void seek(const py::bytes& key_bytes) {
        check_parent_db_is_open();
        it_->Seek(static_cast<std::string>(key_bytes));
    }

    void next() {
        check_parent_db_is_open();
        it_->Next();
    }

    void prev() {
        check_parent_db_is_open();
        it_->Prev();
    }

    py::object key() {
        check_parent_db_is_open();
        if (it_->Valid()) {
            return py::bytes(it_->key().ToString());
        }
        return py::none();
    }

    py::object value() {
        check_parent_db_is_open();
        if (it_->Valid()) {
            return py::bytes(it_->value().ToString());
        }
        return py::none();
    }

    void check_status() {
        check_parent_db_is_open();
        rocksdb::Status status = it_->status();
        if (!status.ok()) {
            throw RocksDBException("RocksDB Iterator error: " + status.ToString());
        }
    }
};

// --- PyRocksDB: The original class, now implicitly operating on the default CF ---
class PyRocksDB {
protected: 
    rocksdb::DB* db_;
    PyOptions opened_options_;
    std::string path_;
    std::map<std::string, std::shared_ptr<PyColumnFamilyHandle>> cf_handles_;
    
    // <<< ADDED: Atomic flag to track closed state for iterator safety
    std::atomic<bool> is_closed_{false};
    
    // <<< ADDED: Friend class declaration to allow iterator access to is_closed_
    friend class PyRocksDBIterator;

public:
    PyRocksDB(const std::string& path, PyOptions* py_options = nullptr) : db_(nullptr), path_(path) {
        rocksdb::Options actual_options;
        rocksdb::ColumnFamilyOptions default_cf_options;

        if (py_options != nullptr) {
            actual_options = py_options->options_;
            default_cf_options = py_options->cf_options_;
        } else {
            // Default behavior if no options are passed in Python
            actual_options.create_if_missing = true;
            // Ensure these are aligned for default as well
            actual_options.compression = rocksdb::kSnappyCompression; // Common RocksDB default
            default_cf_options.compression = rocksdb::kSnappyCompression;
        }

        opened_options_.options_ = actual_options;
        opened_options_.cf_options_ = default_cf_options;

        std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors;
        std::vector<std::string> existing_cf_names;
        bool db_likely_exists = false;
        
        // Check for the presence of the 'CURRENT' file, which indicates a RocksDB instance
        std::string current_file_path = path_ + "/CURRENT";
        #ifdef _WIN32
            if (_access(current_file_path.c_c_str(), 0) == 0) { // Check if file exists
                db_likely_exists = true;
            }
        #else
            struct stat buffer;
            if (stat(current_file_path.c_str(), &buffer) == 0) {
                db_likely_exists = true; // CURRENT file exists, so DB likely exists
            }
        #endif

        if (!db_likely_exists && actual_options.create_if_missing) {
            // Database does not exist, and we are allowed to create it.
            // Start with just the default column family.
            cf_descriptors.push_back(rocksdb::ColumnFamilyDescriptor(
                rocksdb::kDefaultColumnFamilyName, default_cf_options));
        } else if (db_likely_exists) {
            // Database directory exists and looks like a RocksDB instance.
            // Attempt to list existing column families.
            rocksdb::Status s_list = rocksdb::DB::ListColumnFamilies(actual_options, path_, &existing_cf_names);

            if (!s_list.ok()) {
                // If ListColumnFamilies fails for an existing DB, it's an error.
                throw RocksDBException("Failed to list existing column families at " + path_ + ": " + s_list.ToString());
            } else {
                // Successfully listed column families. Populate descriptors.
                for (const auto& name : existing_cf_names) {
                    cf_descriptors.push_back(rocksdb::ColumnFamilyDescriptor(name, default_cf_options));
                }
            }
        } else {
            // db_likely_exists is false AND actual_options.create_if_missing is false.
            // This is an error condition: DB doesn't exist, and we're not allowed to create it.
            throw RocksDBException("Database does not exist at " + path_ + " and create_if_missing is false.");
        }

        std::vector<rocksdb::ColumnFamilyHandle*> handles;
        rocksdb::Status s = rocksdb::DB::Open(actual_options, path_, cf_descriptors, &handles, &db_);

        if (!s.ok()) {
            throw RocksDBException("Failed to open RocksDB at " + path_ + ": " + s.ToString());
        }

        // Store the handles
        for (size_t i = 0; i < handles.size(); ++i) {
            cf_handles_[cf_descriptors[i].name] = std::make_shared<PyColumnFamilyHandle>(handles[i], cf_descriptors[i].name);
        }
    }

    // Explicit close method to release RocksDB resources and lock
    void close() {
        if (db_ != nullptr) {
            // <<< CHANGED: Set closed flag before doing anything else
            is_closed_ = true;

            for (auto const& [name, handle_ptr] : cf_handles_) {
                handle_ptr->cf_handle_ = nullptr;
            }
            cf_handles_.clear(); 
            
            delete db_; 
            db_ = nullptr;
        }
    }

    virtual ~PyRocksDB() {
        close();
    }

    void put(const py::bytes& key_bytes, const py::bytes& value_bytes) {
        if (db_ == nullptr) {
            throw RocksDBException("Database is not open or has been closed.");
        }
        rocksdb::Status status = db_->Put(rocksdb::WriteOptions(),
                                          get_default_cf_handle(),
                                          static_cast<std::string>(key_bytes),
                                          static_cast<std::string>(value_bytes));
        if (!status.ok()) {
            throw RocksDBException("Failed to put key-value pair: " + status.ToString());
        }
    }

    py::object get(const py::bytes& key_bytes) {
        if (db_ == nullptr) {
            throw RocksDBException("Database is not open or has been closed.");
        }
        std::string value_str;
        rocksdb::Status status = db_->Get(rocksdb::ReadOptions(),
                                          get_default_cf_handle(),
                                          static_cast<std::string>(key_bytes), &value_str);

        if (status.ok()) {
            return py::bytes(value_str);
        } else if (status.IsNotFound()) {
            return py::none();
        } else {
            throw RocksDBException("Failed to get value for key: " + status.ToString());
        }
    }

    void del(const py::bytes& key_bytes) {
        if (db_ == nullptr) {
            throw RocksDBException("Database is not open or has been closed.");
        }
        rocksdb::Status status = db_->Delete(rocksdb::WriteOptions(),
                                             get_default_cf_handle(),
                                             static_cast<std::string>(key_bytes));
        if (!status.ok()) {
            throw RocksDBException("Failed to delete key: " + status.ToString());
        }
    }

    void write(PyWriteBatch& py_write_batch) {
        if (db_ == nullptr) {
            throw RocksDBException("Database is not open or has been closed.");
        }
        rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &py_write_batch.wb_);
        if (!status.ok()) {
            throw RocksDBException("Failed to write batch: " + status.ToString());
        }
    }

    std::shared_ptr<PyRocksDBIterator> new_iterator() {
        if (db_ == nullptr) {
            throw RocksDBException("Database is not open or has been closed.");
        }
        rocksdb::ReadOptions read_options;
        // <<< CHANGED: Pass `this` to the iterator constructor
        return std::make_shared<PyRocksDBIterator>(db_->NewIterator(read_options, get_default_cf_handle()), this);
    }

    PyOptions get_options() const {
        return opened_options_;
    }

protected: 
    rocksdb::ColumnFamilyHandle* get_default_cf_handle() const {
        if (db_ == nullptr) {
            throw RocksDBException("Database is not open or has been closed. Cannot get default CF handle.");
        }
        auto it = cf_handles_.find(rocksdb::kDefaultColumnFamilyName);
        if (it == cf_handles_.end() || !it->second->is_valid()) {
            throw RocksDBException("Default column family handle not found or is invalid.");
        }
        return it->second->cf_handle_;
    }
};

// <<< ADDED: Implementation for the iterator's check method
void PyRocksDBIterator::check_parent_db_is_open() const {
    if (parent_db_->is_closed_) {
        throw RocksDBException("Database is closed.");
    }
}


// --- PyRocksDBExtended: New class for Column Family management ---
class PyRocksDBExtended : public PyRocksDB {
public:
    // Inherit constructor
    using PyRocksDB::PyRocksDB;

    // New methods for Column Family specific operations
    void put_cf(PyColumnFamilyHandle& cf_handle, const py::bytes& key_bytes, const py::bytes& value_bytes) {
        if (db_ == nullptr) {
            throw RocksDBException("Database is not open or has been closed.");
        }
        if (!cf_handle.is_valid()) {
            throw RocksDBException("ColumnFamilyHandle is invalid (might be dropped) for put_cf.");
        }
        rocksdb::Status status = db_->Put(rocksdb::WriteOptions(),
                                          cf_handle.cf_handle_,
                                          static_cast<std::string>(key_bytes),
                                          static_cast<std::string>(value_bytes));
        if (!status.ok()) {
            throw RocksDBException("Failed to put key-value pair to column family '" + cf_handle.get_name() + "': " + status.ToString());
        }
    }

    py::object get_cf(PyColumnFamilyHandle& cf_handle, const py::bytes& key_bytes) {
        if (db_ == nullptr) {
            throw RocksDBException("Database is not open or has been closed.");
        }
        if (!cf_handle.is_valid()) {
            throw RocksDBException("ColumnFamilyHandle is invalid (might be dropped) for get_cf.");
        }
        std::string value_str;
        rocksdb::Status status = db_->Get(rocksdb::ReadOptions(),
                                          cf_handle.cf_handle_,
                                          static_cast<std::string>(key_bytes), &value_str);

        if (status.ok()) {
            return py::bytes(value_str);
        } else if (status.IsNotFound()) {
            return py::none();
        } else {
            throw RocksDBException("Failed to get value for key from column family '" + cf_handle.get_name() + "': " + status.ToString());
        }
    }

    void del_cf(PyColumnFamilyHandle& cf_handle, const py::bytes& key_bytes) {
        if (db_ == nullptr) {
            throw RocksDBException("Database is not open or has been closed.");
        }
        if (!cf_handle.is_valid()) {
            throw RocksDBException("ColumnFamilyHandle is invalid (might be dropped) for del_cf.");
        }
        rocksdb::Status status = db_->Delete(rocksdb::WriteOptions(),
                                             cf_handle.cf_handle_,
                                             static_cast<std::string>(key_bytes));
        if (!status.ok()) {
            throw RocksDBException("Failed to delete key from column family '" + cf_handle.get_name() + "': " + status.ToString());
        }
    }

    std::vector<std::string> list_column_families() {
        if (db_ == nullptr && is_closed_) {
            throw RocksDBException("Database is not open or has been closed.");
        }

        std::vector<std::string> cf_names;
        std::string current_file_path = path_ + "/CURRENT";
        #ifdef _WIN32
            if (_access(current_file_path.c_str(), 0) != 0) {
        #else
            struct stat buffer;
            if (stat(current_file_path.c_str(), &buffer) != 0) {
        #endif
                return {}; // Return empty list if it's not a RocksDB directory
            }

        rocksdb::Status s = rocksdb::DB::ListColumnFamilies(opened_options_.options_, path_, &cf_names);
        if (!s.ok()) {
            throw RocksDBException("Failed to list column families: " + s.ToString());
        }
        return cf_names;
    }

    std::shared_ptr<PyColumnFamilyHandle> create_column_family(const std::string& name, PyOptions* cf_py_options = nullptr) {
        if (db_ == nullptr) {
            throw RocksDBException("Database is not open or has been closed. Cannot create column family.");
        }
        if (cf_handles_.count(name)) {
            throw RocksDBException("Column family '" + name + "' already exists.");
        }

        rocksdb::ColumnFamilyOptions cf_opts = opened_options_.cf_options_;
        if (cf_py_options != nullptr) {
            cf_opts = cf_py_options->cf_options_;
        }

        rocksdb::ColumnFamilyHandle* cf_handle;
        rocksdb::Status s = db_->CreateColumnFamily(cf_opts, name, &cf_handle);
        if (!s.ok()) {
            throw RocksDBException("Failed to create column family '" + name + "': " + s.ToString());
        }
        auto new_handle = std::make_shared<PyColumnFamilyHandle>(cf_handle, name);
        cf_handles_[name] = new_handle;
        return new_handle;
    }

    void drop_column_family(PyColumnFamilyHandle& cf_handle) {
        if (db_ == nullptr) {
            throw RocksDBException("Database is not open or has been closed. Cannot drop column family.");
        }
        if (!cf_handle.is_valid()) {
            throw RocksDBException("ColumnFamilyHandle is invalid (might be dropped already) for drop_column_family.");
        }
        if (cf_handle.get_name() == rocksdb::kDefaultColumnFamilyName) {
            throw RocksDBException("Cannot drop the default column family.");
        }

        // <<< START FIX for resource leak
        rocksdb::ColumnFamilyHandle* raw_handle = cf_handle.cf_handle_;
        std::string cf_name = cf_handle.get_name();

        rocksdb::Status s = db_->DropColumnFamily(raw_handle);
        if (!s.ok()) {
            throw RocksDBException("Failed to drop column family '" + cf_name + "': " + s.ToString());
        }
        
        // CRITICAL: Destroy the handle after dropping the family
        s = db_->DestroyColumnFamilyHandle(raw_handle);
        if (!s.ok()) {
            // This is a problematic state; the CF is dropped but the handle is leaked.
            throw RocksDBException("Dropped column family '" + cf_name + "' but failed to destroy its handle: " + s.ToString());
        }

        cf_handles_.erase(cf_name);
        cf_handle.cf_handle_ = nullptr; // Invalidate the Python-held handle
        // <<< END FIX
    }

    std::shared_ptr<PyRocksDBIterator> new_cf_iterator(PyColumnFamilyHandle& cf_handle) {
        if (db_ == nullptr) {
            throw RocksDBException("Database is not open or has been closed. Cannot create iterator.");
        }
        if (!cf_handle.is_valid()) {
            throw RocksDBException("ColumnFamilyHandle is invalid (might be dropped) for new_cf_iterator.");
        }
        rocksdb::ReadOptions read_options;
        // <<< CHANGED: Pass `this` to the iterator constructor
        return std::make_shared<PyRocksDBIterator>(db_->NewIterator(read_options, cf_handle.cf_handle_), this);
    }

    std::shared_ptr<PyColumnFamilyHandle> get_column_family(const std::string& cf_name) {
        auto it = cf_handles_.find(cf_name);
        if (it == cf_handles_.end()) {
            return py::cast<std::shared_ptr<PyColumnFamilyHandle>>(py::none());
        }
        return it->second;
    }

    std::shared_ptr<PyColumnFamilyHandle> get_default_cf() {
        return get_column_family(rocksdb::kDefaultColumnFamilyName);
    }
};


// --- PYBIND11 MODULE DEFINITION ---
PYBIND11_MODULE(_pyrex, m) {
    m.doc() = "A Python wrapper for RocksDB, providing key-value store functionality with Column Families.";

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

    py::class_<PyOptions>(m, "PyOptions", "Configuration options for opening and managing a RocksDB database and default Column Families.")
        .def(py::init<>(), "Constructs a new PyOptions object with default RocksDB options.")
        .def_property("create_if_missing", &PyOptions::get_create_if_missing, &PyOptions::set_create_if_missing,
                      "If true, the database will be created if it is missing. Default: True.")
        .def_property("error_if_exists", &PyOptions::get_error_if_exists, &PyOptions::set_error_if_exists,
                      "If true, an error is raised if the database already exists. Default: False.")
        .def_property("max_open_files", &PyOptions::get_max_open_files, &PyOptions::set_max_open_files,
                      "Number of open files that can be used by the DB. Default: 5000.")
        .def_property("write_buffer_size", &PyOptions::get_write_buffer_size, &PyOptions::set_write_buffer_size,
                      "Amount of data to build up in a memory buffer (MemTable) before compacting. Default: 64MB.\n"
                      "This applies to the main DB options and default Column Family options.")
        .def_property("compression", &PyOptions::get_compression, &PyOptions::set_compression,
                      "The compression type to use for sst files. Default: Snappy.\n"
                      "This applies to the main DB options and default Column Family options.")
        .def_property("max_background_jobs", &PyOptions::get_max_background_jobs, &PyOptions::set_max_background_jobs,
                      "Maximum number of concurrent background jobs (compactions and flushes). Default: 2.")
        .def("increase_parallelism", &PyOptions::increase_parallelism, py::arg("total_threads"),
             "Increases RocksDB's parallelism by increasing the number of background threads. "
             "Args:\n"
             "    total_threads (int): The total number of background threads to use.",
             py::call_guard<py::gil_scoped_release>())
        .def("optimize_for_small_db", &PyOptions::optimize_for_small_db,
             "Optimizes RocksDB for small databases (less than 1GB) by reducing memory and CPU consumption.",
             py::call_guard<py::gil_scoped_release>())
        .def("use_block_based_bloom_filter", &PyOptions::use_block_based_bloom_filter, py::arg("bits_per_key") = 10.0,
             "Enables a Bloom filter for block-based tables to speed up 'Get' operations. "
             "This sets the filter for default options and default Column Family options.\n"
             "Args:\n"
             "    bits_per_key (float, optional): The number of bits to use per key for the Bloom filter. "
             "        Higher values reduce false positives but increase memory usage. Default: 10.0.",
             py::call_guard<py::gil_scoped_release>())
        .def_property("cf_write_buffer_size", &PyOptions::get_cf_write_buffer_size, &PyOptions::set_cf_write_buffer_size,
                      "Amount of data to build up in a memory buffer (MemTable) for new column families. Default: 64MB.\n"
                      "This applies to the ColumnFamilyOptions member within PyOptions.")
        .def_property("cf_compression", &PyOptions::get_cf_compression, &PyOptions::set_cf_compression,
                      "The compression type to use for sst files in new column families. Default: Snappy.\n"
                      "This applies to the ColumnFamilyOptions member within PyOptions.");

    py::class_<PyColumnFamilyHandle, std::shared_ptr<PyColumnFamilyHandle>>(m, "ColumnFamilyHandle",
              "Represents a handle to a RocksDB Column Family. Used to perform operations on specific data partitions.")
        .def_property_readonly("name", &PyColumnFamilyHandle::get_name,
                               "The name of this column family.")
        .def("is_valid", &PyColumnFamilyHandle::is_valid,
             "Checks if this column family handle is still valid (e.g., has not been dropped).");

    py::class_<PyWriteBatch>(m, "PyWriteBatch", "A batch of write operations (Put, Delete) that can be applied atomically to the database.")
        .def(py::init<>(), "Constructs an empty write batch.")
        .def("put", &PyWriteBatch::put, py::arg("key"), py::arg("value"),
             "Adds a key-value pair to the batch for insertion into the **default** column family.\n"
             "Args:\n"
             "    key (bytes): The key to insert.\n"
             "    value (bytes): The value to associate with the key.")
        .def("put_cf", &PyWriteBatch::put_cf, py::arg("cf_handle"), py::arg("key"), py::arg("value"),
             "Adds a key-value pair to the batch for insertion into a **specific** column family.\n"
             "Args:\n"
             "    cf_handle (ColumnFamilyHandle): The handle to the target column family.\n"
             "    key (bytes): The key to insert.\n"
             "    value (bytes): The value to associate with the key.")
        .def("delete", &PyWriteBatch::del, py::arg("key"),
             "Adds a key to the batch for deletion from the **default** column family.\n"
             "Args:\n"
             "    key (bytes): The key to delete.")
        .def("delete_cf", &PyWriteBatch::del_cf, py::arg("cf_handle"), py::arg("key"),
             "Adds a key to the batch for deletion from a **specific** column family.\n"
             "Args:\n"
             "    cf_handle (ColumnFamilyHandle): The handle to the target column family.\n"
             "    key (bytes): The key to delete.")
        .def("merge", &PyWriteBatch::merge, py::arg("key"), py::arg("value"),
             "Adds a merge operation to the batch for the **default** column family. Requires a merge operator configured.\n"
             "Args:\n"
             "    key (bytes): The key to merge.\n"
             "    value (bytes): The value to merge with the existing value.")
        .def("merge_cf", &PyWriteBatch::merge_cf, py::arg("cf_handle"), py::arg("key"), py::arg("value"),
             "Adds a merge operation to the batch for a **specific** column family. Requires a merge operator configured.\n"
             "Args:\n"
             "    cf_handle (ColumnFamilyHandle): The handle to the target column family.\n"
             "    key (bytes): The key to merge.\n"
             "    value (bytes): The value to merge with the existing value.")
        .def("clear", &PyWriteBatch::clear, "Clears all operations from the batch.");

    py::class_<PyRocksDBIterator, std::shared_ptr<PyRocksDBIterator>>(m, "PyRocksDBIterator", "An iterator for traversing key-value pairs in a RocksDB database.")
        .def("valid", &PyRocksDBIterator::valid, "Checks if the iterator is currently pointing to a valid key-value pair.", py::call_guard<py::gil_scoped_release>())
        .def("seek_to_first", &PyRocksDBIterator::seek_to_first, "Positions the iterator at the first key in the current column family.", py::call_guard<py::gil_scoped_release>())
        .def("seek_to_last", &PyRocksDBIterator::seek_to_last, "Positions the iterator at the last key in the current column family.", py::call_guard<py::gil_scoped_release>())
        .def("seek", &PyRocksDBIterator::seek, py::arg("key"),
             "Positions the iterator at the first key that is greater than or equal to the given key in the current column family.\n"
             "Args:\n"
             "    key (bytes): The key to seek to.",
             py::call_guard<py::gil_scoped_release>())
        .def("next", &PyRocksDBIterator::next, "Advances the iterator to the next key-value pair in the current column family.", py::call_guard<py::gil_scoped_release>())
        .def("prev", &PyRocksDBIterator::prev, "Moves the iterator to the previous key-value pair in the current column family.", py::call_guard<py::gil_scoped_release>())
        .def("key", &PyRocksDBIterator::key,
             "Returns the current key as bytes. Returns None if the iterator is not valid.", py::call_guard<py::gil_scoped_release>())
        .def("value", &PyRocksDBIterator::value,
             "Returns the current value as bytes. Returns None if the iterator is not valid.", py::call_guard<py::gil_scoped_release>())
        .def("check_status", &PyRocksDBIterator::check_status, "Checks the status of the iterator and raises a RocksDBException if an error occurred during iteration.", py::call_guard<py::gil_scoped_release>());

    py::class_<PyRocksDB>(m, "PyRocksDB", "A Python wrapper for RocksDB, providing key-value storage functionality. "
                                        "All operations implicitly target the default column family.")
        .def(py::init<const std::string&, PyOptions*>(), py::arg("path"), py::arg("options") = nullptr,
             "Opens a RocksDB database at the specified path. This instance will primarily interact with the default column family.\n"
             "Args:\n"
             "    path (str): The file system path to the RocksDB database.\n"
             "    options (PyOptions, optional): Custom options to configure the database and default column family options. "
             "        If None, default options will be used (create_if_missing=True).",
             py::call_guard<py::gil_scoped_release>())
        .def("put", &PyRocksDB::put, py::arg("key"), py::arg("value"),
             "Inserts a key-value pair into the **default** column family.\n"
             "Args:\n"
             "    key (bytes): The key to insert. Must be bytes.\n"
             "    value (bytes): The value to associate with the key. Must be bytes.\n"
             "Raises:\n"
             "    RocksDBException: If the put operation fails.",
             py::call_guard<py::gil_scoped_release>())
        .def("get", &PyRocksDB::get, py::arg("key"),
             "Retrieves the value associated with a given key from the **default** column family.\n"
             "Args:\n"
             "    key (bytes): The key to retrieve. Must be bytes.\n"
             "Returns:\n"
             "    bytes or None: The retrieved value as bytes, or None if the key is not found.\n"
             "Raises:\n"
             "    RocksDBException: If the get operation fails for reasons other than key not found.",
             py::call_guard<py::gil_scoped_release>())
        .def("delete", &PyRocksDB::del, py::arg("key"),
             "Deletes a key-value pair from the **default** column family.\n"
             "Args:\n"
             "    key (bytes): The key to delete. Must be bytes.\n"
             "Raises:\n"
             "    RocksDBException: If the delete operation fails.",
             py::call_guard<py::gil_scoped_release>())
        .def("get_options", &PyRocksDB::get_options,
             "Returns the PyOptions object with which the database was opened.\n"
             "Returns:\n"
             "    PyOptions: The options used for this database instance.")
        .def("write", &PyRocksDB::write, py::arg("write_batch"),
             "Applies a batch of write operations atomically. Operations within the batch default to the **default** column family unless specified in the batch itself.\n"
             "Args:\n"
             "    write_batch (PyWriteBatch): The batch of operations to apply.\n"
             "Raises:\n"
             "    RocksDBException: If the write operation fails.",
             py::call_guard<py::gil_scoped_release>())
        .def("new_iterator", &PyRocksDB::new_iterator,
             "Creates and returns a new RocksDB iterator over the **default** column family.\n"
             "Returns:\n"
             "    PyRocksDBIterator: A new iterator instance.",
             py::keep_alive<0, 1>(),
             py::call_guard<py::gil_scoped_release>())
        .def("close", &PyRocksDB::close,
             "Closes the RocksDB database instance, releasing all resources and the database lock.",
             py::call_guard<py::gil_scoped_release>());

    py::class_<PyRocksDBExtended, PyRocksDB>(m, "PyRocksDBExtended",
             "Extends PyRocksDB with explicit Column Family management and operations.")
        .def(py::init<const std::string&, PyOptions*>(), py::arg("path"), py::arg("options") = nullptr,
             "Opens a RocksDB database at the specified path. This class provides methods for explicit Column Family management.\n"
             "Args:\n"
             "    path (str): The file system path to the RocksDB database.\n"
             "    options (PyOptions, optional): Custom options to configure the database and default column family options. "
             "        If None, default options will be used (create_if_missing=True).",
             py::call_guard<py::gil_scoped_release>())
        .def("put_cf", &PyRocksDBExtended::put_cf, py::arg("cf_handle"), py::arg("key"), py::arg("value"),
             "Inserts a key-value pair into a **specific** column family.\n"
             "Args:\n"
             "    cf_handle (ColumnFamilyHandle): The handle to the target column family.\n"
             "    key (bytes): The key to insert. Must be bytes.\n"
             "    value (bytes): The value to associate with the key. Must be bytes.\n"
             "Raises:\n"
             "    RocksDBException: If the put operation fails or the CF handle is invalid.",
             py::call_guard<py::gil_scoped_release>())
        .def("get_cf", &PyRocksDBExtended::get_cf, py::arg("cf_handle"), py::arg("key"),
             "Retrieves the value associated with a given key from a **specific** column family.\n"
             "Args:\n"
             "    cf_handle (ColumnFamilyHandle): The handle to the target column family.\n"
             "    key (bytes): The key to retrieve. Must be bytes.\n"
             "Returns:\n"
             "    bytes or None: The retrieved value as bytes, or None if the key is not found.\n"
             "Raises:\n"
             "    RocksDBException: If the get operation fails or the CF handle is invalid.",
             py::call_guard<py::gil_scoped_release>())
        .def("delete_cf", &PyRocksDBExtended::del_cf, py::arg("cf_handle"), py::arg("key"),
             "Deletes a key-value pair from a **specific** column family.\n"
             "Args:\n"
             "    cf_handle (ColumnFamilyHandle): The handle to the target column family.\n"
             "    key (bytes): The key to delete. Must be bytes.\n"
             "Raises:\n"
             "    RocksDBException: If the delete operation fails or the CF handle is invalid.",
             py::call_guard<py::gil_scoped_release>())
        .def("list_column_families", &PyRocksDBExtended::list_column_families,
             "Lists the names of all existing column families in the database.\n"
             "Returns:\n"
             "    list[str]: A list of column family names.",
             py::call_guard<py::gil_scoped_release>())
        .def("create_column_family", &PyRocksDBExtended::create_column_family, py::arg("name"), py::arg("cf_options") = nullptr,
             "Creates a new column family with the given name and options.\n"
             "Args:\n"
             "    name (str): The name for the new column family.\n"
             "    cf_options (PyOptions, optional): Options specific to this column family. "
             "        If None, the default column family options from the main PyOptions will be used.\n"

             "Returns:\n"
             "    ColumnFamilyHandle: A handle to the newly created column family.\n"
             "Raises:\n"
             "    RocksDBException: If the column family creation fails or if a column family with the given name already exists.",
             py::call_guard<py::gil_scoped_release>())
        .def("drop_column_family", &PyRocksDBExtended::drop_column_family, py::arg("cf_handle"),
             "Drops (deletes) a column family and its data. Cannot drop the default column family.\n"
             "The `ColumnFamilyHandle` object passed becomes invalid after this call.\n"
             "Args:\n"
             "    cf_handle (ColumnFamilyHandle): The handle of the column family to drop.\n"
             "Raises:\n"
             "    RocksDBException: If the column family cannot be dropped, if it's the default column family, or if the CF handle is invalid.",
             py::call_guard<py::gil_scoped_release>())
        .def("new_cf_iterator", &PyRocksDBExtended::new_cf_iterator, py::arg("cf_handle"),
             "Creates and returns a new RocksDB iterator for a **specific** column family.\n"
             "Args:\n"
             "    cf_handle (ColumnFamilyHandle): The column family to iterate over.\n"
             "Returns:\n"
             "    PyRocksDBIterator: A new iterator instance.",
             py::keep_alive<0, 1>(),
             py::call_guard<py::gil_scoped_release>())
        .def_property_readonly("default_cf", &PyRocksDBExtended::get_default_cf,
                               "Returns the ColumnFamilyHandle for the default column family.")
        .def("get_column_family", &PyRocksDBExtended::get_column_family, py::arg("name"),
             "Retrieves a ColumnFamilyHandle by its name from an open RocksDB instance.\n"
             "Returns:\n"
             "    ColumnFamilyHandle or None: The handle if found, None otherwise."
        );
}

