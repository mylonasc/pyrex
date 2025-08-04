#include <nanobind/nanobind.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/map.h>
#include <nanobind/stl/set.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/shared_ptr.h>
#include <nanobind/stl/atomic.h>
#include <nanobind/exception.h>

#include <memory>
#include <vector>
#include <map>
#include <set>
#include <string>
#include <mutex>
#include <atomic>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/iterator.h"

namespace nb = nanobind;

// --- Custom Exception ---
class RocksDBException : public std::runtime_error {
public:
    explicit RocksDBException(const std::string& msg) : std::runtime_error(msg) {}
};

// --- Forward Declarations ---
class PyRocksDB;

// --- PyReadOptions Wrapper ---
class PyReadOptions {
public:
    rocksdb::ReadOptions options_;

    PyReadOptions() = default;

    // Expose properties to Python
    bool get_fill_cache() const { return options_.fill_cache; }
    void set_fill_cache(bool value) { options_.fill_cache = value; }

    bool get_verify_checksums() const { return options_.verify_checksums; }
    void set_verify_checksums(bool value) { options_.verify_checksums = value; }
};

// --- PyWriteOptions Wrapper ---
class PyWriteOptions {
public:
    rocksdb::WriteOptions options_;

    PyWriteOptions() = default;

    // Expose properties to Python
    bool get_sync() const { return options_.sync; }
    void set_sync(bool value) { options_.sync = value; }

    bool get_disable_wal() const { return options_.disableWAL; }
    void set_disable_wal(bool value) { options_.disableWAL = value; }
};


// --- PyOptions Wrapper ---
class PyOptions {
public:
    rocksdb::Options options_;
    rocksdb::ColumnFamilyOptions cf_options_;

    PyOptions() {
        options_.compression = rocksdb::kSnappyCompression;
        cf_options_.compression = rocksdb::kSnappyCompression;
    }
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
    void increase_parallelism(int total_threads) { options_.IncreaseParallelism(total_threads); }
    void optimize_for_small_db() { options_.OptimizeForSmallDb(); }
    void use_block_based_bloom_filter(double bits_per_key = 10.0) {
        rocksdb::BlockBasedTableOptions table_options;
        table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bits_per_key));
        options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
        cf_options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    }
    size_t get_cf_write_buffer_size() const { return cf_options_.write_buffer_size; }
    void set_cf_write_buffer_size(size_t value) { cf_options_.write_buffer_size = value; }
    rocksdb::CompressionType get_cf_compression() const { return cf_options_.compression; }
    void set_cf_compression(rocksdb::CompressionType value) { cf_options_.compression = value; }
};

// --- PyColumnFamilyHandle Wrapper ---
class PyColumnFamilyHandle {
public:
    rocksdb::ColumnFamilyHandle* cf_handle_;
    std::string name_;

    PyColumnFamilyHandle(rocksdb::ColumnFamilyHandle* handle, const std::string& name)
        : cf_handle_(handle), name_(name) {
        if (!cf_handle_) {
            throw RocksDBException("Invalid ColumnFamilyHandle received.");
        }
    }
    const std::string& get_name() const { return name_; }
    bool is_valid() const { return cf_handle_ != nullptr; }
};

// --- PyWriteBatch Wrapper ---
class PyWriteBatch {
public:
    rocksdb::WriteBatch wb_;
    PyWriteBatch() = default;
    void put(const nb::bytes& key, const nb::bytes& value) { 
        wb_.Put(rocksdb::Slice(key.c_str(), key.size()), rocksdb::Slice(value.c_str(), value.size())); 
    }
    void put_cf(PyColumnFamilyHandle& cf, const nb::bytes& key, const nb::bytes& value) {
        if (!cf.is_valid()) throw RocksDBException("ColumnFamilyHandle is invalid.");
        rocksdb::Slice key_slice(key.c_str(), key.size());
        rocksdb::Slice value_slice(value.c_str(), value.size());
        wb_.Put(cf.cf_handle_, key_slice, value_slice);
    }
    void del(const nb::bytes& key) { 
        rocksdb::Slice key_slice(key.c_str(), key.size());
        wb_.Delete(key_slice); 
    }
    void del_cf(PyColumnFamilyHandle& cf, const nb::bytes& key) {
        if (!cf.is_valid()) throw RocksDBException("ColumnFamilyHandle is invalid.");
        rocksdb::Slice key_slice(key.c_str(), key.size());
        wb_.Delete(cf.cf_handle_, key_slice);
    }
    void merge(const nb::bytes& key, const nb::bytes& value) { 
        rocksdb::Slice key_slice(key.c_str(), key.size());
        rocksdb::Slice value_slice(value.c_str(), value.size());
        wb_.Merge(key_slice, value_slice); 
    }
    void merge_cf(PyColumnFamilyHandle& cf, const nb::bytes& key, const nb::bytes& value) {
        if (!cf.is_valid()) throw RocksDBException("ColumnFamilyHandle is invalid.");
        rocksdb::Slice key_slice(key.c_str(), key.size());
        rocksdb::Slice value_slice(value.c_str(), value.size());
        wb_.Merge(cf.cf_handle_, key_slice, value_slice);
    }
    void clear() { wb_.Clear(); }
};

// --- PyRocksDBIterator Class Declaration ---
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
    void seek(const nb::bytes& key);
    void next();
    void prev();
    nb::object key();
    nb::object value();
    void check_status();
};

// --- PyRocksDB Class (Base) ---
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

    // Default options for read/write operations
    std::shared_ptr<PyReadOptions> default_read_options_;
    std::shared_ptr<PyWriteOptions> default_write_options_;


    friend class PyRocksDBIterator;

    void check_db_open() const {
        if (is_closed_ || db_ == nullptr) {
            throw RocksDBException("Database is not open or has been closed.");
        }
    }

    void check_read_only() const {
        if (is_read_only_.load()) {
            throw RocksDBException("Cannot perform put/write/delete operation: Database opened in read-only mode.");
        }
    }

    rocksdb::ColumnFamilyHandle* get_default_cf_handle() const {
        auto it = cf_handles_.find(rocksdb::kDefaultColumnFamilyName);
        if (it == cf_handles_.end() || !it->second->is_valid()) {
            throw RocksDBException("Default column family handle is not available.");
        }
        return it->second->cf_handle_;
    }

public:
    // Default constructor for inheritance
    PyRocksDB() 
        : default_read_options_(std::make_shared<PyReadOptions>()),
          default_write_options_(std::make_shared<PyWriteOptions>())
    {}

    // Public constructor for the simple interface
    PyRocksDB(const std::string& path, PyOptions* py_options,  bool read_only = false) 
        : default_read_options_(std::make_shared<PyReadOptions>()),
          default_write_options_(std::make_shared<PyWriteOptions>())
    {
        this->path_ = path;
        this->is_read_only_.store(read_only);
        rocksdb::Options options;
        if (py_options) {
            options = py_options->options_;
            this->opened_options_ = *py_options;
        } else {
            options.create_if_missing = true;
            this->opened_options_.options_ = options;
        }

        rocksdb::Status s;
        if (read_only) { 
            s = rocksdb::DB::OpenForReadOnly(options, path, &this->db_);
        } else {
            s = rocksdb::DB::Open(options, path, &this->db_);
        }
        if (!s.ok()) {
            throw RocksDBException("Failed to open RocksDB at " + path + ": " + s.ToString());
        }
        this->default_cf_handle_ = this->db_->DefaultColumnFamily();
        this->cf_handles_[rocksdb::kDefaultColumnFamilyName] = std::make_shared<PyColumnFamilyHandle>(this->default_cf_handle_, rocksdb::kDefaultColumnFamilyName);
    }

    virtual ~PyRocksDB() {
        close();
    }

    void close() {
        if (!is_closed_.exchange(true)) {
            {
                std::lock_guard<std::mutex> lock(active_iterators_mutex_);
                for (rocksdb::Iterator* iter_raw_ptr : active_rocksdb_iterators_) {
                    delete iter_raw_ptr;
                }
                active_rocksdb_iterators_.clear();
            }
            for (auto const& [name, handle_ptr] : cf_handles_) {
                handle_ptr->cf_handle_ = nullptr; 
            }
            cf_handles_.clear();
            if (db_) {
                delete db_;
                db_ = nullptr;
            }
        }
    }

    void put(const nb::bytes& key, const nb::bytes& value, std::shared_ptr<PyWriteOptions> write_options = nullptr) {
        check_db_open();
        check_read_only(); 
        
        rocksdb::Slice key_slice(key.c_str(), key.size());
        rocksdb::Slice value_slice(value.c_str(), value.size());
        
        const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
        rocksdb::Status s = db_->Put(opts, default_cf_handle_, key_slice, value_slice);
        if (!s.ok()) throw RocksDBException("Put failed: " + s.ToString());
    }    

    nb::object get(const nb::bytes& key, std::shared_ptr<PyReadOptions> read_options = nullptr) {
        check_db_open();
        std::string value_str;
        rocksdb::Status s;
        
        const auto& opts = read_options ? read_options->options_ : default_read_options_->options_;

        {
            nb::gil_scoped_release release;
            rocksdb::Slice key_slice(key.c_str(), key.size());
            s = db_->Get(opts, default_cf_handle_, key_slice, &value_str);
        }
        if (s.ok()) return nb::bytes(value_str);
        if (s.IsNotFound()) return nb::none();
        throw RocksDBException("Get failed: " + s.ToString());
    }

    void del(const nb::bytes& key, std::shared_ptr<PyWriteOptions> write_options = nullptr) {
        check_db_open();
        check_read_only();
        rocksdb::Slice key_slice(key.c_str(), key.size());
        const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
        rocksdb::Status s = db_->Delete(opts, default_cf_handle_, key_slice);
        if (!s.ok()) throw RocksDBException("Delete failed: " + s.ToString());
    }

    void write(PyWriteBatch& batch, std::shared_ptr<PyWriteOptions> write_options = nullptr) {
        check_db_open();
        check_read_only();
        const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
        rocksdb::Status s = db_->Write(opts, &batch.wb_);
        if (!s.ok()) throw RocksDBException("Write failed: " + s.ToString());
    }

    std::shared_ptr<PyRocksDBIterator> new_iterator(std::shared_ptr<PyReadOptions> read_options = nullptr) {
        check_db_open();
        const auto& opts = read_options ? read_options->options_ : default_read_options_->options_;
        rocksdb::Iterator* raw_iter = db_->NewIterator(opts, default_cf_handle_);
        {
            std::lock_guard<std::mutex> lock(active_iterators_mutex_);
            active_rocksdb_iterators_.insert(raw_iter);
        }
        return std::make_shared<PyRocksDBIterator>(raw_iter, shared_from_this());
    }

    PyOptions get_options() const { return opened_options_; }

    // Getters and setters for default options
    std::shared_ptr<PyReadOptions> get_default_read_options() { return default_read_options_; }
    void set_default_read_options(std::shared_ptr<PyReadOptions> opts) {
        if (!opts) throw RocksDBException("ReadOptions cannot be None.");
        default_read_options_ = opts;
    }

    std::shared_ptr<PyWriteOptions> get_default_write_options() { return default_write_options_; }
    void set_default_write_options(std::shared_ptr<PyWriteOptions> opts) {
        if (!opts) throw RocksDBException("WriteOptions cannot be None.");
        default_write_options_ = opts;
    }
};

// --- PyRocksDBExtended Class (Derived) ---
class PyRocksDBExtended : public PyRocksDB {
public:
    // Constructor for the extended interface with CF support
    PyRocksDBExtended(const std::string& path, PyOptions* py_options, bool read_only = false) {
        this->path_ = path;
        this->is_read_only_ = read_only; 
        rocksdb::Options options;
        if (py_options) {
            options = py_options->options_;
            this->opened_options_ = *py_options;
        } else {
            options.create_if_missing = true;
            this->opened_options_.options_ = options;
            this->opened_options_.cf_options_.compression = rocksdb::kSnappyCompression;
        }

        std::vector<std::string> cf_names;
        rocksdb::Status s = rocksdb::DB::ListColumnFamilies(options, path, &cf_names);

        std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors;
        if (s.IsNotFound() || s.IsIOError()) {
            if (options.create_if_missing) {
                cf_descriptors.push_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, this->opened_options_.cf_options_));
            } else {
                throw RocksDBException("Database not found at " + path + " and create_if_missing is false.");
            }
        } else if (s.ok()) {
            for (const auto& name : cf_names) {
                cf_descriptors.push_back(rocksdb::ColumnFamilyDescriptor(name, this->opened_options_.cf_options_));
            }
        } else {
            throw RocksDBException("Failed to list column families at " + path + ": " + s.ToString());
        }

        std::vector<rocksdb::ColumnFamilyHandle*> handles;
        rocksdb::Status s_open;

        if (read_only) { 
            s_open = rocksdb::DB::OpenForReadOnly(options, path, cf_descriptors, &handles, &this->db_);
        } else {
            s_open = rocksdb::DB::Open(options, path, cf_descriptors, &handles, &this->db_);
        }
        
        if (!s_open.ok()) {
            throw RocksDBException("Failed to open RocksDB at " + path + ": " + s_open.ToString());
        }

        for (size_t i = 0; i < handles.size(); ++i) {
            const std::string& cf_name = cf_descriptors[i].name;
            this->cf_handles_[cf_name] = std::make_shared<PyColumnFamilyHandle>(handles[i], cf_name);
            
            if (cf_name == rocksdb::kDefaultColumnFamilyName) {
                this->default_cf_handle_ = handles[i];
            }
        }
        
        if (!this->default_cf_handle_) {
            throw RocksDBException("Default column family not found after opening.");
        }
    }

    void put_cf(PyColumnFamilyHandle& cf, const nb::bytes& key, const nb::bytes& value, std::shared_ptr<PyWriteOptions> write_options = nullptr) {
        check_db_open();
        check_read_only();
        if (!cf.is_valid()) throw RocksDBException("ColumnFamilyHandle is invalid.");
        rocksdb::Slice key_slice(key.c_str(), key.size());
        rocksdb::Slice value_slice(value.c_str(), value.size());
        const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
        rocksdb::Status s = db_->Put(opts, cf.cf_handle_, key_slice, value_slice);
        if (!s.ok()) throw RocksDBException("put_cf failed: " + s.ToString());
    }

    nb::object get_cf(PyColumnFamilyHandle& cf, const nb::bytes& key, std::shared_ptr<PyReadOptions> read_options = nullptr) {
        check_db_open();
        if (!cf.is_valid()) throw RocksDBException("ColumnFamilyHandle is invalid.");
        rocksdb::Slice key_slice(key.c_str(), key.size());
        std::string value_str;
        const auto& opts = read_options ? read_options->options_ : default_read_options_->options_;
        rocksdb::Status s = db_->Get(opts, cf.cf_handle_, key_slice, &value_str);
        if (s.ok()) return nb::bytes(value_str);
        if (s.IsNotFound()) return nb::none();
        throw RocksDBException("get_cf failed: " + s.ToString());
    }

    void del_cf(PyColumnFamilyHandle& cf, const nb::bytes& key, std::shared_ptr<PyWriteOptions> write_options = nullptr) {
        check_db_open();
        check_read_only();
        if (!cf.is_valid()) throw RocksDBException("ColumnFamilyHandle is invalid.");
        rocksdb::Slice key_slice(key.c_str(), key.size());
        const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
        rocksdb::Status s = db_->Delete(opts, cf.cf_handle_, key_slice);
        if (!s.ok()) throw RocksDBException("del_cf failed: " + s.ToString());
    }

    std::vector<std::string> list_column_families() {
        check_db_open();
        std::vector<std::string> names;
        for (const auto& pair : cf_handles_) {
            names.push_back(pair.first);
        }
        return names;
    }

    std::shared_ptr<PyColumnFamilyHandle> create_column_family(const std::string& name, PyOptions* cf_py_options) {
        check_db_open();
	    check_read_only();
        if (cf_handles_.count(name)) {
            throw RocksDBException("Column family '" + name + "' already exists.");
        }
        rocksdb::ColumnFamilyOptions cf_opts = cf_py_options ? cf_py_options->cf_options_ : opened_options_.cf_options_;
        rocksdb::ColumnFamilyHandle* cf_handle;
        rocksdb::Status s = db_->CreateColumnFamily(cf_opts, name, &cf_handle);
        if (!s.ok()) throw RocksDBException("Failed to create column family '" + name + "': " + s.ToString());
        
        auto new_handle = std::make_shared<PyColumnFamilyHandle>(cf_handle, name);
        cf_handles_[name] = new_handle;
        return new_handle;
    }

    void drop_column_family(PyColumnFamilyHandle& cf_handle) {
        check_db_open();
        check_read_only();
        if (!cf_handle.is_valid()) throw RocksDBException("ColumnFamilyHandle is invalid.");
        if (cf_handle.get_name() == rocksdb::kDefaultColumnFamilyName) throw RocksDBException("Cannot drop the default column family.");

        rocksdb::ColumnFamilyHandle* raw_handle = cf_handle.cf_handle_;
        std::string cf_name = cf_handle.get_name();

        rocksdb::Status s = db_->DropColumnFamily(raw_handle);
        if (!s.ok()) throw RocksDBException("Failed to drop column family '" + cf_name + "': " + s.ToString());

        s = db_->DestroyColumnFamilyHandle(raw_handle);
        if (!s.ok()) throw RocksDBException("Dropped CF but failed to destroy handle: " + s.ToString());

        cf_handles_.erase(cf_name);
        cf_handle.cf_handle_ = nullptr;
    }

    std::shared_ptr<PyColumnFamilyHandle> get_column_family(const std::string& name) {
        check_db_open();
        auto it = cf_handles_.find(name);
        return (it == cf_handles_.end()) ? nullptr : it->second;
    }

    std::shared_ptr<PyColumnFamilyHandle> get_default_cf() {
        check_db_open();
        return get_column_family(rocksdb::kDefaultColumnFamilyName);
    }

    std::shared_ptr<PyRocksDBIterator> new_cf_iterator(PyColumnFamilyHandle& cf_handle, std::shared_ptr<PyReadOptions> read_options = nullptr) {
        check_db_open();
        if (!cf_handle.is_valid()) throw RocksDBException("ColumnFamilyHandle is invalid.");
        
        const auto& opts = read_options ? read_options->options_ : default_read_options_->options_;
        rocksdb::Iterator* raw_iter = db_->NewIterator(opts, cf_handle.cf_handle_);
        {
            std::lock_guard<std::mutex> lock(active_iterators_mutex_);
            active_rocksdb_iterators_.insert(raw_iter);
        }
        return std::make_shared<PyRocksDBIterator>(raw_iter, shared_from_this());
    }
};

// --- PyRocksDBIterator Method Implementations ---
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
void PyRocksDBIterator::seek(const nb::bytes& key) { check_parent_db_is_open(); it_raw_ptr_->Seek(rocksdb::Slice(key.c_str(), key.size())); }
void PyRocksDBIterator::next() { check_parent_db_is_open(); it_raw_ptr_->Next(); }
void PyRocksDBIterator::prev() { check_parent_db_is_open(); it_raw_ptr_->Prev(); }

nb::object PyRocksDBIterator::key() {
    check_parent_db_is_open();
    if (it_raw_ptr_ && it_raw_ptr_->Valid()) {
        rocksdb::Slice key_slice = it_raw_ptr_->key();
        return nb::bytes(key_slice.data(), key_slice.size());
    }
    return nb::none();
}

nb::object PyRocksDBIterator::value() {
    check_parent_db_is_open();
    if (it_raw_ptr_ && it_raw_ptr_->Valid()) {
        rocksdb::Slice value_slice = it_raw_ptr_->value();
        return nb::bytes(value_slice.data(), value_slice.size());
    }
    return nb::none();
}

void PyRocksDBIterator::check_status() {
    check_parent_db_is_open();
    if (it_raw_ptr_) {
        rocksdb::Status s = it_raw_ptr_->status();
        if (!s.ok()) throw RocksDBException("Iterator error: " + s.ToString());
    }
}

// --- nanobind MODULE DEFINITION ---
NB_MODULE(_pyrex, m) {
    m.doc() = R"doc(
        A robust, high-performance Python wrapper for the RocksDB key-value store.

        This module provides two main classes for interacting with RocksDB:
        1. PyRocksDB: A simple interface for standard key-value operations on a
           database with a single (default) column family.
        2. PyRocksDBExtended: An advanced interface that inherits from PyRocksDB and
           adds full support for creating, managing, and using multiple Column Families.
    )doc";

    nb::exception<RocksDBException>(m, "RocksDBException", PyExc_RuntimeError)
        .doc() = R"doc(
        Custom exception raised for RocksDB-specific operational errors.

        This exception is raised when a RocksDB operation fails for reasons
        such as I/O errors, corruption, invalid arguments, or when an operation
        is attempted on a closed database.
    )doc";

    nb::enum_<rocksdb::CompressionType>(m, "CompressionType", R"doc(
        Enum for different compression types supported by RocksDB.
    )doc")
        .value("kNoCompression", rocksdb::kNoCompression, "No compression.")
        .value("kSnappyCompression", rocksdb::kSnappyCompression, "Snappy compression (default).")
        .value("kBZip2Compression", rocksdb::kBZip2Compression, "BZip2 compression.")
        .value("kLZ4Compression", rocksdb::kLZ4Compression, "LZ4 compression.")
        .value("kLZ4HCCompression", rocksdb::kLZ4HCCompression, "LZ4HC (high compression) compression.")
        .value("kXpressCompression", rocksdb::kXpressCompression, "Xpress compression.")
        .value("kZSTD", rocksdb::kZSTD, "Zstandard compression.");
        
    nb::class_<PyReadOptions, std::shared_ptr<PyReadOptions>>(m, "ReadOptions", R"doc(
        Configuration options for read operations (Get, Iterator).
    )doc")
        .def(nb::init<>(), "Constructs a new ReadOptions object with default settings.")
        .def_prop_rw("fill_cache", &PyReadOptions::get_fill_cache, &PyReadOptions::set_fill_cache, "If True, reads will fill the block cache. Defaults to True.")
        .def_prop_rw("verify_checksums", &PyReadOptions::get_verify_checksums, &PyReadOptions::set_verify_checksums, "If True, all data read from underlying storage will be verified against its checksums. Defaults to True.");

    nb::class_<PyWriteOptions, std::shared_ptr<PyWriteOptions>>(m, "WriteOptions", R"doc(
        Configuration options for write operations (Put, Delete, Write).
    )doc")
        .def(nb::init<>(), "Constructs a new WriteOptions object with default settings.")
        .def_prop_rw("sync", &PyWriteOptions::get_sync, &PyWriteOptions::set_sync, "If True, the write will be flushed from the OS buffer cache before the write is considered complete. Defaults to False.")
        .def_prop_rw("disable_wal", &PyWriteOptions::get_disable_wal, &PyWriteOptions::set_disable_wal, "If True, writes will not be written to the Write Ahead Log. Defaults to False.");

    nb::class_<PyOptions>(m, "PyOptions", R"doc(
        Configuration options for opening and managing a RocksDB database.

        This class wraps `rocksdb::Options` and `rocksdb::ColumnFamilyOptions`
        to provide a convenient way to configure database behavior from Python.
    )doc")
        .def(nb::init<>(), "Constructs a new PyOptions object with default settings.")
        .def_prop_rw("create_if_missing", &PyOptions::get_create_if_missing, &PyOptions::set_create_if_missing, "If True, the database will be created if it is missing. Defaults to True.")
        .def_prop_rw("error_if_exists", &PyOptions::get_error_if_exists, &PyOptions::set_error_if_exists, "If True, an error is raised if the database already exists. Defaults to False.")
        .def_prop_rw("max_open_files", &PyOptions::get_max_open_files, &PyOptions::set_max_open_files, "Number of open files that can be used by the DB. Defaults to -1 (unlimited).")
        .def_prop_rw("write_buffer_size", &PyOptions::get_write_buffer_size, &PyOptions::set_write_buffer_size, "Amount of data to build up in a memory buffer (MemTable) before flushing. Defaults to 64MB.")
        .def_prop_rw("compression", &PyOptions::get_compression, &PyOptions::set_compression, "The compression type to use for sst files. Defaults to Snappy.")
        .def_prop_rw("max_background_jobs", &PyOptions::get_max_background_jobs, &PyOptions::set_max_background_jobs, "Maximum number of concurrent background jobs (compactions and flushes).")
        .def("increase_parallelism", &PyOptions::increase_parallelism, nb::arg("total_threads"), R"doc(
            Increases RocksDB's parallelism by tuning background threads.

            Args:
                total_threads (int): The total number of background threads to use.
        )doc", nb::call_guard<nb::gil_scoped_release>())
        .def("optimize_for_small_db", &PyOptions::optimize_for_small_db, R"doc(
            Optimizes RocksDB for small databases by reducing memory and CPU consumption.
        )doc", nb::call_guard<nb::gil_scoped_release>())
        .def("use_block_based_bloom_filter", &PyOptions::use_block_based_bloom_filter, nb::arg("bits_per_key") = 10.0, R"doc(
            Enables a Bloom filter for block-based tables to speed up 'Get' operations.

            Args:
                bits_per_key (float): The number of bits per key for the Bloom filter.
                    Higher values reduce false positives but increase memory usage.
        )doc", nb::call_guard<nb::gil_scoped_release>())
        .def_prop_rw("cf_write_buffer_size", &PyOptions::get_cf_write_buffer_size, &PyOptions::set_cf_write_buffer_size, "Default write_buffer_size for newly created Column Families.")
        .def_prop_rw("cf_compression", &PyOptions::get_cf_compression, &PyOptions::set_cf_compression, "Default compression type for newly created Column Families.");

    nb::class_<PyColumnFamilyHandle, std::shared_ptr<PyColumnFamilyHandle>>(m, "ColumnFamilyHandle", R"doc(
        Represents a handle to a RocksDB Column Family.

        This object is used to perform operations on a specific data partition
        within a `PyRocksDBExtended` instance.
    )doc")
        .def_prop_ro("name", &PyColumnFamilyHandle::get_name, "The name of this column family.")
        .def("is_valid", &PyColumnFamilyHandle::is_valid, "Checks if the handle is still valid (i.e., has not been dropped).");

    nb::class_<PyWriteBatch>(m, "PyWriteBatch", R"doc(
        A batch of write operations (Put, Delete) that can be applied atomically.
    )doc")
        .def(nb::init<>(), "Constructs an empty write batch.")
        .def("put", &PyWriteBatch::put, nb::arg("key"), nb::arg("value"), "Adds a key-value pair to the batch for the default column family.")
        .def("put_cf", &PyWriteBatch::put_cf, nb::arg("cf_handle"), nb::arg("key"), nb::arg("value"), "Adds a key-value pair to the batch for a specific column family.")
        .def("delete", &PyWriteBatch::del, nb::arg("key"), "Adds a key deletion to the batch for the default column family.")
        .def("delete_cf", &PyWriteBatch::del_cf, nb::arg("cf_handle"), nb::arg("key"), "Adds a key deletion to the batch for a specific column family.")
        .def("merge", &PyWriteBatch::merge, nb::arg("key"), nb::arg("value"), "Adds a merge operation to the batch for the default column family.")
        .def("merge_cf", &PyWriteBatch::merge_cf, nb::arg("cf_handle"), nb::arg("key"), nb::arg("value"), "Adds a merge operation to the batch for a specific column family.")
        .def("clear", &PyWriteBatch::clear, "Clears all operations from the batch.");

    nb::class_<PyRocksDBIterator, std::shared_ptr<PyRocksDBIterator>>(m, "PyRocksDBIterator", R"doc(
        An iterator for traversing key-value pairs in a RocksDB database.
    )doc")
        .def("valid", &PyRocksDBIterator::valid, "Returns True if the iterator is currently positioned at a valid entry.", nb::call_guard<nb::gil_scoped_release>())
        .def("seek_to_first", &PyRocksDBIterator::seek_to_first, "Positions the iterator at the first key.", nb::call_guard<nb::gil_scoped_release>())
        .def("seek_to_last", &PyRocksDBIterator::seek_to_last, "Positions the iterator at the last key.", nb::call_guard<nb::gil_scoped_release>())
        .def("seek", &PyRocksDBIterator::seek, nb::arg("key"), "Positions the iterator at the first key >= the given key.", nb::call_guard<nb::gil_scoped_release>())
        .def("next", &PyRocksDBIterator::next, "Moves the iterator to the next entry.", nb::call_guard<nb::gil_scoped_release>())
        .def("prev", &PyRocksDBIterator::prev, "Moves the iterator to the previous entry.", nb::call_guard<nb::gil_scoped_release>())
        .def("key", &PyRocksDBIterator::key, "Returns the current key as bytes, or None if invalid.")
        .def("value", &PyRocksDBIterator::value, "Returns the current value as bytes, or None if invalid.")
        .def("check_status", &PyRocksDBIterator::check_status, "Raises RocksDBException if an error occurred during iteration.", nb::call_guard<nb::gil_scoped_release>());

    nb::class_<PyRocksDB, std::shared_ptr<PyRocksDB>>(m, "PyRocksDB", R"doc(
        A Python wrapper for RocksDB providing simple key-value storage.

        This class interacts exclusively with the 'default' column family.
        For multi-column-family support, use `PyRocksDBExtended`.
    )doc")
        .def(nb::init<const std::string&, PyOptions*, bool>(), 
            nb::arg("path"), 
            nb::arg("options") = nullptr,
            nb::arg("read_only") = false,
            R"doc(
            Opens a RocksDB database at the specified path.

            Args:
                path (str): The file system path to the database.
                options (PyOptions, optional): Custom options for configuration.
                read_only (bool, optional): If True, opens the database in read-only mode.
                                            Defaults to False.
        )doc", nb::call_guard<nb::gil_scoped_release>())
        .def("put", &PyRocksDB::put, nb::arg("key"), nb::arg("value"), nb::arg("write_options") = nullptr, "Inserts a key-value pair.", nb::call_guard<nb::gil_scoped_release>())
        .def("get", &PyRocksDB::get, nb::arg("key"), nb::arg("read_options") = nullptr, "Retrieves the value for a key.")
        .def("delete", &PyRocksDB::del, nb::arg("key"), nb::arg("write_options") = nullptr, "Deletes a key.", nb::call_guard<nb::gil_scoped_release>())
        .def("write", &PyRocksDB::write, nb::arg("write_batch"), nb::arg("write_options") = nullptr, "Applies a batch of operations atomically.", nb::call_guard<nb::gil_scoped_release>())
        .def("new_iterator", &PyRocksDB::new_iterator, nb::arg("read_options") = nullptr, "Creates a new iterator.", nb::keep_alive<0, 1>())
        .def("get_options", &PyRocksDB::get_options, "Returns the options the database was opened with.")
        .def_prop_rw("default_read_options", &PyRocksDB::get_default_read_options, &PyRocksDB::set_default_read_options, "The default ReadOptions used for get and iterator operations.")
        .def_prop_rw("default_write_options", &PyRocksDB::get_default_write_options, &PyRocksDB::set_default_write_options, "The default WriteOptions used for put, delete, and write operations.")
        .def("close", &PyRocksDB::close, "Closes the database, releasing resources and the lock.", nb::call_guard<nb::gil_scoped_release>())
        .def("__enter__", [](PyRocksDB &db) -> PyRocksDB& { return db; })
        .def("__exit__", [](PyRocksDB &db, nb::object /* type */, nb::object /* value */, nb::object /* traceback */) {
            db.close();
        });

    nb::class_<PyRocksDBExtended, PyRocksDB, std::shared_ptr<PyRocksDBExtended>>(m, "PyRocksDBExtended", R"doc(
        An advanced Python wrapper for RocksDB with full Column Family support.
    )doc")
        .def(nb::init<const std::string&, PyOptions*, bool>(), 
            nb::arg("path"), 
            nb::arg("options") = nullptr, 
            nb::arg("read_only") = false, 
            R"doc(
            Opens or creates a RocksDB database with Column Family support.

            Args:
                path (str): The file system path to the database.
                options (PyOptions, optional): Custom options for configuration.
                read_only (bool, optional): If True, opens the database in read-only mode.
                                            Defaults to False.
        )doc", nb::call_guard<nb::gil_scoped_release>())

        .def("put_cf", &PyRocksDBExtended::put_cf, nb::arg("cf_handle"), nb::arg("key"), nb::arg("value"), nb::arg("write_options") = nullptr, "Inserts a key-value pair into a specific column family.", nb::call_guard<nb::gil_scoped_release>())
        .def("get_cf", &PyRocksDBExtended::get_cf, nb::arg("cf_handle"), nb::arg("key"), nb::arg("read_options") = nullptr, "Retrieves the value for a key from a specific column family.")
        .def("delete_cf", &PyRocksDBExtended::del_cf, nb::arg("cf_handle"), nb::arg("key"), nb::arg("write_options") = nullptr, "Deletes a key from a specific column family.", nb::call_guard<nb::gil_scoped_release>())
        .def("list_column_families", &PyRocksDBExtended::list_column_families, "Lists the names of all existing column families.")
        .def("create_column_family", &PyRocksDBExtended::create_column_family, nb::arg("name"), nb::arg("cf_options") = nullptr, "Creates a new column family.", nb::call_guard<nb::gil_scoped_release>())
        .def("drop_column_family", &PyRocksDBExtended::drop_column_family, nb::arg("cf_handle"), "Drops a column family.", nb::call_guard<nb::gil_scoped_release>())
        .def("new_cf_iterator", &PyRocksDBExtended::new_cf_iterator, nb::arg("cf_handle"), nb::arg("read_options") = nullptr, "Creates a new iterator for a specific column family.", nb::keep_alive<0, 1>())
        .def("get_column_family", &PyRocksDBExtended::get_column_family, nb::arg("name"), "Retrieves a ColumnFamilyHandle by its name.")
        .def_prop_ro("default_cf", &PyRocksDBExtended::get_default_cf, "Returns the handle for the default column family.");
}
