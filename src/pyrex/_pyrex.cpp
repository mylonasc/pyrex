#include <nanobind/nanobind.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/map.h>
#include <nanobind/stl/set.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/shared_ptr.h>

#include <atomic>
#include <memory>
#include <vector>
#include <map>
#include <set>
#include <string>
#include <mutex>
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
class PyRocksDBExtended;

// --- PyReadOptions Wrapper ---
class PyReadOptions {
public:
    rocksdb::ReadOptions options_;
    PyReadOptions() = default;
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
    PyWriteBatch() : wb_() {}
    void put(const nb::bytes& key, const nb::bytes& value) {
        wb_.Put(rocksdb::Slice(key.c_str(), key.size()), rocksdb::Slice(value.c_str(), value.size()));
    }
    void put_cf(PyColumnFamilyHandle& cf, const nb::bytes& key, const nb::bytes& value) {
        if (!cf.is_valid()) throw RocksDBException("ColumnFamilyHandle is invalid.");
        wb_.Put(cf.cf_handle_, rocksdb::Slice(key.c_str(), key.size()), rocksdb::Slice(value.c_str(), value.size()));
    }
    void del(const nb::bytes& key) {
        wb_.Delete(rocksdb::Slice(key.c_str(), key.size()));
    }
    void del_cf(PyColumnFamilyHandle& cf, const nb::bytes& key) {
        if (!cf.is_valid()) throw RocksDBException("ColumnFamilyHandle is invalid.");
        wb_.Delete(cf.cf_handle_, rocksdb::Slice(key.c_str(), key.size()));
    }
    void merge(const nb::bytes& key, const nb::bytes& value) {
        wb_.Merge(rocksdb::Slice(key.c_str(), key.size()), rocksdb::Slice(value.c_str(), value.size()));
    }
    void merge_cf(PyColumnFamilyHandle& cf, const nb::bytes& key, const nb::bytes& value) {
        if (!cf.is_valid()) throw RocksDBException("ColumnFamilyHandle is invalid.");
        wb_.Merge(cf.cf_handle_, rocksdb::Slice(key.c_str(), key.size()), rocksdb::Slice(value.c_str(), value.size()));
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

public:
    PyRocksDB()
        : default_read_options_(std::make_shared<PyReadOptions>()),
          default_write_options_(std::make_shared<PyWriteOptions>())
    {}

    PyRocksDB(const std::string& path, std::shared_ptr<PyOptions> py_options, bool read_only = false)
        : path_(path),
          is_read_only_(read_only),
          default_read_options_(std::make_shared<PyReadOptions>()),
          default_write_options_(std::make_shared<PyWriteOptions>())
    {
        rocksdb::Options options;
        if (py_options) {
            options = py_options->options_;
            this->opened_options_ = *py_options;
        } else {
            auto default_opts = std::make_shared<PyOptions>();
            default_opts->set_create_if_missing(true);
            options = default_opts->options_;
            this->opened_options_ = *default_opts;
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

    virtual void close() {
        if (!is_closed_.exchange(true)) {
            {
                std::lock_guard<std::mutex> lock(active_iterators_mutex_);
                for (rocksdb::Iterator* iter_raw_ptr : active_rocksdb_iterators_) {
                    delete iter_raw_ptr;
                }
                active_rocksdb_iterators_.clear();
            }
            for (auto const& [name, handle_ptr] : cf_handles_) {
                if (handle_ptr) handle_ptr->cf_handle_ = nullptr;
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
        const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
        rocksdb::Status s = db_->Put(opts, default_cf_handle_, rocksdb::Slice(key.c_str(), key.size()), rocksdb::Slice(value.c_str(), value.size()));
        if (!s.ok()) throw RocksDBException("Put failed: " + s.ToString());
    }

    nb::object get(const nb::bytes& key, std::shared_ptr<PyReadOptions> read_options = nullptr) {
        check_db_open();
        std::string value_str;
        const auto& opts = read_options ? read_options->options_ : default_read_options_->options_;
        rocksdb::Status s;
        {
            nb::gil_scoped_release release;
            s = db_->Get(opts, default_cf_handle_, rocksdb::Slice(key.c_str(), key.size()), &value_str);
        }
        if (s.ok()) return nb::bytes(value_str.data(), value_str.size());
        if (s.IsNotFound()) return nb::none();
        throw RocksDBException("Get failed: " + s.ToString());
    }

    void del(const nb::bytes& key, std::shared_ptr<PyWriteOptions> write_options = nullptr) {
        check_db_open();
        check_read_only();
        const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
        rocksdb::Status s = db_->Delete(opts, default_cf_handle_, rocksdb::Slice(key.c_str(), key.size()));
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
    PyRocksDBExtended(const std::string& path, std::shared_ptr<PyOptions> py_options, bool read_only = false) {
        this->path_ = path;
        this->is_read_only_.store(read_only);
        rocksdb::Options options;
        if (py_options) {
            options = py_options->options_;
            this->opened_options_ = *py_options;
        } else {
            auto default_opts = std::make_shared<PyOptions>();
            default_opts->set_create_if_missing(true);
            options = default_opts->options_;
            this->opened_options_ = *default_opts;
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
             if (cf_names.empty()) {
                cf_descriptors.push_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, this->opened_options_.cf_options_));
            } else {
                for (const auto& name : cf_names) {
                    cf_descriptors.push_back(rocksdb::ColumnFamilyDescriptor(name, this->opened_options_.cf_options_));
                }
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
            for(auto h : handles) delete h;
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
            close();
            throw RocksDBException("Default column family not found after opening.");
        }
    }

    void close() override {
        if (!is_closed_.exchange(true)) {
            {
                std::lock_guard<std::mutex> lock(active_iterators_mutex_);
                for (rocksdb::Iterator* iter_raw_ptr : active_rocksdb_iterators_) {
                    delete iter_raw_ptr;
                }
                active_rocksdb_iterators_.clear();
            }
            
            if (db_) {
                for (auto const& [name, handle_ptr] : cf_handles_) {
                    if (handle_ptr && handle_ptr->cf_handle_) {
                        db_->DestroyColumnFamilyHandle(handle_ptr->cf_handle_);
                    }
                }
                delete db_;
                db_ = nullptr;
            }
            for (auto const& [name, handle_ptr] : cf_handles_) {
                if (handle_ptr) handle_ptr->cf_handle_ = nullptr;
            }
            cf_handles_.clear();
        }
    }

    void put_cf(PyColumnFamilyHandle& cf, const nb::bytes& key, const nb::bytes& value, std::shared_ptr<PyWriteOptions> write_options = nullptr) {
        check_db_open();
        check_read_only();
        if (!cf.is_valid()) throw RocksDBException("ColumnFamilyHandle is invalid.");
        const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
        rocksdb::Status s = db_->Put(opts, cf.cf_handle_, rocksdb::Slice(key.c_str(), key.size()), rocksdb::Slice(value.c_str(), value.size()));
        if (!s.ok()) throw RocksDBException("put_cf failed: " + s.ToString());
    }

    nb::object get_cf(PyColumnFamilyHandle& cf, const nb::bytes& key, std::shared_ptr<PyReadOptions> read_options = nullptr) {
        check_db_open();
        if (!cf.is_valid()) throw RocksDBException("ColumnFamilyHandle is invalid.");
        std::string value_str;
        const auto& opts = read_options ? read_options->options_ : default_read_options_->options_;
        rocksdb::Status s = db_->Get(opts, cf.cf_handle_, rocksdb::Slice(key.c_str(), key.size()), &value_str);
        if (s.ok()) return nb::bytes(value_str.data(), value_str.size());
        if (s.IsNotFound()) return nb::none();
        throw RocksDBException("get_cf failed: " + s.ToString());
    }

    void del_cf(PyColumnFamilyHandle& cf, const nb::bytes& key, std::shared_ptr<PyWriteOptions> write_options = nullptr) {
        check_db_open();
        check_read_only();
        if (!cf.is_valid()) throw RocksDBException("ColumnFamilyHandle is invalid.");
        const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
        rocksdb::Status s = db_->Delete(opts, cf.cf_handle_, rocksdb::Slice(key.c_str(), key.size()));
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

    std::shared_ptr<PyColumnFamilyHandle> create_column_family(const std::string& name, std::shared_ptr<PyOptions> cf_py_options) {
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
        
        cf_handles_.erase(cf_name);
        cf_handle.cf_handle_ = nullptr;
        
        rocksdb::Status s = db_->DropColumnFamily(raw_handle);
        if (!s.ok()) {
            cf_handles_[cf_name] = std::make_shared<PyColumnFamilyHandle>(raw_handle, cf_name);
            throw RocksDBException("Failed to drop column family '" + cf_name + "': " + s.ToString());
        }
        
        s = db_->DestroyColumnFamilyHandle(raw_handle);
        if (!s.ok()) throw RocksDBException("Dropped CF but failed to destroy handle: " + s.ToString());
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
    if (parent_db_ptr_ && !parent_db_ptr_->is_closed_.load()) {
        std::lock_guard<std::mutex> lock(parent_db_ptr_->active_iterators_mutex_);
        if (it_raw_ptr_ && parent_db_ptr_->active_rocksdb_iterators_.count(it_raw_ptr_)) {
            parent_db_ptr_->active_rocksdb_iterators_.erase(it_raw_ptr_);
            delete it_raw_ptr_;
        }
    } else if (it_raw_ptr_) {
        delete it_raw_ptr_;
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
    m.doc() = "A robust, high-performance Python wrapper for the RocksDB key-value store.";

    nb::exception<RocksDBException>(m, "RocksDBException", PyExc_RuntimeError)
        .doc() = "Custom exception raised for RocksDB-specific operational errors.";

    nb::enum_<rocksdb::CompressionType>(m, "CompressionType", "Enum for different compression types supported by RocksDB.")
        .value("kNoCompression", rocksdb::kNoCompression)
        .value("kSnappyCompression", rocksdb::kSnappyCompression)
        .value("kBZip2Compression", rocksdb::kBZip2Compression)
        .value("kLZ4Compression", rocksdb::kLZ4Compression)
        .value("kLZ4HCCompression", rocksdb::kLZ4HCCompression)
        .value("kXpressCompression", rocksdb::kXpressCompression)
        .value("kZSTD", rocksdb::kZSTD);

    nb::class_<PyReadOptions>(m, "ReadOptions", "Configuration options for read operations (Get, Iterator).")
        .def(nb::init<>())
        .def_prop_rw("fill_cache", &PyReadOptions::get_fill_cache, &PyReadOptions::set_fill_cache)
        .def_prop_rw("verify_checksums", &PyReadOptions::get_verify_checksums, &PyReadOptions::set_verify_checksums);

    nb::class_<PyWriteOptions>(m, "WriteOptions", "Configuration options for write operations (Put, Delete, Write).")
        .def(nb::init<>())
        .def_prop_rw("sync", &PyWriteOptions::get_sync, &PyWriteOptions::set_sync)
        .def_prop_rw("disable_wal", &PyWriteOptions::get_disable_wal, &PyWriteOptions::set_disable_wal);

    nb::class_<PyOptions>(m, "Options", "Configuration options for opening and managing a RocksDB database.")
        .def(nb::init<>())
        .def_prop_rw("create_if_missing", &PyOptions::get_create_if_missing, &PyOptions::set_create_if_missing)
        .def_prop_rw("error_if_exists", &PyOptions::get_error_if_exists, &PyOptions::set_error_if_exists)
        .def_prop_rw("max_open_files", &PyOptions::get_max_open_files, &PyOptions::set_max_open_files)
        .def_prop_rw("write_buffer_size", &PyOptions::get_write_buffer_size, &PyOptions::set_write_buffer_size)
        .def_prop_rw("compression", &PyOptions::get_compression, &PyOptions::set_compression)
        .def_prop_rw("max_background_jobs", &PyOptions::get_max_background_jobs, &PyOptions::set_max_background_jobs)
        .def("increase_parallelism", &PyOptions::increase_parallelism, nb::arg("total_threads"), nb::call_guard<nb::gil_scoped_release>())
        .def("optimize_for_small_db", &PyOptions::optimize_for_small_db, nb::call_guard<nb::gil_scoped_release>())
        .def("use_block_based_bloom_filter", &PyOptions::use_block_based_bloom_filter, nb::arg("bits_per_key") = 10.0, nb::call_guard<nb::gil_scoped_release>())
        .def_prop_rw("cf_write_buffer_size", &PyOptions::get_cf_write_buffer_size, &PyOptions::set_cf_write_buffer_size)
        .def_prop_rw("cf_compression", &PyOptions::get_cf_compression, &PyOptions::set_cf_compression);

    nb::class_<PyColumnFamilyHandle>(m, "ColumnFamilyHandle", "Represents a handle to a RocksDB Column Family.")
        .def_prop_ro("name", &PyColumnFamilyHandle::get_name)
        .def("is_valid", &PyColumnFamilyHandle::is_valid);

    nb::class_<PyWriteBatch>(m, "WriteBatch", "A batch of write operations (Put, Delete) that can be applied atomically.")
        .def(nb::init<>())
        .def("put", &PyWriteBatch::put, nb::arg("key"), nb::arg("value"))
        .def("put_cf", &PyWriteBatch::put_cf, nb::arg("cf_handle"), nb::arg("key"), nb::arg("value"))
        .def("delete", &PyWriteBatch::del, nb::arg("key"))
        .def("delete_cf", &PyWriteBatch::del_cf, nb::arg("cf_handle"), nb::arg("key"))
        .def("merge", &PyWriteBatch::merge, nb::arg("key"), nb::arg("value"))
        .def("merge_cf", &PyWriteBatch::merge_cf, nb::arg("cf_handle"), nb::arg("key"), nb::arg("value"))
        .def("clear", &PyWriteBatch::clear);

    nb::class_<PyRocksDBIterator>(m, "Iterator", "An iterator for traversing key-value pairs in a RocksDB database.")
        .def("valid", &PyRocksDBIterator::valid, nb::call_guard<nb::gil_scoped_release>())
        .def("seek_to_first", &PyRocksDBIterator::seek_to_first, nb::call_guard<nb::gil_scoped_release>())
        .def("seek_to_last", &PyRocksDBIterator::seek_to_last, nb::call_guard<nb::gil_scoped_release>())
        .def("seek", &PyRocksDBIterator::seek, nb::arg("key"), nb::call_guard<nb::gil_scoped_release>())
        .def("next", &PyRocksDBIterator::next, nb::call_guard<nb::gil_scoped_release>())
        .def("prev", &PyRocksDBIterator::prev, nb::call_guard<nb::gil_scoped_release>())
        .def("key", &PyRocksDBIterator::key)
        .def("value", &PyRocksDBIterator::value)
        .def("check_status", &PyRocksDBIterator::check_status, nb::call_guard<nb::gil_scoped_release>());

    nb::class_<PyRocksDB>(m, "DB", "A Python wrapper for RocksDB providing simple key-value storage.")
	.def(nb::init<const std::string &, std::shared_ptr<PyOptions>, bool>(),
             nb::arg("path"), nb::arg("options") = nullptr, nb::arg("read_only") = false, nb::call_guard<nb::gil_scoped_release>())
        .def("put", &PyRocksDB::put, nb::arg("key"), nb::arg("value"), nb::arg("write_options") = nullptr, nb::call_guard<nb::gil_scoped_release>())
        .def("get", &PyRocksDB::get, nb::arg("key"), nb::arg("read_options") = nullptr)
        .def("delete", &PyRocksDB::del, nb::arg("key"), nb::arg("write_options") = nullptr, nb::call_guard<nb::gil_scoped_release>())
        .def("write", &PyRocksDB::write, nb::arg("batch"), nb::arg("write_options") = nullptr, nb::call_guard<nb::gil_scoped_release>())
        .def("new_iterator", &PyRocksDB::new_iterator, nb::arg("read_options") = nullptr, nb::keep_alive<0, 1>())
        .def("get_options", &PyRocksDB::get_options)
        .def_prop_rw("default_read_options", &PyRocksDB::get_default_read_options, &PyRocksDB::set_default_read_options)
        .def_prop_rw("default_write_options", &PyRocksDB::get_default_write_options, &PyRocksDB::set_default_write_options)
        .def("close", &PyRocksDB::close, nb::call_guard<nb::gil_scoped_release>())
        .def("__enter__", [](std::shared_ptr<PyRocksDB> &db) { return db; })
        .def("__exit__", [](std::shared_ptr<PyRocksDB> &db, nb::object, nb::object, nb::object) {
            db->close();
        });

    nb::class_<PyRocksDBExtended, PyRocksDB>(m, "ExtendedDB", "An advanced Python wrapper for RocksDB with full Column Family support.")
	.def(nb::init<const std::string &, std::shared_ptr<PyOptions>, bool>(),
             nb::arg("path"), nb::arg("options") = nullptr, nb::arg("read_only") = false, nb::call_guard<nb::gil_scoped_release>())
        .def("put_cf", &PyRocksDBExtended::put_cf, nb::arg("cf_handle"), nb::arg("key"), nb::arg("value"), nb::arg("write_options") = nullptr, nb::call_guard<nb::gil_scoped_release>())
        .def("get_cf", &PyRocksDBExtended::get_cf, nb::arg("cf_handle"), nb::arg("key"), nb::arg("read_options") = nullptr)
        .def("delete_cf", &PyRocksDBExtended::del_cf, nb::arg("cf_handle"), nb::arg("key"), nb::arg("write_options") = nullptr, nb::call_guard<nb::gil_scoped_release>())
        .def("list_column_families", &PyRocksDBExtended::list_column_families)
        .def("create_column_family", &PyRocksDBExtended::create_column_family, nb::arg("name"), nb::arg("cf_options") = nullptr, nb::call_guard<nb::gil_scoped_release>())
        .def("drop_column_family", &PyRocksDBExtended::drop_column_family, nb::arg("cf_handle"), nb::call_guard<nb::gil_scoped_release>())
        .def("new_cf_iterator", &PyRocksDBExtended::new_cf_iterator, nb::arg("cf_handle"), nb::arg("read_options") = nullptr, nb::keep_alive<0, 1>())
        .def("get_column_family", &PyRocksDBExtended::get_column_family, nb::arg("name"))
        .def_prop_ro("default_cf", &PyRocksDBExtended::get_default_cf);
}
