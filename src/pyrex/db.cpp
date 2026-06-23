#include "db.hpp"

#include "columnar_batch.hpp"
#include "exceptions.hpp"
#include "iterator.hpp"
#include "write_batch.hpp"

#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"

PyRocksDB::PyRocksDB()
    : default_read_options_(std::make_shared<PyReadOptions>()),
      default_write_options_(std::make_shared<PyWriteOptions>()) {}

PyRocksDB::PyRocksDB(const std::string& path, PyOptions* py_options, bool read_only)
    : default_read_options_(std::make_shared<PyReadOptions>()),
      default_write_options_(std::make_shared<PyWriteOptions>()) {
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

PyRocksDB::~PyRocksDB() { close(); }

void PyRocksDB::check_db_open() const {
    if (is_closed_ || db_ == nullptr) {
        throw RocksDBException("Database is not open or has been closed.");
    }
}

void PyRocksDB::check_read_only() const {
    if (is_read_only_.load()) {
        throw RocksDBException("Cannot perform put/write/delete operation: Database opened in read-only mode.");
    }
}

rocksdb::ColumnFamilyHandle* PyRocksDB::get_default_cf_handle() const {
    auto it = cf_handles_.find(rocksdb::kDefaultColumnFamilyName);
    if (it == cf_handles_.end() || !it->second->is_valid()) {
        throw RocksDBException("Default column family handle is not available.");
    }
    return it->second->cf_handle_;
}

void PyRocksDB::close() {
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

void PyRocksDB::put(const py::bytes& key, const py::bytes& value, std::shared_ptr<PyWriteOptions> write_options) {
    check_db_open();
    check_read_only();

    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    rocksdb::Slice value_slice(static_cast<std::string_view>(value));

    const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
    rocksdb::Status s = db_->Put(opts, default_cf_handle_, key_slice, value_slice);
    if (!s.ok()) throw RocksDBException("Put failed: " + s.ToString());
}

py::object PyRocksDB::get(const py::bytes& key, std::shared_ptr<PyReadOptions> read_options) {
    check_db_open();
    std::string value_str;
    rocksdb::Status s;

    const auto& opts = read_options ? read_options->options_ : default_read_options_->options_;

    {
        py::gil_scoped_release release;
        rocksdb::Slice key_slice(static_cast<std::string_view>(key));
        s = db_->Get(opts, default_cf_handle_, key_slice, &value_str);
    }
    if (s.ok()) return py::bytes(value_str);
    if (s.IsNotFound()) return py::none();
    throw RocksDBException("Get failed: " + s.ToString());
}

void PyRocksDB::del(const py::bytes& key, std::shared_ptr<PyWriteOptions> write_options) {
    check_db_open();
    check_read_only();
    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
    rocksdb::Status s = db_->Delete(opts, default_cf_handle_, key_slice);
    if (!s.ok()) throw RocksDBException("Delete failed: " + s.ToString());
}

void PyRocksDB::write(PyWriteBatch& batch, std::shared_ptr<PyWriteOptions> write_options) {
    check_db_open();
    check_read_only();
    const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
    rocksdb::Status s = db_->Write(opts, &batch.wb_);
    if (!s.ok()) throw RocksDBException("Write failed: " + s.ToString());
}

void PyRocksDB::write_columnar_batch(const py::object& keys, const py::object& values, std::shared_ptr<PyWriteOptions> write_options, const std::string& on_null) {
    check_db_open();
    check_read_only();

    if (on_null != "error") {
        throw py::value_error("on_null must be 'error'");
    }

    ByteColumn key_column = extract_byte_column(keys, "keys");
    ByteColumn value_column = extract_byte_column(values, "values");

    if (key_column.length_ != value_column.length_) {
        throw py::value_error("keys length " + std::to_string(key_column.length_) + " does not match values length " + std::to_string(value_column.length_));
    }

    if (key_column.length_ == 0) {
        return;
    }

    const auto opts = write_options ? write_options->options_ : default_write_options_->options_;
    rocksdb::Status s;
    {
        py::gil_scoped_release release;
        rocksdb::WriteBatch batch;
        // The hot path stays in C++: Arrow offsets/data buffers can feed RocksDB slices directly.
        // This is especially useful for serialized column payloads where Python only submits one chunk.
        for (size_t i = 0; i < key_column.length_; ++i) {
            std::string_view key = key_column.value(i);
            std::string_view value = value_column.value(i);
            batch.Put(rocksdb::Slice(key.data(), key.size()), rocksdb::Slice(value.data(), value.size()));
        }
        s = db_->Write(opts, &batch);
    }
    if (!s.ok()) throw RocksDBException("Write columnar batch failed: " + s.ToString());
}

std::shared_ptr<PyRocksDBIterator> PyRocksDB::new_iterator(std::shared_ptr<PyReadOptions> read_options) {
    check_db_open();
    const auto& opts = read_options ? read_options->options_ : default_read_options_->options_;
    rocksdb::Iterator* raw_iter = db_->NewIterator(opts, default_cf_handle_);
    {
        std::lock_guard<std::mutex> lock(active_iterators_mutex_);
        active_rocksdb_iterators_.insert(raw_iter);
    }
    return std::make_shared<PyRocksDBIterator>(raw_iter, shared_from_this());
}

PyOptions PyRocksDB::get_options() const { return opened_options_; }

std::shared_ptr<PyReadOptions> PyRocksDB::get_default_read_options() { return default_read_options_; }

void PyRocksDB::set_default_read_options(std::shared_ptr<PyReadOptions> opts) {
    if (!opts) throw RocksDBException("ReadOptions cannot be None.");
    default_read_options_ = opts;
}

std::shared_ptr<PyWriteOptions> PyRocksDB::get_default_write_options() { return default_write_options_; }

void PyRocksDB::set_default_write_options(std::shared_ptr<PyWriteOptions> opts) {
    if (!opts) throw RocksDBException("WriteOptions cannot be None.");
    default_write_options_ = opts;
}

PyRocksDBExtended::PyRocksDBExtended(const std::string& path, PyOptions* py_options, bool read_only) {
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
        throw RocksDBException("Failed to open RocksDB at " + path + ": " + s.ToString());
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

void PyRocksDBExtended::put_cf(PyColumnFamilyHandle& cf, const py::bytes& key, const py::bytes& value, std::shared_ptr<PyWriteOptions> write_options) {
    check_db_open();
    check_read_only();
    if (!cf.is_valid()) throw RocksDBException("ColumnFamilyHandle is invalid.");
    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    rocksdb::Slice value_slice(static_cast<std::string_view>(value));
    const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
    rocksdb::Status s = db_->Put(opts, cf.cf_handle_, key_slice, value_slice);
    if (!s.ok()) throw RocksDBException("put_cf failed: " + s.ToString());
}

py::object PyRocksDBExtended::get_cf(PyColumnFamilyHandle& cf, const py::bytes& key, std::shared_ptr<PyReadOptions> read_options) {
    check_db_open();
    if (!cf.is_valid()) throw RocksDBException("ColumnFamilyHandle is invalid.");
    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    std::string value_str;
    const auto& opts = read_options ? read_options->options_ : default_read_options_->options_;
    rocksdb::Status s = db_->Get(opts, cf.cf_handle_, key_slice, &value_str);
    if (s.ok()) return py::bytes(value_str);
    if (s.IsNotFound()) return py::none();
    throw RocksDBException("get_cf failed: " + s.ToString());
}

void PyRocksDBExtended::del_cf(PyColumnFamilyHandle& cf, const py::bytes& key, std::shared_ptr<PyWriteOptions> write_options) {
    check_db_open();
    check_read_only();
    if (!cf.is_valid()) throw RocksDBException("ColumnFamilyHandle is invalid.");
    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
    rocksdb::Status s = db_->Delete(opts, cf.cf_handle_, key_slice);
    if (!s.ok()) throw RocksDBException("del_cf failed: " + s.ToString());
}

std::vector<std::string> PyRocksDBExtended::list_column_families() {
    check_db_open();
    std::vector<std::string> names;
    for (const auto& pair : cf_handles_) {
        names.push_back(pair.first);
    }
    return names;
}

std::shared_ptr<PyColumnFamilyHandle> PyRocksDBExtended::create_column_family(const std::string& name, PyOptions* cf_py_options) {
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

void PyRocksDBExtended::drop_column_family(PyColumnFamilyHandle& cf_handle) {
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

std::shared_ptr<PyColumnFamilyHandle> PyRocksDBExtended::get_column_family(const std::string& name) {
    check_db_open();
    auto it = cf_handles_.find(name);
    return (it == cf_handles_.end()) ? nullptr : it->second;
}

std::shared_ptr<PyColumnFamilyHandle> PyRocksDBExtended::get_default_cf() {
    check_db_open();
    return get_column_family(rocksdb::kDefaultColumnFamilyName);
}

std::shared_ptr<PyRocksDBIterator> PyRocksDBExtended::new_cf_iterator(PyColumnFamilyHandle& cf_handle, std::shared_ptr<PyReadOptions> read_options) {
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
