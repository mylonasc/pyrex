#include "transaction_db.hpp"

#include "exceptions.hpp"
#include "transaction.hpp"
#include "write_batch.hpp"

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"

PyTransactionDB::PyTransactionDB(const std::string& path, PyOptions* py_options, PyTransactionDBOptions* txn_db_options) {
    path_ = path;
    rocksdb::Options options;
    if (py_options) {
        options = py_options->options_;
        opened_options_ = *py_options;
    } else {
        options.create_if_missing = true;
        opened_options_.options_ = options;
    }

    rocksdb::TransactionDBOptions transaction_db_options;
    if (txn_db_options) {
        transaction_db_options = txn_db_options->options_;
        opened_transaction_db_options_ = *txn_db_options;
    } else {
        opened_transaction_db_options_.options_ = transaction_db_options;
    }

    // Open all persisted CFs, not just default, so the transactional handle
    // can reopen a database created with named families.
    std::vector<std::string> names;
    auto listed = rocksdb::DB::ListColumnFamilies(options, path, &names);
    if (!listed.ok()) {
        if (!(listed.IsNotFound() || listed.IsIOError()) || !options.create_if_missing)
            throw_rocksdb_status(listed, "Failed to list column families");
        names = {rocksdb::kDefaultColumnFamilyName};
    }
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
    for (const auto& name : names) descriptors.emplace_back(name, rocksdb::ColumnFamilyOptions(options));
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::TransactionDB* raw = nullptr;
    rocksdb::Status s = rocksdb::TransactionDB::Open(options, transaction_db_options, path, descriptors, &handles, &raw);
    if (!s.ok()) throw_rocksdb_status(s, "Failed to open TransactionDB at " + path);
    // PyRocksDB::db_ takes ownership (TransactionDB IS-A DB, virtual dtor).
    db_ = raw;
    txn_db_ = raw;
    owns_cf_handles_ = true;
    for (size_t i = 0; i < names.size(); ++i) {
        cf_handles_[names[i]] = std::make_shared<PyColumnFamilyHandle>(handles[i], names[i]);
        if (names[i] == rocksdb::kDefaultColumnFamilyName) default_cf_handle_ = handles[i];
    }
}

PyTransactionDB::~PyTransactionDB() { close(); }

void PyTransactionDB::register_transaction(PyTransaction* txn) {
    std::lock_guard<std::mutex> lock(active_transactions_mutex_);
    active_transactions_.insert(txn);
}

void PyTransactionDB::unregister_transaction(PyTransaction* txn) {
    std::lock_guard<std::mutex> lock(active_transactions_mutex_);
    active_transactions_.erase(txn);
}

void PyTransactionDB::close() {
    std::unique_lock<std::shared_mutex> operations(transaction_operations_mutex_);
    PyRocksDB::close();
}

void PyTransactionDB::before_close_locked() {
    {
        std::lock_guard<std::mutex> lock(active_transactions_mutex_);
        for (PyTransaction* txn : active_transactions_) {
            txn->invalidate_from_parent_close();
        }
        active_transactions_.clear();
    }
    txn_db_ = nullptr;
}

void PyTransactionDB::check_cf_drop_locked() {
    std::lock_guard<std::mutex> lock(active_transactions_mutex_);
    if (!active_transactions_.empty())
        throw RocksDBException("Cannot drop a column family while transactions are active.");
}

rocksdb::WriteOptions PyTransactionDB::copy_write_options(std::shared_ptr<PyWriteOptions> options) {
    std::lock_guard<std::mutex> lock(arrow_mutex_);
    check_db_open();
    return options ? options->options_ : default_write_options_->options_;
}

void PyTransactionDB::drop_column_family(PyColumnFamilyHandle& cf) {
    std::unique_lock<std::shared_mutex> operations(transaction_operations_mutex_);
    PyRocksDBExtended::drop_column_family(cf);
}

void PyTransactionDB::put(const py::bytes& key, const py::bytes& value, std::shared_ptr<PyWriteOptions> write_options) {
    std::shared_lock<std::shared_mutex> lock(transaction_operations_mutex_);
    check_db_open();
    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    rocksdb::Slice value_slice(static_cast<std::string_view>(value));
    const auto opts = copy_write_options(write_options);
    rocksdb::Status s = db_->Put(opts, key_slice, value_slice);
    if (!s.ok()) throw_rocksdb_status(s, "TransactionDB put failed");
}

void PyTransactionDB::del(const py::bytes& key, std::shared_ptr<PyWriteOptions> write_options) {
    std::shared_lock<std::shared_mutex> lock(transaction_operations_mutex_);
    check_db_open();
    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    const auto opts = copy_write_options(write_options);
    rocksdb::Status s = db_->Delete(opts, key_slice);
    if (!s.ok()) throw_rocksdb_status(s, "TransactionDB delete failed");
}

void PyTransactionDB::write(PyWriteBatch& batch, std::shared_ptr<PyWriteOptions> write_options) {
    std::shared_lock<std::shared_mutex> lock(transaction_operations_mutex_);
    check_db_open();
    {
        std::lock_guard<std::mutex> arrow_lock(arrow_mutex_);
        for (const auto& cf : batch.families_) require_cf(*cf);
    }
    const auto opts = copy_write_options(write_options);
    rocksdb::Status s = db_->Write(opts, &batch.wb_);
    if (!s.ok()) throw_rocksdb_status(s, "TransactionDB write failed");
}

std::shared_ptr<PyTransaction> PyTransactionDB::begin_transaction(std::shared_ptr<PyWriteOptions> write_options, std::shared_ptr<PyTransactionOptions> txn_options) {
    std::shared_lock<std::shared_mutex> operations(transaction_operations_mutex_);
    std::lock_guard<std::mutex> lock(arrow_mutex_);
    check_db_open();
    if (!txn_db_) throw RocksDBException("TransactionDB is not open or has been closed.");
    const auto& write_opts = write_options ? write_options->options_ : default_write_options_->options_;
    rocksdb::TransactionOptions default_txn_options;
    const auto& tx_opts = txn_options ? txn_options->options_ : default_txn_options;
    rocksdb::Transaction* raw_txn = txn_db_->BeginTransaction(write_opts, tx_opts);
    auto self = std::static_pointer_cast<PyTransactionDB>(shared_from_this());
    return std::shared_ptr<PyTransaction>(new PyTransaction(raw_txn, self));
}

std::shared_ptr<PyTransaction> PyTransactionDB::transaction(std::shared_ptr<PyWriteOptions> write_options, std::shared_ptr<PyTransactionOptions> txn_options) {
    return begin_transaction(std::move(write_options), std::move(txn_options));
}

PyTransactionDBOptions PyTransactionDB::get_transaction_db_options() const { return opened_transaction_db_options_; }

void PyTransactionDB::put_cf(PyColumnFamilyHandle& cf, const py::bytes& key, const py::bytes& value,
                            std::shared_ptr<PyWriteOptions> options) {
    std::shared_lock<std::shared_mutex> operations(transaction_operations_mutex_);
    {
        std::lock_guard<std::mutex> lock(arrow_mutex_);
        require_cf(cf);
    }
    const auto opts = copy_write_options(options);
    auto s = db_->Put(opts, cf.cf_handle_, rocksdb::Slice(static_cast<std::string_view>(key)), rocksdb::Slice(static_cast<std::string_view>(value)));
    if (!s.ok()) throw_rocksdb_status(s, "TransactionDB put_cf failed");
}

void PyTransactionDB::del_cf(PyColumnFamilyHandle& cf, const py::bytes& key, std::shared_ptr<PyWriteOptions> options) {
    std::shared_lock<std::shared_mutex> operations(transaction_operations_mutex_);
    {
        std::lock_guard<std::mutex> lock(arrow_mutex_);
        require_cf(cf);
    }
    const auto opts = copy_write_options(options);
    auto s = db_->Delete(opts, cf.cf_handle_, rocksdb::Slice(static_cast<std::string_view>(key)));
    if (!s.ok()) throw_rocksdb_status(s, "TransactionDB delete_cf failed");
}
