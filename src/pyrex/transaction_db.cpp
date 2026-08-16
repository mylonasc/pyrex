#include "transaction_db.hpp"

#include "exceptions.hpp"
#include "transaction.hpp"
#include "write_batch.hpp"

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"

PyTransactionDB::PyTransactionDB(const std::string& path, PyOptions* py_options, PyTransactionDBOptions* txn_db_options)
    : default_read_options_(std::make_shared<PyReadOptions>()),
      default_write_options_(std::make_shared<PyWriteOptions>()) {
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

    rocksdb::Status s = rocksdb::TransactionDB::Open(options, transaction_db_options, path, &db_);
    if (!s.ok()) throw_rocksdb_status(s, "Failed to open TransactionDB at " + path);
}

PyTransactionDB::~PyTransactionDB() { close(); }

void PyTransactionDB::check_db_open() const {
    if (is_closed_.load() || db_ == nullptr) {
        throw RocksDBException("TransactionDB is not open or has been closed.");
    }
}

void PyTransactionDB::register_transaction(PyTransaction* txn) {
    std::lock_guard<std::mutex> lock(active_transactions_mutex_);
    active_transactions_.insert(txn);
}

void PyTransactionDB::unregister_transaction(PyTransaction* txn) {
    std::lock_guard<std::mutex> lock(active_transactions_mutex_);
    active_transactions_.erase(txn);
}

void PyTransactionDB::close() {
    if (!is_closed_.exchange(true)) {
        {
            std::lock_guard<std::mutex> lock(active_transactions_mutex_);
            for (PyTransaction* txn : active_transactions_) {
                txn->invalidate_from_parent_close();
            }
            active_transactions_.clear();
        }
        if (db_) {
            delete db_;
            db_ = nullptr;
        }
    }
}

void PyTransactionDB::put(const py::bytes& key, const py::bytes& value, std::shared_ptr<PyWriteOptions> write_options) {
    check_db_open();
    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    rocksdb::Slice value_slice(static_cast<std::string_view>(value));
    const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
    rocksdb::Status s = db_->Put(opts, key_slice, value_slice);
    if (!s.ok()) throw_rocksdb_status(s, "TransactionDB put failed");
}

py::object PyTransactionDB::get(const py::bytes& key, std::shared_ptr<PyReadOptions> read_options) {
    check_db_open();
    std::string value_str;
    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    const auto& opts = read_options ? read_options->options_ : default_read_options_->options_;
    rocksdb::Status s = db_->Get(opts, key_slice, &value_str);
    if (s.ok()) return py::bytes(value_str);
    if (s.IsNotFound()) return py::none();
    throw_rocksdb_status(s, "TransactionDB get failed");
}

void PyTransactionDB::del(const py::bytes& key, std::shared_ptr<PyWriteOptions> write_options) {
    check_db_open();
    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
    rocksdb::Status s = db_->Delete(opts, key_slice);
    if (!s.ok()) throw_rocksdb_status(s, "TransactionDB delete failed");
}

void PyTransactionDB::write(PyWriteBatch& batch, std::shared_ptr<PyWriteOptions> write_options) {
    check_db_open();
    const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
    rocksdb::Status s = db_->Write(opts, &batch.wb_);
    if (!s.ok()) throw_rocksdb_status(s, "TransactionDB write failed");
}

std::shared_ptr<PyTransaction> PyTransactionDB::begin_transaction(std::shared_ptr<PyWriteOptions> write_options, std::shared_ptr<PyTransactionOptions> txn_options) {
    check_db_open();
    const auto& write_opts = write_options ? write_options->options_ : default_write_options_->options_;
    rocksdb::TransactionOptions default_txn_options;
    const auto& tx_opts = txn_options ? txn_options->options_ : default_txn_options;
    rocksdb::Transaction* raw_txn = db_->BeginTransaction(write_opts, tx_opts);
    return std::shared_ptr<PyTransaction>(new PyTransaction(raw_txn, shared_from_this()));
}

std::shared_ptr<PyTransaction> PyTransactionDB::transaction(std::shared_ptr<PyWriteOptions> write_options, std::shared_ptr<PyTransactionOptions> txn_options) {
    return begin_transaction(std::move(write_options), std::move(txn_options));
}

PyOptions PyTransactionDB::get_options() const { return opened_options_; }

PyTransactionDBOptions PyTransactionDB::get_transaction_db_options() const { return opened_transaction_db_options_; }

std::shared_ptr<PyReadOptions> PyTransactionDB::get_default_read_options() { return default_read_options_; }

void PyTransactionDB::set_default_read_options(std::shared_ptr<PyReadOptions> opts) {
    if (!opts) throw RocksDBException("ReadOptions cannot be None.");
    default_read_options_ = opts;
}

std::shared_ptr<PyWriteOptions> PyTransactionDB::get_default_write_options() { return default_write_options_; }

void PyTransactionDB::set_default_write_options(std::shared_ptr<PyWriteOptions> opts) {
    if (!opts) throw RocksDBException("WriteOptions cannot be None.");
    default_write_options_ = opts;
}
