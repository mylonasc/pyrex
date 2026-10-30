#include "transaction.hpp"
#include "arrow_scan.hpp"

#include "exceptions.hpp"
#include "transaction_db.hpp"
#include "write_batch.hpp"

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"

namespace {
class TransactionWriteBatchHandler : public rocksdb::WriteBatch::Handler {
private:
    rocksdb::Transaction* txn_;
    std::map<uint32_t, rocksdb::ColumnFamilyHandle*> families_;

public:
    TransactionWriteBatchHandler(rocksdb::Transaction* txn, const std::map<std::string, std::shared_ptr<PyColumnFamilyHandle>>& families)
        : txn_(txn) {
        for (const auto& entry : families) families_[entry.second->cf_handle_->GetID()] = entry.second->cf_handle_;
    }

    rocksdb::Status PutCF(uint32_t column_family_id, const rocksdb::Slice& key, const rocksdb::Slice& value) override {
        auto it = families_.find(column_family_id);
        if (it == families_.end()) return rocksdb::Status::InvalidArgument("unknown transaction column family");
        return txn_->Put(it->second, key, value);
    }

    rocksdb::Status DeleteCF(uint32_t column_family_id, const rocksdb::Slice& key) override {
        auto it = families_.find(column_family_id);
        if (it == families_.end()) return rocksdb::Status::InvalidArgument("unknown transaction column family");
        return txn_->Delete(it->second, key);
    }

    rocksdb::Status MergeCF(uint32_t /* column_family_id */, const rocksdb::Slice& /* key */, const rocksdb::Slice& /* value */) override {
        return rocksdb::Status::InvalidArgument("transaction write batch merge is not supported yet");
    }
};
}

PyTransaction::PyTransaction(rocksdb::Transaction* txn, std::shared_ptr<PyTransactionDB> parent_db)
    : txn_(txn), parent_db_(std::move(parent_db)) {
    if (!txn_) {
        throw RocksDBException("Failed to create transaction: null pointer received.");
    }
    parent_db_->register_transaction(this);
}

PyTransaction::OperationGuard::OperationGuard(const PyTransaction& txn)
    : db_lock(txn.parent_db_->transaction_operations_mutex_), txn_lock(txn.operation_mutex_) {}

void PyTransaction::check_cf(PyColumnFamilyHandle& cf) const {
    std::lock_guard<std::shared_mutex> lock(parent_db_->arrow_mutex_);
    parent_db_->require_cf(cf);
}

PyTransaction::~PyTransaction() {
    OperationGuard guard(*this);
    invalidate_iterators();
    if (txn_) {
        txn_->Rollback();
        delete txn_;
        txn_ = nullptr;
    }
    if (parent_db_) {
        parent_db_->unregister_transaction(this);
    }
}

void PyTransaction::check_active() const {
    if (!active_ || !txn_) {
        throw RocksDBException("Transaction is no longer active.");
    }
}

void PyTransaction::check_mutable() const {
    check_active();
    if (active_arrow_scans_.load())
        throw RocksDBException("Transaction has active Arrow scans; close or exhaust them before mutation, snapshot reset, commit or rollback.");
}

void PyTransaction::invalidate_from_parent_close() {
    invalidate_iterators();
    if (txn_) {
        txn_->Rollback();
        delete txn_;
        txn_ = nullptr;
    }
    active_ = false;
}

void PyTransaction::register_iterator(PyTransactionIterator* it) {
    std::lock_guard<std::mutex> lock(active_iterators_mutex_);
    active_iterators_.insert(it);
}

void PyTransaction::unregister_iterator(PyTransactionIterator* it) {
    std::lock_guard<std::mutex> lock(active_iterators_mutex_);
    active_iterators_.erase(it);
}

void PyTransaction::invalidate_iterators() {
    std::lock_guard<std::mutex> lock(active_iterators_mutex_);
    for (PyTransactionIterator* it : active_iterators_) {
        it->invalidate_from_parent_completion();
    }
    active_iterators_.clear();
}

void PyTransaction::put(const py::bytes& key, const py::bytes& value) {
    OperationGuard guard(*this);
    check_mutable();
    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    rocksdb::Slice value_slice(static_cast<std::string_view>(value));
    rocksdb::Status s = txn_->Put(key_slice, value_slice);
    if (!s.ok()) throw_rocksdb_status(s, "Transaction put failed");
}

void PyTransaction::put_cf(PyColumnFamilyHandle& cf, const py::bytes& key, const py::bytes& value) {
    OperationGuard guard(*this);
    check_mutable();
    check_cf(cf);
    auto s = txn_->Put(cf.cf_handle_, rocksdb::Slice(static_cast<std::string_view>(key)),
                       rocksdb::Slice(static_cast<std::string_view>(value)));
    if (!s.ok()) throw_rocksdb_status(s, "Transaction put_cf failed");
}

py::object PyTransaction::get_cf(PyColumnFamilyHandle& cf, const py::bytes& key, std::shared_ptr<PyReadOptions> read_options) {
    std::string key_storage = key;
    auto opts = read_options ? read_options->native_options() : rocksdb::ReadOptions();
    std::string value;
    rocksdb::Status s;
    {
        py::gil_scoped_release release;
        OperationGuard guard(*this);
        check_active();
        check_cf(cf);
        opts.snapshot = txn_->GetSnapshot();
        s = txn_->Get(opts, cf.cf_handle_, rocksdb::Slice(key_storage), &value);
    }
    if (s.ok()) return py::bytes(value);
    if (s.IsNotFound()) return py::none();
    throw_rocksdb_status(s, "Transaction get_cf failed");
}

void PyTransaction::del_cf(PyColumnFamilyHandle& cf, const py::bytes& key) {
    OperationGuard guard(*this);
    check_mutable();
    check_cf(cf);
    auto s = txn_->Delete(cf.cf_handle_, rocksdb::Slice(static_cast<std::string_view>(key)));
    if (!s.ok()) throw_rocksdb_status(s, "Transaction delete_cf failed");
}

py::object PyTransaction::get(const py::bytes& key, std::shared_ptr<PyReadOptions> read_options) {
    std::string key_storage = key;
    std::string value_str;
    rocksdb::ReadOptions opts = read_options ? read_options->native_options() : rocksdb::ReadOptions();
    rocksdb::Status s;
    {
        py::gil_scoped_release release;
        OperationGuard guard(*this);
        check_active();
        opts.snapshot = txn_->GetSnapshot();
        s = txn_->Get(opts, rocksdb::Slice(key_storage), &value_str);
    }
    if (s.ok()) return py::bytes(value_str);
    if (s.IsNotFound()) return py::none();
    throw_rocksdb_status(s, "Transaction get failed");
}

py::object PyTransaction::get_for_update(const py::bytes& key, std::shared_ptr<PyReadOptions> read_options, bool exclusive, bool do_validate, bool read_value) {
    std::string key_storage = key;
    std::string value_str;
    rocksdb::ReadOptions opts = read_options ? read_options->native_options() : rocksdb::ReadOptions();
    std::string* value = read_value ? &value_str : nullptr;
    rocksdb::Status s;
    {
        py::gil_scoped_release release;
        OperationGuard guard(*this);
        check_mutable();
        opts.snapshot = txn_->GetSnapshot();
        s = txn_->GetForUpdate(opts, rocksdb::Slice(key_storage), value, exclusive, do_validate);
    }
    if (!read_value && s.ok()) return py::none();
    if (s.ok()) return py::bytes(value_str);
    if (s.IsNotFound()) return py::none();
    throw_rocksdb_status(s, "Transaction get_for_update failed");
}

void PyTransaction::del(const py::bytes& key) {
    OperationGuard guard(*this);
    check_mutable();
    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    rocksdb::Status s = txn_->Delete(key_slice);
    if (!s.ok()) throw_rocksdb_status(s, "Transaction delete failed");
}

void PyTransaction::write(PyWriteBatch& batch) {
    OperationGuard guard(*this);
    check_mutable();
    auto handler = [&] {
        std::lock_guard<std::shared_mutex> lock(parent_db_->arrow_mutex_);
        for (const auto& cf : batch.families_) parent_db_->require_cf(*cf);
        return TransactionWriteBatchHandler(txn_, parent_db_->cf_handles_);
    }();
    rocksdb::Status s = batch.wb_.Iterate(&handler);
    if (!s.ok()) throw_rocksdb_status(s, "Transaction write batch failed");
}

void PyTransaction::commit(std::shared_ptr<PyWriteOptions> write_options) {
    OperationGuard guard(*this);
    check_mutable();
    if (write_options) {
        txn_->SetWriteOptions(write_options->options_);
    }
    rocksdb::Status s = txn_->Commit();
    if (!s.ok()) throw_rocksdb_status(s, "Transaction commit failed");
    invalidate_iterators();
    active_ = false;
    if (parent_db_) {
        parent_db_->unregister_transaction(this);
    }
    delete txn_;
    txn_ = nullptr;
}

void PyTransaction::rollback() {
    OperationGuard guard(*this);
    check_mutable();
    rocksdb::Status s = txn_->Rollback();
    if (!s.ok()) throw_rocksdb_status(s, "Transaction rollback failed");
    invalidate_iterators();
    active_ = false;
    if (parent_db_) {
        parent_db_->unregister_transaction(this);
    }
    delete txn_;
    txn_ = nullptr;
}

void PyTransaction::set_snapshot() {
    OperationGuard guard(*this);
    check_mutable();
    txn_->SetSnapshot();
}

std::optional<uint64_t> PyTransaction::snapshot_sequence_number() const {
    py::gil_scoped_release release;
    OperationGuard guard(*this);
    check_active();
    const auto* snapshot = txn_->GetSnapshot();
    if (!snapshot) return std::nullopt;
    return snapshot->GetSequenceNumber();
}

std::shared_ptr<PyTransactionIterator> PyTransaction::new_iterator(std::shared_ptr<PyReadOptions> read_options) {
    py::gil_scoped_release release;
    OperationGuard guard(*this);
    check_active();
    rocksdb::ReadOptions opts = read_options ? read_options->iterator_options() : rocksdb::ReadOptions();
    opts.snapshot = txn_->GetSnapshot();
    rocksdb::Iterator* raw_iter = txn_->GetIterator(opts);
    return std::make_shared<PyTransactionIterator>(raw_iter, shared_from_this());
}

std::shared_ptr<PyArrowScan> PyTransaction::scan_arrow_impl(std::shared_ptr<PyColumnFamilyHandle> cf,
        py::object prefix, py::object start, py::object stop, size_t batch_size, size_t max_batch_bytes,
        std::shared_ptr<PyReadOptions> read_options, bool allow_overflow, bool collect_metrics) {
    if (batch_size < 1 || batch_size > 16 * 1024 * 1024 || max_batch_bytes < 1 ||
            max_batch_bytes > size_t{4} * 1024 * 1024 * 1024)
        throw py::value_error("Transaction.scan_arrow: invalid batch_size or max_batch_bytes");
    auto bound = [](const py::object& obj, bool& present, std::string& storage) {
        present = !obj.is_none();
        if (!present) return;
        if (!py::isinstance<py::bytes>(obj)) throw py::type_error("Transaction.scan_arrow bounds must be bytes or None");
        storage = py::cast<std::string>(obj);
    };
    PyArrowScan::Config config;
    bound(prefix, config.has_prefix, config.prefix);
    bound(start, config.has_start, config.start);
    bound(stop, config.has_stop, config.stop);
    if (config.has_start && config.has_stop && config.start.compare(config.stop) > 0)
        throw py::value_error("Transaction.scan_arrow: start must be <= stop");
    config.batch_size = batch_size;
    config.max_batch_bytes = max_batch_bytes;
    config.allow_overflow = allow_overflow;
    config.collect_metrics = collect_metrics;
    return std::make_shared<PyArrowScan>(parent_db_, cf, config, nullptr, read_options, shared_from_this());
}

std::shared_ptr<PyArrowScan> PyTransaction::scan_arrow(py::object prefix, py::object start, py::object stop,
        size_t batch_size, size_t max_batch_bytes, std::shared_ptr<PyReadOptions> read_options,
        bool allow_overflow, bool collect_metrics) {
    return scan_arrow_impl(nullptr, prefix, start, stop, batch_size, max_batch_bytes,
                           read_options, allow_overflow, collect_metrics);
}

std::shared_ptr<PyArrowScan> PyTransaction::scan_arrow_cf(std::shared_ptr<PyColumnFamilyHandle> cf,
        py::object prefix, py::object start, py::object stop, size_t batch_size, size_t max_batch_bytes,
        std::shared_ptr<PyReadOptions> read_options, bool allow_overflow, bool collect_metrics) {
    if (!cf) throw py::value_error("cf_handle must not be None");
    return scan_arrow_impl(cf, prefix, start, stop, batch_size, max_batch_bytes,
                           read_options, allow_overflow, collect_metrics);
}

py::dict PyTransaction::arrow_capabilities() const {
    auto result = parent_db_->arrow_capabilities();
    result["snapshot_scope"] = "transaction_local_frozen_writes";
    result["scan_arrow_ranges"] = false;
    result["scan_arrow_ranges_cf"] = false;
    result["multi_get_arrow"] = false;
    result["multi_get_arrow_cf"] = false;
    result["snapshot_at"] = "transaction_snapshot_or_scan_creation";
    result["mutation_policy"] = "reject_while_native_arrow_resources_active";
    result["explicit_shared_snapshot_argument"] = false;
    return result;
}

bool PyTransaction::is_active() const {
    py::gil_scoped_release release;
    OperationGuard guard(*this);
    return active_ && txn_ != nullptr;
}

PyTransactionIterator::PyTransactionIterator(rocksdb::Iterator* it, std::shared_ptr<PyTransaction> parent_txn)
    : it_raw_ptr_(it), parent_txn_(std::move(parent_txn)) {
    if (!it_raw_ptr_) {
        throw RocksDBException("Failed to create transaction iterator: null pointer received.");
    }
    parent_txn_->register_iterator(this);
}

PyTransactionIterator::~PyTransactionIterator() {
    PyTransaction::OperationGuard guard(*parent_txn_);
    if (parent_txn_) {
        parent_txn_->unregister_iterator(this);
    }
    if (it_raw_ptr_) {
        delete it_raw_ptr_;
    }
    it_raw_ptr_ = nullptr;
}

void PyTransactionIterator::check_parent_transaction_is_active() const {
    if (!it_raw_ptr_ || !parent_txn_ || !parent_txn_->active_) {
        throw RocksDBException("Transaction is no longer active.");
    }
}

void PyTransactionIterator::invalidate_from_parent_completion() {
    if (it_raw_ptr_) {
        delete it_raw_ptr_;
        it_raw_ptr_ = nullptr;
    }
}

bool PyTransactionIterator::valid() { PyTransaction::OperationGuard guard(*parent_txn_); check_parent_transaction_is_active(); return it_raw_ptr_->Valid(); }
void PyTransactionIterator::seek_to_first() { PyTransaction::OperationGuard guard(*parent_txn_); check_parent_transaction_is_active(); it_raw_ptr_->SeekToFirst(); }
void PyTransactionIterator::seek_to_last() { PyTransaction::OperationGuard guard(*parent_txn_); check_parent_transaction_is_active(); it_raw_ptr_->SeekToLast(); }
void PyTransactionIterator::seek(const py::bytes& key) { PyTransaction::OperationGuard guard(*parent_txn_); check_parent_transaction_is_active(); it_raw_ptr_->Seek(static_cast<std::string>(key)); }
void PyTransactionIterator::next() { PyTransaction::OperationGuard guard(*parent_txn_); check_parent_transaction_is_active(); it_raw_ptr_->Next(); }
void PyTransactionIterator::prev() { PyTransaction::OperationGuard guard(*parent_txn_); check_parent_transaction_is_active(); it_raw_ptr_->Prev(); }

py::object PyTransactionIterator::key() {
    PyTransaction::OperationGuard guard(*parent_txn_);
    check_parent_transaction_is_active();
    if (it_raw_ptr_ && it_raw_ptr_->Valid()) {
        return py::bytes(it_raw_ptr_->key().ToString());
    }
    return py::none();
}

py::object PyTransactionIterator::value() {
    PyTransaction::OperationGuard guard(*parent_txn_);
    check_parent_transaction_is_active();
    if (it_raw_ptr_ && it_raw_ptr_->Valid()) {
        return py::bytes(it_raw_ptr_->value().ToString());
    }
    return py::none();
}

void PyTransactionIterator::check_status() {
    PyTransaction::OperationGuard guard(*parent_txn_);
    check_parent_transaction_is_active();
    rocksdb::Status s = it_raw_ptr_->status();
    if (!s.ok()) throw_rocksdb_status(s, "Transaction iterator error");
}
