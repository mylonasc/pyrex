#include "arrow_scan.hpp"

#include <cerrno>
#include <algorithm>
#include <chrono>
#include <cstring>

#include "db.hpp"
#include "exceptions.hpp"
#include "options.hpp"
#include "transaction.hpp"
#include "transaction_db.hpp"

#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"

struct ArrowScanMetricsState {
    mutable std::mutex mutex;
    ArrowScanStatistics totals;
};

// One value payload buffer per attempted scan batch, never reused after
// publication. Gather copies RocksDB slices directly here; successful output
// adopts the malloc/realloc allocation instead of copying each value again.
// Keys retain the compatible string staging path; point-prefetch is separate.
struct ArrowValueStaging {
    char* data = nullptr;
    size_t used = 0;
    size_t capacity = 0;
    std::vector<int32_t> offsets{0};
    ~ArrowValueStaging() { std::free(data); }
    void append(const rocksdb::Slice& value, size_t limit) {
        const size_t needed = used + value.size();  // checked against INT32_MAX by caller
        offsets.push_back(static_cast<int32_t>(needed));
        if (needed > capacity) {
            const size_t requested = std::max(needed, std::min(limit, capacity ? capacity * 2 : size_t{256}));
            void* next = std::realloc(data, requested);
            if (!next) throw std::bad_alloc();
            data = static_cast<char*>(next);
            capacity = requested;
        }
        if (value.size()) std::memcpy(data + used, value.data(), value.size());
        used = needed;
    }
    void* release_exact() {
        // Shrink before adoption to preserve exact requested output capacity.
        // realloc may move/copy internally; this is not a zero-copy claim.
        void* result = std::realloc(data, used ? used : 1);
        if (!result) return nullptr;  // old allocation stays locally owned
        data = nullptr;
        return result;
    }
};

namespace {

constexpr size_t kCancellationPollRows = 256;

// Progress is published at polling boundaries, not by taking a metrics lock
// per row. The destructor also flushes a partial/failed gather attempt.
class GatherProgress {
public:
    GatherProgress(std::atomic<uint64_t>* total, const std::vector<std::string>& keys,
                   const std::vector<std::string>& values, uint64_t* staged,
                   const uint64_t& key_bytes, const uint64_t& value_bytes, bool keys_only = false)
        : total_(total), keys_(keys), values_(values), staged_(staged),
          key_bytes_(key_bytes), value_bytes_(value_bytes), keys_only_(keys_only) {}
    ~GatherProgress() {
        flush();
        if (staged_) *staged_ = key_bytes_ + value_bytes_;
    }
    void flush() {
        if (!total_) return;
        const auto count = keys_only_ ? keys_.size() : std::min(keys_.size(), values_.size());
        if (count > recorded_)
            total_->fetch_add(static_cast<uint64_t>(count - recorded_), std::memory_order_relaxed);
        recorded_ = count;
    }
private:
    std::atomic<uint64_t>* total_;
    const std::vector<std::string>& keys_;
    const std::vector<std::string>& values_;
    size_t recorded_ = 0;
    uint64_t* staged_;
    const uint64_t& key_bytes_;
    const uint64_t& value_bytes_;
    bool keys_only_;
};

// Batch-phase clocks and accounting are opt-in; disabled scans do not call
// the clock or lock a metrics mutex. The caller keeps metrics alive.
class PhaseTimer {
public:
    PhaseTimer(ArrowScanMetricsState* state, uint64_t ArrowScanStatistics::* field)
        : state_(state), field_(field) {
        if (state_) start_ = std::chrono::steady_clock::now();
    }
    ~PhaseTimer() {
        if (!state_) return;
        const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - start_).count();
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->totals.*field_ += static_cast<uint64_t>(elapsed);
    }
private:
    ArrowScanMetricsState* state_;
    uint64_t ArrowScanStatistics::* field_;
    std::chrono::steady_clock::time_point start_{};
};

// Unsigned bytewise comparison, matching RocksDB's BytewiseComparator.
int compare_bytes(const char* a_data, size_t a_size, const char* b_data, size_t b_size) {
    const size_t common = a_size < b_size ? a_size : b_size;
    if (common > 0) {
        const int cmp = std::memcmp(a_data, b_data, common);
        if (cmp != 0) return cmp < 0 ? -1 : 1;
    }
    if (a_size == b_size) return 0;
    return a_size < b_size ? -1 : 1;
}

char* dup_cstr(const char* s) {
    const size_t n = std::strlen(s) + 1;
    char* out = static_cast<char*>(std::malloc(n));
    if (!out) throw std::bad_alloc();
    std::memcpy(out, s, n);
    return out;
}

void free_schema(ArrowSchema* schema) {
    if (!schema || !schema->release) return;
    std::free(const_cast<char*>(schema->format));
    std::free(const_cast<char*>(schema->name));
    std::free(const_cast<char*>(schema->metadata));
    for (int64_t i = 0; i < schema->n_children; ++i) {
        ArrowSchema* child = schema->children[i];
        if (child && child->release) child->release(child);
        delete child;
    }
    std::free(schema->children);
    schema->release = nullptr;
}

struct ChildBuffers {
    void* validity = nullptr;
    void* offsets = nullptr;
    void* data = nullptr;
    const void** buffers = nullptr;
    std::shared_ptr<ArrowScanMetricsState> metrics;
    uint64_t tracked_capacity = 0;
};

void free_child_array(ArrowArray* array) {
    if (!array || !array->release) return;
    auto* priv = static_cast<ChildBuffers*>(array->private_data);
    if (priv) {
        std::free(priv->offsets);
        std::free(priv->validity);
        std::free(priv->data);
        std::free(priv->buffers);
        if (priv->metrics) {
            std::lock_guard<std::mutex> lock(priv->metrics->mutex);
            priv->metrics->totals.live_buffer_bytes -= priv->tracked_capacity;
        }
        delete priv;
    }
    array->release = nullptr;
    array->private_data = nullptr;
    // NOTE: the ArrowArray struct itself is freed by the parent release;
    // consumers move/detach children before releasing the parent.
}

struct BatchBuffers {
    const void** parent_buffers = nullptr;
    ArrowArray** child_pointers = nullptr;
    ArrowArray* key_array = nullptr;
    ArrowArray* value_array = nullptr;
};

void free_batch_array(ArrowArray* array) {
    if (!array || !array->release) return;
    auto* priv = static_cast<BatchBuffers*>(array->private_data);
    if (priv) {
        // Recursive release: Arrow C++-based consumers (PyArrow, Polars via
        // arrow-rs style importers, etc.) move only the top-level struct on
        // import and call exactly one release (the top level), leaving child
        // releases untouched. Consumers that move children out detach them
        // by nulling their release, which we skip here. free_child_array
        // nulls release after freeing, so this is also safe if a child was
        // already released individually.
        if (priv->key_array && priv->key_array->release) {
            priv->key_array->release(priv->key_array);
        }
        if (priv->value_array && priv->value_array->release) {
            priv->value_array->release(priv->value_array);
        }
        delete priv->key_array;
        delete priv->value_array;
        std::free(priv->child_pointers);
        std::free(priv->parent_buffers);
        delete priv;
    }
    array->release = nullptr;
    array->private_data = nullptr;
}

// An unpublished batch has the same release path as consumer-owned output.
// It can therefore be abandoned on any build-phase cancellation checkpoint
// without leaking partially filled buffers or counting successful output.
struct PendingBatch {
    ArrowArray array{};
    ~PendingBatch() { if (array.release) array.release(&array); }
    void publish(ArrowArray* out) {
        *out = array;
        array.release = nullptr;
    }
};

void fill_binary_child(ArrowArray* child, int64_t n, ChildBuffers* owned) {
    std::memset(child, 0, sizeof(ArrowArray));
    child->length = n;
    child->null_count = 0;
    child->offset = 0;
    child->n_buffers = 3;
    owned->buffers[0] = owned->validity;
    owned->buffers[1] = owned->offsets;
    owned->buffers[2] = owned->data;
    child->buffers = owned->buffers;
    child->n_children = 0;
    child->children = nullptr;
    child->dictionary = nullptr;
    child->release = &free_child_array;
    child->private_data = owned;
}

}  // namespace

bool prefix_successor(const std::string& prefix, std::string& successor) {
    if (prefix.empty()) return false;
    size_t end = prefix.size();
    while (end > 0 && static_cast<unsigned char>(prefix[end - 1]) == 0xff) {
        --end;
    }
    if (end == 0) return false;  // all 0xff: unbounded above
    successor.assign(prefix.data(), end - 1);
    successor.push_back(static_cast<char>(static_cast<unsigned char>(prefix[end - 1]) + 1));
    return true;
}

// ---------------------------------------------------------------------------
// PySnapshot
// ---------------------------------------------------------------------------

PySnapshot::PySnapshot(std::shared_ptr<PyRocksDB> parent, const rocksdb::Snapshot* snapshot)
    : parent_(std::move(parent)), snapshot_(snapshot) {
    if (!parent_ || !snapshot_) {
        throw RocksDBException("Cannot create snapshot: null parent or snapshot.");
    }
}

PySnapshot::~PySnapshot() {
    if (parent_) {
        try {
            parent_->detach_snapshot(this);
        } catch (...) {
            // Destructors must not throw; detach is no-fail by contract.
        }
    }
}

void PySnapshot::release() {
    if (parent_) parent_->detach_snapshot(this);
}

bool PySnapshot::is_valid() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return !released_ && snapshot_ != nullptr;
}

uint64_t PySnapshot::sequence_number() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (released_ || !snapshot_) throw RocksDBException("Snapshot has been released.");
    return snapshot_->GetSequenceNumber();
}

void PySnapshot::release_locked() {
    snapshot_ = nullptr;
    released_ = true;
}

void PySnapshot::invalidate_for_db_close(rocksdb::DB* db_raw) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!released_ && snapshot_ && db_raw) {
        db_raw->ReleaseSnapshot(snapshot_);
    }
    release_locked();
}

// ---------------------------------------------------------------------------
// PyArrowScan
// ---------------------------------------------------------------------------

PyArrowScan::PyArrowScan(std::shared_ptr<PyRocksDB> parent, std::shared_ptr<PyColumnFamilyHandle> cf_handle, const Config& config,
                         std::shared_ptr<PySnapshot> shared_snapshot, std::shared_ptr<PyReadOptions> read_options,
                         std::shared_ptr<PyTransaction> transaction)
    : parent_(std::move(parent)),
      transaction_(std::move(transaction)),
      cf_handle_(std::move(cf_handle)),
      batch_size_(config.batch_size),
      max_batch_bytes_(config.max_batch_bytes),
      allow_overflow_(config.allow_overflow),
      multi_range_(config.multi_range),
      ranges_(config.ranges),
      point_lookup_(config.point_lookup),
      point_keys_(config.point_keys),
      metrics_(config.collect_metrics ? std::make_shared<ArrowScanMetricsState>() : nullptr) {
    if (!parent_) throw RocksDBException("Cannot scan: null database.");

    // --- Effective bounds: intersection of [start, stop) and prefix range.
    std::string prefix_end;
    bool has_prefix_end = false;
    if (config.has_prefix && !config.prefix.empty()) {
        has_prefix_end = prefix_successor(config.prefix, prefix_end);
    }
    const std::string* lo = nullptr;
    if (config.has_prefix && !config.prefix.empty()) lo = &config.prefix;
    if (config.has_start) {
        if (!lo || compare_bytes(config.start.data(), config.start.size(), lo->data(), lo->size()) > 0) {
            lo = &config.start;
        }
    }
    const std::string* hi = nullptr;
    if (config.has_stop) hi = &config.stop;
    if (has_prefix_end) {
        if (!hi || compare_bytes(prefix_end.data(), prefix_end.size(), hi->data(), hi->size()) < 0) {
            hi = &prefix_end;
        }
    }
    if (lo) {
        eff_start_ = *lo;
        has_eff_start_ = true;
    }
    if (hi) {
        eff_stop_ = *hi;
        has_eff_stop_ = true;
    }
    if (multi_range_ && !ranges_.empty()) {
        has_eff_start_ = ranges_[0].start.has_value();
        eff_start_ = ranges_[0].start.value_or("");
        has_eff_stop_ = ranges_[0].stop.has_value();
        eff_stop_ = ranges_[0].stop.value_or("");
    }
    const auto global_stop = multi_range_ && !ranges_.empty() ? ranges_.back().stop
        : (has_eff_stop_ ? std::optional<std::string>(eff_stop_) : std::nullopt);
    if (global_stop) upper_bound_storage_ = *global_stop;

    // Never wait for the GIL while holding the DB lifecycle lock.
    py::gil_scoped_release release;
    // Same order as other transaction operations and DB close: transaction
    // operations guard -> transaction mutex -> DB lifecycle -> scan mutex.
    std::unique_ptr<PyTransaction::OperationGuard> transaction_guard;
    if (transaction_) {
        transaction_guard = std::make_unique<PyTransaction::OperationGuard>(*transaction_);
        transaction_->check_active();
    }
    std::lock_guard<std::shared_mutex> arrow_lock(parent_->arrow_mutex_);
    if (parent_->is_closed_.load() || !parent_->db_) {
        throw RocksDBException("Cannot scan: database is closed.");
    }
    rocksdb::DB* db_raw = parent_->db_;

    rocksdb::ColumnFamilyHandle* cf_raw = nullptr;
    if (cf_handle_) {
        parent_->require_cf(*cf_handle_);
        cf_raw = cf_handle_->cf_handle_;
        cf_name_ = cf_handle_->get_name();
    } else {
        auto it = parent_->cf_handles_.find(rocksdb::kDefaultColumnFamilyName);
        if (it == parent_->cf_handles_.end() || !it->second->is_valid()) {
            throw RocksDBException("Cannot scan: default column family handle is not available.");
        }
        cf_raw = it->second->cf_handle_;
        cf_name_ = rocksdb::kDefaultColumnFamilyName;
    }

    const rocksdb::Comparator* cmp = db_raw->GetOptions(cf_raw).comparator;
    const char* bytewise_name = rocksdb::BytewiseComparator()->Name();
    if (!cmp || std::string(cmp->Name()) != bytewise_name) {
        throw RocksDBException(std::string("scan_arrow requires the default bytewise comparator, found '") +
                               (cmp ? cmp->Name() : "<null>") + "'.");
    }

    const rocksdb::Snapshot* view = nullptr;
    if (transaction_ && transaction_->txn_->GetSnapshot()) {
        view = transaction_->txn_->GetSnapshot();
        snapshot_seq_ = view->GetSequenceNumber();
    } else if (shared_snapshot) {
        if (shared_snapshot->parent_.get() != parent_.get()) {
            throw RocksDBException("scan_arrow: snapshot belongs to a different database instance.");
        }
        std::lock_guard<std::mutex> snap_lock(shared_snapshot->mutex_);
        if (shared_snapshot->released_ || !shared_snapshot->snapshot_) {
            throw RocksDBException("scan_arrow: snapshot has been released.");
        }
        view = shared_snapshot->snapshot_;
        snapshot_seq_ = view->GetSequenceNumber();
        shared_snapshot_ = shared_snapshot;
    } else {
        view = db_raw->GetSnapshot();
        if (!view) throw RocksDBException("scan_arrow: failed to acquire snapshot.");
        owned_snapshot_ = view;
        snapshot_seq_ = view->GetSequenceNumber();
    }

    try {
        rocksdb::ReadOptions opts = (read_options ? read_options : parent_->default_read_options_)->iterator_options();
        // Never inherit prefix-only iteration semantics. Bounds are owned by
        // this scan and a bytewise complete result is the public contract.
        opts.total_order_seek = true;
        fill_cache_ = opts.fill_cache;
        verify_checksums_ = opts.verify_checksums;
        readahead_size_ = point_lookup_ ? 0 : opts.readahead_size;
        if (point_lookup_) opts.readahead_size = 0;
        opts.snapshot = view;
        if (point_lookup_) {
            point_cf_ = cf_raw;
            point_options_ = std::make_unique<rocksdb::ReadOptions>(opts);
            parent_->active_arrow_scans_.insert(this);
            if (shared_snapshot_) {
                std::lock_guard<std::mutex> snap_lock(shared_snapshot_->mutex_);
                ++shared_snapshot_->point_stream_pins_;
            }
            return;
        }
        if (global_stop) {
            upper_bound_slice_ = rocksdb::Slice(upper_bound_storage_);
            opts.iterate_upper_bound = &upper_bound_slice_;
        }

        iter_ = transaction_ ? transaction_->txn_->GetIterator(opts, cf_raw) : db_raw->NewIterator(opts, cf_raw);
        if (!iter_) throw RocksDBException("scan_arrow: failed to create iterator.");
        if (has_eff_start_) {
            iter_->Seek(rocksdb::Slice(eff_start_));
        } else {
            iter_->SeekToFirst();
        }
        const auto status = iter_->status();
        if (!status.ok()) throw RocksDBException("scan_arrow: seek failed: " + status.ToString());
        started_ = true;
        // Registration and establishment are atomic with respect to close/drop.
        parent_->active_arrow_scans_.insert(this);
        if (transaction_) {
            transaction_->active_arrow_scans_.fetch_add(1);
            transaction_registered_ = true;
        }
    } catch (...) {
        delete iter_;
        iter_ = nullptr;
        if (owned_snapshot_) db_raw->ReleaseSnapshot(owned_snapshot_);
        owned_snapshot_ = nullptr;
        shared_snapshot_.reset();
        throw;
    }
}

PyArrowScan::~PyArrowScan() {
    if (parent_) {
        try {
            parent_->detach_scan_resources(this, false);
            parent_->unregister_scan(this);
        } catch (...) {
        }
    }
}

void PyArrowScan::close() {
    if (!parent_) return;
    parent_->detach_scan_resources(this, false);
    parent_->unregister_scan(this);
}

bool PyArrowScan::cancel() {
    auto expected = Control::Active;
    return control_.compare_exchange_strong(expected, Control::CancelRequested);
}

bool PyArrowScan::cancel_requested() const {
    return control_.load() == Control::CancelRequested;
}

bool PyArrowScan::is_closed() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return closed_;
}

uint64_t PyArrowScan::sequence_number() const { return snapshot_seq_; }

py::dict PyArrowScan::configuration() const {
    py::dict result;
    result["contract_version"] = 1;
    result["source"] = transaction_ ? "transaction_scan" : point_lookup_ ? "points" : multi_range_ ? "range_union" : "scan";
    result["snapshot_scope"] = transaction_ ? "transaction_local_frozen_writes" : "same_db_committed_state";
    result["batch_size"] = batch_size_;
    result["max_batch_bytes"] = max_batch_bytes_;
    result["allow_overflow"] = allow_overflow_;
    result["collect_metrics"] = static_cast<bool>(metrics_);
    result["fill_cache"] = fill_cache_;
    result["verify_checksums"] = verify_checksums_;
    result["readahead_size"] = readahead_size_;
    result["total_order_seek"] = true;
    result["column_family"] = cf_name_;
    result["value_nullable"] = point_lookup_;
    result["value_staging_layout"] = point_lookup_ ? "strings" : "contiguous";
    return result;
}

std::optional<ArrowScanStatistics> PyArrowScan::statistics() const {
    if (!metrics_) return std::nullopt;
    std::lock_guard<std::mutex> lock(metrics_->mutex);
    auto snapshot = metrics_->totals;
    snapshot.rows_gathered = rows_gathered_.load(std::memory_order_relaxed);
    return snapshot;
}

void PyArrowScan::invalidate_for_db_close(rocksdb::DB* db_raw) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto deferred = finish_locked(db_raw);
    // Keep the Python-facing object until teardown is outside the parent
    // lock. DB close invalidates its native snapshot separately below.
    shared_snapshot_ = std::move(deferred);
    closed_ = true;
}

std::shared_ptr<PySnapshot> PyArrowScan::finish_locked(rocksdb::DB* db_raw) {
    auto expected = Control::Active;
    control_.compare_exchange_strong(expected, Control::Terminal);
    if (iter_) {
        delete iter_;
        iter_ = nullptr;
    }
    if (owned_snapshot_) {
        if (db_raw) db_raw->ReleaseSnapshot(owned_snapshot_);
        owned_snapshot_ = nullptr;
    }
    if (point_options_) {
        point_options_.reset();
        point_cf_ = nullptr;
        std::vector<std::string>().swap(point_keys_);
        std::vector<std::string>().swap(pending_values_);
        std::vector<uint8_t>().swap(pending_valid_);
        if (shared_snapshot_) {
            std::lock_guard<std::mutex> snap_lock(shared_snapshot_->mutex_);
            --shared_snapshot_->point_stream_pins_;
            if (shared_snapshot_->released_ && !shared_snapshot_->point_stream_pins_) {
                if (db_raw && shared_snapshot_->snapshot_)
                    db_raw->ReleaseSnapshot(shared_snapshot_->snapshot_);
                shared_snapshot_->snapshot_ = nullptr;
                parent_->active_snapshots_.erase(shared_snapshot_.get());
            }
        }
    }
    if (transaction_registered_) {
        // Native iterator/snapshot are gone before mutation can be admitted.
        transaction_registered_ = false;
        transaction_->active_arrow_scans_.fetch_sub(1);
    }
    // The last reference can call back into the parent. Destroy it only
    // AFTER the parent and scan locks have been released by the caller.
    return std::move(shared_snapshot_);
}

PyArrowScan::GatherResult PyArrowScan::gather_next_batch(std::vector<std::string>& keys, std::vector<std::string>& values,
                                                      uint64_t& staged_payload_bytes, std::vector<uint8_t>& valid,
                                                      ArrowValueStaging& staged_values) {
    if (cancel_requested()) return GatherResult::Cancelled;
    if (closed_) throw RocksDBException("Arrow scan is closed.");
    if (exhausted_) return GatherResult::End;
    if (point_lookup_) return gather_points(keys, values, staged_payload_bytes, valid);
    if (multi_range_ && ranges_.empty()) return GatherResult::End;
    if (stop_reached_) {
        exhausted_ = true;
        return GatherResult::End;
    }
    if (!iter_) throw RocksDBException("Arrow scan is no longer valid: database was closed.");

    keys.clear();
    values.clear();
    uint64_t key_total = 0;
    uint64_t value_total = 0;
    GatherProgress progress(metrics_ ? &rows_gathered_ : nullptr, keys, values,
                            metrics_ ? &staged_payload_bytes : nullptr, key_total, value_total, true);
    // Reserve one batch worth of row slots; payloads stream into the strings.
    keys.reserve(batch_size_ < 1024 ? batch_size_ : 1024);
    staged_values.offsets.reserve(std::min(batch_size_, size_t{1024}) + 1);

    bool hit_stop = false;

    while (keys.size() < batch_size_) {
        if ((keys.size() & (kCancellationPollRows - 1)) == 0) {
            progress.flush();
            if (cancel_requested()) return GatherResult::Cancelled;
        }
        if (!iter_->Valid() || (has_eff_stop_ &&
                compare_bytes(iter_->key().data(), iter_->key().size(), eff_stop_.data(), eff_stop_.size()) >= 0)) {
            if (!iter_->status().ok())
                throw RocksDBException("scan_arrow: iterator error: " + iter_->status().ToString());
            if (multi_range_ && range_position_ + 1 < ranges_.size()) {
                const auto& range = ranges_[++range_position_];
                has_eff_stop_ = range.stop.has_value();
                eff_stop_ = range.stop.value_or("");
                // Only the first canonical range can have an unbounded start.
                iter_->Seek(rocksdb::Slice(*range.start));
                if (cancel_requested()) return GatherResult::Cancelled;
                continue;
            }
            hit_stop = true;
            break;
        }
        rocksdb::Slice key = iter_->key();
        rocksdb::Slice value = iter_->value();
        const auto key_len = static_cast<uint64_t>(key.size());
        const auto value_len = static_cast<uint64_t>(value.size());

        if (key_total + key_len > static_cast<uint64_t>(INT32_MAX) ||
            value_total + value_len > static_cast<uint64_t>(INT32_MAX)) {
            throw RocksDBException("scan_arrow: record exceeds int32 Arrow offset range; schema uses int32 binary offsets.");
        }
        const uint64_t n = static_cast<uint64_t>(keys.size());
        // 8 bytes of offset overhead per row (two int32 offset slots).
        const uint64_t prospective = key_total + key_len + value_total + value_len + 8 * (n + 2);
        if (!keys.empty() && prospective > static_cast<uint64_t>(max_batch_bytes_)) {
            break;  // emit the current batch; the iterator stays on this record
        }
        if (keys.empty() && prospective > static_cast<uint64_t>(max_batch_bytes_) && !allow_overflow_) {
            throw RocksDBException("scan_arrow: single record of " + std::to_string(key_len + value_len) +
                                   " payload bytes exceeds max_batch_bytes=" + std::to_string(max_batch_bytes_) +
                                   "; raise max_batch_bytes or pass allow_overflow=True.");
        }
        staged_values.append(value, max_batch_bytes_);
        value_total += value_len;
        keys.emplace_back(key.data(), key.size());
        key_total += key_len;
        iter_->Next();
    }

    rocksdb::Status s = iter_->status();
    if (!s.ok()) {
        throw RocksDBException("scan_arrow: iterator error: " + s.ToString());
    }
    if (hit_stop) stop_reached_ = true;
    if (cancel_requested()) return GatherResult::Cancelled;
    return keys.empty() ? GatherResult::End : GatherResult::Batch;
}

PyArrowScan::GatherResult PyArrowScan::gather_points(std::vector<std::string>& keys,
        std::vector<std::string>& values, uint64_t& staged_payload_bytes, std::vector<uint8_t>& valid) {
    if (!point_options_) throw RocksDBException("multi_get_arrow: database or stream is closed.");
    uint64_t key_total = 0, value_total = 0;
    GatherProgress progress(metrics_ ? &rows_gathered_ : nullptr, keys, values,
                           metrics_ ? &staged_payload_bytes : nullptr, key_total, value_total);
    while (point_position_ < point_keys_.size() && keys.size() < batch_size_) {
        if ((keys.size() & (kCancellationPollRows - 1)) == 0) {
            progress.flush();
            if (cancel_requested()) return GatherResult::Cancelled;
        }
        if (pending_position_ == pending_values_.size()) {
            // Amortize native calls, but cap one uninterruptible MultiGet at
            // 256 records. Values can be large: output caps are not RSS caps.
            const size_t count = std::min({size_t{256}, batch_size_ - keys.size(),
                                          point_keys_.size() - point_position_});
            std::vector<rocksdb::Slice> slices;
            slices.reserve(count);
            for (size_t i = 0; i < count; ++i) slices.emplace_back(point_keys_[point_position_ + i]);
            pending_values_.clear();
            auto statuses = parent_->db_->MultiGet(*point_options_,
                std::vector<rocksdb::ColumnFamilyHandle*>(count, point_cf_), slices, &pending_values_);
            if (metrics_) {
                std::lock_guard<std::mutex> lock(metrics_->mutex);
                ++metrics_->totals.point_lookup_calls;
            }
            pending_valid_.clear();
            for (const auto& status : statuses) {
                if (!status.ok() && !status.IsNotFound())
                    throw RocksDBException("multi_get_arrow: " + status.ToString());
                pending_valid_.push_back(status.ok());
            }
            pending_position_ = 0;
            if (cancel_requested()) return GatherResult::Cancelled;
        }
        const auto& key = point_keys_[point_position_];
        const auto& value = pending_values_[pending_position_];
        const uint64_t n = keys.size() + 1;
        const uint64_t prospective = key_total + key.size() + value_total + value.size()
                                     + 8 * (n + 1) + (n + 7) / 8;
        if (prospective > max_batch_bytes_) {
            if (!keys.empty()) break;
            if (!allow_overflow_)
                throw RocksDBException("multi_get_arrow: single record exceeds max_batch_bytes.");
        }
        if (key_total + key.size() > INT32_MAX || value_total + value.size() > INT32_MAX)
            throw RocksDBException("multi_get_arrow: record exceeds int32 Arrow offset range.");
        keys.push_back(key);
        key_total += key.size();
        values.push_back(value);
        value_total += value.size();
        valid.push_back(pending_valid_[pending_position_]);
        ++pending_position_;
        ++point_position_;
    }
    if (cancel_requested()) return GatherResult::Cancelled;
    return keys.empty() ? GatherResult::End : GatherResult::Batch;
}

int PyArrowScan::cancelled_next() {
    cancellation_observed_.store(true, std::memory_order_release);
    if (metrics_) {
        std::lock_guard<std::mutex> lock(metrics_->mutex);
        ++metrics_->totals.cancelled_calls;
    }
    if (parent_) parent_->detach_scan_resources(this, false);
    return ECANCELED;
}

int PyArrowScan::fill_schema(ArrowSchema* out) {
    if (!out) return EINVAL;
    std::memset(out, 0, sizeof(*out));
    out->release = &free_schema;
    try {
        out->children = static_cast<ArrowSchema**>(std::calloc(2, sizeof(ArrowSchema*)));
        if (!out->children) throw std::bad_alloc();
        out->n_children = 2;
        for (int i = 0; i < 2; ++i) {
            out->children[i] = new ArrowSchema{};
            out->children[i]->release = &free_schema;
        }
        auto* key_schema = out->children[0];
        auto* value_schema = out->children[1];
        key_schema->format = dup_cstr("z");  // binary, non-null
        key_schema->name = dup_cstr("key");
        key_schema->release = &free_schema;
        value_schema->format = dup_cstr("z");
        value_schema->name = dup_cstr("value");
        if (point_lookup_) value_schema->flags = 2;  // ARROW_FLAG_NULLABLE
        value_schema->release = &free_schema;

        out->format = dup_cstr("+s");  // struct
        out->name = dup_cstr("");
        return 0;
    } catch (const std::bad_alloc&) {
        free_schema(out);
        return ENOMEM;
    } catch (...) {
        free_schema(out);
        return EINVAL;
    }
}

int PyArrowScan::fill_next(ArrowArray* out) {
    if (!out) return EINVAL;
    std::memset(out, 0, sizeof(*out));
    if (metrics_) {
        std::lock_guard<std::mutex> lock(metrics_->mutex);
        ++metrics_->totals.get_next_calls;
    }

    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<uint8_t> valid;
    ArrowValueStaging staged_values;
    bool clean_end = false;
    bool cancelled = false;
    uint64_t staged_payload_bytes = 0;
    std::string error;

    {
        std::unique_ptr<PyTransaction::OperationGuard> transaction_guard;
        if (transaction_ && control_.load() == Control::Active) {
            transaction_guard = std::make_unique<PyTransaction::OperationGuard>(*transaction_);
            transaction_->check_active();
        }
        std::unique_lock<std::mutex> lock(mutex_);
        PhaseTimer gather_timer(metrics_.get(), &ArrowScanStatistics::gather_time_ns);
        try {
            const auto result = gather_next_batch(keys, values, staged_payload_bytes, valid, staged_values);
            clean_end = result == GatherResult::End;
            cancelled = result == GatherResult::Cancelled;
        } catch (const std::exception& e) {
            error = e.what();
        } catch (...) {
            error = "scan_arrow: unknown native error";
        }
        if (!error.empty()) {
            last_error_ = error;
        }
    }

    if (metrics_) {
        std::lock_guard<std::mutex> lock(metrics_->mutex);
        if (staged_payload_bytes > metrics_->totals.peak_staged_payload_bytes)
            metrics_->totals.peak_staged_payload_bytes = staged_payload_bytes;
    }
    if (cancelled || cancel_requested()) return cancelled_next();
    if (!error.empty()) {
        // Deterministic failure: tear the scan down so a later get_next
        // keeps failing instead of masquerading as end-of-stream.
        if (parent_) parent_->detach_scan_resources(this, false);
        return EIO;
    }
    if (clean_end) {
        // If an accepted request wins this transition, EOS must become an
        // error. If EOF wins, subsequent cancel() requests return false.
        auto expected = Control::Active;
        control_.compare_exchange_strong(expected, Control::Terminal);
        if (cancel_requested()) return cancelled_next();
        if (parent_) parent_->detach_scan_resources(this, true);
        out->release = nullptr;  // end-of-stream marker
        return 0;
    }

    // Build owned Arrow buffers outside the scan lock; payloads already live
    // in `keys`/`values`, so this is a single memcpy per column.
    PhaseTimer build_timer(metrics_.get(), &ArrowScanStatistics::build_time_ns);
    const int64_t n = static_cast<int64_t>(keys.size());
    uint64_t key_bytes = 0;
    uint64_t value_bytes = point_lookup_ ? 0 : staged_values.used;
    for (size_t i = 0; i < keys.size(); ++i) {
        if ((i & (kCancellationPollRows - 1)) == 0 && cancel_requested())
            return cancelled_next();
        key_bytes += keys[i].size();
        if (point_lookup_) value_bytes += values[i].size();
    }

    auto* priv = new (std::nothrow) BatchBuffers();
    if (!priv) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            last_error_ = "scan_arrow: allocation failed";
        }
        if (parent_) parent_->detach_scan_resources(this, false);
        return ENOMEM;
    }
    auto* key_owned = new (std::nothrow) ChildBuffers();
    auto* value_owned = new (std::nothrow) ChildBuffers();
    if (!key_owned || !value_owned) {
        delete key_owned;
        delete value_owned;
        delete priv;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            last_error_ = "scan_arrow: allocation failed";
        }
        if (parent_) parent_->detach_scan_resources(this, false);
        return ENOMEM;
    }

    key_owned->offsets = std::malloc(static_cast<size_t>((static_cast<uint64_t>(n) + 1) * 4));
    value_owned->offsets = std::malloc(static_cast<size_t>((static_cast<uint64_t>(n) + 1) * 4));
    const uint64_t validity_bytes = point_lookup_ ? (static_cast<uint64_t>(n) + 7) / 8 : 0;
    if (validity_bytes) value_owned->validity = std::calloc(static_cast<size_t>(validity_bytes), 1);
    // Arrow data buffers must be non-null even when the column payload is
    // empty (e.g. all values are empty bytes), so allocate a dummy byte.
    key_owned->data = std::malloc(key_bytes ? static_cast<size_t>(key_bytes) : 1);
    value_owned->data = point_lookup_ ? std::malloc(value_bytes ? static_cast<size_t>(value_bytes) : 1)
                                      : staged_values.release_exact();
    key_owned->buffers = static_cast<const void**>(std::malloc(3 * sizeof(void*)));
    value_owned->buffers = static_cast<const void**>(std::malloc(3 * sizeof(void*)));
    priv->parent_buffers = static_cast<const void**>(std::malloc(sizeof(void*)));
    priv->child_pointers = static_cast<ArrowArray**>(std::malloc(2 * sizeof(ArrowArray*)));
    priv->key_array = new (std::nothrow) ArrowArray();
    priv->value_array = new (std::nothrow) ArrowArray();

    if ((validity_bytes && !value_owned->validity) || !key_owned->offsets || !key_owned->data || !key_owned->buffers || !value_owned->offsets || !value_owned->data ||
        !value_owned->buffers || !priv->parent_buffers || !priv->child_pointers || !priv->key_array ||
        !priv->value_array) {
        std::free(key_owned->offsets);
        std::free(key_owned->data);
        std::free(key_owned->buffers);
        std::free(value_owned->offsets);
        std::free(value_owned->validity);
        std::free(value_owned->data);
        std::free(value_owned->buffers);
        delete key_owned;
        delete value_owned;
        std::free(priv->parent_buffers);
        std::free(priv->child_pointers);
        delete priv->key_array;
        delete priv->value_array;
        delete priv;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            last_error_ = "scan_arrow: batch allocation failed";
        }
        if (parent_) parent_->detach_scan_resources(this, false);
        return ENOMEM;
    }

    fill_binary_child(priv->key_array, n, key_owned);
    fill_binary_child(priv->value_array, n, value_owned);
    priv->parent_buffers[0] = nullptr;
    priv->child_pointers[0] = priv->key_array;
    priv->child_pointers[1] = priv->value_array;
    PendingBatch pending;
    pending.array.length = n;
    pending.array.n_buffers = 1;
    pending.array.buffers = priv->parent_buffers;
    pending.array.n_children = 2;
    pending.array.children = priv->child_pointers;
    pending.array.release = &free_batch_array;
    pending.array.private_data = priv;

    auto* ko = static_cast<int32_t*>(key_owned->offsets);
    auto* vo = static_cast<int32_t*>(value_owned->offsets);
    auto* kd = static_cast<char*>(key_owned->data);
    auto* vd = static_cast<char*>(value_owned->data);
    ko[0] = 0;
    vo[0] = 0;
    for (int64_t i = 0; i < n; ++i) {
        if ((static_cast<uint64_t>(i) & (kCancellationPollRows - 1)) == 0 && cancel_requested())
            return cancelled_next();
        if (!keys[static_cast<size_t>(i)].empty()) {
            std::memcpy(kd, keys[static_cast<size_t>(i)].data(), keys[static_cast<size_t>(i)].size());
            kd += keys[static_cast<size_t>(i)].size();
        }
        ko[i + 1] = ko[i] + static_cast<int32_t>(keys[static_cast<size_t>(i)].size());
        if (point_lookup_) {
            if (!values[static_cast<size_t>(i)].empty()) {
                std::memcpy(vd, values[static_cast<size_t>(i)].data(), values[static_cast<size_t>(i)].size());
                vd += values[static_cast<size_t>(i)].size();
            }
            vo[i + 1] = vo[i] + static_cast<int32_t>(values[static_cast<size_t>(i)].size());
        } else {
            vo[i + 1] = staged_values.offsets[static_cast<size_t>(i) + 1];
        }
        if (point_lookup_) {
            if (valid[static_cast<size_t>(i)])
                static_cast<uint8_t*>(value_owned->validity)[i / 8] |= uint8_t{1} << (i % 8);
            else ++priv->value_array->null_count;
        }
    }

    // Publication decision: cancellation observed here discards the entire
    // owned batch. A request after this checkpoint may still race with a
    // fully committed batch; the following get_next reports cancellation.
    if (cancel_requested()) return cancelled_next();

    // Publish counters only for a fully constructed batch. Each child owns
    // its own capacity accounting because consumers may detach/move children
    // and release them independently of the parent ArrowArray.
    if (metrics_) {
        const uint64_t offset_bytes = (static_cast<uint64_t>(n) + 1) * 8;
        const uint64_t key_capacity = (static_cast<uint64_t>(n) + 1) * 4 + (key_bytes ? key_bytes : 1);
        const uint64_t value_capacity = (static_cast<uint64_t>(n) + 1) * 4 + (value_bytes ? value_bytes : 1) + validity_bytes;
        const uint64_t capacity = key_capacity + value_capacity;
        const uint64_t used = key_bytes + value_bytes + offset_bytes + validity_bytes;
        key_owned->metrics = metrics_;
        value_owned->metrics = metrics_;
        key_owned->tracked_capacity = key_capacity;
        value_owned->tracked_capacity = value_capacity;
        std::lock_guard<std::mutex> lock(metrics_->mutex);
        auto& s = metrics_->totals;
        ++s.batches_emitted;
        s.rows_emitted += static_cast<uint64_t>(n);
        s.key_bytes_emitted += key_bytes;
        s.value_bytes_emitted += value_bytes;
        if (point_lookup_) s.value_bytes_copied_to_output += value_bytes;
        s.offset_bytes_emitted += offset_bytes;
        s.validity_bytes_emitted += validity_bytes;
        s.output_bytes_emitted += used;
        s.buffer_capacity_bytes_emitted += capacity;
        s.allocation_slack_bytes_emitted += capacity - used;
        s.live_buffer_bytes += capacity;
        if (s.live_buffer_bytes > s.peak_live_buffer_bytes) s.peak_live_buffer_bytes = s.live_buffer_bytes;
    }

    pending.publish(out);
    return 0;
}

const char* PyArrowScan::last_error_cstr() {
    if (cancellation_observed_.load(std::memory_order_acquire)) return "scan_arrow: cancelled";
    std::lock_guard<std::mutex> lock(mutex_);
    return last_error_.empty() ? "scan_arrow: native allocation or stream failure" : last_error_.c_str();
}

// ---------------------------------------------------------------------------
// ArrowArrayStream trampolines (global scope to match the friend
// declarations in arrow_scan.hpp).
// ---------------------------------------------------------------------------

namespace {
struct StreamExport {
    std::shared_ptr<PyArrowScan> scan;
};
}  // namespace

int stream_get_schema(ArrowArrayStream* stream, ArrowSchema* out) {
    if (!stream || !stream->private_data || !out) return EINVAL;
    auto* exp = static_cast<StreamExport*>(stream->private_data);
    return exp->scan->fill_schema(out);
}

int stream_get_next(ArrowArrayStream* stream, ArrowArray* out) {
    if (!stream || !stream->private_data || !out) return EINVAL;
    auto* exp = static_cast<StreamExport*>(stream->private_data);
    // No GIL handling here: consumers (e.g. PyArrow) may call get_next with
    // the GIL released, and fill_next touches no Python API (native RocksDB
    // iteration, malloc'd buffers, mutexes only), so it is safe either way.
    try {
        return exp->scan->fill_next(out);
    } catch (const std::bad_alloc&) {
        try { exp->scan->close(); } catch (...) {}
        std::memset(out, 0, sizeof(*out));
        return ENOMEM;
    } catch (...) {
        try { exp->scan->close(); } catch (...) {}
        std::memset(out, 0, sizeof(*out));
        return EIO;
    }
}

const char* stream_get_last_error(ArrowArrayStream* stream) {
    if (!stream || !stream->private_data) return "scan_arrow: invalid stream";
    auto* exp = static_cast<StreamExport*>(stream->private_data);
    return exp->scan->last_error_cstr();
}

void stream_release(ArrowArrayStream* stream) {
    if (!stream || !stream->release) return;
    auto* exp = static_cast<StreamExport*>(stream->private_data);
    // Releasing a consumer must cancel native resources even if Python
    // still holds an ArrowScan reference. Never throw through the C ABI.
    try { if (exp) exp->scan->close(); } catch (...) {}
    delete exp;
    stream->private_data = nullptr;
    stream->release = nullptr;
}

namespace {
void stream_capsule_destructor(PyObject* capsule) {
    void* ptr = PyCapsule_GetPointer(capsule, "arrow_array_stream");
    if (ptr == nullptr) {
        PyErr_Clear();  // consumer already tore the stream down; nothing to do
        return;
    }
    auto* stream = static_cast<ArrowArrayStream*>(ptr);
    if (stream->release) stream->release(stream);
    delete stream;
}
}  // namespace

py::object PyArrowScan::export_stream(py::object /*requested_schema*/) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (exported_) {
        throw RocksDBException("Arrow scan stream has already been exported; a scan is single-consumer.");
    }
    if (closed_) {
        throw RocksDBException("Arrow scan is closed.");
    }
    auto* stream = new (std::nothrow) ArrowArrayStream();
    if (!stream) throw RocksDBException("scan_arrow: failed to allocate Arrow stream.");
    auto* exp = new (std::nothrow) StreamExport();
    if (!exp) {
        delete stream;
        throw RocksDBException("scan_arrow: failed to allocate Arrow stream.");
    }
    exp->scan = shared_from_this();
    stream->get_schema = &stream_get_schema;
    stream->get_next = &stream_get_next;
    stream->get_last_error = &stream_get_last_error;
    stream->release = &stream_release;
    stream->private_data = exp;
    exported_ = true;
    PyObject* capsule = PyCapsule_New(static_cast<void*>(stream), "arrow_array_stream", &stream_capsule_destructor);
    if (!capsule) {
        // export_stream already owns mutex_: stream_release would call
        // close() and recursively acquire it. No consumer exists yet.
        delete exp;
        delete stream;
        exported_ = false;
        throw py::error_already_set();
    }
    return py::reinterpret_steal<py::object>(capsule);
}
