#include "db.hpp"
#include <algorithm>

#include <cstring>

#include "arrow_scan.hpp"
#include "columnar_batch.hpp"
#include "exceptions.hpp"
#include "iterator.hpp"
#include "write_batch.hpp"

#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"
#include <pybind11/stl.h>

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

std::shared_ptr<PyColumnFamilyHandle> PyRocksDB::require_cf(PyColumnFamilyHandle& cf) const {
    check_db_open();
    auto it = cf_handles_.find(cf.get_name());
    if (it == cf_handles_.end() || it->second.get() != &cf || !cf.is_valid()) {
        throw RocksDBException("ColumnFamilyHandle is invalid or belongs to a different database instance.");
    }
    return it->second;
}

rocksdb::ReadOptions PyRocksDB::snapshot_read_options(std::shared_ptr<PyReadOptions> options,
                                                    const std::shared_ptr<PySnapshot>& snapshot) const {
    check_db_open();
    auto opts = (options ? options : default_read_options_)->native_options();
    if (snapshot) {
        if (snapshot->parent_.get() != this) {
            throw RocksDBException("Snapshot belongs to a different database instance.");
        }
        std::lock_guard<std::mutex> lock(snapshot->mutex_);
        if (snapshot->released_ || !snapshot->snapshot_) throw RocksDBException("Snapshot has been released.");
        opts.snapshot = snapshot->snapshot_;
    }
    return opts;
}

void PyRocksDB::close() {
    if (!is_closed_.exchange(true)) {
        // arrow_mutex_ serializes snapshot/scan teardown with concurrent
        // scan creation and snapshot release; it is held until db_ is gone
        // so no scan path can touch a half-destroyed DB.
        std::lock_guard<std::shared_mutex> arrow_lock(arrow_mutex_);
        before_close_locked();
        for (PyArrowScan* scan : active_arrow_scans_) {
            scan->invalidate_for_db_close(db_);
        }
        active_arrow_scans_.clear();
        for (PySnapshot* snapshot : active_snapshots_) {
            snapshot->invalidate_for_db_close(db_);
        }
        active_snapshots_.clear();
        {
            std::lock_guard<std::mutex> lock(active_iterators_mutex_);
            for (rocksdb::Iterator* iter_raw_ptr : active_rocksdb_iterators_) {
                delete iter_raw_ptr;
            }
            active_rocksdb_iterators_.clear();
        }
        for (auto const& [name, handle_ptr] : cf_handles_) {
            if (db_ && handle_ptr->cf_handle_ && (owns_cf_handles_ || name != rocksdb::kDefaultColumnFamilyName)) {
                db_->DestroyColumnFamilyHandle(handle_ptr->cf_handle_);
            }
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
    std::shared_lock<std::shared_mutex> lock(arrow_mutex_);
    check_db_open();
    check_read_only();

    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    rocksdb::Slice value_slice(static_cast<std::string_view>(value));

    const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
    rocksdb::Status s = db_->Put(opts, default_cf_handle_, key_slice, value_slice);
    if (!s.ok()) throw RocksDBException("Put failed: " + s.ToString());
}

py::object PyRocksDB::get_fast_impl(const py::bytes& key) {
    // Borrow the Python key buffer instead of copying it into a std::string.
    // This is safe across the GIL release: the `key` argument handle keeps
    // its Python bytes object alive for the whole call, bytes are immutable,
    // even though other Python threads may run while this thread releases
    // the GIL. Extract the pointer while holding the GIL.
    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    std::string value_str;
    rocksdb::Status s;
    {
        py::gil_scoped_release release;
        std::shared_lock<std::shared_mutex> lock(arrow_mutex_);
        if (is_closed_ || db_ == nullptr || default_cf_handle_ == nullptr) {
            throw RocksDBException("Database is not open or has been closed.");
        }
        const auto& opts = default_read_options_->native_options();
        s = db_->Get(opts, default_cf_handle_, key_slice, &value_str);
    }
    if (s.ok()) return py::bytes(value_str);
    if (s.IsNotFound()) return py::none();
    throw_rocksdb_status(s, "Get failed");
}

py::object PyRocksDB::get_safe_impl(PyColumnFamilyHandle* cf, const py::bytes& key,
                                    std::shared_ptr<PyReadOptions> read_options,
                                    std::shared_ptr<PySnapshot> snapshot) {
    // Preserved 0.6.0/0.7.0 implementation for snapshot/CF/custom-option
    // reads: owned key copy, ReadOptions copy with snapshot pinning, and
    // validated CF-handle resolution. Immutable native option templates
    // make configuration capture race-free. Shared lifetime protection lets
    // snapshot/CF readers overlap; close/CF-drop remain exclusive.
    std::string key_storage = key;
    std::string value_str;
    rocksdb::Status s;
    {
        py::gil_scoped_release release;
        std::shared_lock<std::shared_mutex> lock(arrow_mutex_);
        auto opts = snapshot_read_options(read_options, snapshot);
        auto* raw_cf = cf ? require_cf(*cf)->cf_handle_ : get_default_cf_handle();
        s = db_->Get(opts, raw_cf, rocksdb::Slice(key_storage), &value_str);
    }
    if (s.ok()) return py::bytes(value_str);
    if (s.IsNotFound()) return py::none();
    throw_rocksdb_status(s, "Get failed");
}

py::object PyRocksDB::get_impl(PyColumnFamilyHandle* cf, const py::bytes& key,
                              std::shared_ptr<PyReadOptions> read_options, std::shared_ptr<PySnapshot> snapshot) {
    // Flexible dispatch (issue #36): the overwhelmingly common ordinary
    // point read (default CF, default options, no explicit snapshot) uses
    // the fast path; everything else keeps the previous safe implementation.
    if (cf == nullptr && !read_options && !snapshot) {
        return get_fast_impl(key);
    }
    return get_safe_impl(cf, key, std::move(read_options), std::move(snapshot));
}

py::object PyRocksDB::get(const py::bytes& key, std::shared_ptr<PyReadOptions> read_options,
                         std::shared_ptr<PySnapshot> snapshot) {
    return get_impl(nullptr, key, std::move(read_options), std::move(snapshot));
}

py::list PyRocksDB::multi_get_impl(PyColumnFamilyHandle* cf, const py::sequence& keys,
                                 std::shared_ptr<PyReadOptions> read_options, std::shared_ptr<PySnapshot> snapshot) {
    const size_t count = py::len(keys);
    if (count > 65536) throw py::value_error("multi_get accepts at most 65536 keys per call");
    std::vector<std::string> storage;
    storage.reserve(count);
    for (auto key : keys) {
        if (!py::isinstance<py::bytes>(key)) throw py::type_error("multi_get keys must be bytes");
        storage.push_back(py::cast<std::string>(key));
    }
    std::vector<rocksdb::Slice> slices;
    slices.reserve(count);
    for (const auto& key : storage) slices.emplace_back(key);
    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses;
    {
        py::gil_scoped_release release;
        // Shared (was exclusive): concurrent MultiGets now proceed in
        // parallel; close/CF-drop hold the mutex exclusively. Body otherwise
        // unchanged (owned key copies, ReadOptions copy, CF validation).
        std::shared_lock<std::shared_mutex> lock(arrow_mutex_);
        auto opts = snapshot_read_options(read_options, snapshot);
        auto* raw_cf = cf ? require_cf(*cf)->cf_handle_ : get_default_cf_handle();
        if (count) statuses = db_->MultiGet(opts, std::vector<rocksdb::ColumnFamilyHandle*>(count, raw_cf), slices, &values);
    }
    py::list out;
    for (size_t i = 0; i < count; ++i) {
        if (statuses[i].ok()) out.append(py::bytes(values[i]));
        else if (statuses[i].IsNotFound()) out.append(py::none());
        else throw_rocksdb_status(statuses[i], "MultiGet failed");
    }
    return out;
}

py::list PyRocksDB::multi_get(const py::sequence& keys, std::shared_ptr<PyReadOptions> options,
                             std::shared_ptr<PySnapshot> snapshot) {
    return multi_get_impl(nullptr, keys, std::move(options), std::move(snapshot));
}

void PyRocksDB::del(const py::bytes& key, std::shared_ptr<PyWriteOptions> write_options) {
    std::shared_lock<std::shared_mutex> lock(arrow_mutex_);
    check_db_open();
    check_read_only();
    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
    rocksdb::Status s = db_->Delete(opts, default_cf_handle_, key_slice);
    if (!s.ok()) throw RocksDBException("Delete failed: " + s.ToString());
}

void PyRocksDB::write(PyWriteBatch& batch, std::shared_ptr<PyWriteOptions> write_options) {
    std::shared_lock<std::shared_mutex> lock(arrow_mutex_);
    check_db_open();
    check_read_only();
    for (const auto& cf : batch.families_) require_cf(*cf);
    const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
    rocksdb::Status s = db_->Write(opts, &batch.wb_);
    if (!s.ok()) throw RocksDBException("Write failed: " + s.ToString());
}

void PyRocksDB::write_columnar_batch(const py::object& keys, const py::object& values, std::shared_ptr<PyWriteOptions> write_options, const std::string& on_null) {
    {
        std::lock_guard<std::shared_mutex> lock(arrow_mutex_);
        check_db_open();
        check_read_only();
    }

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

    rocksdb::Status s;
    {
        py::gil_scoped_release release;
        std::shared_lock<std::shared_mutex> lock(arrow_mutex_);
        check_db_open();
        const auto opts = write_options ? write_options->options_ : default_write_options_->options_;
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
    py::gil_scoped_release release;
    std::lock_guard<std::shared_mutex> arrow_lock(arrow_mutex_);
    check_db_open();
    const auto& opts = (read_options ? read_options : default_read_options_)->native_options();
    rocksdb::Iterator* raw_iter = db_->NewIterator(opts, default_cf_handle_);
    {
        std::lock_guard<std::mutex> lock(active_iterators_mutex_);
        active_rocksdb_iterators_.insert(raw_iter);
    }
    return std::make_shared<PyRocksDBIterator>(raw_iter, shared_from_this());
}

PyOptions PyRocksDB::get_options() const { return opened_options_; }

std::shared_ptr<PyReadOptions> PyRocksDB::get_default_read_options() {
    std::shared_lock<std::shared_mutex> lock(arrow_mutex_);
    return default_read_options_;
}

void PyRocksDB::set_default_read_options(std::shared_ptr<PyReadOptions> opts) {
    if (!opts) throw RocksDBException("ReadOptions cannot be None.");
    std::lock_guard<std::shared_mutex> lock(arrow_mutex_);
    default_read_options_ = opts;
}

std::shared_ptr<PyWriteOptions> PyRocksDB::get_default_write_options() { return default_write_options_; }

void PyRocksDB::set_default_write_options(std::shared_ptr<PyWriteOptions> opts) {
    if (!opts) throw RocksDBException("WriteOptions cannot be None.");
    std::lock_guard<std::shared_mutex> lock(arrow_mutex_);
    default_write_options_ = opts;
}

// ---------------------------------------------------------------------------
// Snapshots and batched Arrow scans (feature #31).
// ---------------------------------------------------------------------------

namespace {

constexpr size_t kMaxBatchSize = 16 * 1024 * 1024;
constexpr size_t kMaxBatchBytes = size_t{4} * 1024 * 1024 * 1024;

int compare_bounds(const std::string& a, const std::string& b) {
    const size_t common = a.size() < b.size() ? a.size() : b.size();
    if (common > 0) {
        const int cmp = std::memcmp(a.data(), b.data(), common);
        if (cmp != 0) return cmp < 0 ? -1 : 1;
    }
    if (a.size() == b.size()) return 0;
    return a.size() < b.size() ? -1 : 1;
}

std::pair<bool, std::string> parse_bound(const py::object& obj, const char* name) {
    if (obj.is_none()) return {false, {}};
    if (py::isinstance<py::bytes>(obj)) return {true, obj.cast<py::bytes>()};
    throw py::type_error(std::string("scan_arrow: '") + name + "' must be bytes or None");
}

std::shared_ptr<PyArrowScan> scan_arrow_impl(std::shared_ptr<PyRocksDB> self, std::shared_ptr<PyColumnFamilyHandle> cf,
                                             py::object prefix, py::object start, py::object stop, size_t batch_size,
                                             size_t max_batch_bytes, std::shared_ptr<PyReadOptions> read_options,
                                             std::shared_ptr<PySnapshot> snapshot, bool allow_overflow, bool collect_metrics) {
    if (batch_size < 1 || batch_size > kMaxBatchSize) {
        throw py::value_error("scan_arrow: batch_size must be in [1, " + std::to_string(kMaxBatchSize) + "]");
    }
    if (max_batch_bytes < 1 || max_batch_bytes > kMaxBatchBytes) {
        throw py::value_error("scan_arrow: max_batch_bytes must be in [1, " + std::to_string(kMaxBatchBytes) + "]");
    }
    auto [has_prefix, prefix_bytes] = parse_bound(prefix, "prefix");
    auto [has_start, start_bytes] = parse_bound(start, "start");
    auto [has_stop, stop_bytes] = parse_bound(stop, "stop");
    if (has_start && has_stop && compare_bounds(start_bytes, stop_bytes) > 0) {
        throw py::value_error("scan_arrow: start must be <= stop (start is inclusive, stop is exclusive)");
    }

    PyArrowScan::Config config;
    config.prefix = std::move(prefix_bytes);
    config.has_prefix = has_prefix;
    config.start = std::move(start_bytes);
    config.has_start = has_start;
    config.stop = std::move(stop_bytes);
    config.has_stop = has_stop;
    config.batch_size = batch_size;
    config.max_batch_bytes = max_batch_bytes;
    config.allow_overflow = allow_overflow;
    config.collect_metrics = collect_metrics;

    // The constructor pins the snapshot, pushes bounds down and seeks; only
    // register with the parent once it is fully established.
    auto scan = std::make_shared<PyArrowScan>(self, cf, config, snapshot, read_options);
    return scan;
}

}  // namespace

namespace {
std::shared_ptr<PyArrowScan> point_arrow_impl(std::shared_ptr<PyRocksDB> self,
        std::shared_ptr<PyColumnFamilyHandle> cf, py::object keys, size_t batch_size,
        size_t max_batch_bytes, std::shared_ptr<PyReadOptions> read_options,
        std::shared_ptr<PySnapshot> snapshot, bool allow_overflow, bool collect_metrics) {
    if (batch_size < 1 || batch_size > kMaxBatchSize || max_batch_bytes < 1 || max_batch_bytes > kMaxBatchBytes)
        throw py::value_error("multi_get_arrow: invalid batch_size or max_batch_bytes");
    if (py::len(keys) > 65536) throw py::value_error("multi_get_arrow accepts at most 65536 keys per stream");
    if (py::hasattr(keys, "type")) {
        const std::string type = py::str(keys.attr("type"));
        if (type != "binary" && type != "large_binary")
            throw py::type_error("multi_get_arrow keys must be binary or large_binary");
    }
    auto column = extract_byte_column(keys, "keys");
    PyArrowScan::Config config;
    config.point_lookup = true;
    config.batch_size = batch_size;
    config.max_batch_bytes = max_batch_bytes;
    config.allow_overflow = allow_overflow;
    config.collect_metrics = collect_metrics;
    config.point_keys.reserve(column.length_);
    for (size_t i = 0; i < column.length_; ++i) config.point_keys.emplace_back(column.value(i));
    return std::make_shared<PyArrowScan>(self, cf, config, snapshot, read_options);
}
}  // namespace

std::shared_ptr<PyArrowScan> PyRocksDB::multi_get_arrow(py::object keys, size_t batch_size,
        size_t max_batch_bytes, std::shared_ptr<PyReadOptions> read_options,
        std::shared_ptr<PySnapshot> snapshot, bool allow_overflow, bool collect_metrics) {
    return point_arrow_impl(shared_from_this(), nullptr, keys, batch_size, max_batch_bytes,
                            read_options, snapshot, allow_overflow, collect_metrics);
}

std::shared_ptr<PyArrowScan> PyRocksDBExtended::multi_get_arrow_cf(std::shared_ptr<PyColumnFamilyHandle> cf,
        py::object keys, size_t batch_size, size_t max_batch_bytes, std::shared_ptr<PyReadOptions> read_options,
        std::shared_ptr<PySnapshot> snapshot, bool allow_overflow, bool collect_metrics) {
    if (!cf) throw py::value_error("cf_handle must not be None");
    return point_arrow_impl(shared_from_this(), cf, keys, batch_size, max_batch_bytes,
                            read_options, snapshot, allow_overflow, collect_metrics);
}

namespace {
std::shared_ptr<PyArrowScan> ranges_arrow_impl(std::shared_ptr<PyRocksDB> self,
        std::shared_ptr<PyColumnFamilyHandle> cf, py::object ranges, py::object prefixes,
        size_t batch_size, size_t max_batch_bytes, std::shared_ptr<PyReadOptions> read_options,
        std::shared_ptr<PySnapshot> snapshot, bool allow_overflow, bool collect_metrics) {
    if (batch_size < 1 || batch_size > kMaxBatchSize || max_batch_bytes < 1 || max_batch_bytes > kMaxBatchBytes)
        throw py::value_error("scan_arrow_ranges: invalid batch_size or max_batch_bytes");
    if (ranges.is_none() && prefixes.is_none())
        throw py::value_error("scan_arrow_ranges: supply ranges and/or prefixes (empty lists select no records)");
    std::vector<PyArrowScan::Range> input;
    auto require_sequence = [](const py::object& obj) {
        if (!py::isinstance<py::list>(obj) && !py::isinstance<py::tuple>(obj))
            throw py::type_error("scan_arrow_ranges: ranges/prefixes must be lists or tuples");
        if (py::len(obj) > 65536) throw py::value_error("scan_arrow_ranges: at most 65536 selectors per argument");
    };
    if (!ranges.is_none()) {
        require_sequence(ranges);
        for (auto item : ranges) {
            auto pair = py::reinterpret_borrow<py::object>(item);
            if ((!py::isinstance<py::list>(pair) && !py::isinstance<py::tuple>(pair)) || py::len(pair) != 2)
                throw py::type_error("scan_arrow_ranges: each range must be a (start, stop) pair");
            auto [has_start, start] = parse_bound(pair[py::int_(0)], "start");
            auto [has_stop, stop] = parse_bound(pair[py::int_(1)], "stop");
            if (has_start && has_stop && compare_bounds(start, stop) > 0)
                throw py::value_error("scan_arrow_ranges: start must be <= stop");
            if (has_start && has_stop && start == stop) continue;
            input.push_back({has_start ? std::optional<std::string>(start) : std::nullopt,
                             has_stop ? std::optional<std::string>(stop) : std::nullopt});
        }
    }
    if (!prefixes.is_none()) {
        require_sequence(prefixes);
        for (auto item : prefixes) {
            if (!py::isinstance<py::bytes>(item)) throw py::type_error("scan_arrow_ranges: prefixes must be bytes");
            const auto prefix = py::cast<std::string>(item);
            std::string stop;
            const bool bounded = prefix_successor(prefix, stop);
            input.push_back({prefix.empty() ? std::nullopt : std::optional<std::string>(prefix),
                             bounded ? std::optional<std::string>(stop) : std::nullopt});
        }
    }
    std::sort(input.begin(), input.end(), [](const auto& a, const auto& b) {
        if (!a.start) return b.start.has_value();
        if (!b.start) return false;
        return compare_bounds(*a.start, *b.start) < 0;
    });
    PyArrowScan::Config config;
    config.multi_range = true;
    for (const auto& range : input) {
        if (config.ranges.empty() || (config.ranges.back().stop && range.start &&
                compare_bounds(*config.ranges.back().stop, *range.start) < 0)) {
            config.ranges.push_back(range);
        } else if (!range.stop || (config.ranges.back().stop &&
                compare_bounds(*range.stop, *config.ranges.back().stop) > 0)) {
            config.ranges.back().stop = range.stop;
        }
    }
    config.batch_size = batch_size;
    config.max_batch_bytes = max_batch_bytes;
    config.allow_overflow = allow_overflow;
    config.collect_metrics = collect_metrics;
    return std::make_shared<PyArrowScan>(self, cf, config, snapshot, read_options);
}
}  // namespace

std::shared_ptr<PyArrowScan> PyRocksDB::scan_arrow_ranges(py::object ranges, py::object prefixes,
        size_t batch_size, size_t max_batch_bytes, std::shared_ptr<PyReadOptions> read_options,
        std::shared_ptr<PySnapshot> snapshot, bool allow_overflow, bool collect_metrics) {
    return ranges_arrow_impl(shared_from_this(), nullptr, ranges, prefixes, batch_size, max_batch_bytes,
                             read_options, snapshot, allow_overflow, collect_metrics);
}

std::shared_ptr<PyArrowScan> PyRocksDBExtended::scan_arrow_ranges_cf(std::shared_ptr<PyColumnFamilyHandle> cf,
        py::object ranges, py::object prefixes, size_t batch_size, size_t max_batch_bytes,
        std::shared_ptr<PyReadOptions> read_options, std::shared_ptr<PySnapshot> snapshot,
        bool allow_overflow, bool collect_metrics) {
    if (!cf) throw py::value_error("cf_handle must not be None");
    return ranges_arrow_impl(shared_from_this(), cf, ranges, prefixes, batch_size, max_batch_bytes,
                             read_options, snapshot, allow_overflow, collect_metrics);
}

void PyRocksDB::unregister_scan(PyArrowScan* scan) {
    std::lock_guard<std::shared_mutex> lock(arrow_mutex_);
    active_arrow_scans_.erase(scan);
}

void PyRocksDB::detach_scan_resources(PyArrowScan* scan, bool mark_exhausted) {
    std::shared_ptr<PySnapshot> deferred;
    std::lock_guard<std::shared_mutex> arrow_lock(arrow_mutex_);
    std::lock_guard<std::mutex> scan_lock(scan->mutex_);
    deferred = scan->finish_locked(db_);
    if (mark_exhausted) {
        scan->exhausted_ = true;
    } else {
        scan->closed_ = true;
    }
}

void PyRocksDB::detach_snapshot(PySnapshot* snapshot) {
    std::lock_guard<std::shared_mutex> arrow_lock(arrow_mutex_);
    std::lock_guard<std::mutex> snap_lock(snapshot->mutex_);
    snapshot->released_ = true;
    if (snapshot->point_stream_pins_) return;  // existing point streams retain their view
    if (snapshot->snapshot_ && db_) {
        db_->ReleaseSnapshot(snapshot->snapshot_);
    }
    snapshot->release_locked();
    active_snapshots_.erase(snapshot);
}

std::shared_ptr<PySnapshot> PyRocksDB::create_snapshot() {
    py::gil_scoped_release release;
    std::shared_ptr<PySnapshot> snapshot;
    std::lock_guard<std::shared_mutex> arrow_lock(arrow_mutex_);
    check_db_open();
    const rocksdb::Snapshot* raw = db_->GetSnapshot();
    if (!raw) throw RocksDBException("create_snapshot failed: null snapshot returned.");
    try {
        snapshot = std::make_shared<PySnapshot>(shared_from_this(), raw);
        active_snapshots_.insert(snapshot.get());
    } catch (...) {
        if (snapshot) snapshot->invalidate_for_db_close(db_);
        else db_->ReleaseSnapshot(raw);
        throw;
    }
    return snapshot;
}

std::shared_ptr<PyArrowScan> PyRocksDB::scan_arrow(py::object prefix, py::object start, py::object stop, size_t batch_size,
                                                   size_t max_batch_bytes, std::shared_ptr<PyReadOptions> read_options,
                                                   std::shared_ptr<PySnapshot> snapshot, bool allow_overflow, bool collect_metrics) {
    return scan_arrow_impl(shared_from_this(), nullptr, std::move(prefix), std::move(start), std::move(stop), batch_size,
                           max_batch_bytes, read_options, snapshot, allow_overflow, collect_metrics);
}

PyRocksDBExtended::PyRocksDBExtended(const std::string& path, PyOptions* py_options, bool read_only) {
    owns_cf_handles_ = true;
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
    std::shared_lock<std::shared_mutex> lock(arrow_mutex_);
    check_db_open();
    check_read_only();
    require_cf(cf);
    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    rocksdb::Slice value_slice(static_cast<std::string_view>(value));
    const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
    rocksdb::Status s = db_->Put(opts, cf.cf_handle_, key_slice, value_slice);
    if (!s.ok()) throw RocksDBException("put_cf failed: " + s.ToString());
}

py::object PyRocksDBExtended::get_cf(PyColumnFamilyHandle& cf, const py::bytes& key,
                                    std::shared_ptr<PyReadOptions> read_options, std::shared_ptr<PySnapshot> snapshot) {
    return get_impl(&cf, key, std::move(read_options), std::move(snapshot));
}

py::list PyRocksDBExtended::multi_get_cf(PyColumnFamilyHandle& cf, const py::sequence& keys,
                                       std::shared_ptr<PyReadOptions> options, std::shared_ptr<PySnapshot> snapshot) {
    return multi_get_impl(&cf, keys, std::move(options), std::move(snapshot));
}

void PyRocksDBExtended::del_cf(PyColumnFamilyHandle& cf, const py::bytes& key, std::shared_ptr<PyWriteOptions> write_options) {
    std::shared_lock<std::shared_mutex> lock(arrow_mutex_);
    check_db_open();
    check_read_only();
    require_cf(cf);
    rocksdb::Slice key_slice(static_cast<std::string_view>(key));
    const auto& opts = write_options ? write_options->options_ : default_write_options_->options_;
    rocksdb::Status s = db_->Delete(opts, cf.cf_handle_, key_slice);
    if (!s.ok()) throw RocksDBException("del_cf failed: " + s.ToString());
}

std::vector<std::string> PyRocksDBExtended::list_column_families() {
    std::lock_guard<std::shared_mutex> lock(arrow_mutex_);
    check_db_open();
    std::vector<std::string> names;
    for (const auto& pair : cf_handles_) {
        names.push_back(pair.first);
    }
    return names;
}

std::shared_ptr<PyColumnFamilyHandle> PyRocksDBExtended::create_column_family(const std::string& name, PyOptions* cf_py_options) {
    std::lock_guard<std::shared_mutex> lock(arrow_mutex_);
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
    std::lock_guard<std::shared_mutex> arrow_lock(arrow_mutex_);
    check_db_open();
    check_read_only();
    require_cf(cf_handle);
    check_cf_drop_locked();
    if (cf_handle.get_name() == rocksdb::kDefaultColumnFamilyName) throw RocksDBException("Cannot drop the default column family.");

    rocksdb::ColumnFamilyHandle* raw_handle = cf_handle.cf_handle_;
    std::string cf_name = cf_handle.get_name();

    {
        for (PyArrowScan* scan : active_arrow_scans_) {
            if (scan->uses_cf_active(cf_name)) {
                throw RocksDBException("Cannot drop column family '" + cf_name + "': an active Arrow scan is using it. Close the scan first.");
            }
        }
    }

    rocksdb::Status s = db_->DropColumnFamily(raw_handle);
    if (!s.ok()) throw RocksDBException("Failed to drop column family '" + cf_name + "': " + s.ToString());

    s = db_->DestroyColumnFamilyHandle(raw_handle);
    if (!s.ok()) throw RocksDBException("Dropped CF but failed to destroy handle: " + s.ToString());

    cf_handles_.erase(cf_name);
    cf_handle.cf_handle_ = nullptr;
}

std::shared_ptr<PyColumnFamilyHandle> PyRocksDBExtended::get_column_family(const std::string& name) {
    std::lock_guard<std::shared_mutex> lock(arrow_mutex_);
    check_db_open();
    auto it = cf_handles_.find(name);
    return (it == cf_handles_.end()) ? nullptr : it->second;
}

std::shared_ptr<PyColumnFamilyHandle> PyRocksDBExtended::get_default_cf() {
    return get_column_family(rocksdb::kDefaultColumnFamilyName);
}

std::shared_ptr<PyRocksDBIterator> PyRocksDBExtended::new_cf_iterator(PyColumnFamilyHandle& cf_handle, std::shared_ptr<PyReadOptions> read_options) {
    py::gil_scoped_release release;
    std::lock_guard<std::shared_mutex> arrow_lock(arrow_mutex_);
    check_db_open();
    require_cf(cf_handle);

    const auto& opts = (read_options ? read_options : default_read_options_)->native_options();
    rocksdb::Iterator* raw_iter = db_->NewIterator(opts, cf_handle.cf_handle_);
    {
        std::lock_guard<std::mutex> lock(active_iterators_mutex_);
        active_rocksdb_iterators_.insert(raw_iter);
    }
    return std::make_shared<PyRocksDBIterator>(raw_iter, shared_from_this());
}

std::shared_ptr<PyArrowScan> PyRocksDBExtended::scan_arrow_cf(PyColumnFamilyHandle& cf_handle, py::object prefix, py::object start,
                                                              py::object stop, size_t batch_size, size_t max_batch_bytes,
                                                              std::shared_ptr<PyReadOptions> read_options,
                                                              std::shared_ptr<PySnapshot> snapshot, bool allow_overflow, bool collect_metrics) {
    std::shared_ptr<PyColumnFamilyHandle> cf;
    {
        std::lock_guard<std::shared_mutex> arrow_lock(arrow_mutex_);
        cf = require_cf(cf_handle);
    }
    return scan_arrow_impl(shared_from_this(), cf, std::move(prefix), std::move(start), std::move(stop), batch_size,
                           max_batch_bytes, read_options, snapshot, allow_overflow, collect_metrics);
}
