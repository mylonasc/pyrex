#include "options.hpp"

#include "rocksdb/filter_policy.h"
#include "rocksdb/table.h"

bool PyReadOptions::get_fill_cache() const { return options_.fill_cache; }
void PyReadOptions::set_fill_cache(bool value) { options_.fill_cache = value; }
bool PyReadOptions::get_verify_checksums() const { return options_.verify_checksums; }
void PyReadOptions::set_verify_checksums(bool value) { options_.verify_checksums = value; }

bool PyWriteOptions::get_sync() const { return options_.sync; }
void PyWriteOptions::set_sync(bool value) { options_.sync = value; }
bool PyWriteOptions::get_disable_wal() const { return options_.disableWAL; }
void PyWriteOptions::set_disable_wal(bool value) { options_.disableWAL = value; }

PyOptions::PyOptions() {
    options_.compression = rocksdb::kSnappyCompression;
    cf_options_.compression = rocksdb::kSnappyCompression;
}

bool PyOptions::get_create_if_missing() const { return options_.create_if_missing; }
void PyOptions::set_create_if_missing(bool value) { options_.create_if_missing = value; }
bool PyOptions::get_error_if_exists() const { return options_.error_if_exists; }
void PyOptions::set_error_if_exists(bool value) { options_.error_if_exists = value; }
int PyOptions::get_max_open_files() const { return options_.max_open_files; }
void PyOptions::set_max_open_files(int value) { options_.max_open_files = value; }
size_t PyOptions::get_write_buffer_size() const { return options_.write_buffer_size; }
void PyOptions::set_write_buffer_size(size_t value) { options_.write_buffer_size = value; }
rocksdb::CompressionType PyOptions::get_compression() const { return options_.compression; }
void PyOptions::set_compression(rocksdb::CompressionType value) { options_.compression = value; }
int PyOptions::get_max_background_jobs() const { return options_.max_background_jobs; }
void PyOptions::set_max_background_jobs(int value) { options_.max_background_jobs = value; }
void PyOptions::increase_parallelism(int total_threads) { options_.IncreaseParallelism(total_threads); }
void PyOptions::optimize_for_small_db() { options_.OptimizeForSmallDb(); }
void PyOptions::use_block_based_bloom_filter(double bits_per_key) {
    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bits_per_key));
    options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    cf_options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
}
size_t PyOptions::get_cf_write_buffer_size() const { return cf_options_.write_buffer_size; }
void PyOptions::set_cf_write_buffer_size(size_t value) { cf_options_.write_buffer_size = value; }
rocksdb::CompressionType PyOptions::get_cf_compression() const { return cf_options_.compression; }
void PyOptions::set_cf_compression(rocksdb::CompressionType value) { cf_options_.compression = value; }
