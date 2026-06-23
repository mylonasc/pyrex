#pragma once

#include "rocksdb/options.h"

class PyReadOptions {
public:
    rocksdb::ReadOptions options_;

    PyReadOptions() = default;

    bool get_fill_cache() const;
    void set_fill_cache(bool value);
    bool get_verify_checksums() const;
    void set_verify_checksums(bool value);
};

class PyWriteOptions {
public:
    rocksdb::WriteOptions options_;

    PyWriteOptions() = default;

    bool get_sync() const;
    void set_sync(bool value);
    bool get_disable_wal() const;
    void set_disable_wal(bool value);
};

class PyOptions {
public:
    rocksdb::Options options_;
    rocksdb::ColumnFamilyOptions cf_options_;

    PyOptions();

    bool get_create_if_missing() const;
    void set_create_if_missing(bool value);
    bool get_error_if_exists() const;
    void set_error_if_exists(bool value);
    int get_max_open_files() const;
    void set_max_open_files(int value);
    size_t get_write_buffer_size() const;
    void set_write_buffer_size(size_t value);
    rocksdb::CompressionType get_compression() const;
    void set_compression(rocksdb::CompressionType value);
    int get_max_background_jobs() const;
    void set_max_background_jobs(int value);
    void increase_parallelism(int total_threads);
    void optimize_for_small_db();
    void use_block_based_bloom_filter(double bits_per_key = 10.0);
    size_t get_cf_write_buffer_size() const;
    void set_cf_write_buffer_size(size_t value);
    rocksdb::CompressionType get_cf_compression() const;
    void set_cf_compression(rocksdb::CompressionType value);
};
