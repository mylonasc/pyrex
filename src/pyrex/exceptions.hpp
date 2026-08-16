#pragma once

#include <stdexcept>
#include <string>

#include "rocksdb/status.h"

class RocksDBException : public std::runtime_error {
public:
    explicit RocksDBException(const std::string& msg) : std::runtime_error(msg) {}
};

class RocksDBConflictError : public RocksDBException {
public:
    explicit RocksDBConflictError(const std::string& msg) : RocksDBException(msg) {}
};

class RocksDBTimeoutError : public RocksDBException {
public:
    explicit RocksDBTimeoutError(const std::string& msg) : RocksDBException(msg) {}
};

class RocksDBBusyError : public RocksDBException {
public:
    explicit RocksDBBusyError(const std::string& msg) : RocksDBException(msg) {}
};

class RocksDBCorruptionError : public RocksDBException {
public:
    explicit RocksDBCorruptionError(const std::string& msg) : RocksDBException(msg) {}
};

class RocksDBIOError : public RocksDBException {
public:
    explicit RocksDBIOError(const std::string& msg) : RocksDBException(msg) {}
};

class RocksDBInvalidArgumentError : public RocksDBException {
public:
    explicit RocksDBInvalidArgumentError(const std::string& msg) : RocksDBException(msg) {}
};

[[noreturn]] inline void throw_rocksdb_status(const rocksdb::Status& status, const std::string& context) {
    std::string msg = context + ": " + status.ToString();
    if (status.IsBusy()) throw RocksDBBusyError(msg);
    if (status.IsTimedOut()) throw RocksDBTimeoutError(msg);
    if (status.IsTryAgain()) throw RocksDBConflictError(msg);
    if (status.IsCorruption()) throw RocksDBCorruptionError(msg);
    if (status.IsIOError()) throw RocksDBIOError(msg);
    if (status.IsInvalidArgument()) throw RocksDBInvalidArgumentError(msg);
    throw RocksDBException(msg);
}
