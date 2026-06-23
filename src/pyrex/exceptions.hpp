#pragma once

#include <stdexcept>
#include <string>

class RocksDBException : public std::runtime_error {
public:
    explicit RocksDBException(const std::string& msg) : std::runtime_error(msg) {}
};
