#include "column_family.hpp"

#include "exceptions.hpp"

PyColumnFamilyHandle::PyColumnFamilyHandle(rocksdb::ColumnFamilyHandle* handle, const std::string& name)
    : cf_handle_(handle), name_(name) {
    if (!cf_handle_) {
        throw RocksDBException("Invalid ColumnFamilyHandle received.");
    }
}

const std::string& PyColumnFamilyHandle::get_name() const { return name_; }
bool PyColumnFamilyHandle::is_valid() const { return cf_handle_ != nullptr; }
