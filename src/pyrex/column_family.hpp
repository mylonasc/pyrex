#pragma once

#include <string>

#include "rocksdb/db.h"

class PyColumnFamilyHandle {
public:
    rocksdb::ColumnFamilyHandle* cf_handle_;
    std::string name_;

    PyColumnFamilyHandle(rocksdb::ColumnFamilyHandle* handle, const std::string& name);
    const std::string& get_name() const;
    bool is_valid() const;
};
