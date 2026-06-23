#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <pybind11/pybind11.h>

namespace py = pybind11;

class ByteColumn {
public:
    enum class OffsetWidth { Fixed, Int32, Int64 };

    py::object owner_;
    py::object offsets_owner_;
    py::object data_owner_;
    std::vector<std::string> fallback_values_;
    const char* data_ = nullptr;
    const int32_t* offsets32_ = nullptr;
    const int64_t* offsets64_ = nullptr;
    size_t length_ = 0;
    OffsetWidth offset_width_ = OffsetWidth::Fixed;

    std::string_view value(size_t index) const;
};

ByteColumn extract_byte_column(const py::object& input, const char* name);
