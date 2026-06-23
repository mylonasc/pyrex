#include "columnar_batch.hpp"

#include <cstring>
#include <stdexcept>

namespace {

bool is_bytes_sequence(const py::object& input) {
    return py::isinstance<py::list>(input) || py::isinstance<py::tuple>(input);
}

ByteColumn extract_sequence(const py::object& input, const char* name) {
    ByteColumn column;
    column.owner_ = input;
    column.length_ = py::len(input);
    column.fallback_values_.reserve(column.length_);

    py::sequence seq = py::reinterpret_borrow<py::sequence>(input);
    for (size_t i = 0; i < column.length_; ++i) {
        py::handle item = seq[i];
        if (!py::isinstance<py::bytes>(item)) {
            throw py::type_error(std::string(name) + " item at index " + std::to_string(i) + " is not bytes");
        }
        column.fallback_values_.push_back(static_cast<std::string>(py::reinterpret_borrow<py::bytes>(item)));
    }

    return column;
}

ByteColumn::OffsetWidth arrow_offset_width(const std::string& type_name, const char* name) {
    if (type_name == "binary" || type_name == "string") {
        return ByteColumn::OffsetWidth::Int32;
    }
    if (type_name == "large_binary" || type_name == "large_string") {
        return ByteColumn::OffsetWidth::Int64;
    }
    throw py::value_error(std::string("unsupported ") + name + " Arrow type: " + type_name);
}

ByteColumn extract_arrow_array(const py::object& input, const char* name) {
    ByteColumn column;
    column.owner_ = input;
    column.length_ = py::len(input);

    py::object null_count_obj = input.attr("null_count");
    auto null_count = null_count_obj.cast<int64_t>();
    if (null_count != 0) {
        throw py::value_error(std::string(name) + " contains null values");
    }

    std::string type_name = py::str(input.attr("type"));
    column.offset_width_ = arrow_offset_width(type_name, name);

    // Arrow/Polars callers already keep variable-width bytes in contiguous buffers;
    // reading offsets here avoids per-row Python bytes materialization before RocksDB copies into WriteBatch.
    py::tuple buffers = input.attr("buffers")().cast<py::tuple>();
    if (buffers.size() < 3 || buffers[1].is_none() || buffers[2].is_none()) {
        throw py::value_error(std::string(name) + " Arrow array does not expose offsets and data buffers");
    }

    column.offsets_owner_ = py::reinterpret_borrow<py::object>(buffers[1]);
    column.data_owner_ = py::reinterpret_borrow<py::object>(buffers[2]);

    py::buffer offsets_buffer(column.offsets_owner_);
    py::buffer data_buffer(column.data_owner_);
    py::buffer_info offsets_info = offsets_buffer.request();
    py::buffer_info data_info = data_buffer.request();

    column.data_ = static_cast<const char*>(data_info.ptr);
    if (column.offset_width_ == ByteColumn::OffsetWidth::Int32) {
        if (offsets_info.size * offsets_info.itemsize < static_cast<ssize_t>((column.length_ + 1) * sizeof(int32_t))) {
            throw py::value_error(std::string(name) + " Arrow offsets buffer is too small");
        }
        column.offsets32_ = static_cast<const int32_t*>(offsets_info.ptr);
    } else {
        if (offsets_info.size * offsets_info.itemsize < static_cast<ssize_t>((column.length_ + 1) * sizeof(int64_t))) {
            throw py::value_error(std::string(name) + " Arrow offsets buffer is too small");
        }
        column.offsets64_ = static_cast<const int64_t*>(offsets_info.ptr);
    }

    return column;
}

}  // namespace

std::string_view ByteColumn::value(size_t index) const {
    if (!fallback_values_.empty() || offset_width_ == OffsetWidth::Fixed) {
        return fallback_values_[index];
    }

    if (offset_width_ == OffsetWidth::Int32) {
        const int32_t start = offsets32_[index];
        const int32_t end = offsets32_[index + 1];
        return std::string_view(data_ + start, static_cast<size_t>(end - start));
    }

    const int64_t start = offsets64_[index];
    const int64_t end = offsets64_[index + 1];
    return std::string_view(data_ + start, static_cast<size_t>(end - start));
}

ByteColumn extract_byte_column(const py::object& input, const char* name) {
    if (is_bytes_sequence(input)) {
        return extract_sequence(input, name);
    }

    if (py::hasattr(input, "buffers") && py::hasattr(input, "type") && py::hasattr(input, "null_count")) {
        return extract_arrow_array(input, name);
    }

    throw py::type_error(std::string(name) + " must be an Arrow binary/string array or a sequence of bytes");
}
