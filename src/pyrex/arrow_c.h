// arrow_c.h
//
// Minimal vendored subset of the Apache Arrow C Data / C Stream interfaces
// (https://arrow.apache.org/docs/format/CDataInterface.html,
//  https://arrow.apache.org/docs/format/CStreamInterface.html).
//
// Only the struct layouts and release-callback contracts used by the native
// batched scan are reproduced here so the core stream does not depend on
// Arrow C++ / nanoarrow at build time. Layouts must stay ABI-compatible
// with the Arrow specification.

#pragma once

#include <cstdint>

#ifdef __cplusplus
extern "C" {
#endif

#define ARROW_C_DATA_VERSION 1
#define ARROW_C_STREAM_VERSION 1

struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema* schema);
    void* private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray* array);
    void* private_data;
};

struct ArrowArrayStream {
    int (*get_schema)(struct ArrowArrayStream* stream, struct ArrowSchema* out);
    int (*get_next)(struct ArrowArrayStream* stream, struct ArrowArray* out);
    const char* (*get_last_error)(struct ArrowArrayStream* stream);
    void (*release)(struct ArrowArrayStream* stream);
    void* private_data;
};

#ifdef __cplusplus
}
#endif
