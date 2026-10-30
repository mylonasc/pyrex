// Linux/glibc-only test interposer. No test hooks enter the shipped module.
#include <cstdlib>
#include "arrow_c.h"

extern "C" void* __libc_malloc(size_t);
extern "C" void* __libc_calloc(size_t, size_t);
static thread_local int countdown = -1;
static thread_local size_t failed_calloc_size = 0;
static bool fail_now() {
    if (countdown < 1) return false;
    return --countdown == 0;
}
extern "C" void* malloc(size_t n) { return fail_now() ? nullptr : __libc_malloc(n); }
extern "C" void* calloc(size_t n, size_t size) {
    if (failed_calloc_size && n * size == failed_calloc_size) {
        failed_calloc_size = 0;
        return nullptr;
    }
    return fail_now() ? nullptr : __libc_calloc(n, size);
}

extern "C" int fail_schema(ArrowArrayStream* stream, int allocation) {
    ArrowSchema out{};
    countdown = allocation;
    int status = stream->get_schema(stream, &out);
    countdown = -1;
    if (out.release) out.release(&out);
    return status;
}
extern "C" int fail_batch(ArrowArrayStream* stream, int allocation) {
    ArrowArray out{};
    countdown = allocation;
    int status = stream->get_next(stream, &out);
    countdown = -1;
    if (out.release) out.release(&out);
    return status;
}

extern "C" int fail_point_bitmap(ArrowArrayStream* stream) {
    ArrowArray out{};
    failed_calloc_size = 1250;  // 10000-row value validity bitmap
    int status = stream->get_next(stream, &out);
    failed_calloc_size = 0;
    if (out.release) out.release(&out);
    return status;
}
