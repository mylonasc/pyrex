// Test-only Linux/glibc allocation latch: pause a real get_next while holding
// the gather mutex, or later while allocating Arrow output. No hooks enter
// the shipped extension, and no latency thresholds are needed in CI.
#include <atomic>
#include <cstdlib>
#include <thread>
#include "arrow_c.h"

extern "C" void* __libc_malloc(size_t);
static thread_local int remaining = -1;
static thread_local size_t target_size = 0;
static std::atomic<bool> paused{false};
static std::atomic<bool> resume_requested{false};

extern "C" void* malloc(size_t size) {
    bool stop = (remaining > 0 && --remaining == 0) || (target_size && size == target_size);
    if (stop) {
        remaining = -1;
        target_size = 0;
        paused.store(true, std::memory_order_release);
        while (!resume_requested.load(std::memory_order_acquire)) std::this_thread::yield();
    }
    return __libc_malloc(size);
}

extern "C" int paused_get_next(ArrowArrayStream* stream, int allocation,
                                size_t allocation_size, ArrowArray* out) {
    paused.store(false, std::memory_order_release);
    resume_requested.store(false, std::memory_order_release);
    remaining = allocation;
    target_size = allocation_size;
    const int status = stream->get_next(stream, out);
    remaining = -1;
    target_size = 0;
    return status;
}

extern "C" bool allocation_paused() { return paused.load(std::memory_order_acquire); }
extern "C" void resume_allocation() { resume_requested.store(true, std::memory_order_release); }
