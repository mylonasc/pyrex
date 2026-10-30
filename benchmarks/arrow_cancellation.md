# ABR-03: cancellation contract and validation

`ArrowScan.cancel()` is an atomic request, independent of the gather/lifecycle/
metrics mutexes. It returns `True` once when accepted; repeated requests or
requests after terminal close/error/clean EOF return `False`. The separate
read-only `cancel_requested` flag distinguishes an earlier accepted request
from a request rejected after completion.

## Native behavior

- Gather, output sizing and buffer-copy loops check every **256 records**,
  plus batch/EOF/publication boundaries. A blocking RocksDB call, allocator
  call or single large memcpy is not forcibly interrupted.
- On observation, the raw Arrow C Stream returns `errno.ECANCELED` and stable
  `scan_arrow: cancelled` text, not successful empty output. Repeated pulls
  remain errors. Python/other consumers map the error to their own types.
- An unpublished batch is owned by a temporary Arrow release guard. A
  request observed before the final publication decision discards that
  entire batch without incrementing emitted-row/capacity counters.
- If publication wins a race, an already committed full batch may return
  before the following callback reports cancellation. An unrelated native
  failure can also win before a checkpoint; it remains an error, not EOF.
- EOF and request admission use one atomic state. An accepted request cannot
  subsequently become clean EOF; EOF committed first rejects a late request.
- `close()` remains synchronous/idempotent. An idle requested scan can still
  pin resources until another pull observes cancellation, or until explicit
  close/stream release/DB teardown. Cancellation does not invalidate other
  streams sharing a DB snapshot or buffers already exported to a consumer.

The request path itself takes no native scan lock and queries no clock. From
Python, entering that path still depends on GIL/scheduler availability; the
consumer must permit the other thread to run (PyArrow/CDLL do). There is no
universal wall-clock cancellation deadline.

Opt-in stats add `rows_gathered` (including discarded staging) and
`cancelled_calls`. Live gathered progress is published once per 256-row
window and on attempt exit. It is not necessarily the number of rows already
delivered to the consumer. Used/capacity/retention counters still describe
successfully committed output only: zero emitted capacity does **not** mean
an interrupted build never allocated temporary buffers.

## Controlled in-flight test: no production hooks

Linux/glibc tests preload a small **test-only** allocation latch. It pauses a
real `get_next` at a chosen gather allocation, while the scan mutex is held,
or at a later Arrow output allocation. The control thread calls `cancel()`
and only then resumes the paused allocation. Waiting for the gather mutex in
`cancel()` would deadlock this protocol and fail the bounded child process.
The shipped module has no test hooks, special keys or environment switches.

The callback then returns ECANCELED with no successful/partial batch. Tests
verify no emitted bytes/rows, zero outstanding exported capacity and cleanup
through the existing ownership path. Six illustrative local observations:

| Paused phase | DB | Request call µs | Completion after test resume µs | Gathered progress before → after |
|---|---|---:|---:|---:|
| Gather | Plain | 0.826 | 83.190 | 256 → 512 |
| Gather | Extended | 0.849 | 75.661 | 256 → 512 |
| Gather | TransactionDB | 0.857 | 77.243 | 256 → 512 |
| Build | Plain | 0.800 | 379.792 | 10000 → 10000 |
| Build | Extended | 0.929 | 287.220 | 10000 → 10000 |
| Build | TransactionDB | 0.847 | 384.743 | 10000 → 10000 |

These are **controlled examples**, not latency distributions or production
p99s. The request timer starts after the Python control thread has the GIL;
the completion timer starts after the artificial allocation is resumed and
includes resource/vector cleanup and future handoff. The recorded progress
is coarse; it advances by at most one polling window in the paused-gather
test. CI asserts protocol/ownership/counts, never microsecond thresholds.

Other tests cover before export, between batches, after the last data batch
but before EOF, after EOF/close/native failure, raw diagnostics, consumer
exceptions, shared snapshots/CFs and races with EOF/DB close/CF drop. Returned
buffers remain readable after cancellation and DB teardown.

Reproduce the controlled tests and JSON records:

```sh
PYTHONPATH=/path/to/ABR-03/module python -m pytest \
  tests/test_arrow_scan_cancellation.py -q \
  --basetemp /path/to/runner-owned-test-output
```

The native latch tests require Linux/glibc and a compiler; they are skipped
under ASan, where allocator interposition conflicts. Generic ownership/race
tests run separately without that latch. Whole-process timeouts detect hangs.

Validation checkpoint: the full normal suite passed **241 tests, 2 skipped**
(plain-DB CF cases). With the extension compiled using GCC AddressSanitizer
(`-O1 -fsanitize=address -fno-omit-frame-pointer`), the full suite passed
**234 tests, 9 skipped**, with no ASan errors. The additional skips are the
six allocation-latch cases and the allocator-failure interposition test.
ASan and libstdc++ were preloaded; `ASAN_OPTIONS=detect_leaks=0:halt_on_error=1`.
The linked RocksDB archive was **not instrumented**, and leak detection was
disabled: this is extension memory-safety coverage, not full-stack or leak
certification. Sphinx HTML also builds with warnings treated as errors.

## No-request polling-path overhead

`bench_arrow_cancel_overhead.py` is runnable against both ABR-01 and ABR-03
artifacts. It issues **no cancellation requests**, runs the same native scan /
Arrow value-length computation, verifies row/value totals and reports every
repeat. Use ABR-02 for stronger ordered/resource experiments; this microprobe
is not a replacement for ordered content validation or a process-RSS test.

Local setup: CPython 3.12.3, PyArrow 26.0.0, i7-13700F, CPU 2 pinned, Arrow
CPU/IO controls set to one thread. Both native artifacts use GCC 13.3 `-O2`
and the same portable RocksDB 10.7.5 archive. The dataset has 100K prepared
19-byte keys and 64-byte values. Each process warms each configuration, then
runs seven samples. Three fresh processes/build were interleaved, reversing
build order in the middle round.

Milliseconds per full scan/compute: median of process medians; parentheses
show their min–max, not confidence intervals.

| Batch rows | Metrics | ABR-01 | ABR-03 |
|---:|---|---:|---:|
| 256 | Off | 17.035 (16.964–17.050) | 16.885 (16.850–17.097) |
| 256 | On | 17.158 (17.095–17.188) | 16.920 (16.902–17.003) |
| 4096 | Off | 13.761 (13.756–13.806) | 13.537 (13.533–13.541) |
| 4096 | On | 13.740 (13.691–13.769) | 13.531 (13.460–13.571) |
| 65536 | Off | 15.494 (15.485–15.575) | 15.233 (15.165–15.295) |
| 65536 | On | 17.045 (16.954–17.076) | 15.568 (15.544–15.578) |

No slowdown was observed in this small warm probe. This is the **net code
change**, not an isolated cost measurement of one atomic instruction:
cancellation-aware sizing also replaces redundant metadata/accounting passes.
Compiler layout, allocator state and measurement variation still matter.
Do not infer universally free polling, cold-cache performance or concurrent
throughput from these results. Real cancellation has separate cleanup costs.

```sh
PYTHONPATH=/path/to/native-artifact python benchmarks/bench_arrow_cancel_overhead.py \
  --label ABR-03 --rows 100000 --repeat 7 --cpu 2 \
  --batch-sizes 256 4096 65536 --work-dir /large/scratch > abr03.json
```

Alternate ABR-01/ABR-03 build order and retain source/toolchain provenance.
The native stats in each probe record belong to the **last repeat**, not the
median timing sample. Raw local data: `/tmp/opencode/abr-03/overhead-final/`
and `/tmp/opencode/abr-03/pytest-final/` (not public downloads).
