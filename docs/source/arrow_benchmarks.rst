Isolated Arrow Benchmarks (ABR-02)
==================================

The original quick comparison remains available::

    python benchmarks/bench_arrow_scan.py --rows 100000 --repeat 3

Use its new ``--isolated`` mode for reproducible per-mode measurements::

    python benchmarks/bench_arrow_scan.py --isolated \
      --rows 1000 --repeat 2 --batch-size 128 --include-polars \
      --output results/smoke.json

The same isolated runner is also executable as
``python benchmarks/bench_arrow_isolated.py``. PyArrow is an optional benchmark
dependency, not a new core PyRex dependency. Polars modes are opt-in and
reported explicitly as skipped if Polars is unavailable. Invalid inputs fail
before datasets are created. Worker timeouts, native errors, digest failures
or malformed output produce a nonzero exit and a failed JSON report with the
failed job's configuration; they are not converted to empty successful scans.

Isolation and datasets
----------------------

Each dataset is prepared once in a separate process, using a bounded native
WriteBatch at a time. Each mode, repeat and scan scope then runs in a **new
read-only process**, sequentially. Dataset creation does not inflate the read
worker's RSS high-water mark. Workers are not pooled or shared across modes.
Builds/tests should not run concurrently with a measured experiment.

The deterministic generator creates exactly the requested number of unique,
bytewise-sorted records. Ten percent of keys use ``other/`` and the remainder
use ``sensor/``; very small datasets can contain no decoy keys. Values are
seeded pseudo-random bytes, either fixed-length or uniformly sized from one
to ``--value-size`` bytes. Seed, generator parameters, reference digests,
selected counts, raw samples and repeat distributions are recorded.

Scopes include a full scan, a selective prefix (``sensor/`` by default) and
an inclusive-start/exclusive-stop middle range in the sensor keys. Bounds
are recorded as hexadecimal bytes. Empty selections retain their schema
and are validated normally. ``--prefix X`` alone selects a custom prefix
profile; ``--workloads`` explicitly selects any combination of scopes.

``--work-dir`` is a parent for **runner-owned temporary directories**, not an
existing database to replace. Datasets are removed on completion or worker
failure; unrelated files in that parent are preserved. Preparation refuses
to open an existing dataset path. Reference data can be regenerated from
the manifest, but physical SST/compaction layout is not promised identical.

Modes and ordered parity
------------------------

.. list-table:: Available modes
   :header-rows: 1
   :widths: 34 66

   * - Mode
     - Operation
   * - ``python_iter_collect``
     - Existing iterator collecting Python key/value records.
   * - ``python_iter_then_arrow``
     - Existing iterator collecting bytes, then constructing Arrow columns.
   * - ``native_materialize``
     - Native Arrow scan fully materialized by PyArrow.
   * - ``native_stream_compute``
     - Per-batch Arrow binary-length sum; release each batch before the next.
   * - ``polars_materialize``
     - Native scan materialized by Polars without requested rechunking.
   * - ``polars_stream_compute``
     - Per-batch Polars binary-length sum using a uint64 accumulator.

The first four modes are the default. ``--include-polars`` adds both optional
Polars modes; ``--modes`` selects an explicit subset. Mode order is shuffled
deterministically for every repeat; execution order is preserved in the JSON
sample list. This is a **raw KV** workload, not decoded graph facts, temporal
visibility validation, joins or reasoning.

Every worker validates actual ordered output against references generated
during preparation. For Arrow, validation hashes native key-length, key-data,
value-length and value-data streams without per-row Python bytes conversion.
The versioned digest is independent of batch boundaries, supports sliced
arrays and binary/large-binary interoperability, and includes record lengths
to disambiguate concatenated payloads. The manifest records length endianness;
this digest is not the quick benchmark's older row-framed digest format.
Null binary records or an unexpected schema are rejected.

Streaming validation hashes the **actual consumed batch**, not a second scan
that might conceal an error in the timed path. Compute aggregates, row counts,
key/value byte counts and the ordered digest must all match.

Timings and validation pauses
-----------------------------

Each sample records nanoseconds for:

* DB open and the optional warmup pass;
* scan/iterator establishment and Arrow schema/reader setup;
* extraction/materialization or per-batch consumer compute;
* ordered validation;
* complete operation wall time, including validation;
* consumer release, including an explicit GC pass;
* native scan/DB closure (already exhausted scans may have little left to free).

For materialization, extraction is one continuous interval; validation follows
it. For streaming, extraction is a **segmented** clock: each native pull and
compute is timed, then paused for ordered buffer hashing. Batches are released
before another is requested. ``operation_wall`` includes those pauses and
additional harness bookkeeping; ``extraction`` does not. Samples and summaries
explicitly mark this distinction.

Do not advertise segmented extraction as uninterrupted streaming throughput,
or compare one mode's validation-inclusive wall time with another mode's
extraction-only time. Native ABR-01 clocks are a further decomposition, not
the entire application wall time. Repeat reports include every sample,
median, min/max, mean, standard deviation and nearest-rank p95; small sample
sets are not statistical certification. No timing thresholds are asserted in CI.

Memory and output ownership
---------------------------

RSS is reported per worker, not as differences between modes in one reused
process. The report includes baseline current RSS where ``/proc`` is available,
baseline high-water RSS, high-water marks after the operation/validation and
the worker peak through cleanup. Peak sources and unavailable measurements
are explicit. Worker peaks include interpreter/library imports, DB open,
warmup, validation and consumer output; they exclude dataset preparation and
final report serialization. A high-water increase over baseline is **not**
an allocation count or net memory attributed solely to a mode.

ABR-01 counters are enabled by default for native modes. Snapshots are taken
after the operation, after consumer release and after native/DB close. They
distinguish logical output bytes, requested exported buffer capacity/slack,
current/peak retained capacity, logical staging payload and native gather/build
times. Python iterator modes have no native scan stats.

A materialized table may retain all native output; streaming compute releases
its input batches. Polars/PyArrow may also allocate or copy independent buffers:
those allocations contribute to RSS but are not counted as retained PyRex
exported buffers. Never treat ``max_batch_bytes`` or exported capacity as a
whole-process bound. Metrics do not measure allocator metadata, all staging
capacity, Python allocation counts, physical RocksDB IO bytes or cache history.

``--no-metrics`` enables a native instrumentation ablation and allows the
isolated runner to probe historical versions without ABR-01. Native stats are
then null, not fabricated estimates. The old quick benchmark is unchanged by
default and continues to work on historical versions.

Cache and native build provenance
---------------------------------

* ``--cache warm`` performs a streaming iterator prepass over the selected
  scope in **each** read worker, using the configured read options. It does
  not promise that an entire large dataset fits the RocksDB/OS cache.
* ``--cache reopened`` omits that prepass. The OS cache can still be warm
  from preparation or earlier processes. This is **not a cold-cache mode**.
* No global cache dropping, privileged operation or unrelated file deletion
  is performed. Unsupported ``--cache cold`` is rejected.

``--cpu`` pins each read worker when supported. ``--threads`` sets Arrow CPU/IO
and Polars thread controls before the worker runs. The report records effective
counts/environment; RocksDB background defaults are not changed. Dataset
preparation is not an affinity-controlled read measurement.

The manifest records actual extension/module paths, Python/platform, CPU model
and affinity, Arrow/Polars versions, read controls, scan limits and seed.
Installed PyRex package metadata may not describe a source build selected by
``PYTHONPATH``. Native compiler/flags and RocksDB archive provenance cannot be
reliably inferred from that metadata: supply a descriptive ``--build-label``
and retain build logs/checksums. Comparisons across revisions must normalize
those native dependencies independently.

Scale execution plan
--------------------

Start with a smoke profile, then increase scales deliberately::

    python benchmarks/bench_arrow_scan.py --isolated \
      --scales 10000 100000 1000000 --value-shapes fixed variable \
      --workloads full prefix range --include-polars \
      --repeat 5 --batch-size 4096 --value-size 64 \
      --cpu 2 --threads 1 --cache warm --timeout 1800 \
      --build-label "compiler/flags; RocksDB archive checksum; binding revision" \
      --work-dir /path/with/enough/disk --output results/scales.json

Ten million rows is explicitly opt-in, never part of default/CI smoke runs::

    python benchmarks/bench_arrow_scan.py --isolated \
      --scales 10000000 --value-shapes fixed variable --repeat 5 \
      --workloads full prefix range --include-polars --timeout 3600 \
      --work-dir /large/scratch --output results/ten-million.json

Check disk space and memory first. Python record collection and full Arrow/
Polars materialization intentionally scale with the full result; they can
exhaust memory on oversized profiles. A single Arrow binary column also has
int32 offset limits. Streaming is a different workload, not an excuse to
silently drop a failed materialization mode or compare unmatched selections.

Use the same command/build settings in alternating process/revision order
when evaluating ABR-07 optimizations. Retain raw JSON alongside any summary;
separate setup, extraction, validation, cleanup, native phase timings and
memory conclusions. See :doc:`arrow_reads_roadmap` for the delivery dependencies.
The repository's ``benchmarks/arrow_isolated_results.md`` records an executed
10K/100K matrix and a 1M fixed/full validation profile, with distribution and
ownership examples. It is not a release regression gate or a 10M result.
