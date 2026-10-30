Batched Arrow Scans
===================

.. currentmodule:: pyrex

Native, columnar reads complement ``write_columnar_batch()``: ``scan_arrow()``
iterates RocksDB in C++ and packs raw key/value bytes into Arrow
``binary`` columns. Python only sees one Arrow stream per scan instead of
one ``bytes`` object per record. Consume the stream with any Arrow C Stream
client (PyArrow, Polars, DataFusion) without per-row Python conversion.

Quick start::

    import pyarrow as pa
    import pyrex

    db = pyrex.PyRocksDB("/tmp/sensors")
    reader = pa.RecordBatchReader.from_stream(db.scan_arrow(prefix=b"sensor/"))
    table = reader.read_all()          # struct<key: binary, value: binary>

A scan is lazy and bounded-memory: each ``get_next`` gathers at most
``batch_size`` rows or ``max_batch_bytes`` payload bytes into freshly owned
Arrow buffers. Already exported batches stay valid after iterator
advancement, stream release, and even database closure.

Scan bounds
-----------

* ``prefix`` selects keys starting with the prefix. ``b""``/``None`` = all keys.
* ``start`` is inclusive, ``stop`` is exclusive; both are pushed down to
  RocksDB via ``iterate_upper_bound`` where possible.
* ``prefix`` and ``[start, stop)`` are intersected. ``start > stop`` raises
  ``ValueError``; equal bounds produce an empty scan that still exposes the
  schema.
* Keys are raw bytes (empty keys, embedded nulls, and ``0xff`` bytes are all
  valid). The MVP assumes RocksDB's default bytewise comparator and rejects
  custom comparators explicitly instead of mis-scanning them.

Results follow comparator order with no missing/duplicate records across
batch boundaries. Iterator errors propagate (through the Arrow consumer as
an error carrying the native message) and are never masked as clean
end-of-stream.

Batch sizing
------------

* ``batch_size`` (default 65536): maximum rows per batch.
* ``max_batch_bytes`` (default 8 MiB): maximum key+value payload plus offset
  overhead per batch. A single record larger than the cap raises a clear
  error; pass ``allow_overflow=True`` to emit it alone instead.
* The schema uses int32 offsets for the whole stream. Records that would
  overflow int32 offsets fail explicitly; the schema never changes mid-stream.

These caps bound producer batch memory, not whole-process RSS: the block
cache, pinned snapshot history, and consumer-retained batches are additional.

Native metrics (ABR-01)
-----------------------

Pass ``collect_metrics=True`` to ``scan_arrow`` / ``scan_arrow_cf`` on plain,
extended or transactional DBs to enable native counters and phase timings::

    scan = db.scan_arrow(batch_size=4096, collect_metrics=True)
    reader = pa.RecordBatchReader.from_stream(scan)
    table = reader.read_all()
    reader.close()
    print(scan.stats)    # output buffers are still retained by table
    del table
    # A fresh scan.stats snapshot reflects buffers released by the consumer.

``ArrowScan.stats`` is a read-only property returning a fresh dictionary
snapshot (mutating that dictionary does not change the counters). Without
opt-in it returns ``None``, including after close. Disabled scans allocate
no metrics state and query no phase clocks. No new Python objects are
created per scanned row for metrics.

.. list-table:: Stats fields (integer counts, bytes or nanoseconds)
   :header-rows: 1
   :widths: 34 66

   * - Field
     - Meaning
   * - ``get_next_calls``
     - Native C Stream get_next calls with a valid output pointer, including
       clean exhaustion and failures; not Python calls or RocksDB IO count.
   * - ``rows_emitted``, ``batches_emitted``
     - Successfully constructed/published output only; no partial batch on
       allocation, size-limit, lifecycle or iterator errors.
   * - ``rows_gathered``
     - Complete key/value records copied to staging, including discarded or
       failed batches. Live progress is published every 256 rows and at the
       end of an attempt; it can lag by one 256-row polling window.
   * - ``cancelled_calls``
     - get_next calls entering the ECANCELED path, including repeat attempts
       after cancellation. Not the number of cancel requests; cleanup can
       still be in progress when the counter becomes visible.
   * - ``key_bytes_emitted``, ``value_bytes_emitted``
     - Raw KV payload bytes in successful output, not compressed storage IO.
   * - ``offset_bytes_emitted``
     - Both int32 offset buffers: ``8 * (rows + 1)`` for each emitted batch.
   * - ``output_bytes_emitted``
     - Key bytes + value bytes + offset bytes, cumulatively.
   * - ``buffer_capacity_bytes_emitted``
     - Requested data/offset allocation sizes for successful output. A zero
       payload column still allocates one dummy byte. Excludes C metadata,
       pointer arrays, allocator rounding/overhead and RocksDB resources.
   * - ``allocation_slack_bytes_emitted``
     - Cumulative requested capacity minus logical output bytes; with the
       current exact-size builder, this is the empty-column dummy bytes.
   * - ``live_buffer_bytes``, ``peak_live_buffer_bytes``
     - Outstanding and peak requested output capacity, following actual
       Arrow child release callbacks, including consumer-retained output.
   * - ``peak_staged_payload_bytes``
     - Largest logical key+value payload held by one gather attempt, including
       failed attempts. Not string/vector allocated capacity or staging RSS.
   * - ``gather_time_ns``
     - Cumulative native gather/iterator work under the scan mutex, including
       failed/end attempts; excludes waiting to acquire that mutex.
   * - ``build_time_ns``
     - Cumulative native output sizing/allocation/copy/descriptor/publication
       phases, including failed builds. Does not include consumer compute.

Counters start at zero before the first batch and remain readable after
exhaustion, early close, errors and DB closure. Metrics snapshots are taken
under a separate native mutex and never touch a closed DB. Active phase
times are added when that phase ends; an in-flight call may finish publishing
an already gathered owned batch while another thread closes the scan.

Exported child buffers share independent accounting state: closing a stream
or DB does **not** subtract buffers still owned by consumers. Moving/detaching
one Arrow child and releasing it later subtracts only that child's capacity.
If consumers retain multiple batches, the live high-water mark can exceed
``max_batch_bytes``. The cap remains a per-batch output-used-byte limit
(subject to explicit singleton overflow), not a producer/consumer RSS bound.
``output_bytes_emitted`` also need not equal PyArrow ``table.nbytes``: this
counter includes the initial offset slot for each column of every batch.

Phase clocks are diagnostic, not application wall time: they exclude scan
establishment, native resource teardown, Python boundary overhead, and payload
decoding/consumer operations. Snapshot history, RocksDB cache/storage IO,
staging allocation slack and allocator metadata are not measured by output
capacity counters. Broader resource experiments are tracked in ABR-02.

``benchmarks/bench_arrow_scan.py --collect-metrics`` prints these counters for
the last repeat of each native mode. Its best-of wall time may refer to a
different repeat; do not conflate the two. The default invocation remains
usable against historical versions without ``collect_metrics`` support.
See :doc:`arrow_reads_roadmap` for the indexed remaining features.

Snapshots
---------

Every scan pins a consistent RocksDB read view when it is established, so
concurrent writes cannot change what an open stream observes::

    snap = db.create_snapshot()
    a = db.scan_arrow(prefix=b"a/", snapshot=snap)
    b = db.scan_arrow(prefix=b"b/", snapshot=snap)
    snap.release()   # streams already hold the pinned view

Two streams sharing one snapshot observe the same horizon across intervening
writes; separately created snapshots may see later data. The snapshot must
belong to the same DB instance and must not be released while a scan that
uses it is being created. ``Snapshot.sequence_number`` exposes the pinned
read-view identity for integration checks. A scan created without an
explicit snapshot pins its own view at creation time.

Column families
---------------

``PyRocksDBExtended.scan_arrow_cf(cf_handle, ...)`` scans a named column
family with the same contract. A snapshot created on the DB may be shared
across column families so multi-relation consumers observe one horizon.
Dropping a column family with an active scan on it is rejected; close the
scan first.

TransactionDB
-------------

``TransactionDB.scan_arrow(...)`` exposes the same native scan contract on
transaction-capable databases. ``scan_arrow_cf(cf, ...)`` scans a named CF;
``create_snapshot()`` and ``new_iterator()`` are available as well. A scan
observes *committed* state at its pinned snapshot: writes from still-open
transactions are not visible and appear only after commit. A snapshot must
belong to the same database instance, so snapshots cannot be shared between
a ``TransactionDB`` and a plain ``PyRocksDB``. Transaction-local
read-your-writes stays on the ``Transaction`` handle (``txn.get``); merging
uncommitted writes into a scan remains a follow-up.

``TransactionDB`` opens all persisted column families on reopen and supports
the same CF management and direct CF reads/writes as ``PyRocksDBExtended``.
Transactions support ``put_cf``, ``get_cf`` and ``delete_cf``, and CF
put/delete operations in ``PyWriteBatch``. Dropping a CF is conservatively
rejected while *any* transaction is active, or while an Arrow scan uses it.
Finish transactions and close streams before dropping their families.

Snapshot-consistent materialization
-----------------------------------

On ``PyRocksDB``, ``PyRocksDBExtended`` and ``TransactionDB``, ``get`` and
``multi_get`` accept the same ``snapshot=`` object used by scans. CF variants
``get_cf`` and ``multi_get_cf`` do too. This closes the read-horizon gap in an
index-scan / point-materialization pipeline::

    snap = db.create_snapshot()
    try:
        with db.scan_arrow_cf(index_cf, prefix=b"p/", snapshot=snap) as scan:
            reader = pa.RecordBatchReader.from_stream(scan)
            for batch in reader:
                version_keys = batch.column("value").to_pylist()
                payloads = db.multi_get_cf(facts_cf, version_keys, snapshot=snap)
                # Decode/validate payloads in the application, not PyRex.
    finally:
        snap.release()

``multi_get`` / ``multi_get_cf`` call native RocksDB MultiGet, returning a
Python list in input order. Duplicate keys are preserved; missing keys are
``None`` and empty values are ``b""``. Empty inputs still validate the DB,
CF and snapshot. Inputs must contain at most 65536 byte keys per call;
results are materialized, not a streaming or byte-bounded Arrow API.
An operational error raises rather than returning a partial result list.
Foreign, released and DB-closed snapshots fail clearly.

Keep the snapshot alive through both scan and point reads. A RocksDB sequence
number identifies a storage view, not an authenticated application-level
temporal horizon. GestaltDB-like consumers still need their own visibility,
exact evidence identity, payload decoding and term-encoding checks.

Column-family integration opportunities
---------------------------------------

For a future CF-backed consumer, a single snapshot can cover fact records,
index locators, dictionaries and persisted derived generations. CF-specific
compression/compaction settings can isolate different access patterns while
transactions publish related CF writes atomically. These are architectural
capabilities, not measured reasoning speedups. Do not create a CF per predicate
without measuring its handle/memtable/compaction costs.

GestaltDB currently stores prefixed keys in the default family, so its current
layout needs ``scan_arrow`` plus snapshot-consistent point reads, not a CF
migration. Its ``NativeKeyScan`` adapter currently converts Arrow buffers to
Python byte rows; CF support alone does not provide decoded columnar facts or
zero-Python reasoning. Re-audit its capability matrix and adapters before
enabling transactional/CF acceleration (GestaltDB issue #194).

Lifecycle and safety
--------------------

* Streams are single-consumer: ``__arrow_c_stream__`` may be called exactly
  once (PyCapsule name ``arrow_array_stream`` per the Arrow PyCapsule
  standard). The capsule destructor releases stream state exactly once.
* ``ArrowScan.close()`` (also a context manager) ends iteration early and
  releases the iterator and snapshot resources. Reading after close, or
  after database closure, fails instead of touching freed memory.
* Closing the database invalidates active scans safely: exported batches
  remain valid (owned buffers), subsequent reads fail clearly.
* ``ArrowScan.sequence_number`` reports the read view the scan observes.
* Releasing an Arrow consumer closes the native scan even if an ``ArrowScan``
  variable is retained. Explicit close and repeated release are idempotent.

Reading data back yields raw ``key``/``value`` binary columns. Decoding
application payloads (JSON, MessagePack, term encodings, graph relations)
stays with the caller; predicate filtering and graph semantics do not belong
in PyRex.

Cooperative cancellation (ABR-03)
---------------------------------

``ArrowScan.cancel()`` makes an atomic request without acquiring the scan,
lifecycle or metrics mutex. ``cancel_requested`` is an atomic read-only flag.
This permits a control thread to request termination while a consumer is
inside a long native gather. The request method itself does not wait for
that gather to finish::

    scan = db.scan_arrow(batch_size=65536, collect_metrics=True)
    reader = pa.RecordBatchReader.from_stream(scan)
    # Another thread may already be consuming reader.
    accepted = scan.cancel()
    # The consumer receives a cancellation error at a native checkpoint.
    # Close the reader/scan in its finally block, especially if it stops pulling.

Admission and completion are explicit:

* True means this call accepted the first request. Repeating the request
  returns False; ``cancel_requested`` remains True after cleanup.
* After a clean EOF has been committed, after close, or after a terminal
  native error, the request returns False without relabeling that result.
  Merely returning the last data batch is not yet committing clean EOF.
* Accepted cancellation observed by get_next returns platform
  ``errno.ECANCELED`` with the stable diagnostic ``scan_arrow: cancelled``.
  Repeated pulls return cancellation errors, never an empty successful scan.
  Arrow consumers surface their own error types (PyArrow commonly raises
  ``OSError``); non-Python C Stream consumers have the same error channel.
* A complete batch whose final publication decision precedes a racing
  request may still return successfully. No partially built batch is returned;
  unpublished buffers are freed through their normal Arrow release path.
  A following get_next observes the accepted request instead of clean EOF.
  An independent native failure can also terminate a call before cancellation
  is observed; failures are never converted to successful exhaustion.

Native gathering, output sizing and buffer-copy loops check every **256
records**, plus batch/EOF/publication boundaries. There is no per-row Python
work or per-row metrics lock. A single blocking RocksDB call, allocator call
or large memcpy is not forcibly interrupted. End-to-end responsiveness is
also subject to scheduling and Python GIL availability: a consumer that
holds the GIL for its whole callback prevents another Python thread from
entering the request method. PyArrow and the raw-CDLL tests release it.

``cancel()`` is a request, **not** synchronous close. If no get_next is active,
the iterator/snapshot can remain pinned until a later pull observes the
request, or until ``close()`` / stream release / DB teardown. An active CF
therefore remains protected from drop after a request alone. Cleanup is
idempotent; cancellation of one stream does not cancel other streams sharing
its DB snapshot. Previously exported batches and detached Arrow children
remain valid after cancellation, stream release, CF drop or DB closure.

``close()`` retains its existing deterministic behavior and can wait for a
native operation. ``is_closed`` takes the scan mutex as before; use the
atomic request flag, not that property, on a latency-sensitive control path.
Do not change the stream's single-consumer contract by issuing concurrent
get_next calls from multiple consumers.

Opt-in ``stats`` adds gathered progress and cancelled-call counts, alongside
the existing used/capacity/retention and native-phase metrics. Emitted counters
include only successfully committed output, not a discarded gather/build.
Failed/cancelled build time can include its local failure cleanup; it is not
a universal cancellation latency or a whole-operation timer.

``benchmarks/bench_arrow_cancel_overhead.py`` reports the no-request polling
path with/without metrics, including every repeat. It is runnable against
ABR-01 and ABR-03 artifacts for same-toolchain comparisons. Controlled latency
tests pause an actual native allocation during gather/build and save JSON
with request duration, resumed completion time and gathered progress; these
are bounded subprocess tests with **no performance thresholds** or production
test hooks. See ``benchmarks/arrow_cancellation.md`` for measured results and
reproduction. They do not certify production/worst-case IO cancellation time.

Arrow point lookups (ABR-04)
----------------------------

``multi_get_arrow(keys, ...)`` complements list-returning ``multi_get`` on
plain, extended and committed-state TransactionDB handles.
``multi_get_arrow_cf(cf_handle, keys, ...)`` selects a validated named CF on
extended/TransactionDB handles. Both return an ``ArrowScan`` with the existing
single-export, cancellation, close, context-manager and metrics APIs::

    keys = pa.array([b"second", b"absent", b"first", b"second"], type=pa.binary())
    with db.multi_get_arrow(keys, batch_size=256, collect_metrics=True) as points:
        with pa.RecordBatchReader.from_stream(points) as reader:
            result = reader.read_all()

Output is fixed ``struct<key: binary not null, value: binary nullable>`` with
int32 offsets. Rows follow **input order**, including duplicates. Missing keys
produce a null value; a stored empty value produces valid ``b""``. Empty keys
are valid. Empty input returns this same schema with no native lookup calls.
No graph/application payload decoding is performed.

Input is a bytes list/tuple or a contiguous Arrow ``binary``/``large_binary``
array, including slices, with at most **65536 keys per stream**. Null input
keys and string/numeric/chunked arrays are rejected, not coerced. Native-owned
input copies are taken before returning; destroying or mutating the caller's
buffers afterward cannot change the stream. Arrow buffer offsets, lengths,
alignment and monotonicity are validated before dereferencing payloads.
No Python bytes object is created per output row or per Arrow input key.

The view is pinned at creation, not on first consumption. Explicit snapshots
must belong to the same DB and be unreleased. Releasing a caller's snapshot
handle prevents new uses but defers native snapshot destruction while existing
point streams need it. CF identity/drop protection matches scan streams.
TransactionDB reads committed state only, not an active transaction's writes.

Row/byte limits and singleton overflow have the same defaults/ranges as
``scan_arrow``. The output-byte cap includes both offset buffers, key/value
payloads and the value validity bitmap (one bit per row, rounded up). Dummy
data bytes for empty columns count toward requested capacity/slack, not used
bytes. Limits are **not an RSS bound**: all input keys are owned, and each
native MultiGet can stage up to 256 values before their sizes are known.
Prefetched values can span multiple output batches. ``peak_staged_payload_bytes``
measures selected output staging, not the input copy or prefetched values.

``point_lookup_calls`` counts native MultiGet calls, each with at most 256
keys and no more than the remaining batch rows. ``validity_bytes_emitted``
counts published value bitmaps; it is zero for ordinary non-null scan streams.
``output_bytes_emitted`` and requested/live capacity include these bitmaps.
Gather time includes MultiGet work; the other metrics retain their scan meaning.
Polling and stable cancellation diagnostics are shared with scan streams;
one MultiGet call cannot be forcibly interrupted. Already returned buffers,
including detached value children, remain valid after input/stream/DB release.

``benchmarks/bench_arrow_points.py`` compares native point streams against
existing MultiGet plus Arrow reconstruction, checking an ordered digest that
includes nulls/duplicates before and after warmed, alternating-order repeats.
See ``benchmarks/arrow_points.md`` for measurements and limitations.

Multi-range and multi-prefix unions (ABR-05)
--------------------------------------------

``scan_arrow_ranges(ranges=None, prefixes=None, ...)`` and extended/
TransactionDB ``scan_arrow_ranges_cf(cf_handle, ...)`` select a bytewise
**ordered union**, not caller order. Each range is ``(start, stop)`` with
inclusive start, exclusive stop and ``None`` for an unbounded endpoint.
Prefixes are bytes. Both arguments may be supplied; at least one must be a
list/tuple (up to 65536 selectors per argument). Empty selector lists select
no records; ``(None, None)`` or an empty prefix selects everything.

Ranges are validated and copied at creation, sorted, and merged when touching
or overlapping. Equal endpoints select nothing; reversed endpoints fail.
Duplicate/overlapping ranges or prefixes never duplicate records. Binary
prefixes ending in ``0xff`` use the same successor rules as ordinary scans.
Output follows bytewise key order even if selectors are supplied out of order::

    scan = db.scan_arrow_ranges(
        ranges=[(b"z:", None), (b"a:", b"c:")],
        prefixes=[b"b:", b"x:"], batch_size=4096,
    )

One native iterator and one creation-time snapshot serve the whole union.
Native seeks skip gaps; a final upper bound is pushed down to RocksDB when
bounded. Intermediate upper endpoints are checked natively before advancing
to the next seek, so no full-scan materialization is needed. Row/byte limits,
single-record overflow, metrics and cancellation apply continuously across
transitions; a batch may contain rows from multiple ranges. Snapshot/CF
identity and drop protection match ordinary scans. Output stays non-null
binary key/value with independent owned batches. Existing scan signatures
are unchanged. ``benchmarks/bench_arrow_ranges.py`` compares against separate
scans sharing the same explicit snapshot and verifies ordered digests.

Capability and configuration contract (ABR-06)
------------------------------------------------

``db.arrow_capabilities()`` returns a fresh dictionary with
``contract_version=1`` and the actual API surface for that DB class. It reports
default/named-CF methods, binary offset width/nullability, point/range ordering,
creation-time same-DB committed snapshot scope, independent owned output,
single export, metrics/cancellation and numeric row/byte/input limits. This is
a raw-KV contract, not graph decoding, transaction-local visibility or a
whole-process memory guarantee. Reading it does not touch an open DB.

``stream.configuration`` is a fresh, read-only-property dictionary recording
effective creation-time source, CF, row/byte limits, overflow/metrics choices
and read controls; it remains readable after close. Mutating either returned
dictionary does not reconfigure anything. Consumers should check the contract
version and capabilities rather than infer behavior from a package version.

Existing ``ReadOptions.fill_cache`` and ``verify_checksums`` remain supported.
``ReadOptions.readahead_size`` is an atomic iterator-only hint from 0 to 1 GiB;
zero delegates to RocksDB defaults. Arrow and ordinary DB/CF/transaction
iterators copy it at creation, so later mutation/destruction cannot change
their controls. Point Get/MultiGet paths ignore this irrelevant hint and keep
their existing fast-path configuration. Point streams report effective
readahead as zero. Separate property assignments are not a multi-field atomic
transaction; configure the object before sharing it for stream creation.

Arrow iterators explicitly use total-order seeks and owned bound storage.
Custom comparators are rejected at establishment. ``ReadOptions`` deliberately
does not expose tailing, prefix-only seek modes, raw bound buffers or raw
snapshot pointers: setting these attributes fails, rather than silently
accepting unsafe/incomplete-result combinations. Supply bounds/prefixes and
validated ``Snapshot`` objects through the public scan arguments. Capability
``unsupported_controls`` refers to these unsupported ReadOptions attributes,
not to the supported explicit ``snapshot=`` argument. No mutable/buffer-bearing
control is borrowed from caller storage.

Reference
---------

* ``PyRocksDB.scan_arrow(prefix=None, start=None, stop=None, batch_size=65536,
  max_batch_bytes=8388608, read_options=None, snapshot=None,
  allow_overflow=False, collect_metrics=False) -> ArrowScan``
* ``PyRocksDBExtended.scan_arrow_cf(cf_handle, ...)`` — same keyword contract.
* ``TransactionDB.scan_arrow(...)`` — same keyword contract on the default
  column family; observes committed state (open-transaction writes excluded).
* ``TransactionDB.scan_arrow_cf(cf_handle, ...)`` — same contract across CFs.
* ``get(key, read_options=None, snapshot=None)`` and
  ``multi_get(keys, read_options=None, snapshot=None)``
* ``get_cf(cf, key, read_options=None, snapshot=None)`` and
  ``multi_get_cf(cf, keys, read_options=None, snapshot=None)``
* ``PyRocksDB.create_snapshot() -> Snapshot`` (also on ``TransactionDB``;
  snapshots are per-instance and never shared across DB objects)
* ``Snapshot.release()``, ``Snapshot.is_valid()``, ``Snapshot.sequence_number``
* ``ArrowScan.close()``, ``ArrowScan.is_closed``, ``ArrowScan.sequence_number``,
  ``ArrowScan.stats``, ``ArrowScan.cancel()``, ``ArrowScan.cancel_requested``

See ``benchmarks/bench_arrow_scan.py`` for the reproducible comparison of
Python iterators vs. native scans, and ``examples_simple/arrow_scan.py`` for
a minimal end-to-end example.
The optional ``--isolated`` runner records fresh-process repeats, digest
parity, per-worker RSS and ABR-01 capacity/timings; see :doc:`arrow_benchmarks`
for its cache/validation/memory boundaries and scale execution plan.
