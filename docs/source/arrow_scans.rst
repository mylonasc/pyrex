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

Reading data back yields raw ``key``/``value`` binary columns. Decoding
application payloads (JSON, MessagePack, term encodings, graph relations)
stays with the caller; predicate filtering and graph semantics do not belong
in PyRex.

Reference
---------

* ``PyRocksDB.scan_arrow(prefix=None, start=None, stop=None, batch_size=65536,
  max_batch_bytes=8388608, read_options=None, snapshot=None,
  allow_overflow=False) -> ArrowScan``
* ``PyRocksDBExtended.scan_arrow_cf(cf_handle, ...)`` — same keyword contract.
* ``PyRocksDB.create_snapshot() -> Snapshot``
* ``Snapshot.release()``, ``Snapshot.is_valid``, ``Snapshot.sequence_number``
* ``ArrowScan.close()``, ``ArrowScan.is_closed``, ``ArrowScan.sequence_number``

See ``benchmarks/bench_arrow_scan.py`` for the reproducible comparison of
Python iterators vs. native scans, and ``examples_simple/arrow_scan.py`` for
a minimal end-to-end example.
