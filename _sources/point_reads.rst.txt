Point reads: specialized and general paths
==========================================

The binding has a specialized ordinary-read dispatch alongside the existing
general implementation. The public call forms remain supported on
``PyRocksDB``, ``PyRocksDBExtended`` and ``TransactionDB``::

    db.get(key)
    db.get(key=key)
    db.get(key, read_options=None, snapshot=None)
    db.get(key, read_options=options, snapshot=snapshot)
    db.get_cf(cf, key, read_options=options, snapshot=snapshot)

Specialized ordinary reads
--------------------------

The binding checks optional arguments as borrowed Python handles and converts
them to native shared ownership only when real objects are supplied. It avoids
shared-pointer conversion of ``None`` on ordinary calls without changing
argument names, defaults or positional/keyword support. A one-key overload
was tested but rejected: it improved ordinary reads at the expense of
explicit-options/snapshot calls, due to failed-overload dispatch.

The ordinary native read is selected when the CF is implicit and both
options and snapshot are ``None``. Explicit ``None`` therefore still takes
the ordinary native path, just as omitting the optional arguments does.

This path borrows the immutable Python bytes key, uses the cached default
CF handle and borrows an immutable native read configuration. The argument
keeps the key alive; its pointer is extracted before releasing the GIL.
Other Python threads may run during the read. DB-open/pointer checks run
only under lifecycle protection, including during races with ``close()``.

General reads (retained)
------------------------

Explicit options, snapshots and CFs retain the general implementation:
owned key storage, copied read options for snapshot overrides, and
snapshot/CF validation. An explicitly released or foreign snapshot, or a
dropped/foreign CF handle, still raises ``RocksDBException``. No validation
is bypassed to improve latency. General reads also use shared lifetime
protection through the native operation.

Safe mutable read options
-------------------------

``ReadOptions`` currently exposes two mutable booleans, ``fill_cache`` and
``verify_checksums``. Their combinations select four immutable native
configurations. Setters update an atomic flag word; each read selects one
complete configuration with one atomic load. Changing a flag cannot modify
an options object already borrowed by an in-flight native read.

This applies to DB reads, MultiGet, scans, iterators and transaction reads.
Snapshot/bounds overrides modify a local copy, never the immutable template.
Replacing ``db.default_read_options`` is protected by the lifecycle lock,
as is retrieving its Python-visible handle. Existing handles remain mutable
and retain their identity. Separate flag assignments are not a transaction;
a concurrent reader may observe the state between two assignments.

Lifetime protection versus data-operation synchronization
---------------------------------------------------------

RocksDB itself supports concurrent reads and writes on one DB. Ordinary
reads and writes therefore take shared lifecycle protection; they are not
serialized against each other by an exclusive binding mutex.

Close, CF creation/destruction, snapshot release and scan bookkeeping keep
exclusive access. The lock remains held throughout each native operation,
not merely while looking up raw pointers: DB/CF/snapshot resources must
remain alive. Lock order remains lifecycle lock, then scan/snapshot mutex.
Read locks are released before reacquiring the GIL, avoiding lock/GIL
inversion with Python option setters. Transaction-local operations retain
their separate operation/transaction locks and commit/rollback semantics.

This is not a declaration of free-threaded Python support. ``WriteOptions``,
write batches and iterators have their existing sharing/mutation contracts;
do not concurrently mutate a write-options object or batch in use.

Measurement and remaining opportunities
---------------------------------------

``benchmarks/bench_point_read.py`` uses identical one-argument Python
wrappers around cached callables for every mode. It verifies results before
timing, rotates mode order, records sample distributions/module paths and
supports CPU pinning, long keys, larger values, misses and decode controls.
Decode-only/decoded-read probes are synthetic, not GestaltDB measurements.
See ``benchmarks/point_reads.md`` in the repository for measured results.

Compare source builds of both historical revisions and the candidate using
the same interpreter, pybind11, compiler/flags, compression libraries and
RocksDB archive. Comparing an older PyPI wheel with a local build does not
isolate the remaining cost of code or safety. Earlier claims that the full
remaining gap was unavoidable lifecycle overhead were unsupported.

No result here establishes an unavoidable performance floor. Operation
leases/deferred reclamation could replace the shared mutex if separately
proved and measured. RocksDB's ``PinnableSlice`` is also a candidate to
avoid the intermediate value copy, but the pin must be released before DB
teardown. Simply holding the lifecycle lock while reacquiring the GIL to
create Python bytes can deadlock with setters; that requires a deliberate
lifetime/lock design, not a mechanical replacement of ``std::string``.
Neither experiment is needed to adopt the safe binding optimizations here.

References
----------

* `RocksDB concurrency <https://github.com/facebook/rocksdb/wiki/Basic-Operations#concurrency>`_
* `RocksDB Slice lifetime <https://github.com/facebook/rocksdb/wiki/Basic-Operations#slice>`_
* `RocksDB reads and PinnableSlice <https://github.com/facebook/rocksdb/wiki/Basic-Operations#reads>`_
* `RocksDB 10.7.5 DB API <https://github.com/facebook/rocksdb/blob/v10.7.5/include/rocksdb/db.h>`_
