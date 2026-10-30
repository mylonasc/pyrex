API Reference
=============

This page summarizes the public Python API exposed by ``pyrex-rocksdb``.

PyRocksDB
---------

``PyRocksDB(path, options=None, read_only=False)`` opens a RocksDB database that
uses the default column family.

Methods:

* ``put(key: bytes, value: bytes, write_options=None) -> None``
* ``get(key: bytes, read_options=None, snapshot=None) -> bytes | None``
* ``multi_get(keys, read_options=None, snapshot=None) -> list[bytes | None]``
* ``delete(key: bytes, write_options=None) -> None``
* ``write(write_batch: PyWriteBatch, write_options=None) -> None``
* ``write_columnar_batch(keys, values, *, write_options=None, on_null="error") -> None``
* ``new_iterator(read_options=None) -> PyRocksDBIterator``
* ``create_snapshot() -> Snapshot``
* ``scan_arrow(prefix=None, start=None, stop=None, batch_size=65536, max_batch_bytes=8388608, read_options=None, snapshot=None, allow_overflow=False, collect_metrics=False) -> ArrowScan``
* ``get_options() -> PyOptions``
* ``close() -> None``

``write_columnar_batch`` accepts Arrow binary/string arrays or ``list[bytes]`` /
``tuple[bytes]`` fallback inputs. It validates lengths and nulls before writing
and applies all rows through one native RocksDB ``WriteBatch``.

PyRocksDBExtended
-----------------

``PyRocksDBExtended`` extends ``PyRocksDB`` with column-family management.

Methods:

* ``put_cf(cf_handle, key: bytes, value: bytes, write_options=None) -> None``
* ``get_cf(cf_handle, key: bytes, read_options=None, snapshot=None) -> bytes | None``
* ``multi_get_cf(cf_handle, keys, read_options=None, snapshot=None) -> list[bytes | None]``
* ``delete_cf(cf_handle, key: bytes, write_options=None) -> None``
* ``list_column_families() -> list[str]``
* ``create_column_family(name: str, cf_options=None) -> ColumnFamilyHandle``
* ``drop_column_family(cf_handle) -> None``
* ``get_column_family(name: str) -> ColumnFamilyHandle | None``
* ``new_cf_iterator(cf_handle, read_options=None) -> PyRocksDBIterator``
* ``scan_arrow_cf(cf_handle, prefix=None, start=None, stop=None, batch_size=65536, max_batch_bytes=8388608, read_options=None, snapshot=None, allow_overflow=False, collect_metrics=False) -> ArrowScan``
* ``default_cf`` returns the default column-family handle.

ArrowScan and Snapshot
----------------------

``ArrowScan`` is a lazy single-consumer stream returned by ``scan_arrow()``
and ``scan_arrow_cf()``. It exports the Arrow C Stream interface via
``__arrow_c_stream__`` (PyCapsule ``arrow_array_stream``) with schema
``struct<key: binary (non-null), value: binary (non-null)>`` and supports
``close()``, ``cancel()``, ``cancel_requested``, ``is_closed``,
``sequence_number``, ``stats``, and context-manager use.
``stats`` is ``None`` by default; scans created with ``collect_metrics=True``
provide a fresh dictionary of native output counters, phase times and requested
buffer capacities. Accounting follows exported child-buffer releases and
remains readable after stream or DB closure. It does not measure process RSS.

``cancel() -> bool`` requests cooperative cancellation without waiting for the
scan/lifecycle mutex. It returns True once if accepted, False after an earlier
request or terminal close/error/EOF. ``cancel_requested`` records whether a
request was accepted and remains True after cancellation cleanup. Observation
by the stream returns ``ECANCELED`` / ``scan_arrow: cancelled``, not successful
exhaustion; already committed batches remain valid. Use ``close()`` for
deterministic resource release, including idle scans that will not be pulled.

``multi_get_arrow(keys, batch_size=65536, max_batch_bytes=8388608,
read_options=None, snapshot=None, allow_overflow=False, collect_metrics=False)``
returns the same single-consumer ``ArrowScan`` lifecycle object, with nullable
``value`` for missing keys. Extended/TransactionDB also expose
``multi_get_arrow_cf(cf_handle, keys, ...)``. These methods preserve input
order and duplicates; existing ``multi_get`` methods still return lists.
See :doc:`arrow_scans` for input validation, snapshot and memory contracts.

``db.arrow_capabilities()`` returns the versioned raw-KV contract for that DB
class. ``stream.configuration`` records effective creation-time controls.
Both return independent dictionaries and remain readable after close.
``ReadOptions.readahead_size`` is a captured iterator-only hint, 0..1 GiB;
point reads ignore it. See :doc:`arrow_scans` for unsupported-control policy.

``Snapshot`` is a pinned read view from ``create_snapshot()`` shareable by
multiple scans. Methods: ``release()``, ``is_valid()``, ``sequence_number``.

The same snapshot also pins ``get`` / ``multi_get`` and their CF variants.
MultiGet preserves input order/duplicates and returns ``None`` for missing
keys; at most 65536 keys per call. Results are Python lists, not Arrow streams.

See ``arrow_scans`` for the full scan/range/snapshot/lifecycle contract.

PyWriteBatch
------------

``PyWriteBatch`` accumulates write operations that are applied atomically with
``PyRocksDB.write``.

Methods:

* ``put(key: bytes, value: bytes) -> None``
* ``put_cf(cf_handle, key: bytes, value: bytes) -> None``
* ``delete(key: bytes) -> None``
* ``delete_cf(cf_handle, key: bytes) -> None``
* ``merge(key: bytes, value: bytes) -> None``
* ``merge_cf(cf_handle, key: bytes, value: bytes) -> None``
* ``clear() -> None``

TransactionDB
-------------

``TransactionDB(path, options=None, transaction_db_options=None)`` opens a
RocksDB database with pessimistic transaction support.

Methods:

* ``put(key: bytes, value: bytes, write_options=None) -> None``
* ``get(key: bytes, read_options=None, snapshot=None) -> bytes | None``
* ``multi_get(keys, read_options=None, snapshot=None) -> list[bytes | None]``
* ``delete(key: bytes, write_options=None) -> None``
* ``write(write_batch: PyWriteBatch, write_options=None) -> None``
* ``begin_transaction(write_options=None, transaction_options=None) -> Transaction``
* ``transaction(write_options=None, transaction_options=None) -> Transaction``
* ``new_iterator(read_options=None) -> PyRocksDBIterator`` (committed state)
* ``create_snapshot() -> Snapshot``
* ``scan_arrow(prefix=None, start=None, stop=None, batch_size=65536, max_batch_bytes=8388608, read_options=None, snapshot=None, allow_overflow=False, collect_metrics=False) -> ArrowScan`` (committed state; uncommitted transaction writes are not visible)
* ``get_options() -> PyOptions``
* ``get_transaction_db_options() -> TransactionDBOptions``
* ``close() -> None``

Also exposes CF management and direct CF operations:
``create_column_family``, ``get_column_family``, ``list_column_families``,
``drop_column_family``, ``default_cf``, ``put_cf``, ``get_cf``, ``multi_get_cf``,
``delete_cf`` and ``scan_arrow_cf``. Reopening opens all persisted CFs.
One explicit ``Snapshot`` can span scans and point reads across those CFs.
Dropping a CF while any transaction is active is rejected.

``transaction()`` is an alias for ``begin_transaction()`` intended for context
manager usage. Transaction context managers require an explicit ``commit()``;
they roll back automatically if still active on exit.

Transaction
-----------

``Transaction`` represents one active RocksDB transaction.

Methods and properties:

* ``put(key: bytes, value: bytes) -> None``
* ``get(key: bytes, read_options=None) -> bytes | None``
* ``put_cf(cf_handle, key: bytes, value: bytes) -> None``
* ``get_cf(cf_handle, key: bytes, read_options=None) -> bytes | None``
* ``delete_cf(cf_handle, key: bytes) -> None``
* ``get_for_update(key: bytes, read_options=None, exclusive=True, do_validate=True, read_value=True) -> bytes | None``
* ``delete(key: bytes) -> None``
* ``write(write_batch: PyWriteBatch) -> None``
* ``commit(write_options=None) -> None``
* ``rollback() -> None``
* ``set_snapshot() -> None``
* ``new_iterator(read_options=None) -> PyTransactionIterator``
* ``is_active -> bool``

Transaction reads see prior writes in the same transaction. ``write`` accepts
existing ``PyWriteBatch`` objects for default/named column-family ``put`` and
``delete`` operations.

``get_for_update`` reads and tracks a key for transaction conflict checking. Set
``read_value=False`` to lock or track the key without fetching the value; the
method returns ``None`` in that mode.

PyTransactionIterator
---------------------

Transaction iterators traverse the transaction view and can be used for prefix
or range scans by seeking to the lower bound and stopping in Python.

Methods:

* ``valid() -> bool``
* ``seek_to_first() -> None``
* ``seek_to_last() -> None``
* ``seek(key: bytes) -> None``
* ``next() -> None``
* ``prev() -> None``
* ``key() -> bytes | None``
* ``value() -> bytes | None``
* ``check_status() -> None``

PyRocksDBIterator
-----------------

Iterators traverse keys in RocksDB byte order.

Methods:

* ``valid() -> bool``
* ``seek_to_first() -> None``
* ``seek_to_last() -> None``
* ``seek(key: bytes) -> None``
* ``next() -> None``
* ``prev() -> None``
* ``key() -> bytes | None``
* ``value() -> bytes | None``
* ``check_status() -> None``

Options
-------

``PyOptions`` configures database open options and column-family defaults.

Common properties and methods:

* ``create_if_missing``
* ``error_if_exists``
* ``max_open_files``
* ``write_buffer_size``
* ``compression``
* ``max_background_jobs``
* ``cf_write_buffer_size``
* ``cf_compression``
* ``increase_parallelism(total_threads)``
* ``optimize_for_small_db()``
* ``use_block_based_bloom_filter(bits_per_key=10.0)``

``WriteOptions`` configures write operations.

Properties:

* ``sync``
* ``disable_wal``

``ReadOptions`` configures read operations.

Properties:

* ``fill_cache``
* ``verify_checksums``

``TransactionDBOptions`` configures transaction-capable database open options.

Properties:

* ``transaction_lock_timeout``
* ``default_lock_timeout``
* ``max_num_locks``
* ``num_stripes``

``TransactionOptions`` configures each transaction.

Properties:

* ``set_snapshot``
* ``lock_timeout``
* ``expiration``
* ``deadlock_detect``

CompressionType
---------------

The ``CompressionType`` enum exposes RocksDB compression choices, including
``kNoCompression``, ``kSnappyCompression``, ``kLZ4Compression``, and ``kZSTD``.

Exceptions
----------

``RocksDBException`` is raised for RocksDB operational errors, closed database
usage, invalid column-family handles, and read-only write attempts.

More specific subclasses are exposed for callers that need retry or recovery
policy:

* ``RocksDBConflictError``
* ``RocksDBTimeoutError``
* ``RocksDBBusyError``
* ``RocksDBCorruptionError``
* ``RocksDBIOError``
* ``RocksDBInvalidArgumentError``

Capabilities
------------

``has_transactions`` is ``True`` when the installed build exposes the
transaction API.
