API Reference
=============

This page summarizes the public Python API exposed by ``pyrex-rocksdb``.

PyRocksDB
---------

``PyRocksDB(path, options=None, read_only=False)`` opens a RocksDB database that
uses the default column family.

Methods:

* ``put(key: bytes, value: bytes, write_options=None) -> None``
* ``get(key: bytes, read_options=None) -> bytes | None``
* ``delete(key: bytes, write_options=None) -> None``
* ``write(write_batch: PyWriteBatch, write_options=None) -> None``
* ``write_columnar_batch(keys, values, *, write_options=None, on_null="error") -> None``
* ``new_iterator(read_options=None) -> PyRocksDBIterator``
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
* ``get_cf(cf_handle, key: bytes, read_options=None) -> bytes | None``
* ``delete_cf(cf_handle, key: bytes, write_options=None) -> None``
* ``list_column_families() -> list[str]``
* ``create_column_family(name: str, cf_options=None) -> ColumnFamilyHandle``
* ``drop_column_family(cf_handle) -> None``
* ``get_column_family(name: str) -> ColumnFamilyHandle | None``
* ``new_cf_iterator(cf_handle, read_options=None) -> PyRocksDBIterator``
* ``default_cf`` returns the default column-family handle.

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
* ``get(key: bytes, read_options=None) -> bytes | None``
* ``delete(key: bytes, write_options=None) -> None``
* ``write(write_batch: PyWriteBatch, write_options=None) -> None``
* ``begin_transaction(write_options=None, transaction_options=None) -> Transaction``
* ``transaction(write_options=None, transaction_options=None) -> Transaction``
* ``get_options() -> PyOptions``
* ``get_transaction_db_options() -> TransactionDBOptions``
* ``close() -> None``

``transaction()`` is an alias for ``begin_transaction()`` intended for context
manager usage. Transaction context managers require an explicit ``commit()``;
they roll back automatically if still active on exit.

Transaction
-----------

``Transaction`` represents one active RocksDB transaction.

Methods and properties:

* ``put(key: bytes, value: bytes) -> None``
* ``get(key: bytes, read_options=None) -> bytes | None``
* ``get_for_update(key: bytes, read_options=None, exclusive=True, do_validate=True, read_value=True) -> bytes | None``
* ``delete(key: bytes) -> None``
* ``write(write_batch: PyWriteBatch) -> None``
* ``commit(write_options=None) -> None``
* ``rollback() -> None``
* ``set_snapshot() -> None``
* ``new_iterator(read_options=None) -> PyTransactionIterator``
* ``is_active -> bool``

Transaction reads see prior writes in the same transaction. ``write`` accepts
existing ``PyWriteBatch`` objects for default column-family ``put`` and
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
