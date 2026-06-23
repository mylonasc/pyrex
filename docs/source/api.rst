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

CompressionType
---------------

The ``CompressionType`` enum exposes RocksDB compression choices, including
``kNoCompression``, ``kSnappyCompression``, ``kLZ4Compression``, and ``kZSTD``.

Exceptions
----------

``RocksDBException`` is raised for RocksDB operational errors, closed database
usage, invalid column-family handles, and read-only write attempts.
