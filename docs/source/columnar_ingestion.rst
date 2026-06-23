Columnar Batch Ingestion
========================

``PyRocksDB.write_columnar_batch`` writes many key/value records through one
native RocksDB ``WriteBatch`` call. It is intended for Arrow-compatible data
pipelines where keys and values are already stored in contiguous binary or
string buffers.

Why Use It
----------

The traditional Python write-batch API is atomic, but still calls from Python
into C++ for every row:

.. code-block:: python

   batch = pyrex.PyWriteBatch()
   for key, value in rows:
       batch.put(key, value)
   db.write(batch)

``write_columnar_batch`` moves that per-row loop into C++:

.. code-block:: python

   db.write_columnar_batch(keys, values)

This reduces Python loop overhead, Python-to-C++ call overhead, and Python
``bytes`` materialization when Arrow binary or string arrays are passed.

Supported Inputs
----------------

The method accepts:

* ``pyarrow.Array`` with type ``binary`` or ``large_binary``
* ``pyarrow.Array`` with type ``string`` or ``large_string``
* Polars ``Series`` indirectly through ``series.to_arrow()``
* ``list[bytes]`` and ``tuple[bytes]`` as compatibility fallbacks

Polars is not imported or required by ``pyrex-rocksdb``. Convert Polars data to
Arrow before calling the API.

Nulls and Validation
--------------------

The first implementation supports only ``on_null="error"``. If any key or value
is null, the method raises ``ValueError`` before writing anything.

Keys and values must have the same length. Mismatched lengths raise
``ValueError`` before writing anything.

PyArrow Example
---------------

.. code-block:: python

   import pyarrow as pa
   import pyrex

   keys = pa.array([b"k1", b"k2", b"k3"], type=pa.binary())
   values = pa.array([b"v1", b"v2", b"v3"], type=pa.binary())

   with pyrex.PyRocksDB("example_rocksdb") as db:
       db.write_columnar_batch(keys, values)
       assert db.get(b"k2") == b"v2"

Polars Example
--------------

.. code-block:: python

   import polars as pl
   import pyrex

   df = pl.DataFrame({
       "key": ["k1", "k2", "k3"],
       "value": [b"v1", b"v2", b"v3"],
   })

   with pyrex.PyRocksDB("example_rocksdb") as db:
       db.write_columnar_batch(
           df["key"].to_arrow(),
           df["value"].to_arrow(),
       )

Serialized Column Example
-------------------------

Higher-level libraries can serialize whole columns or chunks and store those
serialized payloads as values. The key/value primitive remains generic; it does
not know about graph, table, or dataframe semantics.

.. code-block:: python

   import io
   import pyarrow as pa
   import polars as pl
   import pyrex

   df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

   def serialize_column(name):
       array = df[name].to_arrow()
       batch = pa.RecordBatch.from_arrays([array], names=[name])
       sink = io.BytesIO()
       with pa.ipc.new_stream(sink, batch.schema) as writer:
           writer.write_batch(batch)
       return sink.getvalue()

   keys = pa.array([b"column:id", b"column:name"], type=pa.binary())
   values = pa.array([
       serialize_column("id"),
       serialize_column("name"),
   ], type=pa.binary())

   with pyrex.PyRocksDB("columns_rocksdb") as db:
       db.write_columnar_batch(keys, values)

Write Options
-------------

The method accepts the existing ``WriteOptions`` object:

.. code-block:: python

   opts = pyrex.WriteOptions()
   opts.disable_wal = True

   db.write_columnar_batch(keys, values, write_options=opts)

Disabling WAL can improve write throughput but reduces durability. If the
process or machine crashes before data reaches stable storage, recent writes may
be lost.

Benchmark
---------

The repository includes a focused benchmark:

.. code-block:: bash

   python benchmarks/bench_columnar_batch.py --rows 100000 --repeat 3 --disable-wal
   python benchmarks/bench_columnar_batch.py --rows 1000000 --repeat 3 --disable-wal

On the development machine used for the initial implementation, the native
Arrow binary path was about ``1.7x`` faster than a Python loop calling
``PyWriteBatch.put`` for each row.
