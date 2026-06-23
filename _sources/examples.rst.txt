.. _pyrex_examples:

Examples
========

This section provides concise examples of how to use the `pyrex` library, a Python wrapper for RocksDB.
The examples focus on demonstrating core functionalities directly.

Basic Put and Get
-----------------

Open a database, store a key-value pair, and retrieve it.

.. literalinclude:: ../../examples_simple/basic_put_get.py
   :language: python
   :caption: basic_put_get.py
   :linenos:

Configuring Options
-------------------

Customize database behavior using `PyOptions`.

.. literalinclude:: ../../examples_simple/config_options.py
   :language: python
   :caption: config_options.py
   :linenos:

Atomic Write Batch
------------------

Perform multiple `put` and `delete` operations in a single, atomic transaction.

.. literalinclude:: ../../examples_simple/write_batch.py
   :language: python
   :caption: write_batch.py
   :linenos:

Columnar Batch Ingestion
------------------------

Write Arrow-compatible key/value arrays with one Python call per chunk.

.. code-block:: python

   import pyarrow as pa
   import pyrex

   keys = pa.array([b"k1", b"k2", b"k3"], type=pa.binary())
   values = pa.array([b"v1", b"v2", b"v3"], type=pa.binary())

   with pyrex.PyRocksDB("example_columnar_db") as db:
       db.write_columnar_batch(keys, values)
       assert db.get(b"k1") == b"v1"

Iterating Data
--------------

Traverse key-value pairs using `PyRocksDBIterator` for forward and backward scans.

.. literalinclude:: ../../examples_simple/iterator_usage.py
   :language: python
   :caption: iterator_usage.py
   :linenos:

Error Handling
--------------

Catch RocksDB-specific exceptions for robust applications.

.. literalinclude:: ../../examples_simple/error_handling.py
   :language: python
   :caption: error_handling.py
   :linenos:


