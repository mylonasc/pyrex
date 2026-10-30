Transactions
============

``pyrex.TransactionDB`` opens RocksDB with pessimistic transaction support. Use
it when multiple writes and reads must be grouped into one atomic unit.

Basic Transaction
-----------------

Transactions use explicit commit semantics. The context manager rolls back if
the transaction is still active when the ``with`` block exits.

.. code-block:: python

   import pyrex

   with pyrex.TransactionDB("example_txn_db") as db:
       with db.transaction() as txn:
           txn.put(b"k", b"v")
           assert txn.get(b"k") == b"v"
           txn.commit()

       assert db.get(b"k") == b"v"

Rollback On Exception
---------------------

If an exception leaves the transaction block before ``commit()`` is called,
``__exit__`` rolls the transaction back.

.. code-block:: python

   import pyrex

   with pyrex.TransactionDB("example_txn_db") as db:
       try:
           with db.transaction() as txn:
               txn.put(b"k", b"temporary")
               raise RuntimeError("abort")
       except RuntimeError:
           pass

       assert db.get(b"k") is None

Write Batch Inside A Transaction
--------------------------------

Existing ``PyWriteBatch`` objects can be applied inside a transaction. The
current transaction batch support covers default/named column-family ``put`` and
``delete`` operations.

.. code-block:: python

   import pyrex

   with pyrex.TransactionDB("example_txn_db") as db:
       batch = pyrex.PyWriteBatch()
       batch.put(b"a", b"1")
       batch.delete(b"old")

       with db.transaction() as txn:
           txn.write(batch)
           txn.commit()

Transaction Iterators
---------------------

Transaction iterators see the transaction view, including local writes and
deletes where RocksDB supports them. Prefix scans are performed by seeking to a
prefix and stopping when keys no longer match.

.. code-block:: python

   import pyrex

   with pyrex.TransactionDB("example_txn_db") as db:
       with db.transaction() as txn:
           txn.put(b"user:1", b"alice")
           txn.put(b"user:2", b"bob")

           it = txn.new_iterator()
           it.seek(b"user:")
           while it.valid() and it.key().startswith(b"user:"):
               print(it.key(), it.value())
               it.next()

           txn.rollback()

Column Families and Shared Read Views
-------------------------------------

Since 0.6.0, ``TransactionDB`` supports named column families, atomic
``txn.put_cf`` / ``txn.get_cf`` / ``txn.delete_cf``, and native
``db.scan_arrow_cf``. One ``db.create_snapshot()`` can be shared by scans,
``db.get`` / ``db.get_cf`` and ``db.multi_get`` / ``db.multi_get_cf``.
This preserves the storage horizon from index extraction through point
materialization, even while other transactions commit.

Native DB scans/point reads observe committed state only; read-your-writes
remains on the ``txn`` handle. ``TransactionOptions.set_snapshot=True``
pins repeatable transaction reads, including ``get_cf``. A transaction's
internal snapshot is not interchangeable with a DB ``Snapshot``.

CFs are reopened automatically. CF drops are rejected while any transaction
is active or a relevant Arrow scan is open. Transactions should be used by
one thread at a time; do not mutate option objects or batches while another
thread is using them in a native operation.

.. literalinclude:: ../../examples_simple/snapshot_cf_reads.py
   :language: python
   :caption: snapshot_cf_reads.py

See :doc:`arrow_scans` for bounds, byte limits, ownership and integration
opportunities for higher-level systems such as GestaltDB.

Snapshot Sequence Provenance
----------------------------

``Transaction.snapshot_sequence_number`` is a read-only ``int | None`` property.
It reports the sequence of the transaction's actual pinned RocksDB snapshot,
or ``None`` if the active transaction has no snapshot. Pin a snapshot with
``txn.set_snapshot()`` or ``TransactionOptions.set_snapshot=True``. Sequence
zero is valid, including for a snapshot of a new, empty database.

``TransactionDB.latest_sequence_number`` is a read-only ``int`` property reporting
the database's latest sequence. Concurrent commits can advance it without
changing an existing transaction snapshot. Never substitute this independently
sampled value for the pinned snapshot sequence.

.. code-block:: python

   with pyrex.TransactionDB("example_txn_db") as db:
       db.put(b"k", b"old")
       with db.transaction() as reader:
           reader.set_snapshot()
           pinned_sequence = reader.snapshot_sequence_number
           with db.transaction() as writer:
               writer.put(b"k", b"new")
               writer.commit()

           assert db.latest_sequence_number > pinned_sequence
           assert reader.snapshot_sequence_number == pinned_sequence
           assert reader.get(b"k") == b"old"
           # Even iterators created after the commit use the pinned horizon.
           it = reader.new_iterator()
           it.seek(b"k")
           assert it.value() == b"old"

Point reads (including named column families) and transaction iterators use the
reported snapshot, with transaction-local writes overlaid as usual. Calling
``set_snapshot()`` again explicitly replaces the pinned horizon; reading either
property never acquires or resets a snapshot and never writes to the database.

Both accessors are constant-time native lookups with the existing operation and
lifetime locks. They do not scan or hash contents, and ordinary reads and
transactions incur no provenance hashing overhead. Snapshot access raises
``RocksDBException`` after commit, rollback, or database closure; latest-sequence
access raises after database closure.

Sequences describe a horizon within a particular database, not a globally unique
content identity. Downstream provenance schemes must also track database identity
and, where needed, a content digest. A cached digest plus an unchanged latest
sequence can avoid repeat scans under an appropriate downstream synchronization
and database-identity contract; it does not replace the pinned snapshot sequence
or provide historical-state restoration.

Read For Update
---------------

``get_for_update`` reads a key and tracks it for transaction conflict checking.
Pass ``read_value=False`` to lock or track the key without fetching its value.

.. code-block:: python

   import pyrex

   with pyrex.TransactionDB("example_txn_db") as db:
       db.put(b"account:alice", b"100")

       with db.transaction() as txn:
           assert txn.get_for_update(b"account:alice") == b"100"
           txn.get_for_update(b"account:bob", read_value=False)
           txn.commit()

Transaction Options
-------------------

``TransactionDBOptions`` controls database-level transaction settings such as
lock timeouts and lock table sizing. ``TransactionOptions`` controls each
transaction.

.. code-block:: python

   import pyrex

   db_options = pyrex.TransactionDBOptions()
   db_options.default_lock_timeout = 1000
   db_options.transaction_lock_timeout = 1000

   txn_options = pyrex.TransactionOptions()
   txn_options.set_snapshot = True
   txn_options.lock_timeout = 1000
   txn_options.expiration = 30000
   txn_options.deadlock_detect = True

   with pyrex.TransactionDB("example_txn_db", None, db_options) as db:
       txn = db.begin_transaction(None, txn_options)
       txn.put(b"k", b"v")
       txn.commit()

Write Options And Durability
----------------------------

``WriteOptions`` can be supplied when a transaction begins and when it commits.
``disable_wal`` is preserved, but ``disable_wal=True`` means a successful commit
is not fully durable across process or machine crashes.

.. code-block:: python

   import pyrex

   write_options = pyrex.WriteOptions()
   write_options.sync = True

   with pyrex.TransactionDB("example_txn_db") as db:
       with db.transaction(write_options) as txn:
           txn.put(b"durable", b"value")
           txn.commit(write_options)

Retryable Errors
----------------

Transaction lock conflicts and timeouts are mapped to specific Python exception
subclasses so callers can decide whether to retry.

.. code-block:: python

   import pyrex

   try:
       with pyrex.TransactionDB("example_txn_db") as db:
           with db.transaction() as txn:
               txn.put(b"k", b"v")
               txn.commit()
   except (pyrex.RocksDBBusyError,
           pyrex.RocksDBTimeoutError,
           pyrex.RocksDBConflictError):
       # Retry according to the application's policy.
       raise

Complete Example
----------------

.. literalinclude:: ../../examples_simple/transactions.py
   :language: python
   :caption: transactions.py
   :linenos:

Transaction-local Arrow scans (ABR-08)
------------------------------------------

``Transaction.scan_arrow`` and ``scan_arrow_cf`` are distinct from committed
``TransactionDB.scan_arrow``. They include the transaction's own writes and
deletes, without exposing another transaction's uncommitted changes::

    import pyarrow as pa

    with db.transaction() as txn:
        txn.put(b"private", b"value")
        with txn.scan_arrow(batch_size=4096) as scan:
            with pa.RecordBatchReader.from_stream(scan) as reader:
                result = reader.read_all()
        txn.commit()
    # result owns its buffers and remains valid after transaction/DB closure.

The scan schema, prefix/range intersection, bytewise comparator, row/used-byte
limits, overflow policy, owned buffers, opt-in metrics and cooperative
cancellation are the same as DB scans. Named-CF handles are validated against
the parent DB by identity. Transaction Arrow methods do not accept a DB
``Snapshot`` argument, and do not add transaction-local points/multi-range APIs.
``txn.arrow_capabilities()`` reports this narrower API and visibility contract;
``scan.configuration["source"]`` is ``transaction_scan``.

Consistency is explicit. If the transaction has a snapshot, the scan uses that
committed horizon. Otherwise it pins a DB snapshot at scan creation. Its own
write batch is **frozen** while any transaction Arrow stream retains native
resources: puts/deletes (including CF variants), write batches, get-for-update,
snapshot reset, commit and rollback raise an active-Arrow-scan error. Ordinary
gets/iterators remain allowed and native gathers serialize with transaction
operations. Without a transaction snapshot, ordinary point gets can observe
later committed writes while the Arrow view remains pinned.

Exhaustion, explicit scan/reader close, observed cancellation or terminal scan
error releases the freeze. Merely returning the last data batch or requesting
cancellation on an idle stream does not. Close all streams before completing
the transaction; nest their contexts inside the transaction context so its
automatic rollback is not blocked. Multiple scans each keep the freeze until
their resources are released. To share one committed horizon across them,
set the transaction snapshot before constructing them.

``sequence_number`` identifies only the committed snapshot horizon, not the
transaction's private write-batch generation or an application visibility
marker. DB close serializes with transaction operations and invalidates native
Arrow iterators **before** deleting their transaction/write batch. Existing
CF-drop protection while transactions are active remains in place. A full
batch already committed before a racing close/cancel may return; its owned
buffers remain valid after rollback/commit/DB close. No partially built output
is successful. ``benchmarks/bench_transaction_arrow.py`` compares the native
stream with transaction-local iterator plus Arrow reconstruction at the same
transaction snapshot, not with committed-state DB scans.
