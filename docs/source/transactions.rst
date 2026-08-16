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
current transaction batch support covers default column-family ``put`` and
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
