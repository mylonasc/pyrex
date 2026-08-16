import os
import shutil

import pyrex


db_path = "/tmp/pyrex_example_transactions"
if os.path.exists(db_path):
    shutil.rmtree(db_path)


with pyrex.TransactionDB(db_path) as db:
    # Context-manager transactions are explicit-commit: if commit() is not
    # called, __exit__ rolls the transaction back.
    with db.transaction() as txn:
        txn.put(b"account:alice", b"100")
        txn.put(b"account:bob", b"50")
        assert txn.get(b"account:alice") == b"100"
        txn.commit()

    print(db.get(b"account:alice").decode())  # 100

    # Rollback discards uncommitted changes.
    with db.transaction() as txn:
        txn.put(b"account:alice", b"0")

    print(db.get(b"account:alice").decode())  # 100

    # Existing PyWriteBatch objects can be applied inside a transaction.
    batch = pyrex.PyWriteBatch()
    batch.put(b"account:carol", b"25")
    batch.delete(b"account:bob")

    txn = db.begin_transaction()
    txn.write(batch)
    txn.commit()

    print(db.get(b"account:carol").decode())  # 25
    print(db.get(b"account:bob"))  # None

    # Transaction iterators include transaction-local writes.
    with db.transaction() as txn:
        txn.put(b"account:dave", b"75")
        it = txn.new_iterator()
        it.seek(b"account:")
        while it.valid() and it.key().startswith(b"account:"):
            print(it.key(), it.value())
            it.next()
        txn.rollback()

shutil.rmtree(db_path)
