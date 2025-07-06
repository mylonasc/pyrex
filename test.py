import pyrex
import os
import shutil

DB_PATH = "/tmp/my_test_rocksdb_py"

if os.path.exists(DB_PATH):
    shutil.rmtree(DB_PATH)
    print(f"Removed existing DB at {DB_PATH}")

try:
    db = pyrocksdb.PyRocksDB(DB_PATH)
    print(f"Successfully opened RocksDB at {DB_PATH}")

    db.put(b"my_key", b"Hello RocksDB from Python!")
    print(f"Put: Key='my_key', Value='Hello RocksDB from Python!'")

    retrieved_value = db.get(b"my_key")
    if retrieved_value is not None:
        print(f"Get: Key='my_key', Retrieved Value='{retrieved_value.decode()}'")
    else:
        print(f"Get: Key='my_key', Value not found.")

except pyrocksdb.RocksDBException as e:
    print(f"An error occurred with RocksDB: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    print("Python script finished.")
