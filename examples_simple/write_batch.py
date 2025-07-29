import pyrex
import os
import shutil

db_path = "/tmp/pyrex_example_batch"
if os.path.exists(db_path):
    shutil.rmtree(db_path)

db = pyrex.PyRocksDB(db_path)

# Prepare some initial data
db.put(b"key1", b"value1")
db.put(b"key2", b"value2")

# Create a write batch
batch = pyrex.PyWriteBatch()
batch.put(b"key3", b"value3")       # Add new key
batch.delete(b"key1")               # Delete existing key
batch.put(b"key2", b"updated_value2") # Update existing key

db.write(batch) # Apply the batch atomically

# Verify results
print(f"Key1: {db.get(b'key1')}")       # Expected: None
print(f"Key2: {db.get(b'key2').decode()}") # Expected: updated_value2
print(f"Key3: {db.get(b'key3').decode()}") # Expected: value3

del db
shutil.rmtree(db_path)
