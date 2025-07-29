import pyrex
import os
import shutil

db_path = "/tmp/pyrex_example_basic"
if os.path.exists(db_path):
    shutil.rmtree(db_path)

db = pyrex.PyRocksDB(db_path)

# Put a key-value pair
db.put(b"my_key", b"my_value")

# Get the value
value = db.get(b"my_key")
print(f"Retrieved: {value.decode()}") # Expected: my_value

# Get a non-existent key
none_value = db.get(b"non_existent")
print(f"Retrieved non-existent: {none_value}") # Expected: None

del db # Close the DB
shutil.rmtree(db_path) # Clean up
