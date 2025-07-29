import pyrex
import os
import shutil

db_path = "/tmp/pyrex_example_error"
if os.path.exists(db_path):
    shutil.rmtree(db_path)

# Create an options object to trigger an error
options = pyrex.PyOptions()
options.create_if_missing = True
options.error_if_exists = True

try:
    # First open creates the DB
    db1 = pyrex.PyRocksDB(db_path, options)
    db1.put(b"foo", b"bar")
    del db1 # Close it so we can try to open again

    # This second open should fail because error_if_exists is True
    print("Attempting to open existing DB with error_if_exists=True...")
    db2 = pyrex.PyRocksDB(db_path, options) # This line should raise an exception
    print("This line should not be reached.") # If it is, the error wasn't caught
except pyrex.RocksDBException as e:
    print(f"Caught expected RocksDBException: {e}")
    # Assert that the error message contains the expected string
    assert "exists (error_if_exists is true)" in str(e)
finally:
    # Clean up the database directory regardless of success or failure
    if os.path.exists(db_path):
        shutil.rmtree(db_path)
        print(f"Cleaned up database at {db_path}")
