import pyrex
import os
import shutil

db_path = "/tmp/pyrex_example_config"
if os.path.exists(db_path):
    shutil.rmtree(db_path)

options = pyrex.PyOptions()
options.create_if_missing = True
options.max_open_files = 1000
options.compression = pyrex.CompressionType.kLZ4Compression
options.optimize_for_small_db() # Apply an optimization preset

db = pyrex.PyRocksDB(db_path, options)

# Verify some configured options (values might be adjusted by RocksDB internally)
retrieved_options = db.get_options()
print(f"Configured max_open_files: {retrieved_options.max_open_files}") # Expected: 5000 (adjusted by optimize_for_small_db)
print(f"Configured compression: {retrieved_options.compression}") # Expected: CompressionType.kLZ4Compression

del db
shutil.rmtree(db_path)
