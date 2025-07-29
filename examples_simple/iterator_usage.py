import pyrex
import os
import shutil

db_path = "/tmp/pyrex_example_iterator"
if os.path.exists(db_path):
    shutil.rmtree(db_path)

db = pyrex.PyRocksDB(db_path)

# Insert sample data
db.put(b"alpha", b"A")
db.put(b"beta", b"B")
db.put(b"gamma", b"G")

it = db.new_iterator()

print("Forward iteration:")
it.seek_to_first()
while it.valid():
    print(f"  Key: {it.key().decode()}, Value: {it.value().decode()}")
    it.next()
it.check_status() # Check for errors

print("\nSeek and backward iteration:")
it.seek(b"gamma") # Seek to 'gamma'
while it.valid():
    print(f"  Key: {it.key().decode()}, Value: {it.value().decode()}")
    it.prev() # Move backward

del db
shutil.rmtree(db_path)
