import threading
import time
import os
import shutil
import pytest
import pyrex  # Assuming your compiled module is named pyrex

DB_PATH = "./test_db"

# --- Fixture to set up and tear down the database ---
@pytest.fixture
def db():
    """A fixture that creates a fresh RocksDB instance for each test."""
    print("at DB")
    if os.path.exists(DB_PATH):
        shutil.rmtree(DB_PATH)
    # Use PyRocksDBExtended as it covers all functionality
    db_instance = pyrex.PyRocksDBExtended(DB_PATH)
    yield db_instance
    # The close() method is critical for resource cleanup.
    # We also check if it's already closed to avoid errors in tests that close it manually.
    try:
        db_instance.close()
    except pyrex.RocksDBException as e:
        # It's okay if the DB is already closed, which some tests do intentionally.
        if "Database is closed" not in str(e):
            raise

# --- Test Cases ---

def test_iterator_use_after_db_close_segfault(db):
    """
    SURFACES: Dangling Iterators (Use-After-Free)
    
    This test is designed to cause a segfault. It creates an iterator,
    then explicitly closes the database. The iterator object still exists
    in Python, but its underlying C++ pointer is now dangling. Accessing
    any of its methods should cause a crash.
    """
    print("at test_iterator")
    db.put(b"key1", b"value1")
    iterator = db.new_iterator()
    iterator.seek_to_first()
    assert iterator.valid()
    assert iterator.key() == b"key1"

    # Now, close the database while the iterator is still "alive".
    db.close()

    # The `iterator` variable now holds a dangling pointer.
    # Any operation on it should trigger a segfault if not handled correctly.
    # The C++ wrapper should throw a "Database is closed" exception instead.
    with pytest.raises(pyrex.RocksDBException, match="Database is closed"):
        iterator.valid()

    with pytest.raises(pyrex.RocksDBException, match="Database is closed"):
        iterator.next()
        
    print("\nSUCCESS: test_iterator_use_after_db_close_segfault passed. The wrapper correctly prevented a segfault.")


def test_concurrent_close_and_iterate_deadlock():
    """
    SURFACES: Iterator and DB close() Race Condition / Deadlock
    
    This test simulates a scenario where one thread is closing the database
    while another thread might still be using an iterator or while the Python
    garbage collector is destroying iterator objects. This can cause a deadlock
    if the mutex protecting the active iterators list is locked incorrectly.
    """
    print("at test_concurent")
    if os.path.exists(DB_PATH):
        shutil.rmtree(DB_PATH)
    
    db_instance = pyrex.PyRocksDBExtended(DB_PATH)
    db_instance.put(b"key1", b"value1")
    
    # Keep a reference to the iterator to ensure it's not garbage collected early.
    iterator = db_instance.new_iterator()
    
    errors = []
    
    def close_thread_func():
        try:
            # Give the other thread a moment to start iterating
            time.sleep(0.1) 
            print("Thread 1: Closing database...")
            db_instance.close()
            print("Thread 1: Database closed successfully.")
        except Exception as e:
            errors.append(f"Close thread failed: {e}")

    def iterator_thread_func():
        try:
            print("Thread 2: Starting iteration...")
            # This loop will attempt to use the iterator while the other thread closes the DB.
            # It should not hang, but instead raise a RocksDBException.
            for _ in range(5):
                try:
                    iterator.seek_to_first()
                    while iterator.valid():
                        iterator.key()
                        iterator.next()
                        time.sleep(0.05) # Small delay to increase chance of race condition
                except pyrex.RocksDBException as e:
                    print(f"Thread 2: Caught expected exception: {e}")
                    # This is the expected outcome after the DB is closed.
                    assert "Database is closed" in str(e)
                    break # Exit the loop once the DB is closed.
            print("Thread 2: Iteration finished.")
        except Exception as e:
            errors.append(f"Iterator thread failed: {e}")

    t1 = threading.Thread(target=close_thread_func)
    t2 = threading.Thread(target=iterator_thread_func)

    t1.start()
    t2.start()

    t1.join(timeout=5)  # 5-second timeout to detect deadlock
    t2.join(timeout=5)

    if t1.is_alive() or t2.is_alive():
        pytest.fail("Deadlock detected! One or more threads did not finish.")
    
    if errors:
        pytest.fail(f"Test failed with errors: {errors}")

    print("\nSUCCESS: test_concurrent_close_and_iterate_deadlock passed without hanging.")


def test_use_dropped_column_family_handle(db):
    """
    SURFACES: Dropped Column Family Handle (Use-After-Free)
    
    This test creates a column family, gets a handle to it, then drops it.
    It then tries to use the old handle. The wrapper should throw an exception
    rather than crashing.
    """
    print("Creating column family 'cf1'...")
    cf_handle = db.create_column_family("cf1")
    assert cf_handle.is_valid()
    
    db.put_cf(cf_handle, b"key_cf", b"value_cf")
    assert db.get_cf(cf_handle, b"key_cf") == b"value_cf"
    
    print("Dropping column family 'cf1'...")
    db.drop_column_family(cf_handle)
    
    # The handle is now invalid.
    assert not cf_handle.is_valid()
    
    # Any operation using this handle should fail gracefully.
    with pytest.raises(pyrex.RocksDBException, match="ColumnFamilyHandle is invalid"):
        db.put_cf(cf_handle, b"key_new", b"value_new")
        
    with pytest.raises(pyrex.RocksDBException, match="ColumnFamilyHandle is invalid"):
        db.get_cf(cf_handle, b"key_cf")

    print("\nSUCCESS: test_use_dropped_column_family_handle passed. The wrapper correctly prevented use of a dropped handle.")



