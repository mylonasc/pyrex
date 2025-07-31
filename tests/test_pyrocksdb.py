import pyrex
import unittest
import os
import shutil

WRITE_ERROR_READONLY_MSG = 'Cannot perform put/write/delete operation: Database opened in read-only mode.'

class TestPyrex(unittest.TestCase):
    DB_BASE_PATH = "/tmp/test_pyrex_db"

    @classmethod
    def setUpClass(cls):
        os.makedirs(cls.DB_BASE_PATH, exist_ok=True)
        print(f"\nCreated base test directory: {cls.DB_BASE_PATH}")

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.DB_BASE_PATH):
            shutil.rmtree(cls.DB_BASE_PATH)
            print(f"Removed base test directory: {cls.DB_BASE_PATH}")

    def setUp(self):
        self.db_path = os.path.join(self.DB_BASE_PATH, self._testMethodName)
        if os.path.exists(self.db_path):
            shutil.rmtree(self.db_path)
        print(f"\nSetting up DB for test '{self._testMethodName}' at: {self.db_path}")
        self.db = None # Initialize db to None for tearDown's check
        self._additional_dbs = [] # For managing multiple DB instances in a single test

    def tearDown(self):
        # Close any additional DB instances opened in a test
        # These are usually direct instances that weren't wrapped in 'with'
        for extra_db in self._additional_dbs:
            if hasattr(extra_db, 'close') and callable(extra_db.close):
                extra_db.close()
        self._additional_dbs = []

        if hasattr(self, 'db') and self.db is not None:
            # If self.db was opened directly (e.g., test_15), ensure it's closed.
            # If it was opened via a context manager, it's already closed by __exit__.
            # The 'del self.db' will trigger the C++ destructor, which calls close() if not already closed.
            self.db.close() # Explicitly call close() for directly managed 'self.db'
            del self.db
            self.db = None
        if os.path.exists(self.db_path):
            shutil.rmtree(self.db_path)
            print(f"Cleaned up DB at: {self.db_path}")

    # --- Tests for original PyRocksDB functionality (implicitly default CF) ---

    def test_01_basic_put_get(self):
        """
        Test basic put and get operations using original PyRocksDB (default CF).
        """
        self.db = pyrex.PyRocksDB(self.db_path) # Use base class
        self.assertIsNotNone(self.db)

        key = b"test_key_01"
        value = b"test_value_01"
        self.db.put(key, value) # Original put
        retrieved_value = self.db.get(key) # Original get
        self.assertEqual(retrieved_value, value)

    def test_02_get_non_existent_key(self):
        """
        Test getting a key that does not exist from default CF.
        """
        self.db = pyrex.PyRocksDB(self.db_path)
        non_existent_key = b"non_existent_key"
        retrieved_value = self.db.get(non_existent_key)
        self.assertIsNone(retrieved_value)

    def test_03_open_with_default_options(self):
        """
        Test opening the database with default options.
        """
        self.db = pyrex.PyRocksDB(self.db_path)
        options = self.db.get_options()
        self.assertTrue(options.create_if_missing)
        self.assertIsInstance(options.max_open_files, int)
        self.assertEqual(options.compression, pyrex.CompressionType.kSnappyCompression)

    def test_04_open_with_custom_options(self):
        """
        Test opening the database with custom options.
        """
        options = pyrex.PyOptions()
        options.create_if_missing = True
        options.error_if_exists = False
        options.max_open_files = 500
        options.write_buffer_size = 16 * 1024 * 1024
        options.compression = pyrex.CompressionType.kLZ4Compression # Applies to main options
        options.cf_compression = pyrex.CompressionType.kLZ4Compression # Applies to cf_options_
        options.max_background_jobs = 4
        options.increase_parallelism(4)
        options.optimize_for_small_db()

        self.db = pyrex.PyRocksDB(self.db_path, options)
        retrieved_options = self.db.get_options()

        self.assertTrue(retrieved_options.create_if_missing)
        self.assertFalse(retrieved_options.error_if_exists)
        # Note: optimize_for_small_db changes max_open_files to 5000 and write_buffer_size to 2MB
        self.assertEqual(retrieved_options.max_open_files, 5000)
        self.assertEqual(retrieved_options.write_buffer_size, 2 * 1024 * 1024)
        self.assertEqual(retrieved_options.compression, pyrex.CompressionType.kLZ4Compression)
        self.assertEqual(retrieved_options.max_background_jobs, 4)
        # Verify CF specific options were also set if they align with the main options after optimization
        self.assertEqual(retrieved_options.cf_compression, pyrex.CompressionType.kLZ4Compression)


    def test_05_error_if_exists(self):
        """
        Test the error_if_exists option.
        """
        db1 = pyrex.PyRocksDB(self.db_path)
        db1.put(b"key", b"value")
        db1.close() # Ensure db1 is closed before attempting to open with error_if_exists

        options = pyrex.PyOptions()
        options.create_if_missing = False # DB must exist
        options.error_if_exists = True    # Should error if DB exists

        with self.assertRaises(pyrex.RocksDBException) as cm:
            self.db = pyrex.PyRocksDB(self.db_path, options)
        self.assertIn("exists (error_if_exists is true)", str(cm.exception))

    def test_06_open_failure_invalid_path(self):
        """
        Test opening the database with an invalid path (e.g., no permissions).
        """
        test_dir = os.path.join(self.DB_BASE_PATH, "restricted_parent")
        os.makedirs(test_dir, exist_ok=True)

        if os.name == 'posix' and os.geteuid() != 0:
            os.chmod(test_dir, 0o555) # r-xr-xr-x (no write permission)
            restricted_path = os.path.join(test_dir, "db_in_restricted")
            with self.assertRaises(pyrex.RocksDBException) as cm:
                self.db = pyrex.PyRocksDB(restricted_path)
            self.assertIn("Permission denied", str(cm.exception) or "Error opening DB (permission denied)")
            os.chmod(test_dir, 0o755) # Restore permissions for cleanup
        else:
            self.skipTest("Skipping test_06_open_failure_invalid_path due to OS or root privileges. Cannot reliably simulate permission denied.")


    def test_07_write_batch_put_and_delete(self):
        """
        Test atomic write batch operations (put and delete) on default CF.
        """
        self.db = pyrex.PyRocksDB(self.db_path)
        batch = pyrex.PyWriteBatch()
        batch.put(b"batch_key_1", b"batch_value_1")
        batch.put(b"batch_key_2", b"batch_value_2")
        batch.delete(b"batch_key_1")
        self.db.write(batch)

        self.assertIsNone(self.db.get(b"batch_key_1"))
        self.assertEqual(self.db.get(b"batch_key_2"), b"batch_value_2")

        batch.clear()
        batch.put(b"new_key", b"new_value")
        self.db.write(batch)
        self.assertEqual(self.db.get(b"new_key"), b"new_value")

    def test_08_iterator_basic_traversal(self):
        """
        Test basic iterator traversal (seek_to_first, next, key, value) on default CF.
        """
        self.db = pyrex.PyRocksDB(self.db_path)
        data = {
            b"apple": b"red",
            b"banana": b"yellow",
            b"cherry": b"red",
            b"date": b"brown"
        }
        for k, v in data.items():
            self.db.put(k, v)

        it = self.db.new_iterator()
        self.assertIsNotNone(it)


        it.seek_to_first()
        found_keys = []
        while it.valid():
            found_keys.append(it.key())
            it.next()

        self.assertEqual(sorted(found_keys), sorted(list(data.keys())))

        # it.check_status()
        # print("4")

    def test_09_iterator_seek_and_prev(self):
        """
        Test iterator seek and previous traversal on default CF.
        """
        self.db = pyrex.PyRocksDB(self.db_path)
        data = {
            b"a1": b"v1", b"a2": b"v2", b"a3": b"v3",
            b"b1": b"v4", b"b2": b"v5",
            b"c1": b"v6"
        }

        for k, v in data.items():
            self.db.put(k, v)


        it = self.db.new_iterator()
        it.seek(b"b1")
        self.assertTrue(it.valid())
        self.assertEqual(it.key(), b"b1")
        self.assertEqual(it.value(), b"v4")

        it.prev()
        self.assertTrue(it.valid())
        self.assertEqual(it.key(), b"a3")

        it.prev()
        self.assertTrue(it.valid())
        self.assertEqual(it.key(), b"a2")

        it.seek_to_last()
        self.assertTrue(it.valid())
        self.assertEqual(it.key(), b"c1")
        it.check_status()

    def test_10_iterator_empty_db(self):
        """
        Test iterator behavior on an empty database (default CF).
        """
        self.db = pyrex.PyRocksDB(self.db_path)
        it = self.db.new_iterator()
        self.assertFalse(it.valid())
        it.seek_to_first()
        self.assertFalse(it.valid())
        it.seek_to_last()
        self.assertFalse(it.valid())
        self.assertIsNone(it.key())
        self.assertIsNone(it.value())
        it.check_status()

    # --- New tests for PyRocksDBExtended (Column Family functionality) ---

    def test_11_column_family_creation_and_access(self):
        """
        Test creating, listing, and accessing data in new column families.
        """
        self.db = pyrex.PyRocksDBExtended(self.db_path) # Use extended class
        self.assertIsNotNone(self.db)

        # Initially, only default CF
        cfs = self.db.list_column_families()
        self.assertEqual(len(cfs), 1)
        self.assertIn("default", cfs)

        # Create a new CF
        cf1 = self.db.create_column_family("my_cf_1")
        self.assertIsNotNone(cf1)
        self.assertEqual(cf1.name, "my_cf_1")

        # Create another CF with custom options
        cf_options_special = pyrex.PyOptions()
        # Using the property assignment directly
        cf_options_special.cf_compression = pyrex.CompressionType.kZSTD
        cf2 = self.db.create_column_family("my_cf_2", cf_options_special)
        self.assertIsNotNone(cf2)
        self.assertEqual(cf2.name, "my_cf_2")

        cfs_after_creation = self.db.list_column_families()
        self.assertEqual(len(cfs_after_creation), 3)
        self.assertIn("default", cfs_after_creation)
        self.assertIn("my_cf_1", cfs_after_creation)
        self.assertIn("my_cf_2", cfs_after_creation)

        # Put/Get on specific CFs
        self.db.put_cf(cf1, b"key_cf1", b"value_cf1")
        self.db.put_cf(cf2, b"key_cf2", b"value_cf2_compressed")

        self.assertEqual(self.db.get_cf(cf1, b"key_cf1"), b"value_cf1")
        self.assertEqual(self.db.get_cf(cf2, b"key_cf2"), b"value_cf2_compressed")

        # Ensure keys are isolated
        self.assertIsNone(self.db.get(b"key_cf1")) # Not in default CF
        self.assertIsNone(self.db.get_cf(cf1, b"key_cf2")) # Not in cf1

        # Retrieve CF handle by name
        retrieved_cf1 = self.db.get_column_family("my_cf_1")
        self.assertIsNotNone(retrieved_cf1)
        self.assertEqual(retrieved_cf1.name, "my_cf_1")
        self.assertEqual(self.db.get_cf(retrieved_cf1, b"key_cf1"), b"value_cf1")

    def test_12_write_batch_with_column_families(self):
        """
        Test write batch operations involving specific column families.
        """
        self.db = pyrex.PyRocksDBExtended(self.db_path)
        cf_foo = self.db.create_column_family("foo")
        cf_bar = self.db.create_column_family("bar")

        batch = pyrex.PyWriteBatch()
        batch.put(b"default_key", b"default_val") # Default CF
        batch.put_cf(cf_foo, b"foo_key_1", b"foo_val_1")
        batch.put_cf(cf_bar, b"bar_key_1", b"bar_val_1")
        batch.delete_cf(cf_foo, b"foo_key_1") # Delete from foo CF
        self.db.write(batch)

        self.assertEqual(self.db.get(b"default_key"), b"default_val")
        self.assertIsNone(self.db.get_cf(cf_foo, b"foo_key_1"))
        self.assertEqual(self.db.get_cf(cf_bar, b"bar_key_1"), b"bar_val_1")

    def test_13_iterator_specific_column_family(self):
        """
        Test iterating over a specific column family.
        """
        self.db = pyrex.PyRocksDBExtended(self.db_path)
        cf_data = self.db.create_column_family("data_cf")

        data_cf_items = {
            b"item_A": b"data_A",
            b"item_B": b"data_B",
            b"item_C": b"data_C"
        }
        default_cf_items = {
            b"default_X": b"val_X",
            b"default_Y": b"val_Y"
        }

        for k, v in data_cf_items.items():
            self.db.put_cf(cf_data, k, v)
        for k, v in default_cf_items.items():
            self.db.put(k, v)

        # Iterate over data_cf
        it_data = self.db.new_cf_iterator(cf_data)
        found_data_keys = []
        it_data.seek_to_first()
        while it_data.valid():
            found_data_keys.append(it_data.key())
            it_data.next()
        self.assertEqual(sorted(found_data_keys), sorted(list(data_cf_items.keys())))
        it_data.check_status()

        # Iterate over default CF (using base class new_iterator)
        it_default = self.db.new_iterator()
        found_default_keys = []
        it_default.seek_to_first()
        while it_default.valid():
            found_default_keys.append(it_default.key())
            it_default.next()
        self.assertEqual(sorted(found_default_keys), sorted(list(default_cf_items.keys())))
        it_default.check_status()

    def test_14_drop_column_family(self):
        """
        Test dropping a column family and its effects.
        """
        self.db = pyrex.PyRocksDBExtended(self.db_path)
        cf_to_drop = self.db.create_column_family("temp_cf")
        self.db.put_cf(cf_to_drop, b"key_in_temp", b"value_in_temp")
        self.assertIsNotNone(self.db.get_cf(cf_to_drop, b"key_in_temp"))

        # Cannot drop default CF
        with self.assertRaises(pyrex.RocksDBException) as cm:
            self.db.drop_column_family(self.db.default_cf)
        self.assertIn("Cannot drop the default column family", str(cm.exception))

        self.db.drop_column_family(cf_to_drop)
        self.assertFalse(cf_to_drop.is_valid())

        # Verify it's no longer listed
        cfs = self.db.list_column_families()
        self.assertNotIn("temp_cf", cfs)

        # Trying to use a dropped CF handle should raise an error
        with self.assertRaises(pyrex.RocksDBException) as cm:
            self.db.get_cf(cf_to_drop, b"key_in_temp")
        self.assertIn("ColumnFamilyHandle is invalid", str(cm.exception))

        # Reopen DB and check if CF is still gone
        del self.db # Ensure the current DB instance is released
        self.db = pyrex.PyRocksDBExtended(self.db_path)
        cfs_reopened = self.db.list_column_families()
        self.assertNotIn("temp_cf", cfs_reopened)

        # Ensure data from other CFs is still accessible
        self.db.put(b"another_key_default", b"another_value_default")
        self.assertEqual(self.db.get(b"another_key_default"), b"another_value_default")


    def test_15_lock_held_error(self):
        """
        Test that attempting to open a database while its lock is held raises RocksDBException.
        """
        # We need to manage additional DB instances created in this test
        # so tearDown can clean them up.
        self._additional_dbs = []

        # 1. Open the database (it will acquire and hold the lock)
        db1 = pyrex.PyRocksDB(self.db_path)
        self.assertIsNotNone(db1)
        self._additional_dbs.append(db1) # Add to list for tearDown cleanup
        print(f"\nOpened first DB instance at {self.db_path}")

        # 2. Attempt to open the same database path again without closing the first instance
        with self.assertRaises(pyrex.RocksDBException) as cm:
            db2 = pyrex.PyRocksDB(self.db_path)
            self._additional_dbs.append(db2) # Add even if it fails, for potential partial init
            self.fail("Expected RocksDBException (lock held) but it did not occur.")

        # 3. Verify the exception message
        exception_message = str(cm.exception)
        print(f"Caught expected exception: {exception_message}")
        self.assertIn("lock hold by current process", exception_message)
        self.assertIn(self.db_path + "/LOCK", exception_message)

        # The tearDown method will now ensure db1 (and any other instances) are closed.

    # --- New tests for context manager and read-only functionality ---

    def test_16_context_manager_basic_rw(self):
        """
        Test basic put/get operations using PyRocksDB with a context manager.
        """
        key = b"cm_key"
        value = b"cm_value"

        # Open, put, get, and ensure auto-close
        with pyrex.PyRocksDB(self.db_path) as db:
            # We can't directly check db.is_closed_.load() because it's not exposed
            # but the context manager ensures it's open within the block.
            db.put(key, value)
            retrieved = db.get(key)
            self.assertEqual(retrieved, value)

        # Verify the data persists by reopening
        with pyrex.PyRocksDB(self.db_path) as db_reopen:
            retrieved_after_reopen = db_reopen.get(key)
            self.assertEqual(retrieved_after_reopen, value)

    def test_17_context_manager_extended_rw(self):
        """
        Test put/get operations with Column Families using PyRocksDBExtended with a context manager.
        """
        cf_name = "my_cm_cf"
        key = b"cm_cf_key"
        value = b"cm_cf_value"

        with pyrex.PyRocksDBExtended(self.db_path) as db:
            # We can't directly check db.is_closed_.load() because it's not exposed
            cf_handle = db.create_column_family(cf_name)
            db.put_cf(cf_handle, key, value)
            retrieved = db.get_cf(cf_handle, key)
            self.assertEqual(retrieved, value)
            self.assertIn(cf_name, db.list_column_families())

        # Verify data and CF persist by reopening
        with pyrex.PyRocksDBExtended(self.db_path) as db_reopen:
            self.assertIn(cf_name, db_reopen.list_column_families())
            reopened_cf_handle = db_reopen.get_column_family(cf_name)
            self.assertIsNotNone(reopened_cf_handle)
            self.assertEqual(reopened_cf_handle.name, cf_name)
            retrieved_after_reopen = db_reopen.get_cf(reopened_cf_handle, key)
            self.assertEqual(retrieved_after_reopen, value)

    def test_18_read_only_open_and_read(self):
        """
        Test opening an existing database in read-only mode and successfully reading data.
        """
        # First, create a DB and put some data in it in read-write mode
        key_rw = b"read_only_test_key"
        value_rw = b"read_only_test_value"
        with pyrex.PyRocksDB(self.db_path) as db_init:
            db_init.put(key_rw, value_rw)
            db_init.put(b"another_key", b"another_value")

        # Now, open the same DB in read-only mode
        with pyrex.PyRocksDB(self.db_path, read_only=True) as db_ro:
            # We can't directly check db_ro.is_read_only_.load() as it's not exposed.
            # The test implicitly verifies this by attempting writes later.
            # The database should be open for reading.
            pass

            # Test successful read
            retrieved = db_ro.get(key_rw)
            self.assertEqual(retrieved, value_rw)
            self.assertEqual(db_ro.get(b"another_key"), b"another_value")
            self.assertIsNone(db_ro.get(b"non_existent_key"))

            # Test iterator in read-only mode
            it = db_ro.new_iterator()
            it.seek_to_first()
            self.assertTrue(it.valid())
            self.assertEqual(it.key(), b"another_key") # Keys are sorted alphabetically
            it.check_status()


    def test_19_read_only_prevent_put(self):
        """
        Test that `put` operation fails in read-only mode.
        """
        # Create a DB first
        with pyrex.PyRocksDB(self.db_path) as db_init:
            db_init.put(b"initial_key", b"initial_value")

        # Open in read-only mode and attempt put
        with pyrex.PyRocksDB(self.db_path, read_only=True) as db_ro:
            with self.assertRaises(pyrex.RocksDBException) as cm:
                db_ro.put(b"new_key", b"new_value")
            self.assertIn(WRITE_ERROR_READONLY_MSG , str(cm.exception))

            # Verify original key is still readable
            self.assertEqual(db_ro.get(b"initial_key"), b"initial_value")
            # Verify new key was NOT added
            self.assertIsNone(db_ro.get(b"new_key"))

    def test_20_read_only_prevent_delete(self):
        """
        Test that `delete` operation fails in read-only mode.
        """
        # Create a DB with data
        key_to_delete = b"to_delete"
        with pyrex.PyRocksDB(self.db_path) as db_init:
            db_init.put(key_to_delete, b"value_to_delete")

        # Open in read-only mode and attempt delete
        with pyrex.PyRocksDB(self.db_path, read_only=True) as db_ro:
            with self.assertRaises(pyrex.RocksDBException) as cm:
                db_ro.delete(key_to_delete)
            self.assertIn(WRITE_ERROR_READONLY_MSG, str(cm.exception))

            # Verify the key was NOT deleted
            self.assertEqual(db_ro.get(key_to_delete), b"value_to_delete")

    def test_21_read_only_prevent_write_batch(self):
        """
        Test that `write` (batch) operation fails in read-only mode.
        """
        # Create a DB with data
        with pyrex.PyRocksDB(self.db_path) as db_init:
            db_init.put(b"batch_original", b"original_value")

        # Open in read-only mode and attempt write batch
        with pyrex.PyRocksDB(self.db_path, read_only=True) as db_ro:
            batch = pyrex.PyWriteBatch()
            batch.put(b"batch_new_key", b"batch_new_value")
            batch.delete(b"batch_original")

            with self.assertRaises(pyrex.RocksDBException) as cm:
                db_ro.write(batch)
            self.assertIn(WRITE_ERROR_READONLY_MSG, str(cm.exception))

            # Verify no changes were applied
            self.assertEqual(db_ro.get(b"batch_original"), b"original_value")
            self.assertIsNone(db_ro.get(b"batch_new_key"))

    def test_22_read_only_prevent_create_cf(self):
        """
        Test that `create_column_family` fails in read-only mode.
        """
        # Create an extended DB first
        with pyrex.PyRocksDBExtended(self.db_path) as db_init:
            db_init.put(b"key", b"value") # Ensure DB is initialized

        # Open in read-only mode and attempt to create CF
        with pyrex.PyRocksDBExtended(self.db_path, read_only=True) as db_ro:
            # We can't directly check db_ro.is_read_only_.load() as it's not exposed.
            with self.assertRaises(pyrex.RocksDBException) as cm:
                db_ro.create_column_family("new_read_only_cf")
            self.assertIn( WRITE_ERROR_READONLY_MSG , str(cm.exception))
            self.assertNotIn("new_read_only_cf", db_ro.list_column_families())

    def test_23_read_only_prevent_drop_cf(self):
        """
        Test that `drop_column_family` fails in read-only mode.
        """
        # Create an extended DB with a CF to drop
        cf_to_drop_name = "cf_to_drop_ro"
        with pyrex.PyRocksDBExtended(self.db_path) as db_init:
            cf_handle = db_init.create_column_family(cf_to_drop_name)
            db_init.put_cf(cf_handle, b"key", b"value")
            self.assertIn(cf_to_drop_name, db_init.list_column_families())

        # Open in read-only mode and attempt to drop CF
        with pyrex.PyRocksDBExtended(self.db_path, read_only=True) as db_ro:
            # We can't directly check db_ro.is_read_only_.load() as it's not exposed.
            cf_handle_ro = db_ro.get_column_family(cf_to_drop_name)
            self.assertIsNotNone(cf_handle_ro) # Should get the handle
            self.assertTrue(cf_handle_ro.is_valid()) # The handle should still be valid from RocksDB's perspective

            with self.assertRaises(pyrex.RocksDBException) as cm:
                db_ro.drop_column_family(cf_handle_ro)
            self.assertIn(WRITE_ERROR_READONLY_MSG, str(cm.exception))

            # Verify CF is still listed and accessible (since drop failed)
            self.assertIn(cf_to_drop_name, db_ro.list_column_families())
            self.assertEqual(db_ro.get_cf(cf_handle_ro, b"key"), b"value")

    def test_24_read_only_non_existent_db(self):
        """
        Test opening a non-existent database in read-only mode (should fail).
        """
        # Ensure the path does not exist initially
        if os.path.exists(self.db_path):
            shutil.rmtree(self.db_path)

        with self.assertRaises(pyrex.RocksDBException) as cm:
            # OpenForReadOnly does not create if missing, so this should fail
            self.db = pyrex.PyRocksDB(self.db_path, read_only=True)
        self.assertIn("No such file or directory", str(cm.exception) or "DB not found")


if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
