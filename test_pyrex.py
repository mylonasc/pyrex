import unittest
import os
import shutil
import pyrex # Import your compiled RocksDB wrapper

class TestPyrex(unittest.TestCase):
    """
    Test suite for the Pyrex (RocksDB) wrapper.
    """
    DB_BASE_PATH = "/tmp/test_pyrex_db"

    @classmethod
    def setUpClass(cls):
        """
        Set up class-level resources.
        Ensures the base test directory exists.
        """
        os.makedirs(cls.DB_BASE_PATH, exist_ok=True)
        print(f"\nCreated base test directory: {cls.DB_BASE_PATH}")

    @classmethod
    def tearDownClass(cls):
        """
        Clean up class-level resources.
        Removes the base test directory and all test databases within it.
        """
        if os.path.exists(cls.DB_BASE_PATH):
            shutil.rmtree(cls.DB_BASE_PATH)
            print(f"Removed base test directory: {cls.DB_BASE_PATH}")

    def setUp(self):
        """
        Set up resources before each test method.
        Creates a unique database path for each test and ensures it's clean.
        """
        # Create a unique database path for each test
        self.db_path = os.path.join(self.DB_BASE_PATH, self._testMethodName)
        if os.path.exists(self.db_path):
            shutil.rmtree(self.db_path)
        print(f"\nSetting up DB for test '{self._testMethodName}' at: {self.db_path}")
        # Initialize db to None for tearDown's check
        self.db = None

    def tearDown(self):
        """
        Clean up resources after each test method.
        Closes the database and removes its directory.
        """
        if hasattr(self, 'db') and self.db is not None:
            del self.db
            self.db = None
        if os.path.exists(self.db_path):
            shutil.rmtree(self.db_path)
            print(f"Cleaned up DB at: {self.db_path}")

    def test_01_basic_put_get(self):
        """
        Test basic put and get operations.
        """
        self.db = pyrex.PyRocksDB(self.db_path)
        self.assertIsNotNone(self.db)

        key = b"test_key_01"
        value = b"test_value_01"
        self.db.put(key, value)

        retrieved_value = self.db.get(key)
        self.assertEqual(retrieved_value, value)

    def test_02_get_non_existent_key(self):
        """
        Test getting a key that does not exist.
        Should return None.
        """
        self.db = pyrex.PyRocksDB(self.db_path)
        self.assertIsNotNone(self.db)

        non_existent_key = b"non_existent_key"
        retrieved_value = self.db.get(non_existent_key)
        self.assertIsNone(retrieved_value)

    def test_03_open_with_default_options(self):
        """
        Test opening the database with default options.
        """
        self.db = pyrex.PyRocksDB(self.db_path)
        self.assertIsNotNone(self.db)

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
        options.compression = pyrex.CompressionType.kSnappyCompression
        options.max_background_jobs = 2
        options.increase_parallelism(4)
        options.optimize_for_small_db()

        self.db = pyrex.PyRocksDB(self.db_path, options)
        self.assertIsNotNone(self.db)

        retrieved_options = self.db.get_options()
        self.assertTrue(retrieved_options.create_if_missing)
        self.assertFalse(retrieved_options.error_if_exists)
        self.assertEqual(retrieved_options.max_open_files, 5000)
        self.assertEqual(retrieved_options.write_buffer_size, 2 * 1024 * 1024)
        self.assertEqual(retrieved_options.compression, pyrex.CompressionType.kSnappyCompression)
        self.assertEqual(retrieved_options.max_background_jobs, 4)

    def test_05_error_if_exists(self):
        """
        Test the error_if_exists option.
        """
        db1 = pyrex.PyRocksDB(self.db_path)
        db1.put(b"key", b"value")
        del db1

        options = pyrex.PyOptions()
        options.create_if_missing = False
        options.error_if_exists = True

        with self.assertRaises(pyrex.RocksDBException) as cm:
            self.db = pyrex.PyRocksDB(self.db_path, options)
        self.assertIn("exists (error_if_exists is true)", str(cm.exception))

    def test_06_open_failure_invalid_path(self):
        """
        Test opening the database with an invalid path (e.g., no permissions).
        """
        restricted_path = "/root/no_access_db"
        if os.name == 'nt':
            restricted_path = "C:\\Windows\\System32\\no_access_db"

        if os.geteuid() == 0 and os.name != 'nt':
            self.skipTest("Skipping test_06_open_failure_invalid_path: Running as root, path might be accessible.")

        try:
            os.makedirs(os.path.dirname(restricted_path), exist_ok=True)
        except OSError:
            pass

        with self.assertRaises(pyrex.RocksDBException) as cm:
            self.db = pyrex.PyRocksDB(restricted_path)
        self.assertIn("Permission denied", str(cm.exception) or "Error opening DB")

    def test_07_write_batch_put_and_delete(self):
        """
        Test atomic write batch operations (put and delete).
        """
        self.db = pyrex.PyRocksDB(self.db_path)
        self.assertIsNotNone(self.db)

        # Create a write batch
        batch = pyrex.PyWriteBatch()
        batch.put(b"batch_key_1", b"batch_value_1")
        batch.put(b"batch_key_2", b"batch_value_2")
        batch.delete(b"batch_key_1") # Delete key_1 within the same batch

        self.db.write(batch)

        # Verify results
        self.assertIsNone(self.db.get(b"batch_key_1")) # Should be deleted
        self.assertEqual(self.db.get(b"batch_key_2"), b"batch_value_2") # Should be present

        # Test clearing the batch
        batch.clear()
        batch.put(b"new_key", b"new_value")
        self.db.write(batch)
        self.assertEqual(self.db.get(b"new_key"), b"new_value")

    def test_08_iterator_basic_traversal(self):
        """
        Test basic iterator traversal (seek_to_first, next, key, value).
        """
        self.db = pyrex.PyRocksDB(self.db_path)
        self.assertIsNotNone(self.db)

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

        # Seek to first and iterate forward
        it.seek_to_first()
        found_keys = []
        while it.valid():
            found_keys.append(it.key())
            it.next()
        self.assertEqual(sorted(found_keys), sorted(list(data.keys())))
        it.check_status() # Check for any iterator errors
        del it # FIXED: Explicitly delete the iterator

    def test_09_iterator_seek_and_prev(self):
        """
        Test iterator seek and previous traversal.
        """
        self.db = pyrex.PyRocksDB(self.db_path)
        self.assertIsNotNone(self.db)

        data = {
            b"a1": b"v1", b"a2": b"v2", b"a3": b"v3",
            b"b1": b"v4", b"b2": b"v5",
            b"c1": b"v6"
        }
        for k, v in data.items():
            self.db.put(k, v)

        it = self.db.new_iterator()

        # Seek to a specific key
        it.seek(b"b1")
        self.assertTrue(it.valid())
        self.assertEqual(it.key(), b"b1")
        self.assertEqual(it.value(), b"v4")

        # Iterate backward
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
        del it # FIXED: Explicitly delete the iterator

    def test_10_iterator_empty_db(self):
        """
        Test iterator behavior on an empty database.
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
        it.check_status() # Should be OK
        del it # FIXED: Explicitly delete the iterator

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)


