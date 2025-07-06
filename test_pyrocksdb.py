import unittest
import os
import shutil
import pyrex # Import your compiled RocksDB wrapper

class TestPyRocksDB(unittest.TestCase):
    """
    Test suite for the PyRocksDB wrapper.
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
            # The C++ destructor handles `delete db_` when `self.db` is garbage collected
            # or when the Python process exits. Explicitly setting to None might help
            # with immediate resource release in some contexts, but is not strictly
            # necessary for correctness due to pybind11's ownership model.
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
        self.assertTrue(options.create_if_missing) # Default should be true
        self.assertIsInstance(options.max_open_files, int)

        # Assert based on the actual default compression observed in the error output.
        # RocksDB's default can sometimes be Snappy if built with it and certain configurations.
        self.assertEqual(options.compression, pyrex.CompressionType.kSnappyCompression)

    def test_04_open_with_custom_options(self):
        """
        Test opening the database with custom options.
        """
        options = pyrex.PyOptions()
        options.create_if_missing = True
        options.error_if_exists = False # Set to False to allow opening for this test
        options.max_open_files = 500
        options.write_buffer_size = 16 * 1024 * 1024 # 16MB
        options.compression = pyrex.CompressionType.kSnappyCompression
        options.max_background_jobs = 2 # This will be overridden by increase_parallelism
        options.increase_parallelism(4) # This method sets max_background_jobs to total_threads
        options.optimize_for_small_db() # This method sets max_open_files to 5000 internally
                                        # AND sets write_buffer_size to 2MB (2 * 1024 * 1024)

        self.db = pyrex.PyRocksDB(self.db_path, options)
        self.assertIsNotNone(self.db)

        # Verify that the options reflect what was set
        retrieved_options = self.db.get_options()
        self.assertTrue(retrieved_options.create_if_missing)
        self.assertFalse(retrieved_options.error_if_exists) # Check the set value
        self.assertEqual(retrieved_options.max_open_files, 5000) # Assert 5000 as set by optimize_for_small_db()
        # Assert 2MB for write_buffer_size as set by optimize_for_small_db()
        self.assertEqual(retrieved_options.write_buffer_size, 2 * 1024 * 1024)
        self.assertEqual(retrieved_options.compression, pyrex.CompressionType.kSnappyCompression)
        # FIXED: Assert 4 for max_background_jobs as set by increase_parallelism(4)
        self.assertEqual(retrieved_options.max_background_jobs, 4)
        # Note: increase_parallelism and optimize_for_small_db are methods
        # that modify internal RocksDB state, not directly readable properties.
        # Testing their *effect* would require more complex benchmarks,
        # but we can at least confirm the options object was passed.

    def test_05_error_if_exists(self):
        """
        Test the error_if_exists option.
        """
        # First, create the DB
        db1 = pyrex.PyRocksDB(self.db_path)
        db1.put(b"key", b"value")
        del db1 # Ensure DB is closed before trying to open again with error_if_exists

        # Now try to open it again with error_if_exists=True
        options = pyrex.PyOptions()
        options.create_if_missing = False # Don't create if missing
        options.error_if_exists = True # Should fail if it exists

        with self.assertRaises(pyrex.RocksDBException) as cm:
            self.db = pyrex.PyRocksDB(self.db_path, options) # Assign to self.db for tearDown
        # The error message from RocksDB is "Invalid argument: ... exists (error_if_exists is true)"
        # We assert for a substring that is consistent.
        self.assertIn("exists (error_if_exists is true)", str(cm.exception))

    def test_06_open_failure_invalid_path(self):
        """
        Test opening the database with an invalid path (e.g., no permissions).
        This test might require specific OS permissions setup to reliably fail.
        """
        # Attempt to open in a restricted directory like /root (Linux/macOS)
        # This will likely fail with a permission denied error.
        restricted_path = "/root/no_access_db"
        if os.name == 'nt': # For Windows, try a path that typically requires admin
            restricted_path = "C:\\Windows\\System32\\no_access_db"

        # Skip if running as root or on systems where this path isn't restricted
        if os.geteuid() == 0 and os.name != 'nt': # Check if running as root on Unix-like
            self.skipTest("Skipping test_06_open_failure_invalid_path: Running as root, path might be accessible.")

        try:
            # Attempt to create the parent directory to ensure the path itself is the issue
            # (not just the non-existence of the parent)
            os.makedirs(os.path.dirname(restricted_path), exist_ok=True)
        except OSError:
            # If we can't even make the parent dir, it's definitely restricted
            pass

        with self.assertRaises(pyrex.RocksDBException) as cm:
            self.db = pyrex.PyRocksDB(restricted_path) # Assign to self.db for tearDown
        self.assertIn("Permission denied", str(cm.exception) or "Error opening DB")


if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)


