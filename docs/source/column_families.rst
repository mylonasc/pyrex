.. \_column\_family\_example:

Column Family Usage Example
===========================

This example demonstrates how to use `PyRocksDBExtended` to manage and interact with multiple column families within a RocksDB database.

.. note::

        Column families are an advanced feature of RocksDB that allow you to logically
        partition your data within a single database instance. This can be useful for
        managing different types of data with different access patterns or configurations.

        First, ensure you have the `pyrex` module installed (your Python wrapper for RocksDB).


.. code-block:: python

        import _pyrex
        import os
        import shutil

        # Define a path for our test database
        DB_PATH = "./rocksdb_cf_example"

        # Clean up any previous database instance for a fresh start
        if os.path.exists(DB_PATH):
            shutil.rmtree(DB_PATH)
            print(f"Cleaned up existing database at {DB_PATH}")

        print("--- Starting Column Family Example ---")

        # 1. Open a PyRocksDBExtended instance
        #    This class provides methods for managing column families.
        #    We use a 'with' statement to ensure the database is properly closed.
        try:
            with _pyrex.PyRocksDBExtended(DB_PATH) as db:
                print(f"\nOpened database at {DB_PATH}")

                # The default column family is always present
                default_cf = db.default_cf
                print(f"Default Column Family: {default_cf.name}")

                # 2. List initial column families (should only be 'default')
                cfs_before_creation = db.list_column_families()
                print(f"Column Families before creation: {cfs_before_creation}")

                # 3. Create new column families
                #    You can optionally pass PyOptions to configure the new CF.
                #    Here, we'll create two new column families: 'users' and 'products'.
                users_cf = db.create_column_family("users")
                products_cf = db.create_column_family("products")

                print(f"\nCreated column families: '{users_cf.name}' and '{products_cf.name}'")

                # 4. List column families again to see the newly created ones
                cfs_after_creation = db.list_column_families()
                print(f"Column Families after creation: {cfs_after_creation}")

                # 5. Put and Get data into specific column families
                print("\n--- Writing and Reading Data in Column Families ---")

                # Put data into the 'users' column family
                db.put_cf(users_cf, b"user:1", b"Alice")
                db.put_cf(users_cf, b"user:2", b"Bob")
                print(f"Put 'Alice' for 'user:1' in '{users_cf.name}' CF")
                print(f"Put 'Bob' for 'user:2' in '{users_cf.name}' CF")

                # Put data into the 'products' column family
                db.put_cf(products_cf, b"prod:A", b"Laptop")
                db.put_cf(products_cf, b"prod:B", b"Mouse")
                print(f"Put 'Laptop' for 'prod:A' in '{products_cf.name}' CF")
                print(f"Put 'Mouse' for 'prod:B' in '{products_cf.name}' CF")

                # Put data into the default column family
                db.put(b"app_setting:theme", b"dark")
                print(f"Put 'dark' for 'app_setting:theme' in '{default_cf.name}' CF")

                # Get data from the 'users' column family
                alice_data = db.get_cf(users_cf, b"user:1")
                print(f"Get 'user:1' from '{users_cf.name}' CF: {alice_data.decode()}")

                # Get data from the 'products' column family
                laptop_data = db.get_cf(products_cf, b"prod:A")
                print(f"Get 'prod:A' from '{products_cf.name}' CF: {laptop_data.decode()}")

                # Attempt to get 'user:1' from the 'products' CF (should be None)
                non_existent_data = db.get_cf(products_cf, b"user:1")
                print(f"Get 'user:1' from '{products_cf.name}' CF (expected None): {non_existent_data}")

                # Get data from the default column family
                theme_data = db.get(b"app_setting:theme")
                print(f"Get 'app_setting:theme' from '{default_cf.name}' CF: {theme_data.decode()}")

                # 6. Delete data from a specific column family
                print("\n--- Deleting Data in Column Families ---")
                db.delete_cf(users_cf, b"user:2")
                print(f"Deleted 'user:2' from '{users_cf.name}' CF")

                # Verify deletion
                bob_data_after_del = db.get_cf(users_cf, b"user:2")
                print(f"Get 'user:2' from '{users_cf.name}' CF after deletion: {bob_data_after_del}")

                # 7. Iterate over a specific column family
                print(f"\n--- Iterating over '{products_cf.name}' Column Family ---")
                with db.new_cf_iterator(products_cf) as it:
                    it.seek_to_first()
                    while it.valid():
                        print(f"  Key: {it.key().decode()}, Value: {it.value().decode()}")
                        it.next()

                # 8. Drop a column family
                print(f"\n--- Dropping Column Family '{products_cf.name}' ---")
                db.drop_column_family(products_cf)
                print(f"Dropped column family: '{products_cf.name}'")

                # 9. List column families again to confirm deletion
                cfs_after_drop = db.list_column_families()
                print(f"Column Families after dropping '{products_cf.name}': {cfs_after_drop}")

                # Attempting to use a dropped column family handle will raise an exception
                try:
                    db.put_cf(products_cf, b"new_prod", b"TV")
                except _pyrex.RocksDBException as e:
                    print(f"Caught expected error when using dropped CF handle: {e}")

        except _pyrex.RocksDBException as e:
            print(f"An error occurred: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        finally:
            # Final cleanup
            if os.path.exists(DB_PATH):
                shutil.rmtree(DB_PATH)
                print(f"\nSuccessfully cleaned up database at {DB_PATH}")
