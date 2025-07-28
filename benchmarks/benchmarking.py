import os
import shutil
import time
import random
import string
import pandas as pd
from tqdm import tqdm

# Try to import plyvel and lmdb
try:
    import plyvel
except ImportError:
    plyvel = None
    print("Warning: plyvel not installed. LevelDB benchmarks will be skipped.")

try:
    import lmdb
except ImportError:
    lmdb = None
    print("Warning: lmdb not installed. LMDB benchmarks will be skipped.")

# --- Configuration ---
DB_DIR = "kv_benchmark_data"
NUM_KEYS = 10_000_000 # Increased number of key-value pairs to operate on
VALUE_SIZE = 500    # Increased size of each value in bytes
BATCH_SIZE = 1000   # Number of operations per batch write
KEY_PREFIX_LENGTH = 8 # Length of the fixed part of the key
RANDOM_KEY_LENGTH = 12 # Length of the random part of the key
MAX_MAP_SIZE_LMDB = 100 * 1024**3 # 100 GB for LMDB environment (adjusted for larger data)

# --- Helper Functions ---
def generate_key(i, random_suffix=False):
    """Generates a key string."""
    if random_suffix:
        return f"{'key_' * (KEY_PREFIX_LENGTH // 4)}{''.join(random.choices(string.ascii_lowercase + string.digits, k=RANDOM_KEY_LENGTH))}"
    return f"{'key_' * (KEY_PREFIX_LENGTH // 4)}{i:0{RANDOM_KEY_LENGTH}d}"

def generate_value(size):
    """Generates a random byte string of a given size."""
    return os.urandom(size)

def clean_db_dir(db_path):
    """Removes the database directory if it exists."""
    if os.path.exists(db_path):
        shutil.rmtree(db_path)
        print(f"Cleaned up {db_path}")

# --- Base Benchmark Class ---
class BaseBenchmark:
    def __init__(self, db_type, db_path):
        self.db_type = db_type
        self.db_path = db_path
        self.db = None
        self.results = []
        self.keys = [] # To store generated keys for read/delete tests

    def _record_result(self, workload, operations, time_taken_sec, notes=""):
        """Records a single benchmark result."""
        ops_per_sec = operations / time_taken_sec if time_taken_sec > 0 else float('inf')
        self.results.append({
            "db_type": self.db_type,
            "workload": workload,
            "operations": operations,
            "time_taken_sec": time_taken_sec,
            "ops_per_sec": ops_per_sec,
            "notes": notes
        })
        print(f"  {workload}: {operations} ops in {time_taken_sec:.4f}s ({ops_per_sec:.2f} ops/sec)")

    def setup(self):
        """Sets up the database for benchmarking."""
        clean_db_dir(self.db_path)
        os.makedirs(self.db_path, exist_ok=True)
        print(f"Setting up {self.db_type} at {self.db_path}")

    def teardown(self):
        """Cleans up the database after benchmarking."""
        # Removed self.db.close() from here.
        # Each subclass is responsible for closing its specific database/environment.
        self.db = None # Ensure db reference is cleared
        print(f"Tearing down {self.db_type} at {self.db_path}")
        clean_db_dir(self.db_path)

    def _populate_initial_data(self, num_keys, sequential=True):
        """Populates the DB with initial data for read/delete tests."""
        print(f"Populating initial data ({num_keys} keys)...")
        self.keys = []
        value = generate_value(VALUE_SIZE)
        if self.db_type == "Plyvel":
            with self.db.write_batch() as wb:
                for i in tqdm(range(num_keys), desc="Populating Plyvel"):
                    key = generate_key(i, random_suffix=not sequential)
                    wb.put(key.encode('utf-8'), value)
                    self.keys.append(key)
        elif self.db_type == "LMDB":
            # Corrected: begin transaction on the environment, not the database object
            with self.env.begin(write=True) as txn:
                for i in tqdm(range(num_keys), desc="Populating LMDB"):
                    key = generate_key(i, random_suffix=not sequential)
                    txn.put(key.encode('utf-8'), value, db=self.db) # Pass db handle for named dbs, or default
                    self.keys.append(key)
        random.shuffle(self.keys) # Shuffle for random reads/deletes

    def run_sequential_writes(self, num_ops=NUM_KEYS):
        raise NotImplementedError

    def run_random_writes(self, num_ops=NUM_KEYS):
        raise NotImplementedError

    def run_batch_writes(self, num_ops=NUM_KEYS, batch_size=BATCH_SIZE):
        raise NotImplementedError

    def run_sequential_reads(self, num_ops=NUM_KEYS):
        raise NotImplementedError

    def run_random_reads(self, num_ops=NUM_KEYS):
        raise NotImplementedError

    def run_random_deletes(self, num_ops=NUM_KEYS):
        raise NotImplementedError

    def run_iteration(self):
        raise NotImplementedError

    def run_all_benchmarks(self):
        print(f"\n--- Running Benchmarks for {self.db_type} ---")
        self.setup()

        # Sequential Writes
        self.run_sequential_writes()
        self.teardown()
        self.setup() # Re-setup for next test

        # Random Writes
        self.run_random_writes()
        self.teardown()
        self.setup()

        # Batch Writes
        self.run_batch_writes()
        self.teardown()
        self.setup()

        # Populate for read/delete/iteration tests
        self._populate_initial_data(NUM_KEYS, sequential=True)
        self.run_sequential_reads()
        self.run_iteration() # Moved iteration here to run on populated DB

        # Populate for random read/delete tests (with random keys)
        self.teardown()
        self.setup()
        self._populate_initial_data(NUM_KEYS, sequential=False)
        self.run_random_reads()
        self.run_random_deletes()


        # Specific compaction test for Plyvel
        if self.db_type == "Plyvel":
            self.run_compaction()

        self.teardown()
        return pd.DataFrame(self.results)

# --- Plyvel Benchmark Class ---
class PlyvelBenchmark(BaseBenchmark):
    def __init__(self, db_path):
        super().__init__("Plyvel", db_path)

    def setup(self):
        super().setup()
        self.db = plyvel.DB(self.db_path, create_if_missing=True)

    def teardown(self):
        if self.db:
            self.db.close() # Plyvel DB object has a close method
        super().teardown()

    def run_sequential_writes(self, num_ops=NUM_KEYS):
        start_time = time.perf_counter()
        value = generate_value(VALUE_SIZE)
        for i in tqdm(range(num_ops), desc="Plyvel Sequential Writes"):
            key = generate_key(i).encode('utf-8')
            self.db.put(key, value)
        end_time = time.perf_counter()
        self._record_result("Sequential Writes", num_ops, end_time - start_time)

    def run_random_writes(self, num_ops=NUM_KEYS):
        start_time = time.perf_counter()
        value = generate_value(VALUE_SIZE)
        for i in tqdm(range(num_ops), desc="Plyvel Random Writes"):
            key = generate_key(i, random_suffix=True).encode('utf-8')
            self.db.put(key, value)
        end_time = time.perf_counter()
        self._record_result("Random Writes", num_ops, end_time - start_time)

    def run_batch_writes(self, num_ops=NUM_KEYS, batch_size=BATCH_SIZE):
        start_time = time.perf_counter()
        value = generate_value(VALUE_SIZE)
        with self.db.write_batch() as wb:
            for i in tqdm(range(num_ops), desc="Plyvel Batch Writes"):
                key = generate_key(i, random_suffix=True).encode('utf-8')
                wb.put(key, value)
        end_time = time.perf_counter()
        self._record_result("Batch Writes", num_ops, end_time - start_time)

    def run_sequential_reads(self, num_ops=NUM_KEYS):
        start_time = time.perf_counter()
        read_count = 0
        for i in tqdm(range(num_ops), desc="Plyvel Sequential Reads"):
            key = generate_key(i).encode('utf-8')
            self.db.get(key)
            read_count += 1
        end_time = time.perf_counter()
        self._record_result("Sequential Reads", read_count, end_time - start_time)

    def run_random_reads(self, num_ops=NUM_KEYS):
        start_time = time.perf_counter()
        read_count = 0
        random.shuffle(self.keys) # Ensure random access
        for key_str in tqdm(self.keys[:num_ops], desc="Plyvel Random Reads"):
            self.db.get(key_str.encode('utf-8'))
            read_count += 1
        end_time = time.perf_counter()
        self._record_result("Random Reads", read_count, end_time - start_time)

    def run_random_deletes(self, num_ops=NUM_KEYS):
        start_time = time.perf_counter()
        delete_count = 0
        random.shuffle(self.keys) # Ensure random access
        for key_str in tqdm(self.keys[:num_ops], desc="Plyvel Random Deletes"):
            self.db.delete(key_str.encode('utf-8'))
            delete_count += 1
        end_time = time.perf_counter()
        self._record_result("Random Deletes", delete_count, end_time - start_time)

    def run_iteration(self):
        start_time = time.perf_counter()
        count = 0
        for key, value in tqdm(self.db, desc="Plyvel Iteration"):
            count += 1
        end_time = time.perf_counter()
        self._record_result("Iteration", count, end_time - start_time)

    def run_compaction(self):
        print("Starting Plyvel Compaction...")
        start_time = time.perf_counter()
        # Compacting the entire key range
        self.db.compact_range(start=None, stop=None)
        end_time = time.perf_counter()
        self._record_result("Compaction", 1, end_time - start_time, "Full DB compaction")

# --- LMDB Benchmark Class ---
class LMDBBenchmark(BaseBenchmark):
    def __init__(self, db_path):
        super().__init__("LMDB", db_path)
        self.env = None

    def setup(self):
        super().setup()
        # LMDB environment setup
        # map_size is crucial, needs to be large enough for your data + overhead
        self.env = lmdb.open(self.db_path, map_size=MAX_MAP_SIZE_LMDB,
                             subdir=True, # Creates a directory for the DB files
                             max_dbs=1,   # Max number of named databases (we use one)
                             sync=False,  # Don't force fsync on every commit for speed
                             writemap=True, # Use write-through memory map for faster writes
                             metasync=False # Don't sync metadata on every commit
                            )
        self.db = self.env.open_db() # Open the default unnamed DB

    def teardown(self):
        if self.env:
            self.env.close() # LMDB environment object has a close method
        super().teardown()

    def run_sequential_writes(self, num_ops=NUM_KEYS):
        start_time = time.perf_counter()
        value = generate_value(VALUE_SIZE)
        with self.env.begin(write=True) as txn:
            for i in tqdm(range(num_ops), desc="LMDB Sequential Writes"):
                key = generate_key(i).encode('utf-8')
                txn.put(key, value, db=self.db)
        end_time = time.perf_counter()
        self._record_result("Sequential Writes", num_ops, end_time - start_time)

    def run_random_writes(self, num_ops=NUM_KEYS):
        start_time = time.perf_counter()
        value = generate_value(VALUE_SIZE)
        with self.env.begin(write=True) as txn:
            for i in tqdm(range(num_ops), desc="LMDB Random Writes"):
                key = generate_key(i, random_suffix=True).encode('utf-8')
                txn.put(key, value, db=self.db)
        end_time = time.perf_counter()
        self._record_result("Random Writes", num_ops, end_time - start_time)

    def run_batch_writes(self, num_ops=NUM_KEYS, batch_size=BATCH_SIZE):
        start_time = time.perf_counter()
        value = generate_value(VALUE_SIZE)
        for i in tqdm(range(0, num_ops, batch_size), desc="LMDB Batch Writes"):
            with self.env.begin(write=True) as txn:
                for j in range(i, min(i + batch_size, num_ops)):
                    key = generate_key(j, random_suffix=True).encode('utf-8')
                    txn.put(key, value, db=self.db)
        end_time = time.perf_counter()
        self._record_result("Batch Writes", num_ops, end_time - start_time)

    def run_sequential_reads(self, num_ops=NUM_KEYS):
        start_time = time.perf_counter()
        read_count = 0
        with self.env.begin() as txn:
            for i in tqdm(range(num_ops), desc="LMDB Sequential Reads"):
                key = generate_key(i).encode('utf-8')
                txn.get(key, db=self.db)
                read_count += 1
        end_time = time.perf_counter()
        self._record_result("Sequential Reads", read_count, end_time - start_time)

    def run_random_reads(self, num_ops=NUM_KEYS):
        start_time = time.perf_counter()
        read_count = 0
        random.shuffle(self.keys) # Ensure random access
        with self.env.begin() as txn:
            for key_str in tqdm(self.keys[:num_ops], desc="LMDB Random Reads"):
                txn.get(key_str.encode('utf-8'), db=self.db)
                read_count += 1
        end_time = time.perf_counter()
        self._record_result("Random Reads", read_count, end_time - start_time)

    def run_random_deletes(self, num_ops=NUM_KEYS):
        start_time = time.perf_counter()
        delete_count = 0
        random.shuffle(self.keys) # Ensure random access
        with self.env.begin(write=True) as txn:
            for key_str in tqdm(self.keys[:num_ops], desc="LMDB Random Deletes"):
                txn.delete(key_str.encode('utf-8'), db=self.db)
                delete_count += 1
        end_time = time.perf_counter()
        self._record_result("Random Deletes", delete_count, end_time - start_time)

    def run_iteration(self):
        start_time = time.perf_counter()
        count = 0
        with self.env.begin() as txn:
            # When iterating a named DB, you need to pass the db handle to cursor()
            cursor = txn.cursor(db=self.db)
            for key, value in tqdm(cursor, desc="LMDB Iteration"):
                count += 1
        end_time = time.perf_counter()
        self._record_result("Iteration", count, end_time - start_time)

    # LMDB does not have an explicit 'compact' operation like LSM-trees.
    # Its B-tree structure handles space reclamation differently.
    # We will just note this here.
    def run_compaction(self):
        self._record_result("Compaction", 0, 0, "N/A - LMDB handles space reclamation differently (B-tree)")


# --- Main Execution ---
if __name__ == "__main__":
    all_results_df = pd.DataFrame()

    # Run Plyvel benchmarks
    if plyvel:
        plyvel_benchmark = PlyvelBenchmark(os.path.join(DB_DIR, "plyvel_db"))
        df_plyvel = plyvel_benchmark.run_all_benchmarks()
        all_results_df = pd.concat([all_results_df, df_plyvel], ignore_index=True)

    # Run LMDB benchmarks
    if lmdb:
        lmdb_benchmark = LMDBBenchmark(os.path.join(DB_DIR, "lmdb_db"))
        df_lmdb = lmdb_benchmark.run_all_benchmarks()
        all_results_df = pd.concat([all_results_df, df_lmdb], ignore_index=True)

    print("\n--- Benchmark Results ---")
    print(all_results_df)

    # Optional: Save results to CSV
    # all_results_df.to_csv("kv_benchmark_results.csv", index=False)
    # print("\nResults saved to kv_benchmark_results.csv")

    print("\nBenchmark complete. Remember to analyze the 'ops_per_sec' for performance comparison.")
    print("Consider varying NUM_KEYS and VALUE_SIZE for different workload scenarios.")

