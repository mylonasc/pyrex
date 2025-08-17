
[![PyPI version](https://img.shields.io/pypi/v/pyrex-rocksdb.svg)](https://pypi.org/project/pyrex-rocksdb/)
[![Python versions](https://img.shields.io/pypi/pyversions/pyrex-rocksdb.svg)](https://img.shields.io/pypi/pyversions/pyrex-rocksdb/)


![pyrex](https://raw.githubusercontent.com/mylonasc/pyrex/main/assets/logo.png)

# Installation


# pyrex-rocksdb
A python wrapper for the original (C++) version of RocksDB.

Currently MacOS and Linux wheels are available.

## Installation

For linux systems, wheels are provided and can be installed from pypi using:

```bash
pip install pyrex-rocksdb
```

For Windows and MacOS I have built an earlier version of the library.
I will re-build once I include certain other important features in the API that are not yet implemented.



## Motivation

This library is intended for providing a fast, write-optimized, in-process key value (KV) store in python. Therefore the "big brothers" of the database are the likes of MongoDB and Cassandra. The difference is that you don't need a separate server to run this (hence "in-process") and it is designed to be fairly portable. 

RocksDB, which is the underlying storage engine of this database, is an LSM-tree engine. An LSM-tree is different from the ballanced tree index databases (e.g., [B-tree](https://en.wikipedia.org/wiki/B-tree)/ and [B+tree](https://en.wikipedia.org/wiki/B%2B_tree) databases). LSM-tree databases offer very high write throughputs and better space efficiency. See more about the motivation for LSM-tree databases (and RocksDB in particular) in [this talk](https://www.youtube.com/watch?v=V_C-T5S-w8g).

### LSM-tree + SSTable engine basics
To understand where `pyrex` provides efficiency gains, it is important to understand some basics about the underlying `RocksDB` engine. 

RocksDB and LevelDB are **key-value stores** with a **Log-Structured Merge-tree (LSM-tree)** architecture. 

The key components of LSM-tree architectures are 
* A **MemTable** that stores in-memory sorted data
* A set of **Sorted-String tables (SSTables)** which are immutable sorted files on disk where data from the MemTable is flushed
* The process of **Compaction**, which is a background process that merges the SSTables to remove redundant data and keep read performance high.

In such databases, fast writes create many small, sorted data files called SSTables. To prevent reads from slowing down by checking too many files, a background process called compaction merges these SSTables together. This process organizes the data into levels, where newer, overlapping files sit in Level 0 and are progressively merged into higher levels (Level 1, Level 2, etc.). Each higher level contains larger, non-overlapping files, which ensures that finding a key remains efficient and old data is purged to save space. There are several optimizations and configurations possible for these processes (configurability and "pluggability" are commonly cited RocksDB advantages). 

However the main big advantage of RocksDB over LevelDB is its **multi-threaded compaction support** (LevelDB supports only single threaded compaction, which comes with significant performance limitations). 
There are several other configurability advantages RocksDB offers over LevelDB. For a more elaborate enumaration of RocksDB advantages please refer to the [RocksDB wiki](https://github.com/facebook/rocksdb/wiki/Features-Not-in-LevelDB). 

Not all are currently supported by the `pyrex` API, but I'm working on supporting more of them. Feel free to open an issue if there is a feature you want to see (or open a pull request).


## Example usage:

Here is a simple example showing the usage of put/get in the DB:

```python
import pyrex
import os
import shutil

DB_PATH = "./test_rocksdb_minimal"

with pyrex.PyRocksDB(DB_PATH) as db:
    db.put(b"my_key", b"my_value")
    retrieved_value = db.get(b"my_key")

print(f"Retrieved: {retrieved_value.decode()}") # Output: Retrieved: my_value

```

for more examples check the relevant folder and the documentation.

## Installation

<details>
  <summary>Note on CICD</summary>
The windows wheels are failing at the moment.
The CICD workflow for package builds works and passes all tests only for MacOS and Linux. 
</details>


