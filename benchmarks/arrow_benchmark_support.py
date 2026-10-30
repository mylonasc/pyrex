"""ABR-02 shared dataset, digest and environment contracts.

Core PyRex does not import this optional benchmark module. Ordered digests
use four streams (key lengths, key payload, value lengths, value payload),
so they are independent of Arrow chunk boundaries and require no Python
object per record when validating Arrow batches.
"""
import hashlib
import importlib.metadata
import os
import platform
import random
import struct
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyrex

try:
    import resource
except ImportError:
    resource = None


MODES = (
    "python_iter_collect", "python_iter_then_arrow",
    "native_materialize", "native_stream_compute",
    "polars_materialize", "polars_stream_compute",
)


class OrderedDigest:
    """Versioned, order-sensitive KV digest, native-endian uint64 lengths."""
    def __init__(self):
        self.parts = [hashlib.sha256() for _ in range(4)]
        self.rows = self.key_bytes = self.value_bytes = 0
        self.pack_length = struct.Struct("=Q").pack

    def update_pair(self, key, value):
        self.parts[0].update(self.pack_length(len(key)))
        self.parts[1].update(key)
        self.parts[2].update(self.pack_length(len(value)))
        self.parts[3].update(value)
        self.rows += 1
        self.key_bytes += len(key)
        self.value_bytes += len(value)

    def update_batch(self, batch):
        if batch.schema.names != ["key", "value"]:
            raise ValueError("expected key/value columns in canonical order")
        for column_index in range(2):
            array = batch.column(column_index)
            if array.null_count or not (pa.types.is_binary(array.type) or pa.types.is_large_binary(array.type)):
                raise ValueError("benchmark requires non-null binary key/value arrays")
            if not len(array):
                continue
            # Slice-aware: consumers may export arrays with nonzero offset;
            # lengths are normalized to uint64 for binary/large_binary parity.
            lengths = pc.cast(pc.binary_length(array), pa.uint64())
            length_buffer = memoryview(lengths.buffers()[1])
            self.parts[column_index * 2].update(
                length_buffer[lengths.offset * 8:(lengths.offset + len(lengths)) * 8])
            offsets = memoryview(array.buffers()[1]).cast(
                "q" if pa.types.is_large_binary(array.type) else "i")
            first, last = offsets[array.offset], offsets[array.offset + len(array)]
            if last > first:
                self.parts[column_index * 2 + 1].update(memoryview(array.buffers()[2])[first:last])
            if column_index == 0:
                self.key_bytes += last - first
            else:
                self.value_bytes += last - first
        self.rows += batch.num_rows

    def result(self):
        combined = hashlib.sha256(b"ABR-02/v1/native-u64/" + sys.byteorder.encode())
        for part in self.parts:
            combined.update(part.digest())
        return {"sha256": combined.hexdigest(), "rows": self.rows,
                "key_bytes": self.key_bytes, "value_bytes": self.value_bytes,
                "length_encoding": sys.byteorder + "-endian-uint64"}


def pairs(rows, value_size, variable, seed):
    """Exactly rows deterministic, bytewise-sorted, unique keys/payloads."""
    rng = random.Random(seed)
    other_rows = rows // 10
    for prefix, count in ((b"other/", other_rows), (b"sensor/", rows - other_rows)):
        for index in range(count):
            size = rng.randrange(1, value_size + 1) if variable else value_size
            yield prefix + f"{index:012d}".encode(), rng.getrandbits(size * 8).to_bytes(size, "little")


def scopes(rows, workloads, prefix):
    sensor_rows = rows - rows // 10
    result = []
    for name in workloads:
        if name == "full":
            item = {"name": name, "prefix_hex": None, "start_hex": None, "stop_hex": None}
        elif name == "prefix":
            item = {"name": name, "prefix_hex": prefix.encode().hex(), "start_hex": None, "stop_hex": None}
        else:
            item = {"name": name, "prefix_hex": None,
                    "start_hex": (b"sensor/" + f"{sensor_rows // 4:012d}".encode()).hex(),
                    "stop_hex": (b"sensor/" + f"{sensor_rows * 3 // 4:012d}".encode()).hex()}
        result.append(item)
    return result


def bounds(scope):
    return {name: bytes.fromhex(scope[name + "_hex"]) if scope[name + "_hex"] is not None else None
            for name in ("prefix", "start", "stop")}


def selected(key, limits):
    return ((limits["prefix"] is None or key.startswith(limits["prefix"])) and
            (limits["start"] is None or key >= limits["start"]) and
            (limits["stop"] is None or key < limits["stop"]))


def iterator_records(db, read_options, limits):
    iterator = db.new_iterator(read_options)
    lower = limits["start"]
    if limits["prefix"] is not None:
        lower = max(lower or b"", limits["prefix"])
    if lower is None:
        iterator.seek_to_first()
    else:
        iterator.seek(lower)
    while iterator.valid():
        key = iterator.key()
        if limits["stop"] is not None and key >= limits["stop"]:
            break
        if limits["prefix"] is not None and not key.startswith(limits["prefix"]):
            break
        yield key, iterator.value()
        iterator.next()
    iterator.check_status()


def prepare_dataset(config):
    start = time.perf_counter_ns()
    digests = {scope["name"]: OrderedDigest() for scope in config["scopes"]}
    limits = {scope["name"]: bounds(scope) for scope in config["scopes"]}
    # Only the coordinator-created temporary path is opened; this helper
    # never deletes or replaces an existing user database directory.
    if Path(config["path"]).exists():
        raise ValueError("dataset path must not already exist")
    with pyrex.PyRocksDB(config["path"]) as db:
        batch = pyrex.PyWriteBatch()
        for index, (key, value) in enumerate(pairs(config["rows"], config["value_size"],
                                                  config["variable"], config["seed"]), 1):
            batch.put(key, value)
            for name, digest in digests.items():
                if selected(key, limits[name]):
                    digest.update_pair(key, value)
            if index % 5000 == 0:
                db.write(batch)
                batch = pyrex.PyWriteBatch()
        if config["rows"] % 5000:
            db.write(batch)
    return {"preparation_time_ns": time.perf_counter_ns() - start,
            "reference": {name: digest.result() for name, digest in digests.items()}}


def version(name):
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


def environment():
    cpu_model = None
    if Path("/proc/cpuinfo").exists():
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu_model = line.partition(":")[2].strip()
                break
    return {
        "pid": os.getpid(), "python": platform.python_version(),
        "platform": platform.platform(), "byteorder": sys.byteorder,
        "cpu_model": cpu_model, "logical_cpus": os.cpu_count(),
        "affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
        "pyrex_module": pyrex.__file__, "pyrex_extension": pyrex._pyrex.__file__,
        "installed_pyrex_distribution": version("pyrex-rocksdb"),
        "pyarrow": pa.__version__, "polars_distribution": version("polars"),
        "arrow_cpu_threads": pa.cpu_count(), "arrow_io_threads": pa.io_thread_count(),
        "polars_max_threads_env": os.environ.get("POLARS_MAX_THREADS"),
        "native_build_provenance": "not introspectable; record toolchain/RocksDB via --build-label",
    }


def peak_rss_bytes():
    if resource is None:
        return None
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


def current_rss_bytes():
    if not Path("/proc/self/statm").exists():
        return None
    resident_pages = int(Path("/proc/self/statm").read_text().split()[1])
    return resident_pages * os.sysconf("SC_PAGE_SIZE")
