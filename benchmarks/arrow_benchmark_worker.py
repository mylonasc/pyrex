"""ABR-02: one read-only workload in one fresh process, no coordinator state."""
import gc
import os
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyrex

from arrow_benchmark_support import (
    OrderedDigest, bounds, current_rss_bytes, environment, iterator_records,
    peak_rss_bytes, records_from_iterator, seek_iterator,
)


class Operation:
    def __init__(self, db, options, config):
        self.db, self.options, self.config = db, options, config
        self.mode = config["mode"]
        self.limits = bounds(config["scope"])
        self.output = self.reader = self.iterator = self.scan = None
        self.digest = OrderedDigest()
        self.extraction_ns = self.validation_ns = 0
        self.polars = None
        if self.mode.startswith("polars_"):
            import polars
            self.polars = polars

    @property
    def streaming(self):
        return self.mode.endswith("stream_compute")

    def setup(self):
        if self.mode.startswith("python_"):
            self.iterator = self.db.new_iterator(self.options)
            seek_iterator(self.iterator, self.limits)
            return
        options = dict(self.limits, batch_size=self.config["batch_size"],
                       max_batch_bytes=self.config["max_batch_bytes"],
                       allow_overflow=self.config["allow_overflow"], read_options=self.options)
        if self.config["collect_metrics"]:
            options["collect_metrics"] = True
        self.scan = self.db.scan_arrow(**options)
        self.reader = pa.RecordBatchReader.from_stream(self.scan)

    def run(self):
        if self.streaming:
            self._run_stream()
            return
        start = time.perf_counter_ns()
        if self.mode == "python_iter_collect":
            self.output = list(records_from_iterator(self.iterator, self.limits))
        elif self.mode == "python_iter_then_arrow":
            keys, values = [], []
            for key, value in records_from_iterator(self.iterator, self.limits):
                keys.append(key)
                values.append(value)
            self.output = pa.table({"key": pa.array(keys, type=pa.binary()),
                                    "value": pa.array(values, type=pa.binary())})
        elif self.mode == "native_materialize":
            self.output = self.reader.read_all()
        elif self.mode == "polars_materialize":
            self.output = self.polars.from_arrow(self.reader, rechunk=False)
        else:
            raise ValueError("unknown benchmark mode: " + self.mode)
        self.extraction_ns = time.perf_counter_ns() - start

    def _run_stream(self):
        rows = value_bytes = 0
        while True:
            start = time.perf_counter_ns()
            try:
                batch = self.reader.read_next_batch()
            except StopIteration:
                self.extraction_ns += time.perf_counter_ns() - start
                break
            if self.polars is None:
                value_bytes += pc.sum(pc.binary_length(batch.column("value"))).as_py() or 0
            else:
                frame = self.polars.from_arrow(batch, rechunk=False)
                value_bytes += frame.select(self.polars.col("value").bin.size().cast(self.polars.UInt64).sum()).item() or 0
                del frame
            rows += batch.num_rows
            self.extraction_ns += time.perf_counter_ns() - start
            # Validate the ACTUAL batches consumed, not a second scan. Pause
            # the extraction clock, hash native buffers, then release the
            # batch before requesting another. Wall time includes this pause.
            start = time.perf_counter_ns()
            self.digest.update_batch(batch)
            self.validation_ns += time.perf_counter_ns() - start
            del batch
        self.output = {"rows": rows, "value_bytes": value_bytes}

    def validate(self):
        start = time.perf_counter_ns()
        if self.mode == "python_iter_collect":
            for key, value in self.output:
                self.digest.update_pair(key, value)
        elif not self.streaming:
            table = self.output if self.polars is None else self.output.to_arrow()
            for batch in table.to_batches(max_chunksize=self.config["batch_size"]):
                self.digest.update_batch(batch)
        result = self.digest.result()
        if self.streaming:
            assert self.output == {"rows": result["rows"], "value_bytes": result["value_bytes"]}, "compute parity failed"
        if result != self.config["expected"]:
            raise ValueError(f"ordered digest/count mismatch: {result} != {self.config['expected']}")
        self.validation_ns += time.perf_counter_ns() - start
        return result

    def stats(self):
        return getattr(self.scan, "stats", None)

    def release_consumer(self):
        self.output = self.iterator = None
        if self.reader is not None:
            self.reader.close()
            self.reader = None
        gc.collect()

    def close(self):
        self.release_consumer()
        if self.scan is not None:
            self.scan.close()


def measure(config):
    if config["cpu"] is not None:
        os.sched_setaffinity(0, {config["cpu"]})
    pa.set_cpu_count(config["threads"])
    pa.set_io_thread_count(config["threads"])
    start = time.perf_counter_ns()
    db = pyrex.PyRocksDB(config["path"], read_only=True)
    db_open_ns = time.perf_counter_ns() - start
    operation = None
    try:
        options = pyrex.ReadOptions()
        options.fill_cache = config["fill_cache"]
        options.verify_checksums = config["verify_checksums"]
        operation = Operation(db, options, config)
        start = time.perf_counter_ns()
        if config["cache"] == "warm":
            # Streaming prepass warms this worker's selected RocksDB path
            # without retaining Python rows or seeding the DB in the worker.
            def warm():
                for _ in iterator_records(db, options, operation.limits):
                    pass
            warm()
        warmup_ns = time.perf_counter_ns() - start
        metadata = environment()
        metadata["loaded_polars_version"] = operation.polars.__version__ if operation.polars is not None else None
        metadata["polars_worker_threads"] = operation.polars.thread_pool_size() if operation.polars is not None else None
        gc.collect()
        baseline_peak = peak_rss_bytes()
        baseline_current = current_rss_bytes()
        start = time.perf_counter_ns()
        operation.setup()
        setup_ns = time.perf_counter_ns() - start
        start = time.perf_counter_ns()
        operation.run()
        operation_peak = peak_rss_bytes()
        stats_after_operation = operation.stats()
        validated = operation.validate()
        operation_wall_ns = time.perf_counter_ns() - start
        validation_peak = peak_rss_bytes()
        start = time.perf_counter_ns()
        operation.release_consumer()
        consumer_release_ns = time.perf_counter_ns() - start
        stats_after_release = operation.stats()
        start = time.perf_counter_ns()
        if operation.scan is not None:
            operation.scan.close()
        db.close()
        close_ns = time.perf_counter_ns() - start
        final_stats = operation.stats()
        final_peak = peak_rss_bytes()
        return {
            "status": "ok", "mode": config["mode"], "repeat": config["repeat"],
            "workload": config["scope"]["name"], "validated": validated,
            "environment": metadata,
            "timings_ns": {
                "db_open": db_open_ns, "warmup": warmup_ns, "scan_setup": setup_ns,
                "extraction": operation.extraction_ns, "validation": operation.validation_ns,
                "operation_wall": operation_wall_ns, "consumer_release_with_gc": consumer_release_ns,
                "native_and_db_close": close_ns,
            },
            "extraction_clock_segmented": operation.streaming,
            "validation_in_stream": operation.streaming,
            "payload_decoding": "none; raw KV extraction/compute only",
            "memory": {
                "baseline_current_rss_bytes": baseline_current,
                "baseline_high_water_rss_bytes": baseline_peak,
                "after_operation_high_water_rss_bytes": operation_peak,
                "after_validation_high_water_rss_bytes": validation_peak,
                "worker_peak_rss_bytes": final_peak,
                "high_water_growth_bytes": None if final_peak is None else max(0, final_peak - baseline_peak),
                "current_rss_source": "/proc/self/statm" if baseline_current is not None else "unavailable",
                "peak_rss_source": "worker getrusage(RUSAGE_SELF)" if final_peak is not None else "unavailable",
                "note": "worker peak includes imports/open/warmup/validation; growth is not an allocation count or net mode RSS",
            },
            "native_stats_after_operation": stats_after_operation,
            "native_stats_after_consumer_release": stats_after_release,
            "native_stats_after_close": final_stats,
        }
    finally:
        if operation is not None:
            operation.close()
        db.close()
