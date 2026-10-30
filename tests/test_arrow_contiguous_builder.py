"""ABR-07: adopted payloads retain exact output capacity and ownership."""
import gc

import pytest
import pyrex

pa = pytest.importorskip("pyarrow")


@pytest.mark.parametrize("value_size", [0, 1, 63, 64, 4096])
@pytest.mark.parametrize("max_bytes", [1024, 65536])
def test_contiguous_values_are_adopted_and_independently_owned(tmp_path, value_size, max_bytes):
    with pyrex.PyRocksDB(str(tmp_path / "db")) as db:
        expected = []
        for i in range(33):
            key, value = f"k{i:04d}".encode(), bytes([i]) * value_size
            db.put(key, value)
            expected.append(dict(key=key, value=value))
        scan = db.scan_arrow(batch_size=7, max_batch_bytes=max_bytes, allow_overflow=True, collect_metrics=True)
        assert scan.configuration["value_staging_layout"] == "contiguous"
        with pa.RecordBatchReader.from_stream(scan) as reader:
            retained = list(reader)
        db.close()
        result = pa.Table.from_batches(retained)
        assert result.to_pylist() == expected
        stats = scan.stats
        assert stats["value_bytes_copied_to_output"] == 0
        assert stats["value_bytes_emitted"] == 33 * value_size
        assert stats["buffer_capacity_bytes_emitted"] == stats["output_bytes_emitted"] + (
            stats["batches_emitted"] if not value_size else 0)
        assert stats["live_buffer_bytes"] == stats["buffer_capacity_bytes_emitted"]
        del result, retained
        gc.collect()
        assert scan.stats["live_buffer_bytes"] == 0


def test_point_prefetch_keeps_its_distinct_copy_accounting(tmp_path):
    with pyrex.PyRocksDB(str(tmp_path / "db")) as db:
        db.put(b"k", b"v" * 64)
        scan = db.multi_get_arrow([b"k", b"missing"], collect_metrics=True)
        with pa.RecordBatchReader.from_stream(scan) as reader:
            assert reader.read_all().num_rows == 2
        assert scan.configuration["value_staging_layout"] == "strings"
        assert scan.stats["value_bytes_copied_to_output"] == scan.stats["value_bytes_emitted"] == 64


def test_builder_benchmark_smoke(tmp_path):
    import json
    from pathlib import Path
    import subprocess
    import sys
    script = Path(__file__).resolve().parents[1] / "benchmarks/bench_arrow_builder.py"
    result = subprocess.run([sys.executable, str(script), "--rows", "100", "--repeat", "2",
                             "--variable", "--metrics", "--work-dir", str(tmp_path)],
                            capture_output=True, text=True, check=True, timeout=30)
    report = json.loads(result.stdout)
    assert len(report["ordered_digest"]) == 64 and len(report["samples_ns"]) == 2
    assert report["native_stats_last_repeat_while_retained"]["value_bytes_copied_to_output"] == 0
