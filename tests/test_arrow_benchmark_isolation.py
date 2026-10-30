"""ABR-02 harness contracts; deliberately no performance thresholds."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest

pytest.importorskip("pyarrow")
ROOT = Path(__file__).resolve().parents[1]
ENTRY = ROOT / "benchmarks/bench_arrow_scan.py"


def invoke(root, *arguments, check=True):
    root.mkdir(exist_ok=True)
    output = root / "report.json"
    command = [sys.executable, str(ENTRY), "--isolated", "--work-dir", str(root),
               "--output", str(output), "--timeout", "30", *arguments]
    completed = subprocess.run(command, capture_output=True, text=True, timeout=90)
    if check:
        assert completed.returncode == 0, completed.stderr + completed.stdout[-3000:]
    report = json.loads(output.read_text()) if output.exists() else None
    if report is not None:
        assert json.loads(completed.stdout) == report
    return completed, report


@pytest.fixture(scope="module")
def full_report(tmp_path_factory):
    root = tmp_path_factory.mktemp("abr02-isolated")
    marker = root / "unrelated-user-file"
    marker.write_text("keep me")
    _, report = invoke(root, "--rows", "20", "--value-size", "11", "--repeat", "2",
                       "--batch-size", "3", "--max-batch-bytes", "128")
    assert marker.read_text() == "keep me"
    return report


def test_each_mode_repeat_scope_is_a_fresh_validated_worker(full_report):
    report = full_report
    assert report["status"] == "ok"
    assert len(report["samples"]) == 4 * 3 * 2
    assert len(report["summary"]) == 4 * 3
    coordinator_pid = report["coordinator"]["pid"]
    reference = report["datasets"][0]["reference"]
    for sample in report["samples"]:
        assert sample["environment"]["pid"] != coordinator_pid
        assert sample["validated"] == reference[sample["workload"]]
        assert sample["environment"]["arrow_cpu_threads"] == 1
        assert sample["environment"]["arrow_io_threads"] == 1
        assert sample["timings_ns"]["operation_wall"] >= (
            sample["timings_ns"]["extraction"] + sample["timings_ns"]["validation"])
    # All result datasets belong to the runner-owned temporary directory and
    # have been removed. The unrelated file in the supplied parent survived.
    assert all(not Path(dataset["parameters"]["path"]).exists() for dataset in report["datasets"])


def test_memory_native_retention_and_repeat_distributions(full_report):
    for sample in full_report["samples"]:
        memory = sample["memory"]
        peak = memory["worker_peak_rss_bytes"]
        if peak is not None:
            assert peak >= memory["baseline_high_water_rss_bytes"]
            assert memory["high_water_growth_bytes"] >= 0
        stats = sample["native_stats_after_operation"]
        if sample["mode"].startswith("python_"):
            assert stats is None
        else:
            assert stats["rows_emitted"] == sample["validated"]["rows"]
            assert stats["output_bytes_emitted"] == (
                sample["validated"]["key_bytes"] + sample["validated"]["value_bytes"] +
                stats["offset_bytes_emitted"])
            assert sample["native_stats_after_consumer_release"]["live_buffer_bytes"] == 0
            assert sample["native_stats_after_close"]["live_buffer_bytes"] == 0
            if sample["mode"].endswith("stream_compute"):
                assert stats["live_buffer_bytes"] == 0
                assert sample["extraction_clock_segmented"] is True
                assert sample["validation_in_stream"] is True
    for summary in full_report["summary"]:
        assert summary["repeats"] == 2
        for distribution in summary["timings_ns"].values():
            assert len(distribution["samples"]) == 2
            assert distribution["min"] <= distribution["median"] <= distribution["max"]


def test_variable_scales_empty_range_and_reopened_not_cold(tmp_path):
    _, report = invoke(tmp_path, "--scales", "1", "11", "--value-shapes", "variable",
                       "--workloads", "range", "--modes", "native_stream_compute",
                       "--repeat", "1", "--cache", "reopened", "--no-metrics")
    assert len(report["datasets"]) == 2
    assert [sample["validated"]["rows"] for sample in report["samples"]] == [0, 5]
    assert report["method"]["cold_cache_claim"] is False
    assert "uncontrolled" in report["method"]["cache"]
    assert all(sample["native_stats_after_operation"] is None for sample in report["samples"])


def test_optional_polars_compute_and_materialization(tmp_path):
    pytest.importorskip("polars")
    _, report = invoke(tmp_path, "--rows", "17", "--variable", "--workloads", "prefix",
                       "--modes", "polars_materialize", "polars_stream_compute", "--repeat", "1")
    for sample in report["samples"]:
        assert sample["validated"]["rows"] == 16
        assert sample["environment"]["polars_worker_threads"] == 1
        assert sample["native_stats_after_consumer_release"]["live_buffer_bytes"] == 0


def test_native_size_failure_is_not_reported_as_success(tmp_path):
    completed, report = invoke(tmp_path, "--rows", "2", "--workloads", "full",
                               "--modes", "native_materialize", "--repeat", "1",
                               "--max-batch-bytes", "1", check=False)
    assert completed.returncode == 1
    assert report["status"] == "failed"
    assert "max_batch_bytes" in report["error"]
    assert report["failed_job"]["parameters"]["max_batch_bytes"] == 1
    assert report["samples"] == report["summary"] == []


@pytest.mark.parametrize("arguments", [
    ["--rows", "0"], ["--repeat", "0"], ["--value-size", "0"],
    ["--cache", "cold"], ["--timeout", "nan"], ["--cpu", "-1"],
])
def test_invalid_inputs_rejected_before_dataset_creation(tmp_path, arguments):
    completed, report = invoke(tmp_path, *arguments, check=False)
    assert completed.returncode == 2
    assert report is None
    assert not list(tmp_path.glob("pyrex-abr02-*"))


def test_timeouts_and_missing_optional_modes_are_explicit(tmp_path, monkeypatch, capsys):
    monkeypatch.syspath_prepend(str(ROOT / "benchmarks"))
    spec = importlib.util.spec_from_file_location("abr02_runner_under_test", ROOT / "benchmarks/bench_arrow_isolated.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    cli = runner.parser()
    args = cli.parse_args(["--rows", "1", "--work-dir", str(tmp_path), "--include-polars"])
    monkeypatch.setattr(runner.importlib.util, "find_spec", lambda _: None)
    def timeout(*_):
        raise subprocess.TimeoutExpired("fake-worker", 30)
    monkeypatch.setattr(runner, "child", timeout)
    assert runner.run(args, cli) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "failed" and "TimeoutExpired" in result["error"]
    assert {item["mode"] for item in result["skipped"]} == {"polars_materialize", "polars_stream_compute"}


def test_worker_refuses_corrupted_expected_digest(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "benchmarks"))
    import arrow_benchmark_support as support
    import arrow_benchmark_worker as worker
    scope = support.scopes(5, ["full"], "sensor/")[0]
    config = {"path": str(tmp_path / "db"), "rows": 5, "value_size": 8, "variable": False,
              "seed": 4, "scopes": [scope]}
    expected = support.prepare_dataset(config)["reference"]["full"]
    expected["sha256"] = "corrupted"
    config.update(mode="native_stream_compute", scope=scope, expected=expected,
                  cpu=None, threads=1, fill_cache=True, verify_checksums=True,
                  cache="reopened", collect_metrics=True, batch_size=2,
                  max_batch_bytes=1024, allow_overflow=False, repeat=0)
    threads = worker.pa.cpu_count(), worker.pa.io_thread_count()
    try:
        with pytest.raises(ValueError, match="ordered digest/count mismatch"):
            worker.measure(config)
    finally:
        worker.pa.set_cpu_count(threads[0])
        worker.pa.set_io_thread_count(threads[1])
