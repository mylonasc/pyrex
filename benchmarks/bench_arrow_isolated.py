"""ABR-02 fresh-process Arrow benchmark runner (also --isolated on the old CLI).

Each dataset is seeded once in a separate process. Every mode/repeat opens
it in a NEW read-only worker, validates actual ordered output and reports
its own RSS/phase counters. No cold-cache claim or global cache manipulation.
"""
import argparse
import importlib.util
import json
import math
import os
from pathlib import Path
import random
import statistics
import subprocess
import sys
import tempfile
import time

from arrow_benchmark_support import MODES, environment, prepare_dataset, scopes

SCRIPT = Path(__file__).resolve()
DEFAULT_MODES = list(MODES[:4])


def distribution(values):
    return {"samples": values, "min": min(values), "max": max(values),
            "median": statistics.median(values), "mean": statistics.mean(values),
            "stdev": statistics.stdev(values) if len(values) > 1 else 0,
            "p95_nearest_rank": sorted(values)[math.ceil(len(values) * 0.95) - 1]}


def summarize(samples):
    grouped = {}
    for sample in samples:
        key = (sample["dataset_id"], sample["workload"], sample["mode"])
        grouped.setdefault(key, []).append(sample)
    result = []
    for (dataset_id, workload, mode), runs in grouped.items():
        timings = {name: distribution([run["timings_ns"][name] for run in runs])
                   for name in runs[0]["timings_ns"]}
        peaks = [run["memory"]["worker_peak_rss_bytes"] for run in runs]
        native = {}
        for phase in ("after_operation", "after_consumer_release", "after_close"):
            stats = [run["native_stats_" + phase] for run in runs]
            native[phase] = None if any(s is None for s in stats) else {
                name: distribution([s[name] for s in stats]) for name in stats[0]}
        result.append({
            "dataset_id": dataset_id, "workload": workload, "mode": mode,
            "rows": runs[0]["validated"]["rows"], "repeats": len(runs),
            "timings_ns": timings,
            "worker_peak_rss_bytes": None if any(p is None for p in peaks) else distribution(peaks),
            "native_stats": native,
            "extraction_clock_segmented": runs[0]["extraction_clock_segmented"],
        })
    return result


def child(task, config, timeout):
    env = dict(os.environ, POLARS_MAX_THREADS=str(config.get("threads", 1)))
    process = subprocess.run([sys.executable, str(SCRIPT), "--worker", task],
                             input=json.dumps(config), text=True, capture_output=True,
                             timeout=timeout, env=env)
    if process.returncode:
        raise RuntimeError(f"{task} worker failed ({config.get('mode', 'dataset')}): {process.stderr.strip()}")
    try:
        result = json.loads(process.stdout)
    except ValueError as exc:
        raise RuntimeError(f"{task} worker produced invalid JSON: {process.stdout[:300]}") from exc
    if process.stderr:
        result["worker_stderr"] = process.stderr
    return result


def parser():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--rows", type=int, default=100000)
    cli.add_argument("--scales", nargs="+", type=int, help="e.g. 10000 100000 1000000; 10000000 is opt-in")
    cli.add_argument("--value-size", type=int, default=64)
    cli.add_argument("--variable", action="store_true", help="legacy flag for one variable-length profile")
    cli.add_argument("--value-shapes", nargs="+", choices=["fixed", "variable"])
    cli.add_argument("--workloads", nargs="+", choices=["full", "prefix", "range"])
    cli.add_argument("--prefix", default=None)
    cli.add_argument("--modes", nargs="+", choices=MODES, default=None)
    cli.add_argument("--include-polars", action="store_true")
    cli.add_argument("--batch-size", type=int, default=4096)
    cli.add_argument("--max-batch-bytes", type=int, default=8 * 1024 * 1024)
    cli.add_argument("--allow-overflow", action="store_true")
    cli.add_argument("--repeat", type=int, default=3)
    cli.add_argument("--seed", type=int, default=1729)
    cli.add_argument("--cache", choices=["warm", "reopened"], default="warm",
                     help="warm=selected iterator prepass; reopened=no prepass, OS cache uncontrolled (NOT cold)")
    cli.add_argument("--threads", type=int, default=1)
    cli.add_argument("--cpu", type=int)
    cli.add_argument("--no-metrics", action="store_true", help="native instrumentation ablation / historical builds")
    cli.add_argument("--no-fill-cache", action="store_true")
    cli.add_argument("--no-verify-checksums", action="store_true")
    cli.add_argument("--build-label", default="unspecified native toolchain/RocksDB")
    cli.add_argument("--work-dir", type=Path, help="parent directory for runner-owned temporary datasets")
    cli.add_argument("--output", type=Path, help="JSON manifest/results; also written on worker failure")
    cli.add_argument("--timeout", type=float, default=300, help="seconds per prepare/measurement worker")
    return cli


def validate(args, cli):
    values = args.scales or [args.rows]
    if min(values + [args.value_size, args.batch_size, args.max_batch_bytes, args.repeat, args.threads]) < 1:
        cli.error("rows/scales, sizes, repeats and threads must be positive")
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        cli.error("timeout must be finite and positive")
    if args.cpu is not None:
        if not hasattr(os, "sched_getaffinity") or args.cpu not in os.sched_getaffinity(0):
            cli.error("requested CPU is not available in this process affinity")
    return list(dict.fromkeys(values))


def run(args, cli):
    sizes = validate(args, cli)
    shapes = list(dict.fromkeys(args.value_shapes or (["variable"] if args.variable else ["fixed"])))
    workloads = list(dict.fromkeys(args.workloads or (["prefix"] if args.prefix is not None else ["full", "prefix", "range"])))
    modes = list(args.modes or DEFAULT_MODES)
    if args.include_polars:
        modes.extend(MODES[4:])
    modes = list(dict.fromkeys(modes))
    skipped = []
    if importlib.util.find_spec("polars") is None:
        skipped = [{"mode": m, "reason": "optional polars package is not installed"}
                   for m in modes if m.startswith("polars_")]
        modes = [m for m in modes if not m.startswith("polars_")]
    if not modes:
        cli.error("no available benchmark modes")
    if args.work_dir is not None:
        args.work_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "schema_version": 1, "feature": "ABR-02", "status": "running",
        "configuration": {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
        "coordinator": environment(), "datasets": [], "samples": [], "summary": [], "skipped": skipped,
        "effective_settings": {
            "scales": sizes, "value_shapes": shapes, "workloads": workloads, "modes": modes,
            "read_options": {"fill_cache": not args.no_fill_cache, "verify_checksums": not args.no_verify_checksums},
            "scan": {"batch_size": args.batch_size, "max_batch_bytes": args.max_batch_bytes,
                     "allow_overflow": args.allow_overflow, "collect_metrics": not args.no_metrics},
            "threads": "Arrow CPU/IO and Polars configured explicitly; RocksDB background defaults unchanged",
            "dataset_writes": "default WriteOptions: WAL enabled, sync not requested; DB closed before measurement",
        },
        "method": {
            "mode_isolation": "one fresh read-only process per mode/repeat/workload",
            "cache": "iterator prepass in each worker" if args.cache == "warm" else "reopened DB, no prepass; OS cache uncontrolled",
            "cold_cache_claim": False, "comparator": "RocksDB default BytewiseComparator",
            "validation": "actual ordered key/value buffers; separate phase clock, included in operation wall time",
            "stream_extraction": "segmented clock; pauses for digest validation (not continuous-throughput certification)",
            "payload_decoding": "none; raw KV only", "worker_parallelism": "sequential",
            "rss": "per-worker high-water through cleanup, including imports/open/warmup/validation; not net allocations",
            "capacity": "ABR-01 requested exported data/offset bytes, not consumer copies, metadata or process RSS",
        },
    }
    started = time.perf_counter_ns()
    try:
        with tempfile.TemporaryDirectory(prefix="pyrex-abr02-", dir=args.work_dir) as temporary:
            for rows in sizes:
                for shape in shapes:
                    dataset_id = f"rows={rows}/shape={shape}/value-size={args.value_size}/seed={args.seed}"
                    selected_scopes = scopes(rows, workloads, args.prefix if args.prefix is not None else "sensor/")
                    config = {"path": str(Path(temporary) / f"db-{len(report['datasets'])}"),
                              "rows": rows, "value_size": args.value_size, "variable": shape == "variable",
                              "seed": args.seed, "scopes": selected_scopes, "threads": args.threads}
                    prepared = child("prepare", config, args.timeout)
                    report["datasets"].append({"id": dataset_id, "parameters": config, **prepared})
                    for scope in selected_scopes:
                        for repeat in range(args.repeat):
                            order = modes.copy()
                            random.Random(args.seed + repeat).shuffle(order)
                            for mode in order:
                                job = dict(config, scope=scope, mode=mode, repeat=repeat,
                                           expected=prepared["reference"][scope["name"]],
                                           cache=args.cache, cpu=args.cpu, batch_size=args.batch_size,
                                           max_batch_bytes=args.max_batch_bytes, allow_overflow=args.allow_overflow,
                                           collect_metrics=not args.no_metrics, fill_cache=not args.no_fill_cache,
                                           verify_checksums=not args.no_verify_checksums)
                                sample = child("measure", job, args.timeout)
                                sample["dataset_id"] = dataset_id
                                sample["scope"] = scope
                                report["samples"].append(sample)
            report["status"] = "ok"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        report["elapsed_ns"] = time.perf_counter_ns() - started
        report["summary"] = summarize(report["samples"])
        text = json.dumps(report, indent=2)
        if args.output is not None:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(text + "\n")
        print(text)
    return 0 if report["status"] == "ok" else 1


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv[:1] == ["--worker"]:
        config = json.load(sys.stdin)
        if argv[1:] == ["prepare"]:
            result = prepare_dataset(config)
        elif argv[1:] == ["measure"]:
            from arrow_benchmark_worker import measure
            result = measure(config)
        else:
            raise ValueError("unknown internal worker task")
        print(json.dumps(result))
        return 0
    cli = parser()
    args = cli.parse_args(argv)
    return run(args, cli)


if __name__ == "__main__":
    raise SystemExit(main())
