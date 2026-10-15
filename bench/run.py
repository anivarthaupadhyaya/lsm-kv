"""LSM-KV Performance Benchmark Suite."""

import argparse
import contextlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tests.resp_client import RespClient, encode_command


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@contextlib.contextmanager
def launch_server(
    data_dir: Path,
    port: Optional[int] = None,
    fsync_policy: str = "never",
    use_bloom: bool = True,
    memtable_limit: Optional[int] = None,
    compaction_threshold: int = 4,
):
    port = port or find_free_port()
    cmd = [
        sys.executable, "-m", "server.server",
        "--port", str(port),
        "--dir", str(data_dir),
        "--fsync-policy", str(fsync_policy),
        "--compaction-threshold", str(compaction_threshold),
    ]
    cmd.append("--use-bloom" if use_bloom else "--no-bloom")
    if memtable_limit is not None:
        cmd.extend(["--memtable-limit", str(memtable_limit)])

    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=str(REPO_ROOT))
    deadline = time.perf_counter() + 6.0
    ready = False
    while time.perf_counter() < deadline:
        try:
            with RespClient("127.0.0.1", port, timeout=0.5) as c:
                if c.ping() == b"PONG":
                    ready = True
                    break
        except Exception:
            time.sleep(0.05)

    if not ready:
        proc.kill()
        proc.wait()
        raise RuntimeError(f"Server on port {port} failed to start")

    try:
        yield port
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def run_workload(
    port: int,
    keys: list[bytes],
    op: str = "SET",
    values: Optional[list[bytes]] = None,
    threads: int = 4,
    pipeline: int = 1,
) -> dict[str, float]:
    n = len(keys)
    chunk_size = (n + threads - 1) // threads
    all_latencies: list[float] = []
    lat_lock = threading.Lock()
    start_barrier = threading.Barrier(threads + 1)

    def worker(th_id: int):
        start_idx = th_id * chunk_size
        end_idx = min(start_idx + chunk_size, n)
        th_keys = keys[start_idx:end_idx]
        th_vals = values[start_idx:end_idx] if values is not None else None
        if not th_keys:
            start_barrier.wait()
            return

        with RespClient("127.0.0.1", port, timeout=10.0) as client:
            th_lats: list[float] = []
            start_barrier.wait()

            if pipeline <= 1:
                for i, k in enumerate(th_keys):
                    t0 = time.perf_counter()
                    if op == "SET":
                        client.set(k, th_vals[i] if th_vals else b"")
                    else:
                        client.get(k)
                    th_lats.append((time.perf_counter() - t0) * 1000.0)
            else:
                for b_start in range(0, len(th_keys), pipeline):
                    b_keys = th_keys[b_start : b_start + pipeline]
                    b_vals = th_vals[b_start : b_start + pipeline] if th_vals else None
                    t0 = time.perf_counter()
                    if op == "SET":
                        raw = b"".join(encode_command("SET", k, b_vals[j] if b_vals else b"") for j, k in enumerate(b_keys))
                    else:
                        raw = b"".join(encode_command("GET", k) for k in b_keys)
                    client.send_raw(raw)
                    for _ in b_keys:
                        client.read_response()
                    elapsed_ms = (time.perf_counter() - t0) * 1000.0
                    th_lats.extend([elapsed_ms / len(b_keys)] * len(b_keys))

            with lat_lock:
                all_latencies.extend(th_lats)

    worker_threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(threads)]
    for t in worker_threads:
        t.start()

    start_barrier.wait()
    t_start = time.perf_counter()
    for t in worker_threads:
        t.join()
    t_end = time.perf_counter()

    duration = max(t_end - t_start, 1e-6)
    all_latencies.sort()
    count = len(all_latencies)
    p50 = all_latencies[int(count * 0.50)] if count else 0.0
    p99 = all_latencies[min(int(count * 0.99), count - 1)] if count else 0.0

    return {
        "ops_per_sec": round(count / duration, 1),
        "p50_ms": round(p50, 3),
        "p99_ms": round(p99, 3),
        "duration_sec": round(duration, 3),
    }


def warmup(port: int, n: int = 100) -> None:
    try:
        with RespClient("127.0.0.1", port, timeout=2.0) as c:
            for i in range(n):
                c.set(f"warmup_{i}", b"x" * 100)
                c.get(f"warmup_{i}")
    except Exception:
        pass


def run_benchmark_suite(args: argparse.Namespace) -> dict[str, Any]:
    n_keys = args.keys or (10000 if args.quick else 100000)
    threads = args.threads
    pipeline = args.pipeline
    val_100b = b"v" * 100
    results: dict[str, Any] = {}

    print(f"--- Running LSM-KV Benchmarks (keys={n_keys}, threads={threads}, pipeline={pipeline}) ---")

    # (1) SET & (2) GET on existing keys
    print("Running (1) SET and (2) GET benchmark...")
    keys = [f"key:{i:08d}".encode() for i in range(n_keys)]
    values = [val_100b] * n_keys
    with tempfile.TemporaryDirectory() as td:
        with launch_server(Path(td), fsync_policy="never") as port:
            warmup(port)
            res_set = run_workload(port, keys, op="SET", values=values, threads=threads, pipeline=pipeline)
            warmup(port, 50)
            res_get = run_workload(port, keys, op="GET", threads=threads, pipeline=pipeline)
            results["set_100k"] = res_set
            results["get_existing"] = res_get

    # (3) GET non-existent keys (bloom ON vs OFF) with >= 8 SSTables
    print("Running (3) Non-existent GETs (Bloom ON vs OFF)...")
    res_bloom = {}
    q_count = min(n_keys, 2000)
    missing_keys = [f"missing:{i:08d}".encode() for i in range(q_count)]
    load_keys = [f"load:{i:08d}".encode() for i in range(350)]
    load_vals = [val_100b] * len(load_keys)

    for bloom in [True, False]:
        with tempfile.TemporaryDirectory() as td:
            with launch_server(Path(td), fsync_policy="never", use_bloom=bloom, memtable_limit=1500, compaction_threshold=0) as port:
                run_workload(port, load_keys, op="SET", values=load_vals, threads=threads, pipeline=pipeline)
                sst_count = len(list(Path(td).glob("sst_*.sst")))
                if sst_count < 8:
                    with RespClient("127.0.0.1", port) as c:
                        for extra in range(8 - sst_count):
                            c.set(f"extra_{extra}", val_100b)
                            c.execute("FLUSH")
                warmup(port, 50)
                res = run_workload(port, missing_keys, op="GET", threads=threads, pipeline=pipeline)
                res["sst_count"] = len(list(Path(td).glob("sst_*.sst")))
                res_bloom["use_bloom" if bloom else "no_bloom"] = res
    results["get_nonexistent_bloom"] = res_bloom

    # (4) SET throughput for each fsync policy
    print("Running (4) SET throughput across fsync policies...")
    fsync_policies = [("always", "always"), ("10ms", "10"), ("never", "never")]
    res_fsync = {}
    fsync_keys_n = min(n_keys, 2000)
    fsync_keys = [f"fsync:{i:08d}".encode() for i in range(fsync_keys_n)]
    fsync_vals = [val_100b] * fsync_keys_n
    for name, policy in fsync_policies:
        with tempfile.TemporaryDirectory() as td:
            with launch_server(Path(td), fsync_policy=policy) as port:
                warmup(port, 20)
                res = run_workload(port, fsync_keys, op="SET", values=fsync_vals, threads=threads, pipeline=pipeline)
                res_fsync[name] = res
    results["fsync_policy_set"] = res_fsync

    # (5) Read latency before and after compaction
    print("Running (5) Read latency before and after compaction...")
    res_compact = {}
    c_keys = [f"ckey:{i:08d}".encode() for i in range(350)]
    c_vals = [val_100b] * len(c_keys)
    with tempfile.TemporaryDirectory() as td:
        with launch_server(Path(td), fsync_policy="never", memtable_limit=1500, compaction_threshold=0) as port:
            run_workload(port, c_keys, op="SET", values=c_vals, threads=threads, pipeline=pipeline)
            with RespClient("127.0.0.1", port) as c:
                for extra in range(max(0, 8 - len(list(Path(td).glob("sst_*.sst"))))):
                    c.set(f"cextra_{extra}", val_100b)
                    c.execute("FLUSH")
            warmup(port, 50)
            res_before = run_workload(port, c_keys, op="GET", threads=threads, pipeline=pipeline)
            res_before["sst_count"] = len(list(Path(td).glob("sst_*.sst")))
            with RespClient("127.0.0.1", port) as c:
                c.execute("COMPACT")
            res_after = run_workload(port, c_keys, op="GET", threads=threads, pipeline=pipeline)
            res_after["sst_count"] = len(list(Path(td).glob("sst_*.sst")))
            res_compact["before"] = res_before
            res_compact["after"] = res_after
    results["read_compaction"] = res_compact

    # Output results
    bench_dir = REPO_ROOT / "bench"
    bench_dir.mkdir(parents=True, exist_ok=True)
    results_path = bench_dir / "results.json"
    with open(results_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print("\nBenchmark Results Summary:")
    print_markdown_table(results, pipeline)
    print(f"\nSaved detailed results to {results_path}")
    return results


def print_markdown_table(res: dict[str, Any], pipeline: int) -> None:
    lines = [
        "| Benchmark | Configuration / Policy | Ops/sec | p50 Latency (ms) | p99 Latency (ms) |",
        "|---|---|---|---|---|",
        f"| 1. SET (100-byte values) | pipeline={pipeline} | {res['set_100k']['ops_per_sec']:.1f} | {res['set_100k']['p50_ms']:.3f} | {res['set_100k']['p99_ms']:.3f} |",
        f"| 2. GET (existing keys) | pipeline={pipeline} | {res['get_existing']['ops_per_sec']:.1f} | {res['get_existing']['p50_ms']:.3f} | {res['get_existing']['p99_ms']:.3f} |",
        f"| 3. GET non-existent | use_bloom=ON ({res['get_nonexistent_bloom']['use_bloom'].get('sst_count', 8)}+ SSTs) | {res['get_nonexistent_bloom']['use_bloom']['ops_per_sec']:.1f} | {res['get_nonexistent_bloom']['use_bloom']['p50_ms']:.3f} | {res['get_nonexistent_bloom']['use_bloom']['p99_ms']:.3f} |",
        f"| 3. GET non-existent | use_bloom=OFF ({res['get_nonexistent_bloom']['no_bloom'].get('sst_count', 8)}+ SSTs) | {res['get_nonexistent_bloom']['no_bloom']['ops_per_sec']:.1f} | {res['get_nonexistent_bloom']['no_bloom']['p50_ms']:.3f} | {res['get_nonexistent_bloom']['no_bloom']['p99_ms']:.3f} |",
        f"| 4. SET fsync policy | always | {res['fsync_policy_set']['always']['ops_per_sec']:.1f} | {res['fsync_policy_set']['always']['p50_ms']:.3f} | {res['fsync_policy_set']['always']['p99_ms']:.3f} |",
        f"| 4. SET fsync policy | 10 ms interval | {res['fsync_policy_set']['10ms']['ops_per_sec']:.1f} | {res['fsync_policy_set']['10ms']['p50_ms']:.3f} | {res['fsync_policy_set']['10ms']['p99_ms']:.3f} |",
        f"| 4. SET fsync policy | never | {res['fsync_policy_set']['never']['ops_per_sec']:.1f} | {res['fsync_policy_set']['never']['p50_ms']:.3f} | {res['fsync_policy_set']['never']['p99_ms']:.3f} |",
        f"| 5. Read latency | before compaction ({res['read_compaction']['before'].get('sst_count', 8)} SSTs) | {res['read_compaction']['before']['ops_per_sec']:.1f} | {res['read_compaction']['before']['p50_ms']:.3f} | {res['read_compaction']['before']['p99_ms']:.3f} |",
        f"| 5. Read latency | after compaction ({res['read_compaction']['after'].get('sst_count', 1)} SST) | {res['read_compaction']['after']['ops_per_sec']:.1f} | {res['read_compaction']['after']['p50_ms']:.3f} | {res['read_compaction']['after']['p99_ms']:.3f} |",
    ]
    print("\n".join(lines))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LSM-KV Performance Benchmark")
    parser.add_argument("--quick", action="store_true", help="Quick smoke test mode (10k keys)")
    parser.add_argument("--keys", type=int, default=None, help="Number of keys for SET/GET")
    parser.add_argument("--threads", type=int, default=4, help="Client thread count")
    parser.add_argument("--pipeline", type=int, nargs="?", const=50, default=1, help="Pipelining batch size")
    suite_args = parser.parse_args()
    run_benchmark_suite(suite_args)
