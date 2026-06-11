"""
experiments.py — Tự động hóa 3 experiment theo đề bài

Experiment 1: Scalability      — 100 tasks, vary 1/2/4/8 workers
Experiment 2: Scheduling Policy — so sánh FIFO vs RR vs Least Loaded
Experiment 3: Failure Recovery  — kill worker giữa chừng

Chạy: python experiments.py [exp1|exp2|exp3|all]
"""
import subprocess
import time
import sys
import os
import signal
import json
import threading
import socket

sys.path.insert(0, os.path.dirname(__file__))
from common.protocol import send_msg, recv_msg
from master.master import Master
from master.master_api import MasterAPI
from worker.worker import Worker

MASTER_HOST = "127.0.0.1"


# ── Helpers ────────────────────────────────────────────────────────────────────

def start_master(policy="ll", worker_port=6000, api_port=6001) -> Master:
    m = Master(MASTER_HOST, worker_port, policy)
    api = MasterAPI(m, host=MASTER_HOST, port=api_port)
    api.start()
    t = threading.Thread(target=m.start, daemon=True)
    t.start()
    time.sleep(0.3)
    return m


def start_worker(worker_id: int, master_port: int) -> threading.Thread:
    w = Worker(worker_id, MASTER_HOST, master_port)
    t = threading.Thread(target=w.run, daemon=True, name=f"W{worker_id}")
    t.start()
    time.sleep(0.2)
    return t, w


def submit_tasks(master: Master, count: int, op="prime_count", inp=300_000):
    ids = []
    for _ in range(count):
        tid = master.submit_task(op, inp)
        ids.append(tid)
    return ids


def wait_completion(master: Master, task_ids: list, timeout=180) -> float:
    deadline = time.time() + timeout
    t0 = time.time()
    while time.time() < deadline:
        with master._lock:
            done = sum(
                1 for tid in task_ids
                if master.all_tasks.get(tid) and
                   master.all_tasks[tid].status == "COMPLETED"
            )
        if done == len(task_ids):
            return round(time.time() - t0, 3)
        time.sleep(0.05)
    return round(time.time() - t0, 3)


def separator(title):
    print("\n" + "="*60)
    print(f"  {title}")
    print("="*60)


# ── Experiment 1: Scalability ──────────────────────────────────────────────────

def exp1_scalability():
    separator("EXPERIMENT 1: SCALABILITY (100 tasks, vary workers)")
    results = {}

    for n_workers in [1, 2, 4]:
        worker_port = 6100 + n_workers * 10
        api_port    = worker_port + 1
        print(f"\n▶ {n_workers} worker(s)...", end="", flush=True)

        master = start_master("ll", worker_port, api_port)
        workers_list = []
        for i in range(1, n_workers + 1):
            _, w = start_worker(i, worker_port)
            workers_list.append(w)

        time.sleep(0.5)   # chờ workers register

        task_ids = submit_tasks(master, 100, "prime_count", 5_000_000)  # 20 tasks mỗi run
        elapsed  = wait_completion(master, task_ids)
        throughput = round(len(task_ids) / elapsed, 2)

        results[n_workers] = {"time": elapsed, "throughput": throughput}
        print(f" {elapsed}s  ({throughput} tasks/s)")

    print("\n── Results ──")
    print(f"{'Workers':>8}  {'Time (s)':>10}  {'Throughput':>12}")
    print("-" * 36)
    for n, r in results.items():
        print(f"{n:>8}  {r['time']:>10}  {r['throughput']:>10} t/s")
    print("\nExpected: more workers → lower time (diminishing returns at high N)")
    return results


# ── Experiment 2: Scheduling Policies ─────────────────────────────────────────

def exp2_policies():
    separator("EXPERIMENT 2: SCHEDULING POLICIES (3 workers, 15 tasks)")
    results = {}

    for policy in ["fifo", "rr", "ll"]:
        worker_port = 6200 + ord(policy[0])
        api_port    = worker_port + 1
        print(f"\n▶ Policy: {policy.upper()}...", end="", flush=True)

        master = start_master(policy, worker_port, api_port)
        for i in range(1, 4):
            start_worker(i, worker_port)

        time.sleep(0.5)

        # Mix task durations để thể hiện sự khác biệt
        task_ids = []
        for _ in range(5):
            task_ids.append(master.submit_task("prime_count",  100_000))  # nhẹ
            task_ids.append(master.submit_task("prime_count",  400_000))  # nặng hơn
            task_ids.append(master.submit_task("monte_carlo_pi", 200_000))

        elapsed = wait_completion(master, task_ids)

        # Tính avg response time
        with master._lock:
            response_times = [
                master.all_tasks[tid].end_time - master.all_tasks[tid].submit_time
                for tid in task_ids
                if master.all_tasks.get(tid)
            ]
        avg_rt = round(sum(response_times) / len(response_times), 3) if response_times else 0

        results[policy] = {"total_time": elapsed, "avg_response": avg_rt}
        print(f" total={elapsed}s  avg_response={avg_rt}s")

    print("\n── Results ──")
    print(f"{'Policy':>8}  {'Total (s)':>10}  {'Avg Resp (s)':>14}")
    print("-" * 38)
    for p, r in results.items():
        print(f"{p.upper():>8}  {r['total_time']:>10}  {r['avg_response']:>14}")
    print("\nExpected: Least Loaded best for mixed-duration tasks")
    return results


# ── Experiment 3: Failure Recovery ────────────────────────────────────────────

def exp3_failure_recovery():
    separator("EXPERIMENT 3: FAILURE RECOVERY")
    worker_port = 6300
    api_port    = 6301

    master = start_master("ll", worker_port, api_port)

    # 3 workers
    _, w1 = start_worker(1, worker_port)
    _, w2 = start_worker(2, worker_port)
    _, w3 = start_worker(3, worker_port)
    time.sleep(0.5)

    print(f"\n▶ Submitting 15 tasks...")
    task_ids = submit_tasks(master, 15, "prime_count", 300_000)

    # Chờ 1 chút để task bắt đầu chạy
    time.sleep(1.5)

    print(f"▶ Killing Worker 2 (disconnect)...")
    w2._connected = False
    try:
        w2._sock.close()
    except Exception:
        pass

    print(f"▶ Waiting for heartbeat timeout + reassignment (~6s)...")
    time.sleep(7)

    with master._lock:
        w2_info = master.workers.get(2)
        alive_workers = [wid for wid, w in master.workers.items() if w.alive]

    print(f"  Worker 2 status: {'FAILED' if w2_info and not w2_info.alive else 'alive'}")
    print(f"  Alive workers: {alive_workers}")

    print(f"▶ Waiting for all tasks to complete (reassigned to W1/W3)...")
    elapsed = wait_completion(master, task_ids, timeout=120)

    with master._lock:
        done  = sum(1 for tid in task_ids if master.all_tasks[tid].status == "COMPLETED")
        total = len(task_ids)

    print(f"\n── Results ──")
    print(f"  Tasks submitted : {total}")
    print(f"  Tasks completed : {done}")
    print(f"  Tasks lost      : {total - done}")
    print(f"  Total time      : {elapsed}s")

    if done == total:
        print("\n  ✓ PASS — No tasks lost despite worker failure!")
    else:
        print(f"\n  ✗ FAIL — {total - done} task(s) lost")

    return {"submitted": total, "completed": done, "lost": total - done}


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"

    if cmd in ("exp1", "all"):
        exp1_scalability()

    if cmd in ("exp2", "all"):
        exp2_policies()

    if cmd in ("exp3", "all"):
        exp3_failure_recovery()

    if cmd not in ("exp1", "exp2", "exp3", "all"):
        print("Usage: python experiments.py [exp1|exp2|exp3|all]")
