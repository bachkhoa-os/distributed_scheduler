"""
client.py — Gửi task đến Master và xem kết quả

Dùng để test hoặc submit batch task cho experiments.

Usage:
    python client.py submit --op prime_count --input 1000000
    python client.py submit --op monte_carlo_pi --input 1000000
    python client.py submit --op factorial --input 5000
    python client.py stats
    python client.py batch --count 20 --op prime_count --input 500000
"""
import socket
import argparse
import json
import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common.protocol import send_msg, recv_msg, make_ack

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 5001   # port riêng cho client API

# ── Client-Master API (thêm vào master) dùng simple JSON over TCP ─────────────
# Master cần mở thêm 1 port cho client; ở đây dùng subprocess gọi qua CLI
# Để đơn giản hơn, client tương tác qua master_api.py

class MasterClient:
    """Giao tiếp với MasterAPI server."""
    def __init__(self, host=MASTER_HOST, port=MASTER_PORT):
        self.host = host
        self.port = port

    def _call(self, payload: dict) -> dict:
        with socket.create_connection((self.host, self.port), timeout=5) as s:
            send_msg(s, payload)
            return recv_msg(s) or {}

    def submit(self, operation: str, input_data) -> dict:
        return self._call({"action": "submit", "operation": operation, "input": input_data})

    def stats(self) -> dict:
        return self._call({"action": "stats"})

    def wait_all(self, timeout=120) -> dict:
        """Poll stats cho đến khi không còn task READY/RUNNING."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            s = self.stats()
            tasks = s.get("tasks", {})
            pending = sum(1 for t in tasks.values() if t["status"] in ("READY", "RUNNING"))
            if pending == 0:
                return s
            time.sleep(0.5)
        return self.stats()


def parse_input(raw: str):
    """Tự động parse JSON nếu có thể, không thì dùng int hoặc string."""
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        try:
            return int(raw)
        except ValueError:
            return raw


def main():
    parser = argparse.ArgumentParser(description="Distributed Scheduler Client")
    parser.add_argument("--host", default=MASTER_HOST)
    parser.add_argument("--port", type=int, default=MASTER_PORT)
    sub = parser.add_subparsers(dest="cmd")

    # submit
    p_sub = sub.add_parser("submit")
    p_sub.add_argument("--op",    required=True,
        choices=["prime_count", "matrix_mult", "monte_carlo_pi", "word_count", "factorial"])
    p_sub.add_argument("--input", required=True)

    # stats
    sub.add_parser("stats")

    # batch
    p_batch = sub.add_parser("batch")
    p_batch.add_argument("--count", type=int, default=10)
    p_batch.add_argument("--op",    default="prime_count")
    p_batch.add_argument("--input", default="500000")

    args = parser.parse_args()
    client = MasterClient(args.host, args.port)

    if args.cmd == "submit":
        inp = parse_input(args.input)
        res = client.submit(args.op, inp)
        print(json.dumps(res, indent=2))

    elif args.cmd == "stats":
        res = client.stats()
        print(json.dumps(res, indent=2))

    elif args.cmd == "batch":
        inp = parse_input(args.input)
        print(f"Submitting {args.count} tasks  op={args.op}  input={inp}")
        t0 = time.time()
        for i in range(args.count):
            r = client.submit(args.op, inp)
            print(f"  [{i+1}/{args.count}] task_id={r.get('task_id')}")

        print(f"\nAll submitted. Waiting for completion...")
        final = client.wait_all(timeout=300)
        elapsed = round(time.time() - t0, 2)
        tasks   = final.get("tasks", {})
        done    = sum(1 for t in tasks.values() if t["status"] == "COMPLETED")
        print(f"\nDone: {done}/{len(tasks)} tasks in {elapsed}s")
        print(f"Throughput: {round(done/elapsed, 2)} tasks/s")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
