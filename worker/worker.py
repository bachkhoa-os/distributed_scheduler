"""
worker.py — Worker Node

Chạy: python worker.py --id 1 [--master-host 127.0.0.1] [--master-port 5000]

Threads:
  Thread 1 (main) — Nhận TASK từ master và thực thi
  Thread 2        — Gửi HEARTBEAT mỗi 2 giây
"""
import socket
import threading
import time
import argparse
import logging
import os

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common.protocol import (
    send_msg, recv_msg,
    make_register, make_result, make_heartbeat,
    MSG_TASK, MSG_ACK,
    HEARTBEAT_INTERVAL,
)
from common.tasks import execute_task

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WORKER-%(name)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)


class Worker:
    def __init__(self, worker_id: int, master_host: str, master_port: int):
        self.worker_id   = worker_id
        self.master_host = master_host
        self.master_port = master_port
        self.cpu_cores   = os.cpu_count() or 1

        self._sock: socket.socket = None
        self._load = 0
        self._load_lock = threading.Lock()
        self._connected = False
        self.log = logging.getLogger(str(worker_id))

    # ── Connect & register ─────────────────────────────────────────────────────

    def connect(self) -> bool:
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.connect((self.master_host, self.master_port))
            # Gửi REGISTER
            send_msg(self._sock, make_register(self.worker_id, self.cpu_cores))
            ack = recv_msg(self._sock)
            if ack and ack.get("type") == MSG_ACK:
                self._connected = True
                self.log.info(f"Connected to master {self.master_host}:{self.master_port}")
                return True
        except Exception as e:
            self.log.error(f"Cannot connect to master: {e}")
        return False

    def connect_with_retry(self, max_retries=10, delay=3):
        for attempt in range(1, max_retries + 1):
            self.log.info(f"Connecting... (attempt {attempt}/{max_retries})")
            if self.connect():
                return True
            time.sleep(delay)
        return False

    # ── Main run loop ──────────────────────────────────────────────────────────

    def run(self):
        if not self.connect_with_retry():
            self.log.error("Failed to connect. Exiting.")
            return

        # Thread 2: Heartbeat sender
        hb_thread = threading.Thread(target=self._heartbeat_loop, daemon=True, name="Heartbeat")
        hb_thread.start()

        # Thread 1 (main): nhận và xử lý task
        self._receive_loop()

    def _receive_loop(self):
        """Nhận TASK liên tục từ master và thực thi trong thread pool."""
        while self._connected:
            msg = recv_msg(self._sock)
            if msg is None:
                self.log.warning("Connection to master lost.")
                self._connected = False
                break

            if msg.get("type") == MSG_TASK:
                # Mỗi task chạy trong thread riêng để không block heartbeat
                t = threading.Thread(
                    target=self._execute_task,
                    args=(msg,),
                    daemon=True,
                )
                t.start()

    # ── Task execution ─────────────────────────────────────────────────────────

    def _execute_task(self, msg: dict):
        task_id   = msg["task_id"]
        operation = msg["operation"]
        input_data = msg["input"]

        with self._load_lock:
            self._load += 1

        self.log.info(f"Executing task {task_id}  op={operation}")
        start = time.time()
        error = ""
        output = None

        try:
            output = execute_task(operation, input_data)
        except Exception as e:
            error = str(e)
            self.log.error(f"Task {task_id} ERROR: {e}")

        elapsed = round(time.time() - start, 3)
        self.log.info(f"Task {task_id} done in {elapsed}s")

        with self._load_lock:
            self._load -= 1

        # Gửi kết quả về master
        try:
            send_msg(self._sock, make_result(task_id, self.worker_id, output, error))
        except Exception as e:
            self.log.warning(f"Failed to send result for task {task_id}: {e}")

    # ── Thread 2: Heartbeat sender ─────────────────────────────────────────────

    def _heartbeat_loop(self):
        while self._connected:
            time.sleep(HEARTBEAT_INTERVAL)
            if not self._connected:
                break
            with self._load_lock:
                load = self._load
            try:
                send_msg(self._sock, make_heartbeat(self.worker_id, load))
            except Exception:
                self._connected = False
                break


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Task Scheduler — Worker")
    parser.add_argument("--id",          type=int, required=True, help="Worker ID (phải unique)")
    parser.add_argument("--master-host", default="127.0.0.1")
    parser.add_argument("--master-port", type=int, default=5000)
    args = parser.parse_args()

    worker = Worker(args.id, args.master_host, args.master_port)
    worker.run()
