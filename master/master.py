import socket
import threading
import time
import argparse
import logging
from dataclasses import dataclass, field
from typing import Optional

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common.protocol import (
    send_msg, recv_msg,
    make_task, make_ack,
    MSG_REGISTER, MSG_RESULT, MSG_HEARTBEAT,
    STATUS_READY, STATUS_RUNNING, STATUS_COMPLETED,
    HEARTBEAT_TIMEOUT,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MASTER] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("master")


# ── Data structures ────────────────────────────────────────────────────────────

@dataclass
class WorkerInfo:
    worker_id: int
    alive: bool = True
    current_load: int = 0
    last_heartbeat: float = field(default_factory=time.time)
    sock: Optional[socket.socket] = None
    addr: tuple = ()
    rr_index: int = 0          # dùng cho Round Robin


@dataclass
class Task:
    task_id: int
    operation: str
    input_data: object
    status: str = STATUS_READY
    assigned_worker: int = -1
    submit_time: float = field(default_factory=time.time)
    start_time: float = 0.0
    end_time: float = 0.0
    result: object = None


# ── Master ─────────────────────────────────────────────────────────────────────

class Master:
    def __init__(self, host: str, port: int, policy: str):
        self.host = host
        self.port = port
        self.policy = policy          # "fifo" | "rr" | "ll"

        # Shared state — bảo vệ bằng lock
        self._lock = threading.Lock()
        self.workers: dict[int, WorkerInfo] = {}
        self.task_queue: list[Task] = []           # READY tasks chờ dispatch
        self.all_tasks: dict[int, Task] = {}       # toàn bộ tasks
        self._next_task_id = 1
        self._rr_worker_ids: list[int] = []        # thứ tự Round Robin
        self._rr_idx = 0

        self._scheduler_event = threading.Event()  # wake scheduler khi có task/worker mới

    # ── Public API ─────────────────────────────────────────────────────────────

    def submit_task(self, operation: str, input_data) -> int:
        """Thêm task vào queue, trả về task_id."""
        with self._lock:
            tid = self._next_task_id
            self._next_task_id += 1
            t = Task(task_id=tid, operation=operation, input_data=input_data)
            self.task_queue.append(t)
            self.all_tasks[tid] = t
        log.info(f"Task {tid} submitted  op={operation}")
        self._scheduler_event.set()
        return tid

    def get_stats(self) -> dict:
        with self._lock:
            return {
                "workers": {
                    wid: {"alive": w.alive, "load": w.current_load}
                    for wid, w in self.workers.items()
                },
                "tasks": {
                    tid: {"status": t.status, "worker": t.assigned_worker}
                    for tid, t in self.all_tasks.items()
                },
            }

    # ── Thread 1: Accept connections ───────────────────────────────────────────

    def start(self):
        # Khởi động background threads
        threading.Thread(target=self._scheduler_loop, daemon=True, name="Scheduler").start()
        threading.Thread(target=self._heartbeat_monitor, daemon=True, name="HeartbeatMonitor").start()

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(32)
        log.info(f"Listening on {self.host}:{self.port}  policy={self.policy}")

        while True:
            conn, addr = server.accept()
            threading.Thread(
                target=self._handle_worker,
                args=(conn, addr),
                daemon=True,
            ).start()

    def _handle_worker(self, conn: socket.socket, addr):
        """Xử lý kết nối từ 1 worker — chạy trong thread riêng."""
        worker_id = None
        try:
            # Bước 1: Nhận REGISTER
            msg = recv_msg(conn)
            if msg is None or msg.get("type") != MSG_REGISTER:
                conn.close()
                return

            worker_id = msg["worker_id"]
            cpu_cores = msg.get("cpu_cores", 1)

            with self._lock:
                self.workers[worker_id] = WorkerInfo(
                    worker_id=worker_id,
                    sock=conn,
                    addr=addr,
                    last_heartbeat=time.time(),
                )
                self._rr_worker_ids.append(worker_id)

            log.info(f"Worker {worker_id} registered  addr={addr}  cores={cpu_cores}")
            send_msg(conn, make_ack("registered"))
            self._scheduler_event.set()

            # Bước 2: Nhận HEARTBEAT và RESULT liên tục
            while True:
                msg = recv_msg(conn)
                if msg is None:
                    break

                mtype = msg.get("type")

                if mtype == MSG_HEARTBEAT:
                    with self._lock:
                        if worker_id in self.workers:
                            w = self.workers[worker_id]
                            w.last_heartbeat = time.time()
                            w.current_load = msg.get("current_load", 0)

                elif mtype == MSG_RESULT:
                    self._handle_result(msg)

        except Exception as e:
            log.warning(f"Worker {worker_id} connection error: {e}")
        finally:
            if worker_id is not None:
                self._mark_worker_failed(worker_id)
            conn.close()

    def _handle_result(self, msg: dict):
        task_id   = msg["task_id"]
        worker_id = msg["worker_id"]
        output    = msg.get("output")
        error     = msg.get("error", "")

        with self._lock:
            t = self.all_tasks.get(task_id)
            if t is None:
                return
            t.status   = STATUS_COMPLETED
            t.result   = output
            t.end_time = time.time()

            w = self.workers.get(worker_id)
            if w:
                w.current_load = max(0, w.current_load - 1)

        elapsed = round(t.end_time - t.start_time, 3)
        if error:
            log.warning(f"Task {task_id} FAILED on Worker {worker_id}: {error}")
        else:
            log.info(f"Task {task_id} COMPLETED on Worker {worker_id}  ({elapsed}s)  result={str(output)[:80]}")

    # ── Thread 2: Scheduler loop ───────────────────────────────────────────────

    def _scheduler_loop(self):
        while True:
            self._scheduler_event.wait(timeout=0.5)
            self._scheduler_event.clear()
            self._dispatch_all()

    def _dispatch_all(self):
        """Giao hết tasks trong queue cho workers theo policy."""
        while True:
            with self._lock:
                if not self.task_queue:
                    break
                worker = self._pick_worker()
                if worker is None:
                    break
                task = self.task_queue.pop(0)
                task.status          = STATUS_RUNNING
                task.assigned_worker = worker.worker_id
                task.start_time      = time.time()
                worker.current_load += 1
                sock = worker.sock

            # Gửi task (ngoài lock để tránh giữ lock khi I/O)
            try:
                send_msg(sock, make_task(task.task_id, task.operation, task.input_data))
                log.info(f"Task {task.task_id} → Worker {worker.worker_id}  op={task.operation}")
            except Exception as e:
                log.warning(f"Send task {task.task_id} to Worker {worker.worker_id} failed: {e}")
                with self._lock:
                    task.status = STATUS_READY
                    self.task_queue.insert(0, task)
                    if worker.worker_id in self.workers:
                        self.workers[worker.worker_id].current_load -= 1

    def _pick_worker(self) -> Optional[WorkerInfo]:
        """
        Chọn worker theo policy hiện tại.
        Gọi từ trong self._lock.
        """
        alive = [w for w in self.workers.values() if w.alive and w.sock]
        if not alive:
            return None

        if self.policy == "fifo":
            # FIFO: worker đầu tiên còn sống (theo thứ tự register)
            return alive[0]

        elif self.policy == "rr":
            # Round Robin: xoay vòng qua danh sách worker alive
            alive_ids = [w.worker_id for w in alive]
            for _ in range(len(alive_ids)):
                wid = self._rr_worker_ids[self._rr_idx % len(self._rr_worker_ids)]
                self._rr_idx += 1
                if wid in alive_ids:
                    return self.workers[wid]
            return alive[0]

        elif self.policy == "ll":
            # Least Loaded: worker có ít task nhất
            return min(alive, key=lambda w: w.current_load)

        return alive[0]

    # ── Thread 3: Heartbeat monitor ────────────────────────────────────────────

    def _heartbeat_monitor(self):
        while True:
            time.sleep(1)
            now = time.time()
            with self._lock:
                for w in list(self.workers.values()):
                    if w.alive and (now - w.last_heartbeat) > HEARTBEAT_TIMEOUT:
                        log.warning(
                            f"Worker {w.worker_id} TIMED OUT "
                            f"({now - w.last_heartbeat:.1f}s since last heartbeat) → FAILED"
                        )
                        self._mark_worker_failed_locked(w.worker_id)

    def _mark_worker_failed(self, worker_id: int):
        with self._lock:
            self._mark_worker_failed_locked(worker_id)

    def _mark_worker_failed_locked(self, worker_id: int):
        """Đánh dấu worker FAILED và reassign tasks — phải gọi trong lock."""
        w = self.workers.get(worker_id)
        if w is None or not w.alive:
            return
        w.alive = False
        w.sock  = None

        # Reassign tất cả RUNNING tasks của worker này về READY
        reassigned = 0
        for t in self.all_tasks.values():
            if t.assigned_worker == worker_id and t.status == STATUS_RUNNING:
                t.status          = STATUS_READY
                t.assigned_worker = -1
                self.task_queue.insert(0, t)   # ưu tiên cao — xếp đầu hàng
                reassigned += 1

        if reassigned:
            log.warning(f"Worker {worker_id}: {reassigned} task(s) reassigned → READY")
            self._scheduler_event.set()


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Task Scheduler — Master")
    parser.add_argument("--host",   default="0.0.0.0")
    parser.add_argument("--port",   type=int, default=5000)
    parser.add_argument("--policy", choices=["fifo", "rr", "ll"], default="ll",
                        help="fifo | rr (Round Robin) | ll (Least Loaded)")
    args = parser.parse_args()

    master = Master(args.host, args.port, args.policy)
    master.start()
