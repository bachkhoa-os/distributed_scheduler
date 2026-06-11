"""
protocol.py — JSON message protocol dùng chung cho Master và Worker
"""
import json
import socket
import struct
from dataclasses import dataclass, asdict
from typing import Any, Optional

# ── Message types ──────────────────────────────────────────────────────────────
MSG_REGISTER  = "REGISTER"
MSG_TASK      = "TASK"
MSG_RESULT    = "RESULT"
MSG_HEARTBEAT = "HEARTBEAT"
MSG_ACK       = "ACK"

# ── Task status ────────────────────────────────────────────────────────────────
STATUS_READY     = "READY"
STATUS_RUNNING   = "RUNNING"
STATUS_COMPLETED = "COMPLETED"
STATUS_FAILED    = "FAILED"

# ── Timing constants ───────────────────────────────────────────────────────────
HEARTBEAT_INTERVAL = 2   # seconds — worker sends every 2s
HEARTBEAT_TIMEOUT  = 6   # seconds — master marks FAILED after 6s silence


# ── Message helpers ────────────────────────────────────────────────────────────

def send_msg(sock: socket.socket, data: dict) -> None:
    """Gửi dict dưới dạng JSON có length prefix (4-byte big-endian)."""
    raw = json.dumps(data).encode("utf-8")
    # Prefix = độ dài message để recv biết đọc bao nhiêu byte
    header = struct.pack(">I", len(raw))
    sock.sendall(header + raw)


def recv_msg(sock: socket.socket) -> Optional[dict]:
    """Nhận 1 message có length prefix; trả None nếu kết nối đóng."""
    try:
        header = _recv_exact(sock, 4)
        if header is None:
            return None
        length = struct.unpack(">I", header)[0]
        raw = _recv_exact(sock, length)
        if raw is None:
            return None
        return json.loads(raw.decode("utf-8"))
    except (ConnectionResetError, OSError):
        return None


def _recv_exact(sock: socket.socket, n: int) -> Optional[bytes]:
    """Đọc đúng n bytes từ socket."""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


# ── Message constructors ───────────────────────────────────────────────────────

def make_register(worker_id: int, cpu_cores: int) -> dict:
    return {"type": MSG_REGISTER, "worker_id": worker_id, "cpu_cores": cpu_cores}

def make_task(task_id: int, operation: str, input_data: Any) -> dict:
    return {"type": MSG_TASK, "task_id": task_id, "operation": operation, "input": input_data}

def make_result(task_id: int, worker_id: int, output: Any, error: str = "") -> dict:
    return {"type": MSG_RESULT, "task_id": task_id, "worker_id": worker_id,
            "output": output, "error": error}

def make_heartbeat(worker_id: int, current_load: int) -> dict:
    return {"type": MSG_HEARTBEAT, "worker_id": worker_id, "current_load": current_load}

def make_ack(status: str = "ok") -> dict:
    return {"type": MSG_ACK, "status": status}
