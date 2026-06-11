import socket
import threading
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common.protocol import send_msg, recv_msg


class MasterAPI:
    """Gắn vào Master object, lắng nghe trên port riêng."""
    def __init__(self, master, host="0.0.0.0", port=5001):
        self.master = master
        self.host   = host
        self.port   = port

    def start(self):
        t = threading.Thread(target=self._serve, daemon=True, name="ClientAPI")
        t.start()

    def _serve(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(16)
        while True:
            conn, _ = srv.accept()
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn: socket.socket):
        try:
            msg = recv_msg(conn)
            if msg is None:
                return
            action = msg.get("action")
            if action == "submit":
                task_id = self.master.submit_task(msg["operation"], msg["input"])
                send_msg(conn, {"status": "ok", "task_id": task_id})
            elif action == "stats":
                send_msg(conn, self.master.get_stats())
            else:
                send_msg(conn, {"status": "error", "msg": "unknown action"})
        except Exception as e:
            send_msg(conn, {"status": "error", "msg": str(e)})
        finally:
            conn.close()
