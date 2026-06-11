"""
run_master.py — Entry point: khởi động Master + ClientAPI cùng lúc

Chạy: python run_master.py [--policy ll] [--port 5000] [--api-port 5001]
"""
import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from master.master import Master
from master.master_api import MasterAPI

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Master Node")
    parser.add_argument("--host",     default="0.0.0.0")
    parser.add_argument("--port",     type=int, default=5000, help="Port cho Workers kết nối")
    parser.add_argument("--api-port", type=int, default=5001, help="Port cho Client API")
    parser.add_argument("--policy",   choices=["fifo", "rr", "ll"], default="ll")
    args = parser.parse_args()

    master = Master(args.host, args.port, args.policy)

    api = MasterAPI(master, host=args.host, port=args.api_port)
    api.start()

    print(f"Master started  worker_port={args.port}  api_port={args.api_port}  policy={args.policy}")
    master.start()   # blocking
