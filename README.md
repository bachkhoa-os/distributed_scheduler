# Distributed Task Scheduler — Python

## Cấu trúc project

```
distributed_scheduler/
├── common/
│   ├── protocol.py     # JSON message protocol + socket helpers
│   └── tasks.py        # CPU-intensive task implementations
├── master/
│   ├── master.py       # Master node (3 threads)
│   └── master_api.py   # Client API server (port 5001)
├── worker/
│   └── worker.py       # Worker node (2 threads)
├── run_master.py       # Entry point: Master + API
├── client.py           # CLI client để submit task
└── tests/
    └── experiments.py  # 3 experiments tự động
```

## Chạy nhanh (3 terminal)

### Terminal 1 — Master
```bash
cd distributed_scheduler
python run_master.py --policy ll
```

### Terminal 2, 3, 4 — Workers
```bash
python worker/worker.py --id 1
python worker/worker.py --id 2
python worker/worker.py --id 3
```

### Terminal 5 — Client
```bash
# Submit 1 task
python client.py submit --op prime_count --input 1000000

# Submit batch 20 tasks
python client.py batch --count 20 --op prime_count --input 500000

# Xem stats
python client.py stats
```

## Chạy Experiments

```bash
# Chạy cả 3 experiments
python -m tests.experiments all

# Chạy từng cái
python -m tests.experiments exp1   # Scalability
python -m tests.experiments exp2   # Scheduling policies
python -m tests.experiments exp3   # Failure recovery
```

## Scheduling Policies

| Policy | Flag | Mô tả |
|--------|------|--------|
| FIFO | `--policy fifo` | Worker đầu tiên còn sống nhận task |
| Round Robin | `--policy rr` | Xoay vòng W1→W2→W3→W1... |
| Least Loaded | `--policy ll` | Worker ít task nhất (mặc định) |

## Task Types

| Op | Input | Mô tả |
|----|-------|-------|
| `prime_count` | `N` (int) | Đếm số nguyên tố ≤ N |
| `matrix_mult` | `{"size": 100}` | Nhân 2 ma trận N×N |
| `monte_carlo_pi` | `samples` (int) | Ước lượng π |
| `word_count` | `"text..."` (str) | Đếm tần suất từ |
| `factorial` | `N` (int) | Tính N! |

## Heartbeat

- Worker gửi mỗi **2 giây**
- Master timeout sau **6 giây** → đánh dấu FAILED
- Tasks RUNNING trên worker chết → về **READY** và được reassign
