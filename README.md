# Distributed Task Scheduler bằng Python

Dự án này mô phỏng một hệ thống lập lịch tác vụ phân tán theo mô hình **Master - Worker**. Master nhận tác vụ từ client, phân phối cho các worker theo chính sách lập lịch được chọn, theo dõi heartbeat để phát hiện worker lỗi và đưa các tác vụ đang chạy trở lại hàng đợi nếu cần.

Hệ thống dùng TCP socket và message JSON có length-prefix, không cần thư viện ngoài.

## Tính năng chính

- Master nhận kết nối từ nhiều worker.
- Client submit tác vụ qua API TCP riêng của Master.
- Hỗ trợ 3 chính sách lập lịch: FIFO, Round Robin và Least Loaded.
- Worker gửi heartbeat định kỳ để Master biết trạng thái sống/chết.
- Tác vụ trên worker bị lỗi sẽ được đưa lại về trạng thái `READY` để giao cho worker khác.
- Có sẵn các tác vụ CPU-intensive và script chạy thí nghiệm tự động.

## Cấu trúc thư mục

```text
.
├── client.py              # CLI client để submit task và xem stats
├── run_master.py          # Entry point chạy Master + Client API
├── common/
│   ├── protocol.py        # JSON protocol, send/receive helper, constants
│   └── tasks.py           # Các hàm xử lý task
├── master/
│   ├── master.py          # Master node, scheduler, heartbeat monitor
│   └── master_api.py      # API TCP cho client, mặc định port 5001
├── worker/
│   └── worker.py          # Worker node, nhận task và gửi heartbeat
└── tests/
    └── experiments.py     # Các thí nghiệm scalability, scheduling, recovery
```

## Yêu cầu

- Python 3.10 trở lên được khuyến nghị.
- Không cần cài thêm package bên ngoài.

## Chạy nhanh

Mở nhiều terminal tại thư mục gốc của repo.

### 1. Chạy Master

```bash
python run_master.py --policy ll
```

Mặc định:

- Worker kết nối vào port `5000`.
- Client kết nối vào API port `5001`.
- Chính sách lập lịch là `ll` (Least Loaded).

Có thể đổi port hoặc policy:

```bash
python run_master.py --host 0.0.0.0 --port 5000 --api-port 5001 --policy rr
```

### 2. Chạy Worker

Chạy mỗi worker ở một terminal riêng. `--id` phải khác nhau giữa các worker.

```bash
python worker/worker.py --id 1
python worker/worker.py --id 2
python worker/worker.py --id 3
```

Nếu Master chạy ở host/port khác:

```bash
python worker/worker.py --id 1 --master-host 127.0.0.1 --master-port 5000
```

### 3. Submit task bằng Client

Submit một task:

```bash
python client.py submit --op prime_count --input 1000000
```

Submit nhiều task và chờ hoàn thành:

```bash
python client.py batch --count 20 --op prime_count --input 500000
```

Xem trạng thái worker và task:

```bash
python client.py stats
```

Nếu API port khác mặc định:

```bash
python client.py --host 127.0.0.1 --port 6001 stats
```

## Các loại task

| Operation | Input mẫu | Kết quả |
| --- | --- | --- |
| `prime_count` | `1000000` | Đếm số nguyên tố nhỏ hơn hoặc bằng `N` |
| `matrix_mult` | `'{"size": 100}'` | Nhân 2 ma trận vuông ngẫu nhiên và trả checksum |
| `monte_carlo_pi` | `1000000` | Ước lượng số pi bằng Monte Carlo |
| `word_count` | `"hello hello world"` | Đếm tổng số từ và top 10 từ xuất hiện nhiều nhất |
| `factorial` | `5000` | Tính `N!` và trả về số chữ số |

Ví dụ:

```bash
python client.py submit --op matrix_mult --input '{"size": 120}'
python client.py submit --op monte_carlo_pi --input 1000000
python client.py submit --op word_count --input "xin chao xin chao distributed scheduler"
python client.py submit --op factorial --input 5000
```

## Chính sách lập lịch

| Policy | Flag | Cách hoạt động |
| --- | --- | --- |
| FIFO | `--policy fifo` | Chọn worker còn sống đầu tiên theo thứ tự đăng ký |
| Round Robin | `--policy rr` | Xoay vòng qua danh sách worker còn sống |
| Least Loaded | `--policy ll` | Chọn worker có số task đang chạy ít nhất |

## Cơ chế heartbeat và recovery

- Worker gửi heartbeat mỗi `2` giây.
- Master đánh dấu worker là failed nếu không nhận heartbeat trong `6` giây.
- Các task đang `RUNNING` trên worker failed sẽ được chuyển về `READY`.
- Scheduler sẽ đưa các task đó lên đầu hàng đợi để giao lại cho worker còn sống.

Các hằng số nằm trong `common/protocol.py`:

```python
HEARTBEAT_INTERVAL = 2
HEARTBEAT_TIMEOUT = 6
```

## Chạy thí nghiệm

Chạy toàn bộ thí nghiệm:

```bash
python -m tests.experiments all
```

Chạy từng thí nghiệm:

```bash
python -m tests.experiments exp1
python -m tests.experiments exp2
python -m tests.experiments exp3
```

Nội dung:

- `exp1`: đo scalability với số lượng worker khác nhau.
- `exp2`: so sánh FIFO, Round Robin và Least Loaded.
- `exp3`: mô phỏng worker bị ngắt kết nối và kiểm tra khả năng giao lại task.

## Giao thức message

Các node trao đổi dict JSON qua TCP socket. Mỗi message có header 4 byte big-endian chứa độ dài payload, sau đó là JSON UTF-8.

Các loại message chính:

- `REGISTER`: worker đăng ký với Master.
- `TASK`: Master gửi task cho worker.
- `RESULT`: worker gửi kết quả về Master.
- `HEARTBEAT`: worker báo tải hiện tại.
- `ACK`: xác nhận đăng ký.

## Lưu ý khi chạy

- Luôn chạy Master trước worker.
- Nếu worker không kết nối được, kiểm tra `--master-host`, `--master-port` và firewall.
- Nếu submit task thất bại, kiểm tra Master API port, mặc định là `5001`.
- Với task CPU nặng như `prime_count` hoặc `matrix_mult`, thời gian chạy phụ thuộc mạnh vào cấu hình máy và số worker.
- `matrix_mult` cần input dạng JSON, nên nên đặt trong dấu nháy đơn trên shell Linux/macOS: `'{"size": 100}'`.

## Ví dụ luồng chạy đầy đủ

Terminal 1:

```bash
python run_master.py --policy ll
```

Terminal 2:

```bash
python worker/worker.py --id 1
```

Terminal 3:

```bash
python worker/worker.py --id 2
```

Terminal 4:

```bash
python client.py batch --count 10 --op prime_count --input 500000
python client.py stats
```
