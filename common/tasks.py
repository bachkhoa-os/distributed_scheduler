"""
tasks.py — Các loại task CPU-intensive theo đề bài
"""
import math
import random
from typing import Any


def execute_task(operation: str, input_data: Any) -> Any:
    """Dispatch task đến hàm tương ứng."""
    handlers = {
        "prime_count":    task_prime_count,
        "matrix_mult":    task_matrix_mult,
        "monte_carlo_pi": task_monte_carlo_pi,
        "word_count":     task_word_count,
        "factorial":      task_factorial,
    }
    fn = handlers.get(operation)
    if fn is None:
        raise ValueError(f"Unknown operation: {operation}")
    return fn(input_data)


# ── Task A: Prime Counting ─────────────────────────────────────────────────────

def task_prime_count(n: int) -> int:
    """Đếm số nguyên tố ≤ n dùng Sieve of Eratosthenes."""
    n = int(n)
    if n < 2:
        return 0
    sieve = bytearray([1]) * (n + 1)
    sieve[0] = sieve[1] = 0
    for i in range(2, int(math.isqrt(n)) + 1):
        if sieve[i]:
            sieve[i*i::i] = bytearray(len(sieve[i*i::i]))
    return sum(sieve)


# ── Task B: Matrix Multiplication ─────────────────────────────────────────────

def task_matrix_mult(params: dict) -> dict:
    """
    Nhân 2 ma trận vuông size x size.
    Input: {"size": 100} — sinh random matrix, trả về checksum.
    """
    size = int(params.get("size", 100))
    rng = random.Random(42)  # seed cố định để tái lập

    A = [[rng.random() for _ in range(size)] for _ in range(size)]
    B = [[rng.random() for _ in range(size)] for _ in range(size)]

    # Naive O(n³) — đảm bảo CPU-intensive
    C = [[0.0] * size for _ in range(size)]
    for i in range(size):
        for k in range(size):
            if A[i][k] == 0:
                continue
            for j in range(size):
                C[i][j] += A[i][k] * B[k][j]

    # Trả checksum (tổng phần tử) thay vì cả matrix để tiết kiệm bandwidth
    checksum = sum(C[i][j] for i in range(size) for j in range(size))
    return {"size": size, "checksum": round(checksum, 4)}


# ── Task C: Monte Carlo π ─────────────────────────────────────────────────────

def task_monte_carlo_pi(samples: int) -> float:
    """Ước lượng π bằng Monte Carlo với `samples` điểm."""
    samples = int(samples)
    rng = random.Random()
    inside = sum(
        1 for _ in range(samples)
        if rng.random() ** 2 + rng.random() ** 2 <= 1.0
    )
    return round(4 * inside / samples, 6)


# ── Task D: Word Count ────────────────────────────────────────────────────────

def task_word_count(text: str) -> dict:
    """Đếm tần suất từ trong text, trả top-10."""
    counts: dict[str, int] = {}
    for word in text.lower().split():
        word = word.strip(".,!?\"';:-()[]")
        if word:
            counts[word] = counts.get(word, 0) + 1
    top10 = sorted(counts.items(), key=lambda x: -x[1])[:10]
    return {"total_words": sum(counts.values()), "top10": dict(top10)}


# ── Bonus: Factorial ──────────────────────────────────────────────────────────

def task_factorial(n: int) -> str:
    """Tính n! — trả về số chữ số (kết quả quá lớn để gửi nguyên)."""
    n = int(n)
    result = math.factorial(n)
    return f"{n}! has {len(str(result))} digits"
