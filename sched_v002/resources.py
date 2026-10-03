"""L1 资源层：容量模型 + 画像（三级 + 下行）+ 账本（账本值/实测值双校验）+ 软资源令牌桶。

- 容量：/proc/meminfo + os.cpu_count() 读取整机能力，MemAvailable 用于「非调度器进程占用」感知。
- 画像：三级估算 —— 个例峰值(cgroup memory.peak) → 滑动窗口 P95 → 保守默认/声明值，
  统一 × overshoot 上浮；样本窗口自然实现「下行修正」（越用越小则估小）。
- 账本：book 值 = Σ 已准出任务预估；actual 值 = Σ cgroup 实测（由 monitor 刷新）。
- 软资源：每个资源名一个令牌桶，占用量来自任务声明，满了就「时间换空间」排队。
"""

from __future__ import annotations

import os
import statistics
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import config, store


# ---------- 整机容量 ----------
def read_total_memory_mb() -> int:
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemTotal:"):
            return int(line.split()[1]) // 1024
    return 0


def read_available_memory_mb() -> int:
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) // 1024
    return 0


def read_cpu_count() -> int:
    return os.cpu_count() or 1


class Capacity:
    def __init__(self) -> None:
        self.total_memory_mb = read_total_memory_mb()
        self.total_cpu = float(read_cpu_count())

    def refresh(self) -> None:
        """定期刷新整机容量（应对热插拔/容器配额变更；由 serve 热加载周期调用）。"""
        self.total_memory_mb = read_total_memory_mb()
        self.total_cpu = float(read_cpu_count())

    def available_memory_mb(self) -> int:
        """整机可用内存（含非调度器进程占用后的余量）。"""
        return read_available_memory_mb()

    def book_available_memory_mb(self, ledger_reserved_mb: int) -> int:
        """账本维度可用内存 = 总量 - 调度器账本已预留（不把其它进程算进硬账本，只作信号）。"""
        return max(0, self.total_memory_mb - ledger_reserved_mb)


# ---------- 画像 ----------
@dataclass
class Profile:
    cpu: float = 0.0
    memory_mb: int = 0
    samples: list[tuple[float, int]] = field(default_factory=list)  # [(cpu, mem_mb)]
    declared_cpu: float = 0.0
    declared_memory_mb: int = 0

    def record_sample(self, cpu: float, mem_mb: int, window: int = 8) -> None:
        self.samples.append((max(0.0, cpu), max(0, int(mem_mb))))
        if len(self.samples) > window:
            self.samples = self.samples[-window:]

    def p95_memory_mb(self) -> int | None:
        if not self.samples:
            return None
        vals = sorted(m for _, m in self.samples)
        idx = min(len(vals) - 1, int(len(vals) * 0.95))
        return vals[idx]

    def p95_cpu(self) -> float | None:
        if not self.samples:
            return None
        vals = sorted(c for c, _ in self.samples)
        idx = min(len(vals) - 1, int(len(vals) * 0.95))
        return vals[idx]

    def estimate(self, overshoot: float) -> tuple[float, int]:
        """(cpu, memory_mb) 预估。有样本用 P95，否则用声明，再否则 0（=未知，交给容量闸拒绝）。"""
        cpu = self.p95_cpu() if self.samples else (self.declared_cpu or 0.0)
        mem = self.p95_memory_mb() if self.samples else (self.declared_memory_mb or 0)
        return round(cpu * overshoot, 2), int(mem * overshoot)


class ProfileStore:
    def __init__(self) -> None:
        self._data: dict[str, dict] = {}

    def load(self) -> None:
        raw = store.load_json(config.profiles_file(), default={})
        self._data = raw if isinstance(raw, dict) else {}

    def get(self, job_id: str, declared_cpu: float = 0.0, declared_mem_mb: int = 0) -> Profile:
        d = self._data.get(job_id) or {}
        return Profile(
            cpu=float(d.get("cpu") or 0.0),
            memory_mb=int(d.get("memory_mb") or 0),
            samples=[(float(c), int(m)) for c, m in (d.get("samples") or [])],
            declared_cpu=float(declared_cpu),
            declared_memory_mb=int(declared_mem_mb),
        )

    def record(self, job_id: str, cpu: float, mem_mb: int, declared_cpu: float, declared_mem_mb: int, window: int = 8) -> Profile:
        prof = self.get(job_id, declared_cpu, declared_mem_mb)
        prof.record_sample(cpu, mem_mb, window=window)
        self._data[job_id] = {
            "cpu": prof.p95_cpu() or declared_cpu,
            "memory_mb": prof.p95_memory_mb() or declared_mem_mb,
            "samples": [list(s) for s in prof.samples],
        }
        return prof

    def save(self) -> None:
        store.save_json(config.profiles_file(), self._data)


# ---------- 账本 ----------
class Ledger:
    """账本值预留（估计值求和）。actual 实测由 monitor 写入。"""

    def __init__(self) -> None:
        self._reserved: dict[str, dict[str, float]] = {}  # run_id -> {"cpu": , "mem_mb": , "soft": {}}
        self._actual: dict[str, dict[str, float]] = {}     # run_id -> monitor 实测

    def reserve(self, run_id: str, cpu: float, mem_mb: int, soft: dict[str, int]) -> None:
        self._reserved[run_id] = {"cpu": float(cpu), "mem_mb": float(mem_mb), "soft": dict(soft)}

    def release(self, run_id: str) -> None:
        self._reserved.pop(run_id, None)
        self._actual.pop(run_id, None)

    def set_actual(self, run_id: str, cpu: float, mem_mb: int) -> None:
        self._actual[run_id] = {"cpu": float(cpu), "mem_mb": float(mem_mb)}

    def reserved_cpu(self) -> float:
        return sum(v["cpu"] for v in self._reserved.values())

    def reserved_mem_mb(self) -> int:
        return int(sum(v["mem_mb"] for v in self._reserved.values()))

    def reserved_soft(self, resource: str) -> int:
        return int(sum(v["soft"].get(resource, 0) for v in self._reserved.values()))

    def actual_cpu(self) -> float:
        return sum(v["cpu"] for v in self._actual.values())

    def actual_mem_mb(self) -> int:
        return int(sum(v["mem_mb"] for v in self._actual.values()))

    def reserved_snapshot(self) -> dict[str, dict]:
        return dict(self._reserved)


# ---------- 软资源令牌桶 ----------
class SoftResourcePool:
    """每个软资源名一个令牌桶。容量由运行时配置声明，任务准出占用令牌，结束归还。"""

    def __init__(self) -> None:
        self._capacities: dict[str, int] = {}
        self._used: dict[str, int] = {}

    def set_capacity(self, name: str, amount: int) -> None:
        self._capacities[name] = max(0, int(amount))

    def capacity(self, name: str) -> int:
        return self._capacities.get(name, 0)

    def used(self, name: str) -> int:
        return self._used.get(name, 0)

    def available(self, name: str) -> int:
        cap = self._capacities.get(name)
        if cap is None:
            return 0
        return cap - self._used.get(name, 0)

    def acquire(self, name: str, amount: int) -> bool:
        if amount <= 0:
            return True
        if self.available(name) < amount:
            return False
        self._used[name] = self._used.get(name, 0) + amount
        return True

    def release(self, name: str, amount: int) -> None:
        self._used[name] = max(0, self._used.get(name, 0) - amount)

    def release_all(self, amounts: dict[str, int]) -> None:
        for k, v in amounts.items():
            self.release(k, v)