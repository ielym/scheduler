"""L2 引擎 · 全局队列排序 + 准出（admission）扫描。

- 双轨制：base 轨道管严格性（priority 降序）；field 轨道管同层 FIFO（enqueued_at 升序）。
- 饿死看门狗：等待超过阈值者临时提权，避免高优洪峰永久饿死低优（兜底退让，不兜底杀）。
- 见缝插针：准出是全局扫描，高优等固定资源时，能塞进缝隙的低优仍可先跑（需求 2.2.3）。
- 首个任务因固定资源不足：等待并尝试其它任务；软资源不足：时间换空间等待。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from . import resources, runlog


@dataclass
class AdmissionResult:
    admitted: list[str]           # 本轮准出的 run_id
    blocked_fixed: list[str]      # 因固定资源不足而等待的 run_id
    blocked_soft: list[str]       # 因软资源不足而等待的 run_id
    reasons: dict[str, str]       # run_id -> 未准出原因


def order_waiting(items: list[dict], starvation_sec: float, now_iso: str) -> list[dict]:
    """双轨排序：priority 降序 → 同层 enqueued_at 升序；饿死看门狗临时提权。

    enqueued_at 为带时区 ISO 串（按字典序即时间序）；等待时长用 wall-clock 差值估，
    跨重启安全（不依赖 monotonic 基准）。
    """
    from .runlog import _iso_seconds_between  # 延迟导入避免环

    def _waited(enqueued: str) -> float:
        return _iso_seconds_between(enqueued, now_iso)

    def _rank(it: dict) -> tuple[int, str]:
        pri = int(it.get("priority") or 0)
        enq = it.get("enqueued_at") or ""
        if _waited(enq) >= starvation_sec:
            pri = 10_000
        return (-pri, enq)

    return sorted(items, key=_rank)


def estimate_job(profile: resources.Profile, declared_cpu: float, declared_mem_mb: int, overshoot: float) -> tuple[float, int]:
    prof = resources.Profile(
        cpu=profile.cpu, memory_mb=profile.memory_mb, samples=profile.samples,
        declared_cpu=declared_cpu, declared_memory_mb=declared_mem_mb,
    )
    est_cpu, est_mem = prof.estimate(overshoot)
    # 画像可能因历史样本偏低，至少不低于声明（声明是任务自报的最低下限）
    est_cpu = max(est_cpu, declared_cpu)
    est_mem = max(est_mem, declared_mem_mb)
    return est_cpu, est_mem


def can_admit(
    est_cpu: float,
    est_mem_mb: int,
    soft_needs: dict[str, int],
    ledger: resources.Ledger,
    capacity: resources.Capacity,
    soft_pool: resources.SoftResourcePool,
    hard_mem_reserve_ratio: float = 1.0,
) -> tuple[bool, str]:
    """固定资源按账本预留判定；软资源按令牌桶判定。"""
    free_mem = capacity.total_memory_mb * hard_mem_reserve_ratio - ledger.reserved_mem_mb()
    if est_mem_mb > 0 and ledger.reserved_mem_mb() + est_mem_mb > capacity.total_memory_mb * hard_mem_reserve_ratio:
        return False, f"固定内存不足（需 {est_mem_mb}MB，账本余 {max(0, int(free_mem))}MB）"
    free_cpu = capacity.total_cpu - ledger.reserved_cpu()
    if est_cpu > 0 and ledger.reserved_cpu() + est_cpu > capacity.total_cpu:
        return False, f"固定 CPU 不足（需 {est_cpu} 核，账本余 {max(0.0, round(free_cpu, 2))} 核）"
    for name, amt in soft_needs.items():
        if soft_pool.capacity(name) <= 0:
            return False, f"软资源 {name} 未配置容量"
        if not soft_pool.available(name) >= amt:
            return False, f"软资源 {name} 不足（需 {amt}，余 {soft_pool.available(name)}）"
    return True, ""


def admission_scan(
    items: list[dict],
    get_declared,
    get_profile,
    ledger: resources.Ledger,
    capacity: resources.Capacity,
    soft_pool: resources.SoftResourcePool,
    overshoot: float,
) -> AdmissionResult:
    """按双轨顺序扫描，尽量准出；软资源/固定资源不足的都留在队列继续等待（不失效）。"""
    res = AdmissionResult([], [], [], {})
    for it in items:
        rid = it["run_id"]
        declared_cpu, declared_mem, soft = get_declared(it["job_id"])
        prof = get_profile(it["job_id"])
        est_cpu, est_mem = estimate_job(prof, declared_cpu, declared_mem, overshoot)
        ok, reason = can_admit(est_cpu, est_mem, soft, ledger, capacity, soft_pool)
        if ok:
            res.admitted.append(rid)
        else:
            if "固定" in reason or "CPU" in reason or "内存" in reason:
                res.blocked_fixed.append(rid)
            else:
                res.blocked_soft.append(rid)
            res.reasons[rid] = reason
    return res