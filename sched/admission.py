"""L2 引擎 · 全局队列排序 + 准出（admission）扫描。

- 双轨制：base 轨道管严格性（priority 降序）；field 轨道管同层 FIFO（enqueued_at 升序）。
- 饿死看门狗：等待超过阈值者临时提权，避免高优洪峰永久饿死低优（兜底退让，不兜底杀）。
- 见缝插针：准出是全局扫描，高优等固定资源时，能塞进缝隙的低优仍可先跑（需求 2.2.3）。
- 首个任务因固定资源不足：等待并尝试其它任务。
- 代理/配额等调度器无法感知真实容量的外部资源不参与准出判定：任务以 100/101
  报错后按错误码退避重试，配合渐进式提交动态避让。
"""

from __future__ import annotations

from dataclasses import dataclass

from . import resources


@dataclass
class AdmissionResult:
    admitted: list[str]           # 本轮准出的 run_id
    blocked: list[str]            # 因固定资源（CPU/内存）不足而等待的 run_id
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
            # 饿死看门狗提权钳制在值域 0~100 之上 1 级（=101）：
            # 保证优先于普通排队，又不越界到天级别
            pri = 101
        return (-pri, enq)

    return sorted(items, key=_rank)


def estimate_job(profile: resources.Profile, declared_cpu: float, declared_mem_mb: int, overshoot: float,
                 capacity: resources.Capacity | None = None) -> tuple[float, int]:
    prof = resources.Profile(
        cpu=profile.cpu, memory_mb=profile.memory_mb, samples=profile.samples,
        declared_cpu=declared_cpu, declared_memory_mb=declared_mem_mb,
    )
    est_cpu, est_mem = prof.estimate(overshoot)
    # 画像可能因历史样本偏低，至少不低于声明（声明是任务自报的最低下限）
    est_cpu = max(est_cpu, declared_cpu)
    est_mem = max(est_mem, declared_mem_mb)
    # 钳制在机器上限内：画像上浮（×overshoot）可能越过上限导致任务永久无法准出。
    # 真正超限的运行由 cgroup memory.max 硬兜底 + monitor 超限退让兜住，不会打爆机器。
    if capacity is not None:
        est_cpu = min(est_cpu, capacity.total_cpu)
        est_mem = min(est_mem, capacity.total_memory_mb)
    return est_cpu, est_mem


def can_admit(
    est_cpu: float,
    est_mem_mb: int,
    ledger: resources.Ledger,
    capacity: resources.Capacity,
    hard_mem_reserve_ratio: float = 1.0,
    extra_cpu: float = 0.0,
    extra_mem_mb: int = 0,
    external_mem_margin_mb: int = 0,
) -> tuple[bool, str]:
    """固定资源（CPU/内存）按账本预留判定。

    extra_*：本轮扫描中已准出但尚未入账（reserve）的虚拟占用，防同轮 over-commit。
    external_mem_margin_mb：对非调度进程占用的感知——est 超过整机
    MemAvailable - margin 时退让等待（不拒绝）。0 = 关闭该判定（测试用）。
    """
    reserved_mem = ledger.reserved_mem_mb() + extra_mem_mb
    reserved_cpu = ledger.reserved_cpu() + extra_cpu
    free_mem = capacity.total_memory_mb * hard_mem_reserve_ratio - reserved_mem
    if est_mem_mb > 0 and reserved_mem + est_mem_mb > capacity.total_memory_mb * hard_mem_reserve_ratio:
        return False, f"固定内存不足（需 {est_mem_mb}MB，账本余 {max(0, int(free_mem))}MB）"
    if est_mem_mb > 0 and external_mem_margin_mb > 0:
        avail_fn = getattr(capacity, "available_memory_mb", None)
        real_avail = avail_fn() if callable(avail_fn) else None
        if real_avail is not None and est_mem_mb > real_avail - external_mem_margin_mb:
            return False, (f"整机可用内存不足（需 {est_mem_mb}MB，MemAvailable 余 "
                           f"{max(0, real_avail - external_mem_margin_mb)}MB，含非调度进程占用）")
    free_cpu = capacity.total_cpu - reserved_cpu
    if est_cpu > 0 and reserved_cpu + est_cpu > capacity.total_cpu:
        return False, f"固定 CPU 不足（需 {est_cpu} 核，账本余 {max(0.0, round(free_cpu, 2))} 核）"
    return True, ""


def admission_scan(
    items: list[dict],
    get_declared,
    get_profile,
    ledger: resources.Ledger,
    capacity: resources.Capacity,
    overshoot: float,
    external_mem_margin_mb: int = 0,
) -> AdmissionResult:
    """按双轨顺序扫描，尽量准出；固定资源不足的留在队列继续等待（不失效）。

    扫描内维护「本轮虚拟账本」（pending_*）：本轮已准出者尚未入账（reserve 在 _admit），
    若不累计会导致同轮多个大任务同时通过判定，预留合计超容量（over-commit）。
    """
    res = AdmissionResult([], [], {})
    pending_cpu, pending_mem = 0.0, 0
    for it in items:
        rid = it["run_id"]
        declared_cpu, declared_mem = get_declared(it["job_id"])
        prof = get_profile(it["job_id"])
        est_cpu, est_mem = estimate_job(prof, declared_cpu, declared_mem, overshoot, capacity)
        ok, reason = can_admit(est_cpu, est_mem, ledger, capacity,
                               extra_cpu=pending_cpu, extra_mem_mb=pending_mem,
                               external_mem_margin_mb=external_mem_margin_mb)
        if ok:
            res.admitted.append(rid)
            pending_cpu += est_cpu
            pending_mem += est_mem
        else:
            res.blocked.append(rid)
            res.reasons[rid] = reason
    return res
