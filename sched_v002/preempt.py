"""L2 引擎 · 抢占选择（贪心最小覆盖近似）+ 豁免等待 + 冷却。

路径 B：被抢占者按 base_priority 从低到高选，缺口够了就停；断点保存→退出99→可恢复。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RunningInfo:
    run_id: str
    job_id: str
    priority: int
    est_cpu: float
    est_mem_mb: int
    soft: dict[str, int]
    progress: int = 0
    started_monotonic: float = 0.0
    exempt_until_monotonic: float = 0.0
    manual: bool = False       # 手动触发的不可抢占任务
    contract_exempt: bool = False  # 声明不可断点恢复的手动任务


def freed_by(targets: list[RunningInfo]) -> tuple[float, int, dict[str, int]]:
    cpu = sum(t.est_cpu for t in targets)
    mem = sum(t.est_mem_mb for t in targets)
    soft: dict[str, int] = {}
    for t in targets:
        for k, v in t.soft.items():
            soft[k] = soft.get(k, 0) + v
    return cpu, mem, soft


def select_preempt_targets(
    running: list[RunningInfo],
    need_cpu: float,
    need_mem_mb: int,
    need_soft: dict[str, int],
) -> list[RunningInfo]:
    """贪心近似最小集合覆盖：按 priority 升序（最低优先者先）逐个加入，直到释放足够。

    不可抢占（manual / contract_exempt）直接跳过，绝不入选。
    """
    candidates = [r for r in running if not r.manual and not r.contract_exempt]
    candidates.sort(key=lambda r: (r.priority, r.started_monotonic))  # 低优先、先启动者先被抢
    chosen: list[RunningInfo] = []
    fcpu, fmem, fsoft = 0.0, 0, {}

    def _enough() -> bool:
        if fcpu < need_cpu or fmem < need_mem_mb:
            return False
        for k, v in need_soft.items():
            if fsoft.get(k, 0) < v:
                return False
        return True

    for cand in candidates:
        if _enough():
            break
        chosen.append(cand)
        fcpu += cand.est_cpu
        fmem += cand.est_mem_mb
        for k, v in cand.soft.items():
            fsoft[k] = fsoft.get(k, 0) + v
    if not _enough():
        return []
    return chosen


def make_running_info(run: dict, est_cpu: float, est_mem_mb: int, soft: dict[str, int]) -> RunningInfo:
    return RunningInfo(
        run_id=run["run_id"],
        job_id=run["job_id"],
        priority=int(run.get("priority") or 0),
        est_cpu=est_cpu,
        est_mem_mb=est_mem_mb,
        soft=soft,
        progress=int(run.get("progress") or 0),
        started_monotonic=float(run.get("_started_monotonic") or 0.0),
        exempt_until_monotonic=float(run.get("_exempt_until_monotonic") or 0.0),
        manual=bool(run.get("manual")),
        contract_exempt=bool(run.get("contract_exempt")),
    )