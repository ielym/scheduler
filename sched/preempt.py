"""抢占选择（贪心最小覆盖近似）+ 保护期冷却。

无豁免抢占：所有进程（daemon / oneshot）同等走抢占 + exit 99 + 断点续跑路径。
被抢占者 60s 保护期（preempt_exempt_sec），保护期内不再被抢。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RunningInfo:
    run_id: str
    subtask_id: str          # 子任务 id
    request_id: str          # 意图 id
    priority: int
    est_cpu: float
    est_mem_mb: int
    progress: int = 0
    started_monotonic: float = 0.0
    exempt_until_monotonic: float = 0.0   # preempt_exempt_sec 保护期结束时间


def freed_by(targets: list[RunningInfo]) -> tuple[float, int]:
    cpu = sum(t.est_cpu for t in targets)
    mem = sum(t.est_mem_mb for t in targets)
    return cpu, mem


def select_preempt_targets(
    running: list[RunningInfo],
    need_cpu: float,
    need_mem_mb: int,
    waiter_priority: int,
    now_monotonic: float,
) -> list[RunningInfo]:
    """贪心近似最小集合覆盖：按 priority 升序（最低优先者先）逐个加入，直到释放足够。

    只选优先级严格低于等待者的任务（D5 无豁免）。
    保护期内（exempt_until_monotonic > now_monotonic）的任务跳过。
    """
    candidates = [r for r in running
                  if r.priority < waiter_priority
                  and r.exempt_until_monotonic <= now_monotonic]
    candidates.sort(key=lambda r: (r.priority, r.started_monotonic))
    chosen: list[RunningInfo] = []
    fcpu, fmem = 0.0, 0
    for cand in candidates:
        if fcpu >= need_cpu and fmem >= need_mem_mb:
            break
        chosen.append(cand)
        fcpu += cand.est_cpu
        fmem += cand.est_mem_mb
    if fcpu < need_cpu or fmem < need_mem_mb:
        return []
    return chosen


def make_running_info(run: dict, est_cpu: float, est_mem_mb: int, now_monotonic: float = 0.0,
                      preempt_exempt_sec: int = 60) -> RunningInfo:
    """从 service 层传递的 run dict 构造 RunningInfo。

    run dict 必须有 subtask_id / request_id / run_id / priority。
    """
    started = float(run.get("_started_monotonic") or now_monotonic)
    return RunningInfo(
        run_id=str(run["run_id"]),
        subtask_id=str(run["subtask_id"]),
        request_id=str(run.get("request_id") or run["run_id"]),
        priority=int(run.get("priority") or 0),
        est_cpu=est_cpu,
        est_mem_mb=est_mem_mb,
        progress=int(run.get("progress") or 0),
        started_monotonic=started,
        exempt_until_monotonic=started + preempt_exempt_sec,
    )
