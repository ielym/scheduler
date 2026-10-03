"""L4 监测：阈值阶梯 + 心跳 + 超限退让。

- 心跳：任务 state.json.updated_at / 文件 mtime 超时 → 判卡死，整组终止（数据风险）。
- 超限退让：cgroup 实测 / 账本预估 > overload_ratio 且持续 → 优雅抢占（而非硬杀）。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RunningObs:
    run_id: str
    job_id: str
    priority: int
    est_mem_mb: int
    actual_mem_mb: int | None      # cgroup memory.current (MB)
    heartbeat_age_sec: float       # 距上次心跳的秒数
    manual: bool = False
    # 单任务心跳超时覆盖（manual 任务放宽用）；None = 用 Monitor 全局值
    heartbeat_timeout_sec: int | None = None


@dataclass
class MonitorAction:
    kind: str          # heartbeat_kill | overload_preempt | none
    run_id: str
    reason: str


class Monitor:
    def __init__(self, heartbeat_timeout_sec: int, overload_ratio: float, overload_sustain_ticks: int,
                 tick_interval_sec: float):
        self.heartbeat_timeout_sec = heartbeat_timeout_sec
        self.overload_ratio = overload_ratio
        self.overload_sustain_ticks = max(1, int(overload_sustain_ticks))
        self.tick_interval_sec = tick_interval_sec
        self._overload_cnt: dict[str, int] = {}

    def tick(self, obs_list: list[RunningObs]) -> list[MonitorAction]:
        actions: list[MonitorAction] = []
        seen: set[str] = set()
        for obs in obs_list:
            seen.add(obs.run_id)
            hb_timeout = obs.heartbeat_timeout_sec or self.heartbeat_timeout_sec
            # manual 任务同样受心跳监测（防无限占用资源），仅阈值放宽；
            # 超限退让对 manual「放宽不豁免」：阈值 ×2，超过仍优雅抢占
            if obs.heartbeat_age_sec > hb_timeout:
                actions.append(MonitorAction("heartbeat_kill", obs.run_id,
                                             f"心跳超时（{obs.heartbeat_age_sec:.0f}s > {hb_timeout}s）"))
                continue
            ratio = self.overload_ratio * (2 if obs.manual else 1)
            if obs.est_mem_mb > 0 and obs.actual_mem_mb is not None and obs.actual_mem_mb > obs.est_mem_mb * ratio:
                self._overload_cnt[obs.run_id] = self._overload_cnt.get(obs.run_id, 0) + 1
                if self._overload_cnt[obs.run_id] >= self.overload_sustain_ticks:
                    actions.append(MonitorAction("overload_preempt", obs.run_id,
                                                 f"实测 {obs.actual_mem_mb}MB > 预估 {obs.est_mem_mb}MB × {ratio}，持续超限"))
            else:
                self._overload_cnt.pop(obs.run_id, None)
        # 清理已不在运行列表的计数
        for rid in list(self._overload_cnt):
            if rid not in seen:
                self._overload_cnt.pop(rid, None)
        return actions