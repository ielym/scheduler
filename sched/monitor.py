"""监测：心跳 + 超限退让。所有进程同等阈值，无豁免。

- 心跳：state.json.heartbeat_at / updated_at 超时 → heartbeat_kill（数据风险，整组终止）。
- 超限退让：cgroup 实测 / 账本预估 > overload_ratio 且持续 → 优雅抢占。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RunningObs:
    run_id: str
    subtask_id: str         # 子任务 id
    priority: int
    est_mem_mb: int
    actual_mem_mb: int | None      # cgroup memory.current (MB)
    heartbeat_age_sec: float       # 距上次心跳的秒数
    heartbeat_timeout_sec: int | None = None   # None = 用 Monitor 全局值


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

    def reconfigure(self, heartbeat_timeout_sec: int, overload_ratio: float,
                    overload_sustain_ticks: int, tick_interval_sec: float) -> None:
        """热更新监测阈值（settings.json 热加载用）。保留超限计数窗口免于重置。"""
        self.heartbeat_timeout_sec = heartbeat_timeout_sec
        self.overload_ratio = overload_ratio
        self.overload_sustain_ticks = max(1, int(overload_sustain_ticks))
        self.tick_interval_sec = tick_interval_sec

    def tick(self, obs_list: list[RunningObs]) -> list[MonitorAction]:
        actions: list[MonitorAction] = []
        seen: set[str] = set()
        for obs in obs_list:
            seen.add(obs.run_id)
            hb_timeout = obs.heartbeat_timeout_sec or self.heartbeat_timeout_sec
            # 所有进程同等心跳阈值，无豁免
            if obs.heartbeat_age_sec > hb_timeout:
                actions.append(MonitorAction("heartbeat_kill", obs.run_id,
                                             f"心跳超时（{obs.heartbeat_age_sec:.0f}s > {hb_timeout}s）"))
                continue
            ratio = self.overload_ratio
            if obs.est_mem_mb > 0 and obs.actual_mem_mb is not None and obs.actual_mem_mb > obs.est_mem_mb * ratio:
                self._overload_cnt[obs.run_id] = self._overload_cnt.get(obs.run_id, 0) + 1
                if self._overload_cnt[obs.run_id] >= self.overload_sustain_ticks:
                    actions.append(MonitorAction("overload_preempt", obs.run_id,
                                                 f"实测 {obs.actual_mem_mb}MB > 预估 {obs.est_mem_mb}MB × {ratio}，持续超限"))
            else:
                self._overload_cnt.pop(obs.run_id, None)
        for rid in list(self._overload_cnt):
            if rid not in seen:
                self._overload_cnt.pop(rid, None)
        return actions
