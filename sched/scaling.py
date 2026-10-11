"""动态运行规模（平缓扩容）、业务健康基线、无报错变慢检测、高优先级保护与累计退让。

设计文档第八至十一节：
- 动态运行目标可升降，无预设最大值；新任务从单次执行开始冷启动探索。
- 增长需真实业务进展、无持续不足、可解释资源状态；一次扩容后不足/退化先撤销近期增加。
- 按任务+业务阶段建立健康基线（归一化速率/单元耗时/在途时长），只观测不记录外部资源。
- 高优先级受阻时逐步让低优先级退让，可扩展到全部低优先级；有界保护、有截止时间、可恢复。
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ProgressSample:
    ts_mono: float
    completed_units: int
    work_units: float
    stage: str
    active_duration_sec: float
    in_flight_duration_sec: float


class HealthTracker:
    """按任务+阶段维护近期进展基线，识别无报错变慢。"""

    def __init__(self, window_sec: float = 300.0, slow_confidence_sec: float = 120.0):
        self.window_sec = window_sec
        self.slow_confidence_sec = slow_confidence_sec
        self._samples: dict[str, deque[ProgressSample]] = {}

    def record(self, task_id: str, sample: ProgressSample) -> None:
        dq = self._samples.setdefault(task_id, deque())
        dq.append(sample)
        while dq and sample.ts_mono - dq[0].ts_mono > self.window_sec:
            dq.popleft()

    def record_progress(self, task_id: str, now_mono: float, completed_units: int,
                        has_unfinished: bool, stage: str,
                        active_duration_sec: float | None,
                        in_flight_duration_sec: float | None,
                        work_units: float | None = None) -> None:
        if not has_unfinished:
            # 无在途工作的完成态：不用于"变慢"判定（等待新输入不算变慢）
            self._clear(task_id)
            return
        self.record(task_id, ProgressSample(
            ts_mono=now_mono,
            completed_units=int(completed_units),
            work_units=float(work_units or 0.0),
            stage=str(stage),
            active_duration_sec=float(active_duration_sec or 0.0),
            in_flight_duration_sec=float(in_flight_duration_sec or 0.0),
        ))

    def _clear(self, task_id: str) -> None:
        self._samples.pop(task_id, None)

    def slow(self, task_id: str, now_mono: float) -> bool:
        """连续证据下：归一化速率持续下降 + 处理/在途耗时持续上升。"""
        dq = self._samples.get(task_id)
        if not dq or len(dq) < 4:
            return False
        # 要求最近样本覆盖至少 slow_confidence_sec
        span = dq[-1].ts_mono - dq[0].ts_mono
        if span < self.slow_confidence_sec:
            return False
        # 持续上升的正在被处理单元数 = 有真实在途工作
        if not all(s.completed_units > 0 for s in list(dq)[-3:]):
            return False
        front = list(dq)[: max(1, len(dq) // 2)]
        back = list(dq)[len(dq) // 2:]
        def rate(seq):
            if not seq:
                return 0.0
            return sum(s.work_units if s.work_units > 0 else 1.0 for s in seq) / max(1e-6, (seq[-1].ts_mono - seq[0].ts_mono or 1.0))
        def in_flight(seq):
            return sum(s.in_flight_duration_sec for s in seq) / max(1, len(seq))
        return rate(back) < rate(front) * 0.8 and in_flight(back) > in_flight(front) * 1.2


@dataclass
class TaskScale:
    run_target: int = 0
    mode: str = "explore"          # explore | steady
    last_grow_mono: float = 0.0
    last_shrink_mono: float = 0.0
    grow_observe_sec: float = 30.0
    shortage_streak: int = 0


class Scaling:
    """每任务动态目标（无写死上限，平缓探索）。"""

    def __init__(self, cfg):
        self.cfg = cfg
        self._tasks: dict[str, TaskScale] = {}

    def get(self, task_id: str) -> TaskScale:
        ts = self._tasks.get(task_id)
        if ts is None:
            ts = TaskScale(grow_observe_sec=self.cfg.control.grow_observe_sec)
            self._tasks[task_id] = ts
        return ts

    def grew_at(self, task_id: str, now_mono: float) -> None:
        ts = self.get(task_id)
        ts.mode = "steady"
        ts.last_grow_mono = now_mono

    def on_shortage(self, task_id: str, now_mono: float, live_count: int) -> None:
        """不足/退化：撤销近期增加，降低动态目标。"""
        ts = self.get(task_id)
        ts.shortage_streak += 1
        ts.last_shrink_mono = now_mono
        ts.mode = "explore"
        # 撤销近期增加：至少减到当前存活数的保守值以下，给恢复留空间
        ts.run_target = max(0, live_count - 1)

    def on_success(self, task_id: str, now_mono: float) -> None:
        ts = self.get(task_id)
        ts.shortage_streak = 0

    def may_grow(self, task_id: str, now_mono: float, live_count: int,
                 has_progress: bool, healthy_resource: bool) -> bool:
        """是否允许 +1：完成启动观察、有真实进展、无持续不足、资源可解释、已达到当前目标。"""
        ts = self.get(task_id)
        if ts.mode != "steady" or ts.shortage_streak > 0:
            return False
        if not has_progress or not healthy_resource:
            return False
        if ts.run_target <= 0:
            return False
        # 平缓探索：观察期 + 一次只 +1
        if now_mono - ts.last_grow_mono < ts.grow_observe_sec:
            return False
        return live_count >= ts.run_target

    def apply_grow(self, task_id: str, now_mono: float) -> None:
        ts = self.get(task_id)
        ts.run_target += 1
        ts.last_grow_mono = now_mono


@dataclass
class ProtectState:
    protected_task: str = ""
    round_started_mono: float = 0.0
    deadline_mono: float = 0.0
    yielded: set[str] = field(default_factory=set)       # 已退让的 attempt_id
    protected_priority: int = 0
    cooled_until: dict[str, float] = field(default_factory=dict)  # task -> 恢复冷却截止
    round_backoff_until: float = 0.0


class Protect:
    """高优先级保护 + 跨任务累计退让（有界、有截止）。"""

    def __init__(self, cfg):
        self.cfg = cfg
        self.st = ProtectState()

    def start_round(self, protected_task: str, priority: int, now_mono: float) -> None:
        """高优先级受阻 → 启动/更新保护轮。"""
        if self.st.protected_task != protected_task:
            self.st.protected_task = protected_task
            self.st.protected_priority = priority
            self.st.round_started_mono = now_mono
            self.st.deadline_mono = now_mono + self.cfg.control.protect_deadline_sec
            self.st.yielded = set()

    def deadline_passed(self, now_mono: float) -> bool:
        return now_mono >= self.st.deadline_mono

    def end_round(self, now_mono: float) -> None:
        """本轮保护结束：解除临时冻结、节流恢复低优先级。"""
        self.st.round_backoff_until = now_mono + self.cfg.control.protect_round_backoff_sec
        self.st.protected_task = ""
        self.st.yielded = set()
        self.st.protected_priority = 0

    def in_backoff(self, now_mono: float) -> bool:
        return now_mono < self.st.round_backoff_until

    def choose_yield_candidate(self, running: list[dict], now_mono: float) -> str | None:
        """选择下一个退让的低优先级执行进程：优先级低者优先，再综合最近扩容/暂停成本/自身不足。"""
        candidates = [r for r in running
                      if int(r.get("priority") or 0) < self.st.protected_priority
                      and r.get("attempt_id") not in self.st.yielded]
        if not candidates:
            return None
        candidates.sort(key=lambda r: (
            int(r.get("priority") or 0),                  # 越低级越先退让
            1 if r.get("shortage_recent") else 0,         # 自身不足的先退
            0 if r.get("grown_recently") else 1,          # 最近扩容的先退
        ))
        return candidates[0].get("attempt_id")

    def note_yielded(self, attempt_id: str) -> None:
        self.st.yielded.add(attempt_id)

    def recovery_order(self, yielded: list[dict], now_mono: float) -> list[dict]:
        """恢复时按优先级高者先、冷却已到者先，逐步放回。"""
        ready = [r for r in yielded
                 if now_mono >= self.st.cooled_until.get(r.get("task_id", ""), 0.0)]
        ready.sort(key=lambda r: -int(r.get("priority") or 0))
        return ready

    def note_recovery_cooled(self, task_id: str, now_mono: float) -> None:
        self.st.cooled_until[task_id] = now_mono + self.cfg.control.protect_round_backoff_sec