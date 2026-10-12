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
    has_unfinished: bool = True
    waiting: float | None = None   # B14：在途等待计数（业务透传，观测用）


class HealthTracker:
    """按任务+业务阶段维护近期进展基线，识别无报错变慢（设计第十一节）。

    基线键为 (task_id, stage, metric_semver)：进展口径随业务版本变更
    （progress_semver 升级意味着"单元定义/统计口径"可能改变），新旧口径
    的速率/耗时不可互相比较，混在同一序列会把口径切换误判为变慢。
    """

    def __init__(self, window_sec: float = 300.0, slow_confidence_sec: float = 120.0):
        self.window_sec = window_sec
        self.slow_confidence_sec = slow_confidence_sec
        self._samples: dict[tuple[str, str, str], deque[ProgressSample]] = {}

    def record(self, task_id: str, stage: str, sample: ProgressSample,
               semver: str = "") -> None:
        key = (task_id, stage, semver)
        dq = self._samples.setdefault(key, deque())
        dq.append(sample)
        while dq and sample.ts_mono - dq[0].ts_mono > self.window_sec:
            dq.popleft()

    def record_progress(self, task_id: str, now_mono: float, completed_units: int,
                        has_unfinished: bool, stage: str,
                        active_duration_sec: float | None,
                        in_flight_duration_sec: float | None,
                        work_units: float | None = None,
                        waiting: float | None = None,
                        semver: str = "") -> None:
        # 完成态（无在途工作）也作为"真实进展证据"进入样本，支撑短任务批量完成的扩容学习
        # （设计第九、十四节）；变慢判定只取 has_unfinished 样本（见 _is_slow），
        # 故完成态不会被误判为变慢（设计第十一节）。
        self.record(task_id, str(stage), ProgressSample(
            ts_mono=now_mono,
            completed_units=int(completed_units),
            work_units=float(work_units or 0.0),
            stage=str(stage),
            active_duration_sec=float(active_duration_sec or 0.0),
            in_flight_duration_sec=float(in_flight_duration_sec or 0.0),
            has_unfinished=bool(has_unfinished),
            waiting=(float(waiting) if waiting is not None else None),
        ), semver=str(semver or ""))

    def has_samples(self, task_id: str) -> bool:
        return any(k[0] == task_id for k in self._samples)

    def has_progress_since(self, task_id: str, since_mono: float) -> bool:
        """自 since_mono 以来是否有新的业务进展样本到达（保护改善判定用）。"""
        for key, dq in self._samples.items():
            if key[0] != task_id:
                continue
            for s in reversed(dq):
                if s.ts_mono >= since_mono:
                    return True
        return False

    def stages_summary(self, task_id: str, now_mono: float) -> list[dict]:
        """该任务各业务阶段的近期健康摘要（监控视图，设计第十四节）。

        每阶段取窗口内最近样本：样本数、累计完成单元、窗口速率（累计工作量
        差分 / 时间跨度）、是否满足变慢判定。只读不改判定状态。
        """
        out: list[dict] = []
        for (tid, stage, semver), dq in self._samples.items():
            if tid != task_id or not dq:
                continue
            s_first, s_last = dq[0], dq[-1]

            def cum(s: ProgressSample) -> float:
                return s.work_units if s.work_units > 0 else float(s.completed_units)

            span = s_last.ts_mono - s_first.ts_mono
            rate = 0.0
            if span > 0 and cum(s_last) > cum(s_first):
                rate = (cum(s_last) - cum(s_first)) / span
            recent = now_mono - s_last.ts_mono <= self.window_sec
            out.append({
                "stage": stage,
                "semver": semver,
                "samples": len(dq),
                "completed_units": s_last.completed_units,
                "rate_per_sec": round(rate, 4),
                "slow": recent and self._is_slow(dq),
            })
        return out

    def slow(self, task_id: str, now_mono: float) -> bool:
        """任一业务阶段出现"连续证据下速率持续下降 + 处理/在途耗时持续上升"即判慢。

        只使用近期样本（窗口内）：陈旧样本不参与判定，避免暂停很久的任务凭
        停摆前的旧基线被误判为变慢（设计第十一节：近期健康基线）。
        """
        for key, dq in self._samples.items():
            if key[0] != task_id or not dq:
                continue
            if now_mono - dq[-1].ts_mono > self.window_sec:
                continue  # 最新样本已陈旧：本阶段近期无证据，不判慢
            if self._is_slow(dq):
                return True
        return False

    def _is_slow(self, dq: deque) -> bool:
        # 只在途工作的样本参与变慢判定；完成态/无在途样本不计入（设计第十一节）
        dq = deque(s for s in dq if s.has_unfinished)
        if len(dq) < 4:
            return False
        # 要求最近样本覆盖至少 slow_confidence_sec
        span = dq[-1].ts_mono - dq[0].ts_mono
        if span < self.slow_confidence_sec:
            return False
        # 近期样本有真实在途工作（完成单元数 > 0）
        if not all(s.completed_units > 0 for s in list(dq)[-3:]):
            return False
        samples = list(dq)
        front = samples[: max(1, len(samples) // 2)]
        back = samples[len(samples) // 2:]

        def cum(s: ProgressSample) -> float:
            # work_units 为累计工作量计数（缺省退化用 completed_units 累计值）
            return s.work_units if s.work_units > 0 else float(s.completed_units)

        def rate(seq: list[ProgressSample]) -> float:
            # 速率 = 累计工作量的差分 / 时间跨度（样本本身是累计值，直接求和会算错）
            if len(seq) < 2:
                return 0.0
            d = cum(seq[-1]) - cum(seq[0])
            if d <= 0:
                return 0.0
            sec = seq[-1].ts_mono - seq[0].ts_mono
            if sec <= 0:
                return 0.0
            return d / sec

        def in_flight(seq):
            return sum(s.in_flight_duration_sec for s in seq) / max(1, len(seq))

        def active_avg(seq):
            return sum(s.active_duration_sec for s in seq) / max(1, len(seq))

        # 正常无输入等待不算变慢（设计第六节）：后半窗口的在途时长主要由
        # 等待输入构成（waiting ≥ 0.8×在途总时长）时，判为业务等待而非处理退化
        back_in_flight = sum(s.in_flight_duration_sec for s in back)
        back_waiting = sum(s.waiting for s in back if s.waiting is not None)
        if back_in_flight > 0 and back_waiting >= 0.8 * back_in_flight:
            return False

        # 设计第十一节：归一化速率持续下降 "且"（处理耗时上升 "或" 在途等待上升）
        rate_declining = rate(back) < rate(front) * 0.8
        process_slower = active_avg(back) > active_avg(front) * 1.2
        wait_rising = in_flight(back) > in_flight(front) * 1.2
        return rate_declining and (process_slower or wait_rising)


@dataclass
class TaskScale:
    run_target: int = 0
    mode: str = "explore"          # explore | steady
    last_grow_mono: float = 0.0
    last_shrink_mono: float = 0.0
    grow_observe_sec: float = 30.0
    shortage_streak: int = 0


class ResourceTrend:
    """执行侧资源趋势（滞回双阈值，设计文档第八节）。

    连续 stable_samples 个样本最大占用率 ≥ unhealthy_ratio 判定不健康；
    连续 stable_samples 个样本 < healthy_ratio 才恢复健康（中间带维持原状）。
    B1：I/O 压力（cgroup psi io full avg10，归一化 0-1）与 CPU/内存一起参与
    判定——psi 缺失时为 0，不影响判定。
    observed=False 表示本轮无任何可用观测（cgroup 不可用）。
    """

    # I/O 压力不健康阈值（psi full avg10 归一化；持续高于该值判不健康）
    IO_UNHEALTHY = 0.25

    def __init__(self, unhealthy_ratio: float = 0.9, healthy_ratio: float = 0.8,
                 stable_samples: int = 3):
        self.unhealthy_ratio = float(unhealthy_ratio)
        self.healthy_ratio = float(healthy_ratio)
        self.stable_samples = max(1, int(stable_samples))
        self.observed = True
        self._unhealthy = False
        self._high_streak = 0
        self._low_streak = 0

    def sample(self, cpu_ratio: float, mem_ratio: float, observed: bool,
               io_pressure: float = 0.0) -> None:
        self.observed = bool(observed)
        m = max(float(cpu_ratio), float(mem_ratio))
        io = float(io_pressure)
        if m >= self.unhealthy_ratio or io >= self.IO_UNHEALTHY:
            self._high_streak += 1
            self._low_streak = 0
        elif m < self.healthy_ratio and io < self.IO_UNHEALTHY:
            self._low_streak += 1
            self._high_streak = 0
        else:  # 滞回中间带：维持原判定
            self._high_streak = 0
            self._low_streak = 0
        if self._high_streak >= self.stable_samples:
            self._unhealthy = True
        if self._low_streak >= self.stable_samples:
            self._unhealthy = False

    def mark_oom(self) -> None:
        """OOM 证据直接判定不健康，需重新累计低占用样本才恢复。"""
        self._unhealthy = True
        self._high_streak = self.stable_samples
        self._low_streak = 0

    def mark_memory_growth(self) -> None:
        """执行内存快速增长（接近容量）证据：直接判定不健康，停止新增（设计第八节）。"""
        self._unhealthy = True
        self._high_streak = self.stable_samples
        self._low_streak = 0

    def unhealthy(self) -> bool:
        return self._unhealthy

    def healthy_enough(self) -> bool:
        return not self._unhealthy


class Scaling:
    """每任务动态目标（无写死上限，平缓探索）。"""

    def __init__(self, cfg):
        self.cfg = cfg
        self._tasks: dict[str, TaskScale] = {}
        self._pause_cost: dict[str, float] = {}  # 任务级暂停耗时 EMA（退让候选排序）

    def get(self, task_id: str) -> TaskScale:
        ts = self._tasks.get(task_id)
        if ts is None:
            ts = TaskScale(grow_observe_sec=self.cfg.control.grow_observe_sec)
            self._tasks[task_id] = ts
        return ts

    def scale_items(self) -> list[tuple[str, TaskScale]]:
        """只读访问各任务动态目标（快照/状态视图用，不暴露内部字典）。"""
        return list(self._tasks.items())

    def note_pause_cost(self, task_id: str, pause_dur_sec: float) -> None:
        """记录一次真实暂停耗时（请求→退出，含检查点落地），维护任务级 EMA。

        设计第十节：低优先级退让候选"综合最近扩容、暂停成本及是否自身报告
        不足排序"——暂停成本低的任务先退让，减少恢复等待。
        """
        if pause_dur_sec < 0:
            return
        cur = float(pause_dur_sec)
        prev = self._pause_cost.get(task_id)
        self._pause_cost[task_id] = cur if prev is None else prev * 0.7 + cur * 0.3

    def pause_cost(self, task_id: str) -> float:
        return self._pause_cost.get(task_id, 0.0)

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
        if ts.mode == "explore":
            # 不足撤销后的首次真实成功：恢复 steady，重新允许探索扩容（设计第八、九节）。
            # 否则 live>=2 的任务在 on_shortage 后 target<=live-1，永不回到冷启动分支，
            # grew_at 永不被调用，任务将永久停在 explore 单并发。
            ts.mode = "steady"

    def may_grow(self, task_id: str, now_mono: float, live_count: int,
                 has_progress: bool, healthy_resource: bool,
                 observe_factor: float = 1.0) -> bool:
        """是否允许 +1：完成启动观察、有真实进展、无持续不足、资源可解释、已达到当前目标。

        observe_factor：观测不足时由调度器传入放大倍数（>1 拉长观察期，减速探索）。
        """
        ts = self.get(task_id)
        if ts.mode != "steady" or ts.shortage_streak > 0:
            return False
        if not has_progress or not healthy_resource:
            return False
        if ts.run_target <= 0:
            return False
        # 平缓探索：观察期 + 一次只 +1
        effective_observe = ts.grow_observe_sec * max(1.0, observe_factor)
        if now_mono - ts.last_grow_mono < effective_observe:
            return False
        return live_count >= ts.run_target

    def apply_grow(self, task_id: str, now_mono: float) -> None:
        ts = self.get(task_id)
        ts.run_target += 1
        ts.last_grow_mono = now_mono


@dataclass
class ProtectState:
    protected_task: str = ""
    protected_priority: int = 0
    round_started_mono: float = 0.0
    deadline_mono: float = 0.0
    deadline_cap_mono: float = 0.0    # 整轮截止总上限（退让延长不得越过，防无限续期）
    last_block_mono: float = 0.0      # 保护对象最近一次受阻/新需求时刻（改善判定用）
    reason: str = ""
    cause: str = ""                   # 保护起因机器标签：shortage | demand | slow
    yielded: set[str] = field(default_factory=set)        # 本轮已退让的 attempt_id
    yielded_tasks: dict[str, int] = field(default_factory=dict)  # 本轮已退让任务 -> 优先级
    pending_resume: dict[str, int] = field(default_factory=dict) # 保护结束后待逐步放回的任务 -> 优先级
    next_release_mono: float = 0.0    # 下一次放回允许时刻（recovery_probe_sec 节奏）
    released: dict[str, float] = field(default_factory=dict)      # 已放回任务 -> 放回时刻
    round_backoff_until: float = 0.0
    last_protected_priority: int = 0  # 上一轮保护对象优先级（失败轮退避用）
    failed_rounds: int = 0            # 连续失败保护轮次数（退避逐渐增加，设计第十节）
    last_yield_mono: float = 0.0      # 最近一次退让时刻（逐步退让+观察 A 用）


class Protect:
    """高优先级保护 + 跨任务累计退让（有界、有截止、失败轮有退避、结束逐步放回）。"""

    def __init__(self, cfg):
        self.cfg = cfg
        self.st = ProtectState()

    def start_round(self, protected_task: str, priority: int, now_mono: float,
                    reason: str = "", cause: str = "",
                    feedback_interval_sec: float = 0.0,
                    lower_tasks: int = 0) -> bool:
        """启动/更新保护轮。

        防劫持规则（设计文档第十节）：
        - 保护进行中，仅当新对象优先级更高才立即切换保护对象；
        - 同级轮转：多个同优先级受阻时，当前轮过半后允许切换到另一同级对象；
        - 失败轮退避期间，仅当新对象优先级高于上一轮保护对象才允许立即重开。
        返回是否生效。
        """
        st = self.st
        window = self._round_window(feedback_interval_sec, lower_tasks)
        if st.protected_task:
            if protected_task == st.protected_task:
                # 同对象再次受阻：仅刷新观察窗与原因，不重置轮次起点与截止时间。
                # 持续不足信号不得把保护轮悄悄无限续期（设计第十节：有界、有截止时间）；
                # 截止时间只在真实发生一次退让时按退让成本有界延长（note_yielded）。
                st.last_block_mono = now_mono
                if reason:
                    st.reason = reason
                if cause:
                    st.cause = cause
                return True
            if priority > st.protected_priority:
                pass  # 更高优先级到达：切换保护对象，重新核实退让状态
            elif priority == st.protected_priority:
                # 同级轮转：当前轮已过半才允许切换，避免一直卡死在单一对象
                if now_mono - st.round_started_mono < self.cfg.control.protect_deadline_sec * 0.5:
                    return False
            else:
                return False  # 更低优先级不能取代当前保护对象
            # 切换保护对象：旧对象的退让账合并进待放回队列（不因切换而丢失，
            # 保护结束后同样逐步放回，设计第十节"可恢复"）；近期已放回的任务
            # 清账——它们仍在运行，将作为新一轮的退让候选被重新选中。
            st.pending_resume.update(st.yielded_tasks)
            st.yielded = set()
            st.yielded_tasks = {}
            st.released = {}
            st.next_release_mono = 0.0
            st.protected_task = protected_task
            st.protected_priority = priority
            st.round_started_mono = now_mono
            st.deadline_mono = now_mono + window
            st.deadline_cap_mono = now_mono + window * 2.0
            st.last_block_mono = now_mono
            st.reason = reason or st.reason
            if cause:
                st.cause = cause
            return True
        if st.pending_resume or st.released:
            # 逐步放回期间 A 再次受阻：暂停继续放回并撤销近期放回（设计第十节）；
            # 无论本轮保护是否被退避拒绝，都不应继续放回/保留放回加剧 A 的受阻
            self.note_re_blocked(now_mono)
        if self.in_backoff(now_mono) and priority <= st.last_protected_priority:
            return False  # 失败轮退避：同级/更低不立即重开
        st.protected_task = protected_task
        st.protected_priority = priority
        st.round_started_mono = now_mono
        st.deadline_mono = now_mono + window
        st.deadline_cap_mono = now_mono + window * 2.0
        st.last_block_mono = now_mono
        st.reason = reason
        st.cause = cause
        st.yielded = set()
        st.yielded_tasks = {}
        return True

    def _round_window(self, feedback_interval_sec: float, lower_tasks: int = 0) -> float:
        """整轮保护窗口初值：按 A 的反馈周期与低优先级任务数推导。

        设计第十节：整轮保护截止"其长度根据低优先级数量、暂停耗时、A 的
        反馈周期和冷却剩余时间计算"——反馈周期长的 A 需要更长的观察窗才有
        机会体现改善；可退让的低优先级任务每个都需一次退让+观察间隔，
        窗口随之加长（暂停耗时由 note_yielded 按实际成本继续延长）；
        总上限（窗口×2）约束退让延长不无限续期。
        """
        w = float(self.cfg.control.protect_deadline_sec)
        if feedback_interval_sec > 0:
            w = max(w, feedback_interval_sec * 4.0)
        if lower_tasks > 0:
            w += min(lower_tasks, 20) * float(self.cfg.control.yield_observe_sec)
        return w

    def note_block(self, now_mono: float) -> None:
        """保护对象再次受阻：刷新改善判定窗口。"""
        self.st.last_block_mono = now_mono

    def deadline_passed(self, now_mono: float) -> bool:
        return now_mono >= self.st.deadline_mono

    def end_round(self, now_mono: float, success: bool = False) -> None:
        """本轮保护结束：退让任务转入逐步放回队列；失败轮退避逐渐增加（设计第十节）。"""
        st = self.st
        if success:
            # 保护改善/无需求：重置失败轮计数，无需退避（逐步放回由 pending_resume 驱动）
            st.failed_rounds = 0
            st.round_backoff_until = now_mono
        else:
            st.failed_rounds += 1
            backoff = self.cfg.control.protect_round_backoff_sec * (2 ** (st.failed_rounds - 1))
            backoff = min(backoff, self.cfg.control.protect_round_backoff_cap_sec)
            st.round_backoff_until = now_mono + backoff
        st.last_protected_priority = st.protected_priority
        # 退让账合并进待放回队列（而非覆盖）：切换保护对象等场景遗留的旧账一并放回
        st.pending_resume.update(st.yielded_tasks)
        st.next_release_mono = 0.0
        st.released = {}
        st.protected_task = ""
        st.protected_priority = 0
        st.yielded = set()
        st.yielded_tasks = {}

    def in_backoff(self, now_mono: float) -> bool:
        return now_mono < self.st.round_backoff_until

    def current_protected_priority(self) -> int:
        """保护进行中返回保护对象优先级（用于低优先级准入门禁），否则 0。"""
        if self.st.protected_task:
            return self.st.protected_priority
        return 0

    def choose_yield_candidate(self, running: list[dict], now_mono: float) -> str | None:
        """选择下一个退让的低优先级执行进程（含 STARTING）：优先级低者优先，再综合最近扩容/暂停成本/自身不足。"""
        candidates = [r for r in running
                      if int(r.get("priority") or 0) < self.st.protected_priority
                      and r.get("attempt_id") not in self.st.yielded]
        if not candidates:
            return None
        candidates.sort(key=lambda r: (
            int(r.get("priority") or 0),                  # 越低级越先退让
            1 if r.get("shortage_recent") else 0,         # 自身不足的先退
            0 if r.get("grown_recently") else 1,          # 最近扩容的先退
            float(r.get("pause_cost") or 0.0),            # 暂停成本低的先退
        ))
        return candidates[0].get("attempt_id")

    def note_yielded(self, attempt_id: str, task_id: str = "",
                     priority: int = 0, pause_cost_sec: float = 0.0) -> None:
        self.st.yielded.add(attempt_id)
        self.st.last_yield_mono = time.monotonic()  # 记录退让时刻，供逐步退让+观察 A
        if task_id:
            self.st.yielded_tasks[task_id] = int(priority)
            # 截止时间按本次退让的实际暂停成本有界延长（设计第十节：长度按
            # 暂停耗时推导），受整轮总上限约束，不能悄悄无限续期
            ext = float(pause_cost_sec) if pause_cost_sec > 0 \
                else float(self.cfg.control.pause_grace_max_sec)
            new_deadline = self.st.deadline_mono + ext
            if self.st.deadline_cap_mono > 0:
                new_deadline = min(new_deadline, self.st.deadline_cap_mono)
            self.st.deadline_mono = new_deadline

    def note_step_mono(self, now_mono: float) -> None:
        """记录一次控制动作时刻（退让或收缩保护对象自身），用于逐步退让的观察节奏。"""
        self.st.last_yield_mono = now_mono

    def release_ready(self, now_mono: float) -> str | None:
        """保护结束后逐步放回：优先级高者先、每个 recovery_probe_sec 放回一个任务。

        变慢退让（cause=="slow"）证据置信度低于明确不足，放回间隔加倍、更谨慎。
        """
        st = self.st
        if not st.pending_resume or st.protected_task:
            return None
        if now_mono < st.next_release_mono:
            return None
        task_id = max(st.pending_resume, key=lambda t: st.pending_resume[t])
        pri = st.pending_resume[task_id]
        del st.pending_resume[task_id]
        st.released[task_id] = pri  # 记录优先级：A 再受阻时据此撤销放回
        interval = self.cfg.control.recovery_probe_sec
        if st.cause == "slow":
            interval *= 2
        st.next_release_mono = now_mono + interval
        return task_id

    def note_re_blocked(self, now_mono: float) -> dict[str, int]:
        """逐步放回期间保护对象再次受阻：暂停继续放回，并撤销近期已放回的任务。

        返回被撤销放回的任务 -> 优先级映射，供调度器重新暂停其存活执行
        （新一轮被失败轮退避拒绝时也必须撤回，不能任由已放回任务持续挤压 A）。
        """
        st = self.st
        st.next_release_mono = now_mono + self.cfg.control.protect_round_backoff_sec
        revoked = dict(st.released)
        st.released = {}
        return revoked