"""动态运行规模（平缓扩容）、业务健康基线、无报错变慢检测、高优先级保护与累计退让。

设计文档第八至十一节：
- 动态运行目标可升降，无预设最大值；新任务从单次执行开始冷启动探索。
- 增长需真实业务进展、无持续不足、可解释资源状态；一次扩容后不足/退化先撤销近期增加。
- 按任务建立健康基线（归一化速率/单元耗时/在途时长），只观测不记录外部资源。
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


class TaskTiming:
    """任务级实测 timing EMA（替代任务自申报的反馈间隔/单步耗时）。

    反馈间隔 = 同任务两次业务事件（started/progress/done/error）到达间隔的
    EMA。初始值用全局默认，随后随实测事件动态更新；任务不声明、不预估。
    """

    def __init__(self, default_interval_sec: float = 5.0, alpha: float = 0.2):
        self.default_interval_sec = float(default_interval_sec)
        self.alpha = float(alpha)
        self._feedback_interval: dict[str, float] = {}
        self._last_event_mono: dict[str, float] = {}

    def note_event(self, task_id: str, now_mono: float) -> None:
        prev = self._last_event_mono.get(task_id)
        if prev is not None and now_mono > prev:
            gap = now_mono - prev
            cur = self._feedback_interval.get(task_id)
            self._feedback_interval[task_id] = gap if cur is None \
                else cur * (1.0 - self.alpha) + gap * self.alpha
        self._last_event_mono[task_id] = now_mono

    def feedback_interval(self, task_id: str) -> float:
        return self._feedback_interval.get(task_id, self.default_interval_sec)

    def snapshot(self) -> dict:
        """持久化当前各任务的进度节奏 EMA（只存间隔秒数，monotonic 不落盘）。"""
        return {"feedback_interval": dict(self._feedback_interval)}

    def restore(self, snap: dict) -> None:
        """重启后回填进度节奏 EMA，避免进度节奏归零（存活不依赖它，见 heartbeat）。"""
        if not isinstance(snap, dict):
            return
        fi = snap.get("feedback_interval")
        if isinstance(fi, dict):
            for k, v in fi.items():
                try:
                    self._feedback_interval[str(k)] = float(v)
                except (TypeError, ValueError):
                    pass


class HealthTracker:
    """按任务维护近期进展基线，识别无报错变慢。

    基线按 task_id 单维度；速率 = completed_units 差分 / 时间。
    耗时与资源消耗由调度器实测（cgroup/墙钟），任务不预估、样本不携带。
    """

    def __init__(self, window_sec: float = 300.0, slow_confidence_sec: float = 120.0,
                 timing: TaskTiming | None = None):
        self.window_sec = window_sec
        self.slow_confidence_sec = slow_confidence_sec
        self.timing = timing or TaskTiming()
        self._samples: dict[str, deque[ProgressSample]] = {}
        # 恢复早期豁免：暂停/重放恢复的任务在豁免窗口内不参与变慢判定
        self._resume_exempt_until: dict[str, float] = {}

    def note_resumed(self, task_id: str, now_mono: float, exempt_sec: float) -> None:
        if exempt_sec > 0:
            until = now_mono + float(exempt_sec)
            prev = self._resume_exempt_until.get(task_id, 0.0)
            self._resume_exempt_until[task_id] = max(prev, until)

    def record_progress(self, task_id: str, now_mono: float, completed_units: int) -> None:
        sample = ProgressSample(ts_mono=now_mono, completed_units=int(completed_units))
        dq = self._samples.setdefault(task_id, deque())
        dq.append(sample)
        while dq and sample.ts_mono - dq[0].ts_mono > self.window_sec:
            dq.popleft()

    def has_samples(self, task_id: str) -> bool:
        return bool(self._samples.get(task_id))

    def has_progress_since(self, task_id: str, since_mono: float) -> bool:
        """自 since_mono 以来是否有新的业务进展样本到达（保护改善判定用）。"""
        dq = self._samples.get(task_id)
        if not dq:
            return False
        return any(s.ts_mono >= since_mono for s in reversed(dq))

    def summary(self, task_id: str, now_mono: float) -> dict | None:
        """该任务近期健康摘要（监控视图）。"""
        dq = self._samples.get(task_id)
        if not dq:
            return None
        s_first, s_last = dq[0], dq[-1]
        span = s_last.ts_mono - s_first.ts_mono
        rate = 0.0
        if span > 0 and s_last.completed_units > s_first.completed_units:
            rate = (s_last.completed_units - s_first.completed_units) / span
        recent = now_mono - s_last.ts_mono <= self.window_sec
        return {
            "samples": len(dq),
            "completed_units": s_last.completed_units,
            "rate_per_sec": round(rate, 4),
            "slow": recent and self._is_slow(dq, task_id),
        }

    def slow(self, task_id: str, now_mono: float) -> bool:
        """近期速率持续下降即判慢（陈旧样本不参与，恢复早期豁免）。"""
        if now_mono < self._resume_exempt_until.get(task_id, 0.0):
            return False
        dq = self._samples.get(task_id)
        if not dq:
            return False
        if now_mono - dq[-1].ts_mono > self.window_sec:
            return False  # 最新样本已陈旧：近期无证据，不判慢
        return self._is_slow(dq, task_id)

    def _is_slow(self, dq: deque, task_id: str = "") -> bool:
        if len(dq) < 4:
            return False
        # 要求最近样本覆盖至少 slow_confidence_sec（观察期参考实测反馈间隔）
        min_span = self.slow_confidence_sec
        fi = self.timing.feedback_interval(task_id)
        if fi > 0:
            min_span = max(min_span, fi * 6.0)
        span = dq[-1].ts_mono - dq[0].ts_mono
        if span < min_span:
            return False
        # 近期样本有真实在途工作（完成单元数 > 0）
        if not all(s.completed_units > 0 for s in list(dq)[-3:]):
            return False
        samples = list(dq)
        front = samples[: max(1, len(samples) // 2)]
        back = samples[len(samples) // 2:]

        def rate(seq):
            # 速率 = completed_units 差分 / 时间跨度（样本本身是累计值）
            if len(seq) < 2:
                return 0.0
            d = seq[-1].completed_units - seq[0].completed_units
            if d <= 0:
                return 0.0
            sec = seq[-1].ts_mono - seq[0].ts_mono
            if sec <= 0:
                return 0.0
            return d / sec
        # 归一化速率持续下降即判慢
        return rate(back) < rate(front) * 0.8


@dataclass
class TaskScale:
    run_target: int = 0
    mode: str = "explore"          # explore | steady
    last_grow_mono: float = 0.0
    last_shortage_mono: float = -1.0  # 最近一次不足时刻（证据衰减观察期锚点）
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
    NORMAL = "normal"   # 正常：照常准入
    HOLD = "hold"       # 停止新增准入（在途执行不受影响）
    SHED = "shed"       # 停止新增 + 主动撤内存（暂停低优先级可释放者）

    def __init__(self, unhealthy_ratio: float = 0.9, healthy_ratio: float = 0.7,
                 stable_samples: int = 3, shed_ratio: float = 0.8):
        self.unhealthy_ratio = float(unhealthy_ratio)
        self.healthy_ratio = float(healthy_ratio)
        self.stable_samples = max(1, int(stable_samples))
        self.shed_ratio = float(shed_ratio)
        self.observed = True
        self._state = self.NORMAL
        self._hold_streak = 0
        self._recover_streak = 0

    @property
    def state(self) -> str:
        return self._state

    def sample(self, cpu_ratio: float, mem_ratio: float, observed: bool,
               io_pressure: float = 0.0) -> None:
        """资源水位状态机（合并原滞回判定与内存硬顶，单一语义）：

        - SHED 即时生效（单采样）：执行内存 ≥ shed_ratio（防爬坡穿透滞回窗）
        - HOLD 需连续 stable_samples 采样：CPU ≥ unhealthy_ratio 或 IO 压力
          ≥ IO_UNHEALTHY（CPU/IO 只停新增——撤内存对降 CPU 无保证）
        - 恢复需连续 stable_samples 采样全指标 < healthy_ratio；SHED 不降级
          为 HOLD（避免中间态抖动）
        """
        self.observed = bool(observed)
        cpu, mem, io = float(cpu_ratio), float(mem_ratio), float(io_pressure)
        if mem >= self.shed_ratio:
            self._hold_streak = self.stable_samples
            self._recover_streak = 0
            self._state = self.SHED
            return
        if cpu >= self.unhealthy_ratio or io >= self.IO_UNHEALTHY:
            self._hold_streak += 1
            self._recover_streak = 0
        elif (cpu < self.healthy_ratio and mem < self.healthy_ratio
              and io < self.IO_UNHEALTHY):
            self._recover_streak += 1
            self._hold_streak = 0
        else:  # 滞回中间带：维持原状态
            self._hold_streak = 0
            self._recover_streak = 0
        if (self._hold_streak >= self.stable_samples
                and self._state == self.NORMAL):
            self._state = self.HOLD
        if self._recover_streak >= self.stable_samples:
            self._state = self.NORMAL

    def mark_oom(self) -> None:
        """OOM 证据：直接进入 SHED（停新增 + 撤内存），需重新累计低占用样本恢复。"""
        self._state = self.SHED
        self._hold_streak = self.stable_samples
        self._recover_streak = 0

    def mark_memory_growth(self) -> None:
        """执行内存快速增长（接近容量）证据：直接进入 SHED（设计第八节）。"""
        self._state = self.SHED
        self._hold_streak = self.stable_samples
        self._recover_streak = 0

    def unhealthy(self) -> bool:
        return self._state != self.NORMAL

    def healthy_enough(self) -> bool:
        return self._state == self.NORMAL

    def shedding(self) -> bool:
        return self._state == self.SHED


class TaskBackoff:
    """每任务独立的资源退避状态机（用户设计）。

    前提：同一任务内的多个作业必然共用同一外部资源；跨任务是否共用未知。
    因此资源错误（exit 75）的退避按任务独立进行，互不牵连：

    - 步降（step-down）：任务 T 的一个作业报资源错误 → 暂停 T 内
      "后执行"（最新启动）的在途执行一个；等待 T 内"未退避"的剩余执行
      正确完成一步：
        * 一步成功 → 资源恢复 → NORMAL，开始逐个放回；
        * 再次资源错误 → 再步降一个（仍是最新者优先）；
        * 全部退避（无在途执行）→ EMPTY：任务级退避，符合预期——
          说明该任务一执行就撞资源问题，按递增间隔探测。
    - 步升 / 准入（step-up）：每任务一次只放一个新执行（新拉起或放回），
      该执行正确完成一步后才允许放下一个（"逐个拉取"）。
    - 探测（EMPTY）：距上次资源错误按 probe_base × 2^level 间隔放一个
      执行；成功一步 → NORMAL（level 清零）；失败 → level+1，间隔翻倍。

    全局不做任何跨任务协调（不知道是否共用资源，各自独立退避）。
    """

    NORMAL = "normal"    # 资源正常（或尚未观测到错误）：正常准入节奏
    VERIFY = "verify"    # 已步降，等剩余在途执行完成一步验证
    EMPTY = "empty"      # 任务全部退避（无在途执行）：按递增间隔探测

    def __init__(self, probe_base_sec: float = 30.0, probe_cap_sec: float = 600.0):
        self.probe_base_sec = float(probe_base_sec)
        self.probe_cap_sec = float(probe_cap_sec)
        self._state: dict[str, str] = {}
        self._last_shortage: dict[str, float] = {}
        self._level: dict[str, int] = {}     # EMPTY 探测级别（间隔递增）
        self._unverified: dict[str, str] = {}  # task -> 未完成首步的在途执行

    # ---- 状态查询 ----
    def state_of(self, task_id: str) -> str:
        return self._state.get(task_id, self.NORMAL)

    def probe_wait_remain(self, task_id: str, now_mono: float) -> float:
        """EMPTY 状态下距下次探测的剩余秒数（非 EMPTY 为 0）。"""
        if self.state_of(task_id) != self.EMPTY:
            return 0.0
        interval = min(self.probe_cap_sec,
                       self.probe_base_sec * (2 ** self._level.get(task_id, 0)))
        return max(0.0, interval - (now_mono - self._last_shortage.get(task_id, 0.0)))

    def probe_level_of(self, task_id: str) -> int:
        return self._level.get(task_id, 0)

    # ---- 状态迁移 ----
    def set_state(self, task_id: str, state: str, now_mono: float = 0.0) -> None:
        prev = self.state_of(task_id)
        self._state[task_id] = state
        if state == self.EMPTY:
            self._last_shortage[task_id] = now_mono
            if prev != self.EMPTY:
                self._level[task_id] = 0
        elif state == self.NORMAL:
            self._level.pop(task_id, None)

    def note_empty_probe_failed(self, task_id: str, now_mono: float) -> None:
        """EMPTY 中的探测失败（又报资源错误且无在途可退避）：级别 +1。"""
        self._level[task_id] = self._level.get(task_id, 0) + 1
        self._last_shortage[task_id] = now_mono

    def note_started(self, task_id: str, attempt_id: str) -> None:
        """新执行拉起（新作业或放回续跑）：登记"未完成首步"。"""
        self._unverified[task_id] = attempt_id

    def clear_attempt(self, task_id: str, attempt_id: str) -> None:
        """执行结束（任意结局）：若是该任务的未验证执行则解除占位。"""
        if self._unverified.get(task_id) == attempt_id:
            del self._unverified[task_id]

    def note_progress(self, task_id: str, attempt_id: str, now_mono: float) -> None:
        """一次正确执行的步骤（progress/checkpoint/成功）：
        ① 该执行通过首步验证（解除"未完成首步"占位，允许放下一个）；
        ② 任务处于 VERIFY/EMPTY → 资源对该任务恢复 → NORMAL，级别清零。
        """
        if self._unverified.get(task_id) == attempt_id:
            del self._unverified[task_id]
        if self.state_of(task_id) != self.NORMAL:
            self._state[task_id] = self.NORMAL
            self._level.pop(task_id, None)

    def admission_allowed(self, task_id: str, now_mono: float) -> tuple[bool, str]:
        """该任务此刻是否允许再放一个新执行（逐个拉取纪律）。

        返回 (允许, 原因)；原因非空表示阻止原因（观测用）。
        """
        if self._unverified.get(task_id):
            return False, "step_pending"   # 上一个新执行尚未完成一步
        st = self.state_of(task_id)
        if st == self.VERIFY:
            return False, "verify"         # 步降后等剩余执行的一步验证
        if st == self.EMPTY and self.probe_wait_remain(task_id, now_mono) > 0:
            return False, "probe_wait"     # 全退避探测间隔未到
        return True, ""

    def snapshot(self, now_mono: float) -> dict:
        """监控视图：各任务退避状态/探测级别/未验证执行。"""
        out: dict[str, dict] = {}
        for tid in sorted(set(self._state) | set(self._unverified)):
            st = self.state_of(tid)
            out[tid] = {
                "state": st,
                "probe_level": self._level.get(tid, 0),
                "probe_wait": round(self.probe_wait_remain(tid, now_mono), 1),
                "unverified": self._unverified.get(tid, "")[:16],
            }
        return out


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
        """记录任务级不足证据（shortage_streak），阻止退避期间的扩容探索。

        资源退避本体由 TaskBackoff 状态机承担（步降暂停同任务在途执行、
        逐步验证、全退避探测、逐个放回）——动态目标不再因不足收缩：
        恢复后按既有目标逐个（一步验证）拉回，无需重新学习。
        """
        ts = self.get(task_id)
        ts.shortage_streak += 1
        ts.last_shortage_mono = now_mono
        ts.mode = "explore"

    def note_recovered_progress(self, task_id: str, now_mono: float) -> None:
        """真实业务进展恢复：不足证据按观察期衰减清零（审查修复，设计第九节
        L153"无持续不足"、L157 长期任务"减慢探索"而非"永久停止探索"）。

        streak 此前仅在作业 SUCCEEDED 时清零——长期任务无完成事件，报一次
        75 后即使恢复真实进展也永不扩容。现在：有新进展样本且距最近一次
        不足已超过一个扩容观察期（不足确已消退）时清零 streak 并恢复 steady。
        """
        ts = self.get(task_id)
        if ts.shortage_streak > 0 and \
                now_mono - ts.last_shortage_mono >= ts.grow_observe_sec:
            ts.shortage_streak = 0
            ts.mode = "steady"

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
    released: dict[str, int] = field(default_factory=dict)      # 已放回任务 -> 优先级
    released_at: dict[str, float] = field(default_factory=dict)  # 已放回任务 -> 放回时刻（恢复步幅推导）
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
                    lower_tasks: int = 0,
                    cooldown_remain_sec: float = 0.0) -> bool:
        """启动/更新保护轮。

        防劫持规则（设计文档第十节）：
        - 保护进行中，仅当新对象优先级更高才立即切换保护对象；
        - 同级轮转：多个同优先级受阻时，当前轮过半后允许切换到另一同级对象；
        - 失败轮退避期间，仅当新对象优先级高于上一轮保护对象才允许立即重开。
        审查修复：同对象且当前为慢起因时，明确不足将 cause 升级为 shortage
        （快速节奏，设计第十一节 L195"一旦出现明确不足则转入快速保护"）——
        由调用方在 start_round 前置检查中处理，这里同对象分支同样接受刷新。
        cooldown_remain_sec：A 的不足退避冷却剩余时间，纳入窗口推导
        （设计第十节 L181：窗口长度含"冷却剩余时间"，保证 A 在保护截止前
        获得有效恢复观察机会）。
        返回是否生效。
        """
        st = self.st
        window = self._round_window(feedback_interval_sec, lower_tasks,
                                     cooldown_remain_sec)
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
            st.released_at = {}
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

    def _round_window(self, feedback_interval_sec: float, lower_tasks: int = 0,
                      cooldown_remain_sec: float = 0.0) -> float:
        """整轮保护窗口初值：按 A 的反馈周期与低优先级任务数推导。

        设计第十节：整轮保护截止"其长度根据低优先级数量、暂停耗时、A 的
        反馈周期和冷却剩余时间计算"——反馈周期长的 A 需要更长的观察窗才有
        机会体现改善；可退让的低优先级任务每个都需一次退让+观察间隔，
        窗口随之加长（暂停耗时由 note_yielded 按实际成本继续延长）；
        A 的不足冷却剩余时间也纳入（审查修复：冷却期长于窗口时 A 得不到
        有效恢复观察机会，保护会在 A 下次探测前截止）；总上限约束退让
        延长不无限续期。
        """
        w = float(self.cfg.control.protect_deadline_sec)
        if feedback_interval_sec > 0:
            w = max(w, feedback_interval_sec * 4.0)
        if lower_tasks > 0:
            w += min(lower_tasks, 20) * float(self.cfg.control.yield_observe_sec)
        if cooldown_remain_sec > 0:
            # 冷却剩余 + 两个反馈周期（冷却结束后 A 需要至少一次探测机会）
            w = max(w, min(cooldown_remain_sec + feedback_interval_sec * 2.0,
                           3600.0))
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
        st.released_at = {}
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
        放回时刻记入 released_at，供 resume_cap 推导恢复步幅。
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
        st.released_at[task_id] = now_mono
        interval = self.cfg.control.recovery_probe_sec
        if st.cause == "slow":
            interval *= 2
        st.next_release_mono = now_mono + interval
        return task_id

    def resume_cap(self, task_id: str, now_mono: float,
                   run_target: int) -> int | None:
        """已放回任务的临时准入上限（审查修复，设计第十节 L177"每次只恢复
        少量运行机会并观察 A"）。

        此前退让只暂停进程不调低 run_target，保护结束后该任务未被暂停过的
        QUEUED 作业按原目标一次性拉满，"逐步放回"对积压任务失效。现在已
        放回任务受临时上限约束：放回初期只给 1 个运行机会，随观察期以
        recovery_probe_sec 节奏逐步 +1，达到原目标后约束自然失效（毕业）。
        返回 None 表示该任务不在放回观察中（无额外约束）。
        """
        released_at = self.st.released_at.get(task_id)
        if released_at is None:
            return None
        if self.st.protected_task:
            return 1  # A 仍在保护/受阻观察中：放回只给最小运行机会
        steps = 1 + int((now_mono - released_at) /
                        float(self.cfg.control.recovery_probe_sec))
        return max(1, min(int(run_target), steps))

    def note_re_blocked(self, now_mono: float) -> dict[str, int]:
        """逐步放回期间保护对象再次受阻：暂停继续放回，并撤销近期已放回的任务。

        返回被撤销放回的任务 -> 优先级映射，供调度器重新暂停其存活执行
        （新一轮被失败轮退避拒绝时也必须撤回，不能任由已放回任务持续挤压 A）。
        撤销的同时清除放回时刻（恢复步幅约束随撤销解除，重新放回时重建）。
        """
        st = self.st
        st.next_release_mono = now_mono + self.cfg.control.protect_round_backoff_sec
        revoked = dict(st.released)
        st.released = {}
        st.released_at = {}
        return revoked