"""调度器主循环（设计文档第十四、十五节）。

启动：取单实例锁 → 恢复执行数据库与进程身份（完成记录核实 / 旧进程协作退出 /
按检查点重放 / 身份未知转 RECOVERY_BLOCKED）→ 独立确认所有生产者常驻（任务级单实例锁接管）。

每轮：消费控制命令 → 处理退出/心跳/进展/不足/检查点/控制确认（心跳过期、启动握手
超时监护）→ 重启恢复推进 → 增量接收（优先级序 + 同级轮转 + 时间片）→ 资源趋势
（CPU/内存/IO/OOM 滞回，观测不足减速探索）→ 动态目标 / 高优先级保护（优先级防劫持、
准入门禁、截止延长、改善释放、逐步放回）→ 不足到期重排（指数退避 + 抖动）→
按优先级/同级轮转/任务内顺序准入并握手启动 → 发布快照。

状态闭环（设计文档第七、十、十二节）：
- BACKOFF --(next_retry_at 到期)--> QUEUED（指数退避 + 随机抖动，封顶）；
- PAUSED --(保护结束逐步放回 / 无保护轮时)--> QUEUED；
- RECOVERING --(重启核实：旧进程退出或宽限超时)--> QUEUED；完成记录已持久化则 SUCCEEDED；
- RECOVERY_BLOCKED：attempt 意图已持久化但进程身份未知，保持可见、不启动重复 attempt。
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import random
import shutil
import signal
import subprocess
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import config, db as dbmod, producer as prodguard, receiver, runner, scaling, store
from .config import JobStatus, EventKind

log = logging.getLogger("sched.scheduler")


def _read_total_mem_mb() -> int:
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) // 1024
    except OSError:
        pass
    return 0


def _read_cpu_count() -> int:
    return os.cpu_count() or 1


def _now_mono() -> float:
    return time.monotonic()


def load_all_specs() -> dict[str, config.TaskSpec]:
    """扫描 tasks/ 加载全部任务清单（serve 与 CLI 共用，不构建服务实例）。"""
    specs: dict[str, config.TaskSpec] = {}
    td = config.tasks_dir()
    if td.is_dir():
        for sub in sorted(td.iterdir()):
            if not sub.is_dir():
                continue
            try:
                spec = config.load_task(sub.name)
                specs[sub.name] = spec
            except config.TaskError as e:
                log.warning("load task %s failed: %s", sub.name, e)
    return specs


class AttMeta:
    def __init__(self, attempt_id: str, job_id: str, task_id: str, priority: int,
                 token: str):
        self.attempt_id = attempt_id
        self.job_id = job_id
        self.task_id = task_id
        self.priority = priority
        self.token = token
        self.after_seq = 0
        self.status = JobStatus.STARTING
        self.had_completed = False
        self.shortage_at_mono = 0.0        # 最近一次显式不足时刻（排序信号，防布尔滞留）
        self.grown_at_mono = 0.0           # 本 attempt 启动（扩容）时刻
        self.last_progress_cu: int | None = None  # 进展单调基准（回退样本拒绝入基线）
        self.pause_command: dict | None = None
        self.pause_deadline_mono = 0.0
        self.pause_requested_mono = 0.0    # 暂停请求发出时刻（暂停时长/检查点延迟统计）
        self.last_ckpt_mono = 0.0          # 最近一次检查点事件到达时刻
        self.cancel_deadline_mono = 0.0
        self.started_mono = _now_mono()
        self.last_event_mono = _now_mono()
        self.last_heartbeat_mono = 0.0   # 上次心跳到达时刻（用于计算承诺反馈间隔）
        self.heartbeat_gap = 0.0         # 最近一次心跳间隔（秒）
        self.feedback_interval = 0.0     # 执行者承诺的反馈间隔（STARTED 事件携带；0=未申报）
        self.max_step_sec = 0.0          # 执行者申报的单步最长耗时（STARTED 携带；0=未申报）


class SchedulerService:
    def __init__(self, cfg: config.SchedulerConfig | None = None):
        self.cfg = cfg or config.load_config()
        self.db = dbmod.DB()
        self.guard = prodguard.ProducerGuard(self.cfg)
        self.scaling = scaling.Scaling(self.cfg)
        self.health = scaling.HealthTracker(
            slow_confidence_sec=self.cfg.control.slow_confidence_sec)
        self.protect = scaling.Protect(self.cfg)
        self.trend = scaling.ResourceTrend(
            unhealthy_ratio=self.cfg.control.resource_unhealthy_ratio,
            healthy_ratio=self.cfg.control.resource_healthy_ratio,
            stable_samples=self.cfg.control.resource_stable_samples,
            shed_ratio=self.cfg.control.exec_mem_cap_ratio)
        self.etrend = scaling.ExternalTrend(
            window_sec=self.cfg.control.external_hold_window_sec,
            trigger_events=self.cfg.control.external_hold_trigger_events,
            trigger_tasks=self.cfg.control.external_hold_trigger_tasks,
            recover_events=self.cfg.control.external_hold_recover_events,
            recover_sec=self.cfg.control.external_hold_recover_sec,
            suspect_lookback_sec=self.cfg.control.external_suspect_lookback_sec)
        self.specs: dict[str, config.TaskSpec] = {}
        self.attempts: dict[str, runner.AttemptRun] = {}
        self.meta: dict[str, AttMeta] = {}
        self._backoff_until: dict[str, float] = {}   # job_id -> 不足探测截止(monotonic)
        self._recover_wait: dict[str, tuple[dict, float]] = {}  # attempt -> (row, deadline)
        self._rr: dict[int, int] = {}                # 优先级同级轮转游标（接收扫描）
        self._adm_rr: dict[int, int] = {}            # 优先级同级轮转游标（准入）
        self._scan_pending: dict[str, int] = {}      # task -> 剩余未扫描文件数
        self._last_start_mono = 0.0
        self._healthy_since_mono = 0.0     # 资源趋势持续健康起点（启动间隔阶梯缩短用）
        self._admit_tokens = 0.0           # 启动份额（令牌制：间隔小于节拍时单轮可多次启动）
        self._last_admit_tick_mono = 0.0   # 上次准入评估时刻（份额累积用）
        self._last_mem_yield_mono = 0.0    # 上次内存退让时刻（增长退让/SHED 撤内存共用节流）
        self._last_sample_mono = 0.0
        self._sample_cpu = 0.0
        self._sample_mem_mb = 0
        self._sample_io = 0
        self._sample_attempt: dict[str, dict] = {}   # 每 attempt 资源摘要（快照导出）
        self._last_oom: dict[str, int] = {}          # attempt_id -> 上次 oom_kill 计数
        self._last_cpu_usec: dict[str, int] = {}     # attempt_id -> 上次 cpu usage_usec（差分用）
        self._last_io_bytes: dict[str, int] = {}     # attempt_id -> 上次 io.stat bytes（差分用）
        self._sample_pressure: dict = {}             # 执行范围压力指标（cgroup psi，监测用）
        self._prev_mem_mb = 0.0                      # 上次采样执行内存（快速增长检测用）
        self._observability_ok = True
        self._observability_logged = True
        # 监控统计（设计第十五节）：事件计数 + 关键路径延迟滑动窗口
        self._stats = {"protect_released": 0, "protect_failed": 0,
                       "oom_events": 0, "recovery_blocked_total": 0}
        self._disc_lat: deque[float] = deque(maxlen=50)   # 发现延迟（outbox 落盘→接收）
        self._start_lat: deque[float] = deque(maxlen=50)  # 启动延迟（排队→拉起）
        self._pause_dur: deque[float] = deque(maxlen=50)  # 暂停请求→实际退出
        self._ckpt_lat: deque[float] = deque(maxlen=50)   # 暂停请求→检查点落地
        self._next_retry_by_task: dict[str, str] = {}     # task -> 最早 next_retry_at（监控）
        self._last_shortage_mono: dict[str, float] = {}   # task -> 最近不足时刻（退避上限推导）
        self._last_rblocked_check_mono = 0.0              # RECOVERY_BLOCKED 复核节流
        self._last_reconcile_mono = 0.0                   # 归档对账节流
        self._reconcile_offset = 0                        # 归档对账分批游标（时间片）
        self._last_full_rescan_mono = 0.0                 # outbox 全量重扫兜底节流
        self._last_archive_gc_mono = 0.0                  # 归档 GC 节流（C1）
        self._status_scan: dict[str, tuple[int, int]] = {} # 状态扫描滚动游标 status -> (offset, len)（C5）
        self._last_cleanup_mono = 0.0                     # 已完结作业清理节流（B13）
        self._lock = threading.RLock()
        self._stopping = threading.Event()
        self._loop_thread: threading.Thread | None = None
        self._lock_fd = -1

    # ---------- 生命周期 ----------
    def _acquire_lock(self) -> bool:
        p = config.lock_file()
        p.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(p), os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            return False
        self._lock_fd = fd
        return True

    def start(self) -> None:
        config.ensure_layout()
        if not self._acquire_lock():
            raise RuntimeError("已有调度器实例在运行")
        self._recover()
        self._loop_thread = threading.Thread(target=self._loop, name="sched-loop", daemon=True)
        self._loop_thread.start()

    def stop(self) -> None:
        self._stopping.set()
        if self._loop_thread:
            self._loop_thread.join(timeout=5)
        # 优雅停机：只撤执行进程树。生产者保持运行（设计第十三节：仅重启调度器默认保持
        # 生产者，显式整套服务关闭由外部生命周期管理）；生产者经任务级单实例锁由新实例接管。
        # 并行协作收尾：先向全部执行进程同时发 TERM（worker 落检查点退出），宽限内
        # 轮询退出并按需升级 KILL——串行逐个 terminate 会把停机拖到 N×grace。
        runs = list(self.attempts.values())
        now_mono = _now_mono()
        for a in runs:
            try:
                a.begin_terminate(now_mono, self.cfg.control.pause_grace_base_sec)
            except Exception:
                pass
        deadline = now_mono + self.cfg.control.pause_grace_base_sec + 15.0
        while runs and _now_mono() < deadline:
            for a in runs[:]:
                try:
                    if not a.is_alive() or a.poll_terminate(_now_mono()) == "gone":
                        runs.remove(a)
                except Exception:
                    runs.remove(a)
            if runs:
                time.sleep(0.1)
        for a in list(self.attempts.values()):
            try:
                a.cleanup()   # 删除 attempt 的 cgroup：优雅停机不得泄漏目录
            except Exception:
                pass
        self.db.close()
        if self._lock_fd >= 0:
            try:
                os.close(self._lock_fd)
            except OSError:
                pass
            self._lock_fd = -1

    def _disk_completed(self, attempt_id: str) -> bool:
        """核实 attempt 是否已完成：执行数据库优先，run_dir/events.jsonl 兜底。

        设计第十二节"重启恢复"要求完成核实不只信执行数据库——调度器崩溃可能
        让"worker 已写完成事件、调度器未及入库"成为窗口。此处重读磁盘工件作为
        第二事实源：找到 attempt_id 匹配且未入库的 COMPLETED 事件时补录入库
        （(attempt_id, seq) 去重幂等）并返回 True。文件缺失/损坏行按"无完成
        记录"处理，不让恢复流程崩溃。
        """
        if self.db.has_event_kind(attempt_id, EventKind.COMPLETED):
            return True
        try:
            p = config.run_dir(attempt_id) / "events.jsonl"
            if not p.is_file():
                return False
            found = False
            with p.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        ev = json.loads(line)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        continue
                    if not isinstance(ev, dict):
                        continue
                    if ev.get("attempt_id") and ev["attempt_id"] != attempt_id:
                        continue
                    if ev.get("kind") != EventKind.COMPLETED:
                        continue
                    self.db.append_event(attempt_id, int(ev.get("seq") or 0),
                                         EventKind.COMPLETED, ev, runner.now_iso())
                    found = True
            return found
        except OSError:
            return False

    def _recover(self) -> None:
        """启动恢复：执行数据库 + 进程身份核实，不清空状态（设计文档第十三节）。"""
        self.refresh_specs()
        # 从持久化 task_state 恢复各任务的动态运行目标/模式，避免重启后全部从零冷启动
        for st in self.db.all_task_states():
            ts = self.scaling.get(st["task_id"])
            ts.run_target = int(st.get("run_target") or 0)
            if st.get("mode"):
                ts.mode = st["mode"]
        for task_id, spec in self.specs.items():
            if spec.enabled:
                self.guard.launch(task_id, spec)
        now = runner.now_iso()
        now_mono = _now_mono()
        # 上次在途 attempt 逐个核实（不再简单置 RECOVERING）
        for att in self.db.live_attempts():
            attempt_id = att["attempt_id"]
            job_id = att["job_id"]
            if self._disk_completed(attempt_id):
                # 完成记录已持久化 → 直接确认成功（执行数据库优先，磁盘工件兜底）
                self.db.commit_transition(
                    attempt_id,
                    {"status": JobStatus.SUCCEEDED, "finished_at": now},
                    job_id, JobStatus.SUCCEEDED, "重启恢复：完成记录已持久化", now)
                continue
            job_row = self.db.get_job(job_id) or {}
            if job_row.get("status") == JobStatus.CANCELLED:
                # C6：取消意图在重启前已持久化 → 终止旧进程、按取消收尾，
                # 不进入"协作退出后重放"让已取消作业复活
                pid = att.get("pid")
                pst = att.get("pid_start_time") or 0.0
                if pid and pst and runner.pid_alive_with_start(pid, pst):
                    runner.terminate_external(
                        pid, att.get("pgid") or pid, pst,
                        self.cfg.control.pause_grace_base_sec,
                        cgroup=str(att.get("cgroup") or ""))
                self.db.commit_transition(
                    attempt_id,
                    {"status": JobStatus.CANCELLED,
                     "reason": "重启恢复：作业已显式取消", "finished_at": now},
                    job_id, JobStatus.CANCELLED, "重启恢复：作业已显式取消", now)
                self._append_status({"ts": now, "kind": "recover_cancelled",
                                     "attempt_id": attempt_id, "job_id": job_id})
                continue
            pid = att.get("pid")
            pst = att.get("pid_start_time") or 0.0
            if pid and pst and runner.pid_alive_with_start(pid, pst):
                # 审查修复（设计第十三节 L221"存活 worker 可以通过事件与控制
                # 协议接管"）：身份核实仍存活的旧 worker 优先接管（重新挂接
                # 事件/控制文件继续观测，业务不中断）；接管失败才回退到
                # "协作退出 + 按检查点重放"的保守路径
                if self._adopt_attempt(att):
                    self._append_status({"ts": now, "kind": "attempt_adopted",
                                         "attempt_id": attempt_id,
                                         "job_id": job_id, "pid": pid})
                    continue
                # 旧执行进程仍存活：先协作 TERM（worker 落检查点退出），宽限后强杀重放
                try:
                    os.killpg(att.get("pgid") or pid, 15)
                except (ProcessLookupError, PermissionError, OSError):
                    pass
                self._recover_wait[attempt_id] = (
                    att, now_mono + self.cfg.control.recover_grace_sec)
                self.db.set_job_status(job_id, JobStatus.RECOVERING,
                                       "调度器重启：已请求旧执行进程协作退出", now)
            elif pid and not pst:
                # 遗留行（早期版本无 pid_start_time）：无法做 pid 身份核实。
                # 直接按"进程已退出"重放有风险——pid 若被复用则误判、pid 若仍是
                # 旧 worker 则重复执行。用 cgroup 名（含 attempt_id，唯一归属）
                # 兜底核实：可核实/已回收才重放，无法核实转 RECOVERY_BLOCKED。
                alive = False
                try:
                    os.kill(int(pid), 0)
                    alive = True
                except OSError:
                    alive = False
                requeued = False
                if alive:
                    cg_name = str(att.get("cgroup") or "")
                    if cg_name:
                        try:
                            from . import cgroup as _cg
                            cg = _cg.Cgroup(cg_name)
                            if cg.available and cg.procs():
                                cg.kill_all()  # cgroup 归属唯一，整组回收
                                requeued = True
                        except Exception:
                            requeued = False
                else:
                    requeued = True
                if requeued:
                    self.db.commit_transition(
                        attempt_id,
                        {"status": JobStatus.PAUSED, "reason":
                         "调度器重启：进程已退出或已按 cgroup 归属回收",
                         "finished_at": now},
                        job_id, JobStatus.QUEUED, "调度器重启：按检查点重放", now)
                else:
                    self.db.set_job_status(
                        job_id, JobStatus.RECOVERY_BLOCKED,
                        "遗留 attempt：pid 存活但无启动时间身份、cgroup 无法核实，需人工核实",
                        now)
            elif pid:
                # 进程已不在（pid+启动时间身份核实通过）：按上一个已提交检查点重放
                self.db.commit_transition(
                    attempt_id,
                    {"status": JobStatus.PAUSED, "reason": "调度器重启：进程已退出",
                     "finished_at": now},
                    job_id, JobStatus.QUEUED, "调度器重启：按检查点重放", now)
            else:
                # attempt 意图已持久化但进程身份未知（启动窗口崩溃）。
                # 审查修复（设计第十三节 L219"先核实握手和进程树，接管或清理
                # 之后才能创建新 attempt"）：用 cgroup 归属（含 attempt_id，
                # 唯一归属）自动核实并清理残留进程；清理确认后按检查点重放，
                # 不再一律依赖人工 requeue。杀不净（不可中断 D 态）才保持
                # RECOVERY_BLOCKED 待人工核实。
                cg_name = str(att.get("cgroup") or "")
                cleaned = True
                if cg_name:
                    try:
                        from . import cgroup as _cg
                        c = _cg.Cgroup(cg_name)
                        if c.available and c.procs():
                            c.kill_all()
                            # 有界复查：KILL 后仍有可运行残留（D 态）→ 无法确认清理
                            for _ in range(3):
                                time.sleep(0.1)
                                if not [p for p in c.procs()
                                        if not runner.proc_never_runs_again(p)]:
                                    break
                            else:
                                cleaned = False
                    except Exception:
                        cleaned = False
                if cleaned:
                    self.db.commit_transition(
                        attempt_id,
                        {"status": JobStatus.PAUSED, "reason":
                         "调度器重启：启动窗口崩溃，已按 cgroup 归属清理",
                         "finished_at": now},
                        job_id, JobStatus.QUEUED,
                        "调度器重启：启动窗口崩溃，清理后按检查点重放", now)
                else:
                    self.db.set_job_status(
                        job_id, JobStatus.RECOVERY_BLOCKED,
                        "启动窗口崩溃且 cgroup 残留进程无法清理，需人工核实", now)
        # 保护退让的重启节流恢复（设计第十节：结束保护后"节流恢复低优先级"）：
        # 上实例保护轮退让/撤销放回的 PAUSED 作业不一次性放回——按任务分组注入
        # 逐步放回队列，保持 recovery_probe_sec 节奏；保护轮本身不重建
        # （重启即视为本轮结束，失败轮退避不跨实例保留）。
        yield_resume: dict[str, int] = {}
        for j in self.db.jobs_in_status(JobStatus.PAUSED, limit=10000):
            r = j.get("reason") or ""
            if not (r.startswith("高优先级保护退让") or r.startswith("撤销放回")):
                continue
            tid = j["task_id"]
            pri = int(j["priority"])
            if tid not in yield_resume or pri > yield_resume[tid]:
                yield_resume[tid] = pri
        if yield_resume:
            st = self.protect.st
            st.pending_resume.update(yield_resume)
            st.next_release_mono = now_mono + self.cfg.control.recovery_probe_sec
            self._append_status({"ts": now, "kind": "protect_resume_recovered",
                                 "tasks": sorted(yield_resume)})
        # 清理上个实例遗留的孤儿 cgroup 进程；排除刚请求协作退出的存活 worker
        # （它们的 cgroup 及祖先/后代不得写 cgroup.kill，否则宽限期形同虚设）
        try:
            from . import cgroup as cg
            exclude = {att.get("cgroup") or ""
                       for att, _dl in self._recover_wait.values()}
            exclude.discard("")
            n = cg.reap_orphans(exclude=exclude or None)
            if n:
                log.info("reaped %d orphan procs under sched cgroup subtree", n)
        except Exception:
            pass

    def _adopt_attempt(self, att: dict) -> bool:
        """接管存活的旧 worker（设计第十三节 L221）：以持久化执行身份重建
        AttemptRun 句柄（非子进程，无退出码），重新挂接事件/控制文件继续
        观测与控制。事件从库内已见最大 seq 续读（避免重放已入库事件产生
        状态噪声）；permit 幂等补写（等待许可中的 STARTING worker 立即解锁）。
        接管后退出码未知时由 decide_outcome 归入 RECOVERING 待核实。
        任何构造失败返回 False（调用方回退到协作退出+重放路径）。
        """
        attempt_id = att["attempt_id"]
        job_id = att["job_id"]
        try:
            job = self.db.get_job(job_id)
            if job is None:
                return False
            task_id = job["task_id"]
            spec = self.specs.get(task_id)
            if spec is None or not spec.enabled:
                return False  # 任务已移除/禁用：不接管，走回收路径
            import json as _json
            aspec = runner.AttemptSpec(
                attempt_id=attempt_id, job_id=job_id, task_id=task_id,
                token=str(att.get("token") or ""),
                task_root=config.task_dir(task_id),
                executor_entry=spec.executor_entry,
                entry_args=_json.loads(job["entry_args_json"]),
                workdir=job["workdir"],
                env=_json.loads(job["env_json"]),
                progress_semver=job["progress_semver"],
            )
            from . import cgroup as _cg
            cg_name = str(att.get("cgroup") or "")
            cgrp = _cg.Cgroup(cg_name) if cg_name else _cg.Cgroup(
                f"{task_id}/{attempt_id}")
            a = runner.AttemptRun.adopted(
                aspec, config.run_dir(attempt_id), cgrp,
                _json.loads(job["input_refs_json"]),
                pid=int(att.get("pid") or 0),
                pgid=int(att.get("pgid") or att.get("pid") or 0),
                start_time=float(att.get("pid_start_time") or 0.0),
                last_seq=self.db.max_event_seq(attempt_id))
            meta = AttMeta(attempt_id, job_id, task_id, int(job["priority"]),
                           aspec.token)
            meta.status = str(att.get("status") or JobStatus.STARTING)
            meta.feedback_interval = spec.feedback_interval_sec or 0.0
            self.attempts[attempt_id] = a
            self.meta[attempt_id] = meta
            # permit 幂等补写：等待许可中的 worker 立即解锁（已授予的无影响）
            try:
                a.write_permit()
            except Exception:
                pass
            return True
        except Exception:
            log.exception("adopt attempt %s failed, fallback to recycle", attempt_id)
            self.attempts.pop(attempt_id, None)
            self.meta.pop(attempt_id, None)
            return False

    def refresh_specs(self) -> dict[str, config.TaskSpec]:
        specs = load_all_specs()
        self.specs = specs
        # 控制节奏随任务反馈周期缩放（设计第十四节：观察时间随处理周期调整）：
        # 扩容观察期不短于 4×反馈间隔，封顶 600s；变慢观察期同步登记反馈间隔
        # （审查修复，设计第十一节 L193"观察期参考实际反馈间隔"）
        for tid, spec in specs.items():
            if spec.feedback_interval_sec:
                ts = self.scaling.get(tid)
                ts.grow_observe_sec = min(
                    600.0, max(self.cfg.control.grow_observe_sec,
                               spec.feedback_interval_sec * 4.0))
            self.health.set_task_feedback(tid, spec.feedback_interval_sec or 0.0)
        return specs

    # ---------- 主循环 ----------
    def _loop(self) -> None:
        last_refresh = time.monotonic()
        while not self._stopping.wait(self.cfg.control.loop_tick_sec):
            try:
                now = _now_mono()
                if now - last_refresh >= 5.0:
                    self.refresh_specs()
                    last_refresh = now
                # 生产者崩溃恢复每轮独立维护（设计第十五节）：poll 存活检查
                # 廉价，不与 task.json 解析（较重）共享 5s 节流
                self._tick_ensure_producers()
                # 循环顺序（设计第十三节 255-265）：控制命令最先（运维指令低延迟）、
                # 在途执行事件/退出次之（先核退出去向再做接纳新请求的决策）、
                # 重启恢复推进、增量接收、资源趋势、控制、不足重排、准入、快照。
                self._tick_commands()
                self._tick_attempts(now)
                self._tick_recover(now)
                self._tick_recover_status(now)
                self._tick_receive(runner.now_iso())
                self._tick_cleanup(now)
                self._tick_archive_gc(now)
                self._tick_resource(now)
                self._tick_control(now)
                self._tick_backoff(now)
                self._tick_external(now)
                self._tick_admit(now)
                self._write_snapshot()
            except Exception:
                log.exception("scheduler loop iteration failed")

    def _tick_ensure_producers(self) -> None:
        # 审查修复（设计第三节 L35"任务间的生产者发现及守护独立进行"）：
        # 单任务守护异常（拉起失败/日志目录不可写等）不得传播到整轮循环——
        # 此前任一任务 launch 抛异常会跳过本轮全部后续步骤（接收/准入/监控）
        for task_id, spec in list(self.specs.items()):
            try:
                if spec.enabled:
                    newly_crashed = self.guard.ensure(task_id, spec)
                    if newly_crashed:
                        # 反复崩溃持续报警（设计第三节），不退避存活生产者
                        self._append_status({"ts": runner.now_iso(),
                                             "kind": "producer_crash",
                                             "task_id": task_id})
                else:
                    # 禁用任务：停守护其生产者（既有请求不丢弃）
                    self.guard.stop_task(task_id)
            except Exception:
                log.exception("producer guard failed for task %s", task_id)

    def _tick_receive(self, now: str) -> None:
        # 周期性完整对账（审查修复，设计第五节 L81"周期性完整对账，确保所有
        # 正式文件最终被发现"）：目录 mtime 增量缓存之外，定期强制全量重扫
        # 各任务 outbox 目录树兜底（seen 去重保留，幂等）；缓解 mtime 精度
        # 异常/第三方原地覆写导致的漏收无对账可发现
        now_mono0 = _now_mono()
        if now_mono0 - self._last_full_rescan_mono >= self.FULL_RESCAN_INTERVAL_SEC:
            self._last_full_rescan_mono = now_mono0
            receiver.force_full_rescan()
        order = self._scan_order()
        summaries = receiver.receive_all(
            self.db, now, {t: self.specs[t] for t in order}, order=order,
            batch_size=self.cfg.control.scan_batch_size)
        pending = {t: int(s.get("deferred") or 0)
                   for t, s in summaries.items() if s.get("deferred")}
        self._scan_pending = pending
        # 发现延迟监控汇入（outbox 文件 mtime → 被接收发现的耗时）
        for s in summaries.values():
            for lat in (s or {}).get("discovery_latencies") or []:
                self._disc_lat.append(float(lat))
        # 周期性轻量对账（默认 10 分钟一次）：archive 归档数 vs 库内作业数
        now_mono = _now_mono()
        if now_mono - self._last_reconcile_mono >= self.RECONCILE_INTERVAL_SEC:
            self._last_reconcile_mono = now_mono
            self._reconcile_archive()

    RECONCILE_INTERVAL_SEC = 600.0
    ARCHIVE_RECONCILE_WINDOW_SEC = 48 * 3600.0  # 窗口对账覆盖时长（GC 保留期以内）
    FULL_RESCAN_INTERVAL_SEC = 3600.0            # outbox 全量重扫兜底周期
    RECONCILE_BATCH = 500                        # 归档对账每批行数（时间片）

    def _reconcile_archive(self) -> None:
        """归档窗口对账（只告警不自动修复，设计文档第十五节：可观测、可发现）。

        C1：接收链要求每个成功入库的作业最终在 archive/<task>/<job_id>.json
        归档（归档是接收最后一步）；配合归档 GC（旧文件按保留期删除），对账
        只针对近窗口内接收的作业：库内有、归档文件缺失 → 告警。旧作业的
        归档已被 GC 清理属正常，不在对账范围。单向检查（库→文件）成立：
        归档失败时文件留在 outbox 下轮重享幂等，不存在"库无而文件在"的
        遗漏证据形态。
        审查修复（对账时间片，设计第十五节 L257"接收与历史对账保留时间片"）：
        走 idx_jobs_queued_at 索引分批核对（每批 RECONCILE_BATCH 行），
        批间由下轮对账周期继续推进（滚动游标），大规模积压时不再一次性
        fetchall 全量载入阻塞主循环。
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(
            seconds=self.ARCHIVE_RECONCILE_WINDOW_SEC)
        ).isoformat(timespec="seconds")
        while True:
            offset = self._reconcile_offset
            rows = self.db.jobs_received_since(
                cutoff, limit=self.RECONCILE_BATCH, offset=offset)
            if not rows:
                self._reconcile_offset = 0  # 窗口核对完毕：游标复位
                return
            for task_id, job_id in rows:
                p = config.runtime_dir() / "archive" / task_id / f"{job_id}.json"
                if not p.exists():
                    log.warning("archive reconciliation: job %s (task %s, received "
                                "within %.0fh) missing archive file",
                                job_id, task_id, self.ARCHIVE_RECONCILE_WINDOW_SEC / 3600)
                    self._append_status({"ts": runner.now_iso(),
                                         "kind": "archive_missing",
                                         "task_id": task_id, "job_id": job_id})
            self._reconcile_offset = offset + len(rows)
            if len(rows) < self.RECONCILE_BATCH:
                self._reconcile_offset = 0
                return

    # ---------- 归档 GC（C1：归档文件有界保留，防无限累积） ----------
    ARCHIVE_GC_INTERVAL_SEC = 12 * 3600.0
    ARCHIVE_RETAIN_SEC = 7 * 86400.0

    def _tick_archive_gc(self, now_mono: float) -> None:
        """删除超过保留期（7 天）的已归档就绪 JSON。

        归档的对账价值由 48h 窗口对账承担（_reconcile_archive），超期归档
        仅占磁盘（实测单任务累积数千文件会拖慢对账与运维巡检）；按 mtime
        判断新旧，删除失败忽略下轮重试。
        """
        if now_mono - self._last_archive_gc_mono < self.ARCHIVE_GC_INTERVAL_SEC:
            return
        self._last_archive_gc_mono = now_mono
        root = config.runtime_dir() / "archive"
        if not root.is_dir():
            return
        cutoff = time.time() - self.ARCHIVE_RETAIN_SEC
        gone = 0
        for d in root.iterdir():
            if not d.is_dir():
                continue
            for p in d.glob("*.json"):
                try:
                    if p.is_file() and p.stat().st_mtime < cutoff:
                        p.unlink()
                        gone += 1
                except OSError:
                    continue
        if gone:
            log.info("archive gc: removed %d archived files older than %d days",
                     gone, int(self.ARCHIVE_RETAIN_SEC // 86400))

    def _scan_order(self) -> list[str]:
        """接收顺序：任务优先级降序，同级任务轮转（设计文档第五节）。"""
        groups: dict[int, list[str]] = {}
        for tid, spec in self.specs.items():
            if spec.enabled:
                groups.setdefault(spec.priority, []).append(tid)
        order: list[str] = []
        for pri in sorted(groups, reverse=True):
            members = sorted(groups[pri])
            if len(members) > 1:
                off = self._rr.get(pri, 0)
                self._rr[pri] = (off + 1) % len(members)
                members = members[off:] + members[:off]
            order.extend(members)
        return order

    # ---------- CLI 控制命令（文件 IPC） ----------
    def _tick_commands(self) -> None:
        try:
            cmds = store.read_jsonl(config.commands_file())
        except Exception:
            return
        if not cmds:
            return
        acks = []
        ids = set()
        for c in cmds:
            cid = c.get("id")
            if cid:
                ids.add(cid)
            try:
                ok, detail = self._exec_command(c)
                acks.append({"id": cid, "ok": bool(ok), "detail": str(detail),
                             "ts": runner.now_iso()})
            except Exception as e:  # noqa: BLE001
                acks.append({"id": cid, "ok": False, "detail": str(e),
                             "ts": runner.now_iso()})
        try:
            # 先落 ack 再移除命令：中途崩溃时命令至多重放（取消/重排幂等），
            # 不会出现"命令已删但确认丢失"让 CLI 永远等不到响应
            for ack in acks:
                store.append_jsonl(config.commands_ack_file(), ack)
            store.remove_jsonl(config.commands_file(),
                               lambda o, _ids=ids: o.get("id") in _ids)
        except Exception:
            log.exception("ack commands failed")

    def _exec_command(self, c: dict) -> tuple[bool, str]:
        kind = c.get("kind")
        target = c.get("target") or ""
        reason = c.get("reason") or ""
        if kind == "cancel_job":
            ok = self.cancel_job(target, reason or "显式取消")
            return ok, "cancelled" if ok else "not found"
        if kind == "cancel_attempt":
            ok = self.cancel_attempt(target, reason or "显式取消")
            return ok, "cancelled" if ok else "not found"
        if kind == "pause_attempt":
            # 任务级"停止全部执行"：协作暂停（检查点保留），作业经 PAUSED
            # 自动回 QUEUED；任务禁用时被准入闸门挡住，重新启用后自动续跑
            meta = self.meta.get(target)
            if meta is None or meta.status not in (JobStatus.RUNNING,
                                                   JobStatus.STARTING):
                return False, "not running"
            self._request_pause(target, reason or "任务级停止")
            return True, "pausing"
        if kind == "requeue_job":
            ok = self.requeue_job(target, reason or "人工重排")
            return ok, "requeued" if ok else "not requeueable"
        return False, f"未知命令类型：{kind}"

    # ---------- 执行尝试事件/退出 ----------
    def _tick_attempts(self, now_mono: float) -> None:
        now = runner.now_iso()
        for attempt_id in list(self.attempts.keys()):
            a = self.attempts.get(attempt_id)
            meta = self.meta.get(attempt_id)
            if a is None or meta is None:
                continue
            self._ingest_events(a, meta)

            if not a.is_alive():
                self._finalize(a, meta, now)
                continue
            if meta.status == JobStatus.PAUSING:
                if now_mono >= meta.pause_deadline_mono:
                    # 暂停宽限超时 → 强撤进程树（非阻塞状态机：TERM → 到期 KILL →
                    # D 态兜底），靠上次已提交检查点恢复。主循环内不做同步等待：
                    # 单个卡死 worker 会把整个控制平面（快照/命令/心跳/准入）
                    # 冻结最长 grace 秒。
                    if a.begin_terminate(now_mono, self.cfg.control.pause_grace_max_sec):
                        log.warning("pause grace exceeded for attempt %s, TERM sent",
                                    attempt_id)
                        self._append_status({
                            "ts": now, "kind": "pause_grace_exceeded",
                            "attempt_id": attempt_id, "job_id": meta.job_id,
                            "task_id": meta.task_id})
                    if not a.is_alive() or a.poll_terminate(now_mono) == "gone":
                        self._finalize(a, meta, now)
                continue
            if meta.status == JobStatus.CANCELLED:
                if now_mono >= meta.cancel_deadline_mono:
                    a.begin_terminate(now_mono, self.cfg.control.pause_grace_base_sec)
                    if not a.is_alive() or a.poll_terminate(now_mono) == "gone":
                        self._finalize(a, meta, now)
                continue
            if meta.status == JobStatus.RUNNING:
                stale_threshold = self._heartbeat_stale_threshold(meta)
                if now_mono - meta.last_event_mono > stale_threshold:
                    # 心跳过期（依据承诺反馈间隔与实际延迟推导，非固定等待）：先告警 + 协作暂停
                    log.warning("attempt %s heartbeat stale >%.1fs, requesting pause",
                                attempt_id, stale_threshold)
                    self._append_status({
                        "ts": now, "kind": "heartbeat_stale", "attempt_id": attempt_id,
                        "job_id": meta.job_id, "task_id": meta.task_id,
                        "stale_sec": round(now_mono - meta.last_event_mono, 1)})
                    self._request_pause(attempt_id, reason="心跳过期，疑似卡住")
            elif meta.status == JobStatus.STARTING:
                if now_mono - meta.started_mono > self.cfg.control.starting_timeout_sec:
                    # 启动握手超时：拉起后长时间无 started 事件（非阻塞撤进程树）
                    log.warning("attempt %s starting handshake timeout", attempt_id)
                    a.begin_terminate(now_mono, self.cfg.control.pause_grace_base_sec)
                    if not a.is_alive() or a.poll_terminate(now_mono) == "gone":
                        self._finalize(a, meta, now, reason_override="启动握手超时")

    def _ingest_events(self, a: runner.AttemptRun, meta: AttMeta) -> None:
        for ev in a.read_events(meta.after_seq):
            seq = int(ev.get("seq") or 0)
            meta.after_seq = max(meta.after_seq, seq)
            meta.last_event_mono = _now_mono()
            kind = ev.get("kind")
            if kind == EventKind.STARTED:
                if meta.status == JobStatus.STARTING:
                    fi = ev.get("feedback_interval_sec")
                    if isinstance(fi, (int, float)) and not isinstance(fi, bool) and fi > 0:
                        meta.feedback_interval = float(fi)  # 反馈间隔契约（STARTED 申报）
                    ms = ev.get("max_step_sec")
                    if isinstance(ms, (int, float)) and not isinstance(ms, bool) and ms > 0:
                        meta.max_step_sec = float(ms)  # B14：单步耗时申报（暂停宽限推导）
                    self._transition(meta, JobStatus.RUNNING)
                    # A7 启动许可：调度器确认 STARTED/RUNNING 后写 permit.json，
                    # worker 在许可前不产生业务副作用（设计第十三节）
                    try:
                        a.write_permit()
                    except Exception:
                        log.warning("write permit failed for attempt %s",
                                    meta.attempt_id)
            elif kind == EventKind.HEARTBEAT:
                now_m = _now_mono()
                if meta.last_heartbeat_mono:
                    meta.heartbeat_gap = now_m - meta.last_heartbeat_mono
                meta.last_heartbeat_mono = now_m
            elif kind == EventKind.PROGRESS:
                self._ingest_progress_event(meta, int(ev.get("completed_units") or 0), ev)
                self.db.append_event(meta.attempt_id, seq, kind, ev, runner.now_iso())
            elif kind == EventKind.SHORTAGE:
                meta.shortage_at_mono = _now_mono()
                self.db.append_event(meta.attempt_id, seq, kind, ev, runner.now_iso())
                self._on_explicit_shortage(meta)
            elif kind == EventKind.CONTROL_ACK:
                self.db.append_event(meta.attempt_id, seq, kind, ev, runner.now_iso())
            elif kind == EventKind.CHECKPOINT:
                meta.last_ckpt_mono = _now_mono()
                self.db.record_checkpoint(meta.attempt_id, int(ev.get("commit_seq") or 0),
                                          str(ev.get("input_ref") or ""), runner.now_iso())
                self.db.append_event(meta.attempt_id, seq, kind, ev, runner.now_iso())
            elif kind == EventKind.COMPLETED:
                meta.had_completed = True
                self.db.append_event(meta.attempt_id, seq, kind, ev, runner.now_iso())
            else:
                self.db.append_event(meta.attempt_id, seq, kind, ev, runner.now_iso())

    def _ingest_progress_event(self, meta: AttMeta, cu: int, ev: dict) -> None:
        """进展样本入健康基线（审查修复，设计第六节 L91"进展必须给出单调递增
        的已完成单元数"）：回退样本拒绝入基线（防污染变慢判定的速率差分），
        仅告警；新 attempt 从 0 重新计数合法（meta 级基准）。"""
        if meta.last_progress_cu is not None and cu < meta.last_progress_cu:
            log.warning("attempt %s progress regressed %d -> %d, "
                        "sample ignored for baseline",
                        meta.attempt_id, meta.last_progress_cu, cu)
            return
        meta.last_progress_cu = cu
        self.health.record_progress(
            meta.task_id, _now_mono(), cu,
            bool(ev.get("has_unfinished")),
            str(ev.get("stage") or ""),
            ev.get("active_duration_sec"),
            ev.get("in_flight_duration_sec"),
            ev.get("work_units"),
            ev.get("waiting"),
            str(ev.get("metric_semver") or ""),
        )

    def _transition(self, meta: AttMeta, status: str, reason: str = "") -> None:
        meta.status = status
        self.db.commit_transition(
            meta.attempt_id, {"status": status}, meta.job_id, status, reason,
            runner.now_iso())

    def _on_explicit_shortage(self, meta: AttMeta) -> None:
        # 高优先级明确不足：把该任务设为保护对象（优先级防劫持在 Protect 内）
        now = _now_mono()
        # 不足证据破坏"稳定健康"：启动间隔阶梯立即回弹到配置初值（设计第九节）
        self._healthy_since_mono = 0.0
        if self.protect.st.protected_task == meta.task_id:
            st = self.protect.st
            if st.cause == "slow":
                # 审查修复（设计第十一节 L195"一旦出现明确不足则转入快速保护"）：
                # 慢保护轮中 A 恶化为明确不足 → cause 升级，退让/放回间隔不再
                # 按慢信号加倍，恢复快速节奏疏通 A
                st.cause = "shortage"
                self._append_status({"ts": runner.now_iso(),
                                     "kind": "protect_cause_upgraded",
                                     "task_id": meta.task_id,
                                     "from": "slow", "to": "shortage"})
            self.protect.note_block(now)
            return
        # A2：保护语义限于高优先级（设计第十节）——报不足对象不严格高于
        # 存活执行/排队作业的最低优先级时，无退让空间可言，不起保护轮
        # （任务按第六节统一不足 75 退出冷却自行恢复）；否则同优先级任务
        # 互相报不足会互冻结。
        if not self._protect_worthy(meta.priority):
            return
        # A2：先快照已放回任务——无保护对象时 start_round 内部 note_re_blocked
        # 会撤销近期放回；若新一轮保护被失败轮退避拒绝（返回 False），已放回
        # 任务既无门禁也无退让约束，将持续挤压再次受阻的高优先级对象，必须撤回。
        released_snapshot = dict(self.protect.st.released)
        spec = self.specs.get(meta.task_id)
        fi = (spec.feedback_interval_sec or 0.0) if spec is not None else 0.0
        ok = self.protect.start_round(meta.task_id, meta.priority, now,
                                      reason="统一外部不足", cause="shortage",
                                      feedback_interval_sec=fi,
                                      lower_tasks=self._lower_task_count(meta.priority),
                                      cooldown_remain_sec=self._backoff_remain_sec(meta.task_id))
        if not ok and released_snapshot:
            self._revoke_released(meta, released_snapshot)

    def _backoff_remain_sec(self, task_id: str) -> float:
        """任务最早不足重试的剩余冷却秒数（保护窗口推导用，设计第十节 L181
        "冷却剩余时间"参与窗口计算——冷却期长于窗口时 A 得不到有效恢复
        观察机会）。"""
        nr = self._next_retry_by_task.get(task_id) or ""
        if not nr:
            return 0.0
        try:
            dt = datetime.fromisoformat(nr)
            if dt.tzinfo is None:
                dt = dt.astimezone()
            return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
        except ValueError:
            return 0.0

    def _protect_worthy(self, priority: int) -> bool:
        """A2：保护对象须严格高于当前存活执行/排队作业的最低优先级。

        无存活执行且无排队作业时无退让空间，不起保护轮（设计第十节
        "高优先级保护"语义）。"""
        pris = [m.priority for m in self.meta.values()
                if m.status in (JobStatus.STARTING, JobStatus.RUNNING, JobStatus.PAUSING)]
        q = self.db.min_queued_priority()
        if q is not None:
            pris.append(q)
        if not pris:
            return False
        return priority > min(pris)

    def _revoke_released(self, blocked: AttMeta, revoked: dict[str, int]) -> None:
        """A2 撤销近期放回（设计第十节：放回可撤销）。

        逐步放回期间受阻对象再次报不足、而新一轮保护被退避拒绝时：对优先级
        低于受阻者的已放回任务，存活执行重新请求协作暂停（落检查点）、
        尚未准入的作业回退 PAUSED，并回灌 pending_resume——退避结束后仍按
        recovery_probe_sec 节奏逐步放回（可恢复），不复活也不永久扣押。
        """
        st = self.protect.st
        now = runner.now_iso()
        for task_id, pri in revoked.items():
            if pri >= blocked.priority:
                continue  # 高于/等于受阻者的放回不撤（优先级语义）
            for aid, m in list(self.meta.items()):
                if m.task_id == task_id and \
                        m.status in (JobStatus.STARTING, JobStatus.RUNNING):
                    self._request_pause(
                        aid, reason=f"撤销放回：{blocked.task_id} 再次受阻")
            for j in self.db.jobs_in_status(JobStatus.QUEUED, task_id=task_id):
                self.db.set_job_status(
                    j["job_id"], JobStatus.PAUSED,
                    f"撤销放回：{blocked.task_id} 再次受阻，等待重新逐步放回", now)
            st.pending_resume[task_id] = pri
            self._append_status({"ts": now, "kind": "release_revoked",
                                 "task_id": task_id,
                                 "blocked_task": blocked.task_id})

    def _finalize(self, a: runner.AttemptRun, meta: AttMeta, now: str,
                  reason_override: str = "") -> None:
        # 退出判定前再读一次尾部事件：worker 退出前最后一帧 completed/checkpoint 可能尚未
        # 被上轮 _ingest_events 读到，否则会被误判为"退出 0 但无完成记录"。
        self._ingest_events(a, meta)
        # B6：提交终态前确认整棵进程树退出（主进程退出 ≠ 树退出，残留孙进程
        # 会脱离 cgroup 记账成为资源黑洞）；有界等待，不冻结控制平面。
        self._ensure_tree_gone(a)
        # 确认整棵进程树退出（D 态最多再等 1s；仍未退出按未知退出码收尾，不阻塞主循环）
        if a.proc is not None:
            try:
                a.proc.wait(timeout=1)
            except subprocess.TimeoutExpired:
                pass
            exit_code = a.proc.returncode
        else:
            # 接管句柄（非子进程）无法 wait：退出码未知 → decide_outcome 归入
            # RECOVERING 待核实（设计第十二节"状态歧义为 RECOVERING"）
            exit_code = None
        status_ctx = JobStatus.PAUSING if meta.status == JobStatus.PAUSING else \
            (JobStatus.CANCELLED if meta.status == JobStatus.CANCELLED else "")
        outcome = runner.decide_outcome(status_ctx, exit_code, meta.had_completed)
        reason = reason_override or outcome.reason
        if not reason_override and outcome.status == JobStatus.PAUSED \
                and meta.pause_command and meta.pause_command.get("reason"):
            # 保留发起暂停的原因（心跳过期/OOM/保护退让），供恢复冷却与审计区分暂停来源
            reason = f"{meta.pause_command['reason']}；{outcome.reason}"
        self.db.commit_transition(
            meta.attempt_id,
            {"status": outcome.status, "exit_code": exit_code, "reason": reason,
             "finished_at": now, "pid": a.pid, "pgid": a.pgid,
             "pid_start_time": a.start_time},
            meta.job_id, outcome.status, reason, now)
        a.cleanup()

        self.attempts.pop(meta.attempt_id, None)
        self.meta.pop(meta.attempt_id, None)
        self._last_oom.pop(meta.attempt_id, None)

        # 暂停闭环统计：暂停请求 → 实际退出耗时；暂停请求 → 检查点落地延迟
        if (outcome.status in (JobStatus.PAUSED, JobStatus.BACKOFF,
                               JobStatus.CANCELLED)
                and meta.pause_requested_mono > 0):
            now_m = _now_mono()
            self._pause_dur.append(now_m - meta.pause_requested_mono)
            if meta.last_ckpt_mono >= meta.pause_requested_mono:
                self._ckpt_lat.append(meta.last_ckpt_mono - meta.pause_requested_mono)

        stid = meta.task_id
        if outcome.status == JobStatus.SUCCEEDED:
            self.scaling.on_success(stid, _now_mono())
            self._persist_scale(stid)  # explore→steady 恢复也持久化，重启不回退
            # B7（设计第十四节）：成功后不足退避计数复位，下次从起点重新探测
            self.db.reset_job_retry(meta.job_id)
        elif outcome.status == JobStatus.BACKOFF:
            # 统一外部不足才收缩动态目标（exp: 资源不足=不足信号）
            self.scaling.on_shortage(stid, _now_mono(), self._live_count(stid))
            self._persist_scale(stid)  # 收缩后立即持久化，重启不回到旧目标
            # 不足证据破坏"稳定健康"：启动间隔阶梯立即回弹（设计第九节）
            self._healthy_since_mono = 0.0
            # B7：本 attempt 已有真实业务进展（启动后收到过检查点）时，
            # 重试计数从头开始，不足恢复后的等待不继承历史退避长度
            self._schedule_backoff_retry(
                meta.job_id,
                had_evidence=meta.last_ckpt_mono > meta.started_mono)
        elif outcome.status == JobStatus.PAUSED:
            # 主动退让不计为任务错误；记录真实暂停耗时（含检查点落地）到
            # 任务级暂停成本 EMA（设计第十节：退让候选综合暂停成本排序）
            if meta.pause_requested_mono > 0:
                self.scaling.note_pause_cost(
                    stid, _now_mono() - meta.pause_requested_mono)
        elif outcome.status == JobStatus.FAILED:
            # 业务逻辑/未知失败不得冒充外部不足，也不收缩动态目标（设计第十二节）；
            # 否则 shortage_streak 只增不清会让 may_grow 永久返回 False，导致准入停滞。
            pass

        self._append_status({
            "ts": now, "kind": "attempt_finalized", "attempt_id": meta.attempt_id,
            "job_id": meta.job_id, "task_id": stid, "exit_code": exit_code,
            "status": outcome.status, "reason": reason,
        })

    def _schedule_backoff_retry(self, job_id: str, had_evidence: bool = False) -> None:
        """不足重试：指数增长 + 随机抖动，封顶后保持长等待但继续探测（设计第十四节）。

        B7（设计第十四节：恢复后逐步减少等待）：本 attempt 已有真实业务进展
        （检查点事件在启动后到达过）时按 1 计数，等待从起点重新增长。
        审查修复（等待上限随"近期失败间隔"调整，设计第十四节 L239）：任务在
        短时间内连续不足（失败间隔 < 300s）说明外部严重不可得，等待上限至少
        提高到 600s——更长等待、更低频探测，避免重试风暴。
        """
        job = self.db.get_job(job_id) or {}
        task_id = str(job.get("task_id") or "")
        # 外部压力闸门输入：一次外部不足事件进入滑动窗口（密度×广度判定）
        self.etrend.note_shortage(task_id, _now_mono())
        retry = 1 if had_evidence else int(job.get("retry_count") or 0) + 1
        base = self.cfg.control.shortage_retry_base_sec
        cap = self.cfg.control.shortage_retry_cap_sec
        # 等待上限随任务处理周期调整（设计第十四节：等待上限和观察时间随
        # 近期失败间隔与处理周期调整）：不低于 8×反馈间隔，封顶 3600s
        spec = self.specs.get(task_id)
        fi = (spec.feedback_interval_sec or 0.0) if spec is not None else 0.0
        if fi > 0:
            cap = min(3600.0, max(cap, fi * 8.0))
        # 近期失败间隔：连续快速失败说明外部严重不可得 → 提高等待上限、
        # 降低探测频率（审查修复，设计第十四节 L239"随近期失败间隔调整"）；
        # 失败间隔 <60s（密集）上限至少 1800s，<300s 至少 900s
        last_mono = self._last_shortage_mono.get(task_id)
        now_mono = _now_mono()
        if last_mono is not None:
            gap = now_mono - last_mono
            if gap < 60.0:
                cap = min(3600.0, max(cap, 1800.0))
            elif gap < 300.0:
                cap = min(3600.0, max(cap, 900.0))
        self._last_shortage_mono[task_id] = now_mono
        delay = min(cap, base * (2 ** (retry - 1)))
        delay *= 1.0 + random.uniform(0.0, 0.25)  # 随机抖动，避免重试风暴
        next_at = (datetime.now(timezone.utc) +
                   timedelta(seconds=delay)).isoformat(timespec="milliseconds")
        self.db.set_job_backoff(job_id, retry, next_at)
        self._backoff_until[job_id] = _now_mono() + delay

    def _ensure_tree_gone(self, a: runner.AttemptRun) -> None:
        """B6：finalize 提交终态前确认整棵执行进程树退出（有界 2s）。

        主进程退出不等于树退出：setsid 的孙进程会脱离进程组，仅靠 pgid 信号
        杀不到；cgroup 可用时以其为准（覆盖 setsid 漏网），不可用时退化为
        killpg。cgroup 内已无进程且主进程已退出则立即返回（正常路径零等待）；
        2s 后仍在（不可中断 D 态）则大声告警放行——状态按证据提交，
        cgroup 由 cleanup() 移除，残留进程留待孤儿回收处置，不冻结控制平面。

        cgroup.procs 里的 pid 需过滤"已不可能再执行代码"的（僵尸瞬态、
        cgroup 降级为静态文件时写入后不更新）——死 pid 杀不掉也不该
        阻塞终态提交。
        """
        deadline = _now_mono() + 2.0
        while _now_mono() < deadline:
            try:
                procs = a.cgrp.procs()
            except Exception:
                procs = []
            procs = [p for p in procs if not runner.proc_never_runs_again(p)]
            if not procs and not a.is_alive():
                return
            try:
                os.killpg(a.pgid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError, OSError):
                pass
            if a.cgrp.available:
                try:
                    a.cgrp.kill_all()
                except OSError:
                    pass
            time.sleep(0.1)
        log.warning("attempt %s process tree not fully gone after kill window "
                    "(D-state?), proceeding with finalize", a.spec.attempt_id)

    def _append_status(self, obj: dict) -> None:
        try:
            store.append_jsonl(config.status_log(), obj)
        except Exception:
            pass

    # ---------- 重启恢复推进（RECOVERING → QUEUED） ----------
    def _tick_recover(self, now_mono: float) -> None:
        for attempt_id, (att, deadline) in list(self._recover_wait.items()):
            pid = att.get("pid")
            pst = att.get("pid_start_time") or 0.0
            alive = bool(pid) and runner.pid_alive_with_start(pid, pst)
            if alive and now_mono < deadline:
                continue
            if alive:
                # 宽限超时：强杀旧进程树，按检查点重放。A1：killpg 杀不到
                # setsid 的孙进程，cgroup 可用时整组无差别终止（设计第七节
                # "确认整个执行进程树退出"），不可用时回退 killpg。
                try:
                    os.killpg(att.get("pgid") or pid, 9)
                except (ProcessLookupError, PermissionError, OSError):
                    pass
                cg_name = str(att.get("cgroup") or "")
                if cg_name:
                    try:
                        from . import cgroup as _cg
                        _cg.Cgroup(cg_name).kill_all()
                    except Exception:
                        pass
                reason = "重启回收：协作退出宽限超时，强杀后按检查点重放"
            else:
                reason = "重启回收：旧进程已退出，按检查点重放"
            now = runner.now_iso()
            if self._disk_completed(attempt_id):
                # 旧 worker 退出/被杀前已写完成事件但未被旧实例入库：确认成功，不重放
                self.db.commit_transition(
                    attempt_id,
                    {"status": JobStatus.SUCCEEDED, "finished_at": now},
                    att["job_id"], JobStatus.SUCCEEDED,
                    "重启回收：磁盘完成事件已确认", now)
                del self._recover_wait[attempt_id]
                self._append_status({"ts": now, "kind": "recover_confirmed",
                                     "attempt_id": attempt_id, "job_id": att["job_id"]})
                continue
            self.db.commit_transition(
                attempt_id, {"status": JobStatus.PAUSED, "reason": reason,
                             "finished_at": now},
                att["job_id"], JobStatus.QUEUED, reason, now)
            del self._recover_wait[attempt_id]
            self._append_status({"ts": now, "kind": "recover_requeued",
                                 "attempt_id": attempt_id, "job_id": att["job_id"],
                                 "reason": reason})

    # ---------- 运行期 RECOVERING 核实（关闭死循环，设计第十二节） ----------
    SCAN_BATCH = 1000

    def _jobs_in_status(self, status: str, limit: int = 1000) -> list[dict]:
        """C5 滚动游标状态扫描：同状态作业数超过单窗口 limit 时，每轮偏移
        一个窗口轮转覆盖，避免固定窗口把窗口外的作业永久饿死（设计第五节：
        优先级序 + 同级轮转 + 时间片，同样适用于状态扫描）；不足一窗口时
        复位到头部（始终看最新）。游标记忆 (offset, 上轮行数)。
        """
        off, last_len = self._status_scan.get(status, (0, 0))
        if last_len < limit:
            off = 0
        rows = self.db.jobs_in_status(status, limit=limit, offset=off)
        self._status_scan[status] = (off + len(rows), len(rows))
        return rows

    def _tick_recover_status(self, now_mono: float = 0.0) -> None:
        """处理"退出 0 但无完成记录"产生的运行时 RECOVERING（decide_outcome）。

        该 attempt 已退出、无法再产生事件，故按其是否有延迟到达的 completed 事件核实：
        有 → SUCCEEDED（完成记录已持久化）；无 → RECOVERY_BLOCKED（无法确认提交结果，
        保持可见、不启动重复 attempt，待人工核实）。跳过仍在重启宽限中的 attempt。

        RECOVERY_BLOCKED 持续核实（30s 节流）：迟到完成事件到达后自动确认成功；
        人工解除出口见 requeue_job（CLI `sched job requeue`）。
        """
        now = runner.now_iso()
        for j in self._jobs_in_status(JobStatus.RECOVERING):
            job_id = j["job_id"]
            if any(att["job_id"] == job_id for att, _ in self._recover_wait.values()):
                continue
            attempts = self.db.list_attempts(job_id=job_id, limit=1)
            completed = bool(attempts) and self._disk_completed(
                attempts[0]["attempt_id"])
            if completed:
                # 核实成功：attempt 与 job 在同一事务中提交终态（审查修复：
                # 此前只更新 job 行，attempt 行停留在 RECOVERING 最多 30s，
                # 与"每次状态变更带执行身份在事务中提交"不一致）
                self.db.commit_transition(
                    attempts[0]["attempt_id"],
                    {"status": JobStatus.SUCCEEDED, "finished_at": now,
                     "reason": "恢复核实：完成记录已持久化"},
                    job_id, JobStatus.SUCCEEDED,
                    "恢复核实：完成记录已持久化", now)
            else:
                self.db.set_job_status(job_id, JobStatus.RECOVERY_BLOCKED,
                                       "退出 0 但无完成记录，无法确认提交结果，需人工核实", now)
                self._stats["recovery_blocked_total"] += 1
                self._append_status({"ts": now, "kind": "recovery_blocked",
                                     "job_id": job_id})
        if now_mono and now_mono - self._last_rblocked_check_mono >= 30.0:
            self._last_rblocked_check_mono = now_mono
            for j in self._jobs_in_status(JobStatus.RECOVERY_BLOCKED):
                job_id = j["job_id"]
                if any(att["job_id"] == job_id for att, _ in self._recover_wait.values()):
                    continue
                attempts = self.db.list_attempts(job_id=job_id, limit=1)
                if attempts and self._disk_completed(
                        attempts[0]["attempt_id"]):
                    self.db.commit_transition(
                        attempts[0]["attempt_id"],
                        {"status": JobStatus.SUCCEEDED, "finished_at": now,
                         "reason": "恢复核实：迟到完成记录已确认"},
                        job_id, JobStatus.SUCCEEDED, "恢复核实：迟到完成记录已确认", now)
                    self._append_status({"ts": now, "kind": "recovery_unblocked",
                                         "job_id": job_id})

    # ---------- 不足退避到期重排（BACKOFF → QUEUED） ----------
    def _tick_backoff(self, now_mono: float) -> None:
        now_utc = datetime.now(timezone.utc)
        next_by_task: dict[str, str] = {}
        for j in self._jobs_in_status(JobStatus.BACKOFF):
            nr = j.get("next_retry_at") or ""
            due = True
            if nr:
                try:
                    dt = datetime.fromisoformat(nr)
                    if dt.tzinfo is None:
                        dt = dt.astimezone()
                    due = now_utc >= dt
                    if (not due and dt > now_utc + timedelta(
                            seconds=self.cfg.control.shortage_retry_cap_sec * 4)):
                        # 时钟跳变保守核实：到期时间远得离谱（系统时钟回拨），按到期处理
                        due = True
                except ValueError:
                    due = True
                # 每任务最早到期时间（监控视图用）
                prev = next_by_task.get(j["task_id"])
                if not prev:
                    next_by_task[j["task_id"]] = nr
                else:
                    try:
                        if datetime.fromisoformat(nr) < datetime.fromisoformat(prev):
                            next_by_task[j["task_id"]] = nr
                    except ValueError:
                        pass
            if due:
                self.db.set_job_status(j["job_id"], JobStatus.QUEUED,
                                       "不足退避到期，重新排队探测", runner.now_iso())
        self._next_retry_by_task = next_by_task

    # ---------- 已完结作业周期清理（B13，设计第十三节） ----------
    CLEANUP_INTERVAL_SEC = 3600.0

    def _tick_cleanup(self, now_mono: float) -> None:
        """每小时清理早于 cleanup_terminal_after_sec 完结作业（成功/取消）的
        attempt 工件：events/checkpoints/attempts 行 + run_dir。

        FAILED 作业的工件保留（失败诊断证据）；RECOVERING/RECOVERY_BLOCKED
        等待核实状态不清理；仍在途的 attempt（内存句柄）跳过。清理只针对
        已确认完结的记录，不影响任何活作业。
        """
        if now_mono - self._last_cleanup_mono < self.CLEANUP_INTERVAL_SEC:
            return
        self._last_cleanup_mono = now_mono
        cutoff = (datetime.now(timezone.utc) - timedelta(
            seconds=self.cfg.control.cleanup_terminal_after_sec)
        ).isoformat(timespec="milliseconds")
        gone = 0
        for attempt_id in self.db.stale_terminal_attempts(cutoff, limit=200):
            if attempt_id in self.attempts or attempt_id in self.meta:
                continue  # 仍在途（防御，正常不应出现）
            try:
                self.db.delete_attempt_artifacts(attempt_id)
                rd = config.run_dir(attempt_id)
                if rd.exists():
                    shutil.rmtree(rd, ignore_errors=True)
                gone += 1
            except Exception:
                log.exception("cleanup attempt %s failed", attempt_id)
        if gone:
            log.info("cleaned %d terminal attempt artifacts (older than %.0f sec)",
                     gone, self.cfg.control.cleanup_terminal_after_sec)

    # ---------- 暂停恢复（PAUSED → QUEUED，逐步放回） ----------
    # 恢复排队前需冷却的暂停来源（防紧密"重启→再暂停"循环，设计第八/十四节）：
    # - 心跳过期：疑似卡死，冷却后再探（原有行为）；
    # - OOM 证据 / 内存增长退让：内存超限任务立即重启会周期性再触发 OOM，
    #   冷却把循环降到低频探测（与 BACKOFF 的"长期保持低频探测"一致）；
    # - 保护自收缩：设计第十节"失败的 A 作业自行保存进度并冷却"。
    _RESUME_COOLDOWN_MARKERS = ("心跳过期", "OOM", "内存增长退让", "无低优先级可退让")

    def _resume_cooldown_sec(self, job: dict) -> float:
        """该 PAUSED 作业恢复排队前的冷却秒数（0=立即恢复）。"""
        reason = str(job.get("reason") or "")
        if any(m in reason for m in self._RESUME_COOLDOWN_MARKERS):
            return self.cfg.control.heartbeat_resume_cooldown_sec
        return 0.0

    def _tick_release(self, now_mono: float) -> None:
        st = self.protect.st
        # 保护结束后逐步放回退让的任务（每次一个，recovery_probe_sec 节奏）
        if not st.protected_task:
            task_id = self.protect.release_ready(now_mono)
            if task_id:
                self._release_task_jobs(task_id, "保护结束，逐步放回")
        # PAUSED 作业恢复正常排队：
        # - 无保护轮：全部按既有冷却规则恢复；
        # - A3：保护轮进行中，优先级不低于保护对象的作业（含保护对象自身
        #   心跳过期暂停的执行）仍按正常冷却恢复——保护轮不得冻结高优先级
        #   作业的恢复通道；仅低优先级作业保持逐步放回节奏（pending_resume）。
        cooldown = self.cfg.control.heartbeat_resume_cooldown_sec
        now_utc = datetime.now(timezone.utc)
        prot_pri = st.protected_priority if st.protected_task else None
        for j in self._jobs_in_status(JobStatus.PAUSED):
            if j["task_id"] in st.pending_resume:
                continue  # 待逐步放回的任务保持节奏
            if prot_pri is not None and int(j["priority"]) < prot_pri:
                continue  # 保护轮中低优先级作业不走直接恢复
            if self._resume_cooldown_sec(j) > 0:
                # 资源型/疑似卡死暂停：先冷却再恢复，否则形成每秒级重启循环；
                # finished_at 缺失/不可解析时按已过冷却处理（不永久扣押）
                paused_at = None
                try:
                    paused_at = datetime.fromisoformat(j.get("finished_at") or "")
                except ValueError:
                    paused_at = None
                if paused_at is not None:
                    if paused_at.tzinfo is None:
                        paused_at = paused_at.astimezone()
                    if (now_utc - paused_at).total_seconds() < cooldown:
                        continue
            self.db.set_job_status(j["job_id"], JobStatus.QUEUED,
                                   "暂停解除，重新排队", runner.now_iso())

    def _release_task_jobs(self, task_id: str, reason: str) -> None:
        for j in self.db.jobs_in_status(JobStatus.PAUSED, task_id=task_id):
            self.db.set_job_status(j["job_id"], JobStatus.QUEUED, reason,
                                   runner.now_iso())

    # ---------- 资源观测（CPU/内存/IO/OOM + 滞回趋势，只观测执行进程） ----------
    def _tick_resource(self, now: float) -> None:
        if now - self._last_sample_mono < self.cfg.control.sample_interval_sec:
            return
        prev_mono = self._last_sample_mono
        self._last_sample_mono = now
        cpu_delta = 0.0
        mem = 0.0
        io = 0
        observed_any = False
        have_attempts = len(self.attempts) > 0
        per_att_cpu: dict[str, float] = {}   # 本采样区间每 attempt CPU 增量（usec）
        per_att_mem: dict[str, float] = {}   # 每 attempt 瞬时内存（字节）
        for attempt_id, a in self.attempts.items():
            # 审查修复（观测不足口径，设计第八节 L133"指标不支持归属时标为
            # 观测不足"）：观测可用性以"实际读到任一指标"为准，而非"cgroup
            # 目录可写"——父级未向 sched 层委派控制器时目录可写但 cpu.stat/
            # memory.current 全部缺失，此前会把资源门禁静默失效（恒判健康）。
            usec = a.cgrp.read_cpu_usage_usec()
            if usec is not None:
                observed_any = True
                prev = self._last_cpu_usec.get(attempt_id)
                if prev is not None:
                    delta = max(0, usec - prev)
                    cpu_delta += delta
                    per_att_cpu[attempt_id] = delta
                self._last_cpu_usec[attempt_id] = usec
            m = a.cgrp.read_current_mem()
            if m is not None:
                observed_any = True
                mem += m
                per_att_mem[attempt_id] = m
            io_b = a.cgrp.read_io_bytes()
            if io_b is not None:
                observed_any = True
                prev = self._last_io_bytes.get(attempt_id)
                if prev is not None:
                    io += max(0, io_b - prev)
                self._last_io_bytes[attempt_id] = io_b
            ok = a.cgrp.read_oom_kill_count()
            if ok is not None:
                prev = self._last_oom.get(attempt_id, 0)
                if ok > prev:
                    self._last_oom[attempt_id] = ok
                    self._on_oom_evidence(attempt_id, ok - prev)
        # 清理已退出 attempt 的累积读数，避免键泄漏
        for aid in list(self._last_cpu_usec):
            if aid not in self.attempts:
                del self._last_cpu_usec[aid]
        for aid in list(self._last_io_bytes):
            if aid not in self.attempts:
                del self._last_io_bytes[aid]
        self._sample_cpu = cpu_delta
        # cgroup memory.current 为字节，此处统一换算为 MB 后再与 total_mem(MB) 比较
        self._sample_mem_mb = mem / (1024 * 1024)
        # I/O 计数取采样区间增量（与 CPU 一致，设计第八节比较"执行 I/O 增量"）
        self._sample_io = io
        # 观测不足：有执行进程但无任何可用 cgroup 观测 → 减慢探索并记录限制
        observability_ok = (not have_attempts) or observed_any
        if observability_ok != self._observability_ok:
            self._observability_ok = observability_ok
            if not observability_ok:
                log.warning("resource observability insufficient (cgroup unavailable), "
                            "slowing growth exploration")
            self._append_status({"ts": runner.now_iso(),
                                 "kind": "observability_changed",
                                 "ok": observability_ok})
        total_cpu = _read_cpu_count()
        total_mem = _read_total_mem_mb()
        # 执行范围压力指标（cgroup psi，设计第八节"执行范围的压力指标"）
        try:
            from . import cgroup as cg
            self._sample_pressure = cg.read_subtree_pressure()
        except Exception:
            self._sample_pressure = {}
        # 差分后按采样间隔归一化为瞬时 CPU 占用率（usage_usec 为累积值，除以核数与逝去时间）
        elapsed = (now - prev_mono) if prev_mono > 0 else self.cfg.control.sample_interval_sec
        cpu_ratio = (self._sample_cpu / 1_000_000.0) / max(1, total_cpu) / max(1e-6, elapsed)
        # 每 attempt 资源摘要（监控视图导出，不参与任何控制决策）
        self._sample_attempt = {
            aid: {"cpu_pct": round(d / 1_000_000.0 / max(1, total_cpu)
                                   / max(1e-6, elapsed) * 100, 1),
                  "mem_mb": round(per_att_mem.get(aid, 0) / (1024 * 1024), 1)}
            for aid, d in per_att_cpu.items()
        }
        # 观测不足语义：/proc/meminfo 不可读（total_mem<=0）时内存占比未知，
        # 置 0 跳过内存门禁，而不是爆表停掉一切准入
        mem_ratio = (self._sample_mem_mb / total_mem) if total_mem > 0 else 0.0
        # B1：I/O 压力参与资源健康判定（psi io full avg10 归一化 0-1；
        # 无 psi 观测时为 0，不影响判定，设计第八节"执行范围的压力指标"）
        io_pressure = float(self._sample_pressure.get("io") or 0.0) / 100.0
        prev_state = self.trend.state
        self.trend.sample(cpu_ratio, mem_ratio, observability_ok,
                          io_pressure=io_pressure)
        # 状态迁移审计（NORMAL → HOLD → SHED，统一资源水位状态机）
        if self.trend.state != prev_state:
            self._append_status({"ts": runner.now_iso(),
                                 "kind": "resource_state",
                                 "from": prev_state, "to": self.trend.state,
                                 "cpu_ratio": round(cpu_ratio, 3),
                                 "mem_ratio": round(mem_ratio, 3)})
            log.warning("resource gate %s -> %s (cpu %.1f%%, mem %.1f%%)",
                        prev_state, self.trend.state,
                        cpu_ratio * 100, mem_ratio * 100)
        # 执行内存快速增长并接近容量 → 立即停止新增，并优先暂停低优先级中
        # 实测可释放内存较多的进程（设计第八节）
        if (self._prev_mem_mb > 0 and self._sample_mem_mb > self._prev_mem_mb):
            growth = self._sample_mem_mb - self._prev_mem_mb
            if (mem_ratio >= self.cfg.control.resource_healthy_ratio
                    and growth > total_mem * self.cfg.control.mem_growth_ratio):
                self.trend.mark_memory_growth()
                self._append_status({"ts": runner.now_iso(),
                                     "kind": "memory_growth",
                                     "growth_mb": round(growth, 1),
                                     "mem_ratio": round(mem_ratio, 3)})
                # 内存增长退让（设计第八节 L137）：复用 OOM 候选选择器——以当前
                # 最高存活优先级为参照只向下让；节流防同因重复退让
                if now - self._last_mem_yield_mono >= max(
                        3.0 * self.cfg.control.sample_interval_sec, 30.0):
                    top = max((m for m in self.meta.values()
                               if m.status in (JobStatus.RUNNING,
                                               JobStatus.STARTING)),
                              key=lambda m: m.priority, default=None)
                    target = self._choose_oom_yield_candidate(
                        top.attempt_id) if top is not None else None
                    if target is not None:
                        self._last_mem_yield_mono = now
                        self._request_pause(
                            target, reason="执行内存快速增长（内存增长退让）")
        self._prev_mem_mb = self._sample_mem_mb
        # SHED（执行内存达上限）：除停新增外，按优先级自高向低为参照挑选
        # 低优先级中实测可释放内存较多者暂停撤内存（与增长退让共用节流）；
        # 水位降到恢复线以下由状态机自动回 NORMAL 重新放行。
        if self.trend.shedding():
            if now - self._last_mem_yield_mono >= max(
                    3.0 * self.cfg.control.sample_interval_sec, 30.0):
                top = max((m for m in self.meta.values()
                           if m.status in (JobStatus.RUNNING,
                                           JobStatus.STARTING)),
                          key=lambda m: m.priority, default=None)
                target = self._choose_oom_yield_candidate(
                    top.attempt_id) if top is not None else None
                if target is not None:
                    self._last_mem_yield_mono = now
                    self._request_pause(
                        target, reason="资源水位 SHED：执行内存达上限（撤内存退让）")

    def _on_oom_evidence(self, attempt_id: str, delta: int) -> None:
        """OOM 证据：停止增长，优先暂停低优先级中可释放内存较多者（设计第八节）。"""
        self.trend.mark_oom()
        self._stats["oom_events"] += 1
        log.warning("oom_kill evidence +%d on attempt %s", delta, attempt_id)
        self._append_status({"ts": runner.now_iso(), "kind": "oom_evidence",
                             "attempt_id": attempt_id, "delta": delta})
        target = self._choose_oom_yield_candidate(attempt_id) or attempt_id
        meta = self.meta.get(target)
        if meta is not None and meta.status in (JobStatus.RUNNING, JobStatus.STARTING):
            self._request_pause(target, reason="执行内存不足（OOM 证据）")

    def _attempt_mem_mb(self, attempt_id: str) -> float:
        a = self.attempts.get(attempt_id)
        if a is None:
            return 0.0
        m = a.cgrp.read_current_mem()
        return (m or 0) / (1024 * 1024)

    def _choose_oom_yield_candidate(self, oom_attempt_id: str) -> str | None:
        """OOM 时选暂停目标：仅严格低于受害者优先级的执行中 attempt（让级语义，
        设计第十节——退让只向下让、不得反向牺牲更高优先级），其中优先级低者
        优先，其次实测可释放（当前内存）较多者。受害者已是最低优先级时无候选，
        由调用方回退暂停受害者自身。"""
        victim = self.meta.get(oom_attempt_id)
        victim_pri = victim.priority if victim is not None else None
        cands = []
        for aid, m in self.meta.items():
            if aid == oom_attempt_id:
                continue
            if m.status not in (JobStatus.RUNNING, JobStatus.STARTING):
                continue
            if victim_pri is None or m.priority >= victim_pri:
                continue
            cands.append((m.priority, self._attempt_mem_mb(aid), aid))
        if not cands:
            return None
        cands.sort(key=lambda x: (x[0], -x[1]))
        return cands[0][2]

    # ---------- 控制：动态目标 / 保护 / 退让 / 放回 ----------
    def _tick_control(self, now: float) -> None:
        st = self.protect.st

        # 1) 保护对象改善 / 无待处理需求 → 结束保护；截止 → 失败结束
        if st.protected_task:
            improved = (now - st.last_block_mono >=
                        self.cfg.control.protect_deadline_sec * 0.5
                        and self.health.has_progress_since(st.protected_task,
                                                           st.last_block_mono))
            no_demand = not self._task_has_demand(st.protected_task)
            if improved or no_demand:
                self.protect.end_round(now, success=True)
                self._stats["protect_released"] += 1
                self._append_status({"ts": runner.now_iso(), "kind": "protect_released",
                                     "task_id": st.protected_task,
                                     "reason": "improved" if improved else "no_demand"})
            elif self.protect.deadline_passed(now):
                self.protect.end_round(now)
                self._stats["protect_failed"] += 1
                self._append_status({"ts": runner.now_iso(), "kind": "protect_ended",
                                     "task_id": st.protected_task,
                                     "reason": "deadline"})

        # 2) 高优先级新需求 + 资源持续不健康 → 主动让低优先级暂停（设计文档第十节）
        if not st.protected_task and not self.protect.in_backoff(now):
            demand = self._highest_queued_demand()
            if demand is not None and self.trend.unhealthy():
                task_id, pri = demand
                min_run_pri = self._min_running_priority()
                if min_run_pri is not None and pri > min_run_pri:
                    spec = self.specs.get(task_id)
                    fi = (spec.feedback_interval_sec or 0.0) if spec is not None else 0.0
                    if self.protect.start_round(
                            task_id, pri, now,
                            reason="高优先级请求到达且执行侧接近饱和",
                            cause="demand", feedback_interval_sec=fi,
                            lower_tasks=self._lower_task_count(pri),
                            cooldown_remain_sec=self._backoff_remain_sec(task_id)):
                        self._append_status({"ts": runner.now_iso(),
                                             "kind": "protect_started",
                                             "task_id": task_id, "priority": pri,
                                             "reason": "priority_demand"})

        # 3) 无报错变慢 → 温和退让（置信度低于明确不足）
        #    A2：仅高优先级变慢才值得保护（严格高于存活/排队最低优先级）
        #    设计第十五节：先处理最高优先级的新需求、明确不足及持续变慢，再扩容。
        if not st.protected_task and not self.protect.in_backoff(now):
            for meta in list(self.meta.values()):
                if self.health.slow(meta.task_id, now) and \
                        self._protect_worthy(meta.priority):
                    spec = self.specs.get(meta.task_id)
                    fi = (spec.feedback_interval_sec or 0.0) if spec is not None else 0.0
                    if self.protect.start_round(meta.task_id, meta.priority, now,
                                                reason="无报错持续变慢", cause="slow",
                                                feedback_interval_sec=fi,
                                                lower_tasks=self._lower_task_count(meta.priority),
                                                cooldown_remain_sec=self._backoff_remain_sec(meta.task_id)):
                        self._append_status({"ts": runner.now_iso(),
                                             "kind": "protect_started",
                                             "task_id": meta.task_id,
                                             "reason": "slowdown"})
                    break

        # 4) 动态目标：健康任务平缓扩容（观测不足时拉长观察期，减速探索）
        obs_factor = 1.0 if self._observability_ok else \
            self.cfg.control.observability_slow_factor
        for task_id in self.specs:
            live = self._live_count(task_id)
            # A4：扩容证据取"近窗口内的新进展样本"——被暂停很久的任务凭停摆前
            # 的陈旧样本判"有进展"会在恢复瞬间立即 +1（无真实进展支撑）
            has_progress = self.health.has_progress_since(
                task_id, now - self.health.window_sec)
            if has_progress:
                # 审查修复（设计第九节 L153/L157）：真实业务进展恢复且距上次
                # 收缩已过观察期 → 不足证据清零、恢复 steady。长期任务无完成
                # 事件，此前 streak 只增不清会永久停止扩容（而非"减慢探索"）。
                self.scaling.note_recovered_progress(task_id, now)
            if self.scaling.may_grow(task_id, now, live, has_progress,
                                     self.trend.healthy_enough(),
                                     observe_factor=obs_factor):
                self.scaling.apply_grow(task_id, now)
                self._persist_scale(task_id)

        # 5) 保护进行中：选择退让候选（含 STARTING）
        # 设计第十节：每次退让一小步，确认执行进程退出并在观察窗口内观察 A 后再退下一步。
        # 变慢起因（cause=="slow"）证据置信度低于明确不足，退让间隔加倍、扩散更谨慎
        # （设计第十一节）。
        if st.protected_task and not self.protect.in_backoff(now):
            running = self._running_infos()
            yield_gap = self.cfg.control.yield_observe_sec * \
                (2.0 if st.cause == "slow" else 1.0)
            if self._has_pausing():
                pass  # 上一退让尚未确认退出
            elif now - st.last_yield_mono < yield_gap:
                pass  # 退让后需观察 A（有界间隔）
            else:
                cand = self.protect.choose_yield_candidate(running, now)
                if cand:
                    self._request_pause(cand, yield_to_protect=True,
                                        reason=f"高优先级保护退让（{st.protected_task}）")
                elif not running and st.yielded:
                    # 无更低优先级可退让且已无存活低优进程 → 本轮保护无改善，结束
                    self.protect.end_round(now)
                    self._stats["protect_failed"] += 1
                else:
                    # 无更低优先级可退让 → 收缩保护对象自身（设计第十节）
                    self_att = self._pick_self_shrink(st.protected_task)
                    if self_att is not None:
                        self._request_pause(
                            self_att,
                            reason=f"无低优先级可退让，收缩 {st.protected_task} 自身")
                        self.scaling.on_shortage(
                            st.protected_task, now,
                            self._live_count(st.protected_task))
                        self._persist_scale(st.protected_task)
                        self.protect.note_step_mono(now)

        # 6) 逐步放回 / PAUSED 恢复
        self._tick_release(now)

    def _persist_scale(self, task_id: str) -> None:
        ts = self.scaling.get(task_id)
        self.db.set_task_state(task_id, run_target=ts.run_target, mode=ts.mode)

    def _task_has_demand(self, task_id: str) -> bool:
        """保护对象是否还有排队需求或存活执行（无需求则不继续保护）。"""
        return (self.db.count_queued(task_id) > 0
                or self._live_count(task_id) > 0)

    def _highest_queued_demand(self) -> tuple[str, int] | None:
        """排队作业中的最高优先级需求（任务级聚合，不受积压规模遮蔽）。

        审查问题一连带修复：改为按任务 GROUP BY 取最高作业优先级，
        不再用全局 receive_seq 前 200 窗口（设计第十节）。
        """
        for task_id, pri in self.db.queued_task_priorities(limit=128):
            spec = self.specs.get(task_id)
            if spec is None or not spec.enabled:
                continue
            return (task_id, pri)
        return None

    def _lower_task_count(self, priority: int) -> int:
        """优先级严格低于给定值的启用任务数（保护窗口推导用，设计第十节）。"""
        return sum(1 for s in self.specs.values()
                   if s.enabled and s.priority < priority)

    def _min_running_priority(self) -> int | None:
        pris = [m.priority for m in self.meta.values()
                if m.status in (JobStatus.STARTING, JobStatus.RUNNING, JobStatus.PAUSING)]
        return min(pris) if pris else None

    def _has_pausing(self) -> bool:
        return any(m.status == JobStatus.PAUSING for m in self.meta.values())

    def _pick_self_shrink(self, task_id: str) -> str | None:
        """选择保护对象自身的一个存活 attempt 收缩（无低优先级可退让时，设计第十节）。"""
        cands = []
        for aid, meta in self.meta.items():
            if meta.task_id != task_id:
                continue
            if meta.status not in (JobStatus.RUNNING, JobStatus.STARTING):
                continue
            cands.append((aid, meta))
        if not cands:
            return None
        # 优先收缩最近报过不足的，其次较早扩容启动的
        cands.sort(key=lambda x: (0 if x[1].shortage_at_mono > 0 else 1,
                                  x[1].grown_at_mono))
        return cands[0][0]

    def _running_infos(self) -> list[dict]:
        now = _now_mono()
        out = []
        for attempt_id, meta in self.meta.items():
            if meta.status not in (JobStatus.RUNNING, JobStatus.STARTING):
                continue
            out.append({
                "attempt_id": attempt_id,
                "task_id": meta.task_id,
                "priority": meta.priority,
                # 排序信号按时间窗口衰减，而非永久布尔（防一次不足/扩容信号滞留整个生命周期）
                "shortage_recent": (meta.shortage_at_mono > 0 and
                                    now - meta.shortage_at_mono <
                                    self.cfg.control.slow_confidence_sec),
                "grown_recently": now - meta.grown_at_mono <
                                  self.cfg.control.grow_observe_sec * 2,
                # 任务级暂停成本 EMA（设计第十节：退让候选综合暂停成本排序）
                "pause_cost": self.scaling.pause_cost(meta.task_id),
            })
        return out

    def _request_pause(self, attempt_id: str, reason: str,
                       *, yield_to_protect: bool = False) -> None:
        a = self.attempts.get(attempt_id)
        meta = self.meta.get(attempt_id)
        if a is None or meta is None:
            return
        if meta.status not in (JobStatus.RUNNING, JobStatus.STARTING):
            return
        cmd_id = runner.new_id("pause")
        grace = self._pause_grace_sec(meta)
        deadline = (datetime.now(timezone.utc) +
                    timedelta(seconds=grace)).isoformat()
        a.write_pause(cmd_id, reason, deadline)
        meta.pause_command = a.pause_command
        meta.pause_requested_mono = _now_mono()
        meta.pause_deadline_mono = _now_mono() + grace
        self._transition(meta, JobStatus.PAUSING)
        a.signal_pause()
        # 只有"高优先级保护退让"才计入保护轮的累计退让账（设计第十节）；
        # 心跳过期、OOM 等其它暂停不得污染 yielded/yielded_tasks 或延长保护截止。
        if yield_to_protect:
            # 截止延长按本次退让的实际暂停宽限推导（设计第十节：长度按暂停耗时计算）
            self.protect.note_yielded(attempt_id, meta.task_id, meta.priority,
                                      pause_cost_sec=grace)
        self._append_status({"ts": runner.now_iso(), "kind": "pause_requested",
                             "attempt_id": attempt_id, "job_id": meta.job_id,
                             "task_id": meta.task_id, "reason": reason,
                             "grace_sec": round(grace, 1)})

    def _pause_grace_sec(self, meta: AttMeta) -> float:
        """暂停宽限：依据承诺反馈间隔 / 任务申报的单步耗时 / 已观测检查点延迟推导，
        不固定等待（设计文档第六、十四节），封顶 pause_grace_max_sec。

        B14：优先用本 attempt STARTED 申报的单步耗时（最新契约），未申报时
        回退 task.json 的任务级声明。
        """
        interval = meta.feedback_interval or self.cfg.control.heartbeat_interval_sec
        grace = max(self.cfg.control.pause_grace_base_sec, interval * 4)
        if meta.max_step_sec:
            grace = max(grace, meta.max_step_sec * 1.5)
        else:
            spec = self.specs.get(meta.task_id)
            if spec is not None and spec.max_step_sec:
                grace = max(grace, spec.max_step_sec * 1.5)
        avg_ckpt = self._avg(self._ckpt_lat)
        if avg_ckpt is not None:
            grace = max(grace, avg_ckpt * 2)
        return min(grace, self.cfg.control.pause_grace_max_sec)

    def _heartbeat_stale_threshold(self, meta: AttMeta) -> float:
        """心跳过期阈值：取配置下限、承诺反馈间隔×倍数、实际最近心跳间隔×倍数的较大者。

        设计第十四节：心跳过期依据承诺反馈间隔和实际延迟计算，不固定等待。
        """
        interval = meta.feedback_interval or self.cfg.control.heartbeat_interval_sec
        mult = self.cfg.control.heartbeat_stale_multiplier
        actual = meta.heartbeat_gap or interval
        return max(self.cfg.control.heartbeat_stale_sec, interval * mult, actual * mult)

    @staticmethod
    def _avg(d: deque) -> float | None:
        return (sum(d) / len(d)) if d else None

    def _live_count(self, task_id: str) -> int:
        return sum(1 for m in self.meta.values()
                   if m.task_id == task_id and
                   m.status in (JobStatus.STARTING, JobStatus.RUNNING, JobStatus.PAUSING))

    # ---------- 准入 ----------
    # 启动间隔阶梯下限（设计第九节：稳定证据下平缓缩短，不锁死每秒上限）
    START_INTERVAL_FLOOR_SEC = 0.125

    def _effective_start_interval(self, now: float) -> float:
        """启动间隔随稳定健康证据平缓缩短（设计第九节 L155）。

        资源趋势持续健康且无不足证据时阶梯下调（每级减半：60s→½、180s→¼、
        300s→⅛，下限 0.125s）；任一不健康/观测不足/不足证据立即回弹配置初值。
        """
        base = self.cfg.control.start_interval_sec
        if not self.trend.healthy_enough() or not self._observability_ok:
            self._healthy_since_mono = 0.0
            return base
        if self._healthy_since_mono <= 0.0:
            self._healthy_since_mono = now
            return base
        healthy_dur = now - self._healthy_since_mono
        factor = 1.0
        if healthy_dur >= 300.0:
            factor = 0.125
        elif healthy_dur >= 180.0:
            factor = 0.25
        elif healthy_dur >= 60.0:
            factor = 0.5
        return max(self.START_INTERVAL_FLOOR_SEC, base * factor)

    def _tick_external(self, now: float) -> None:
        """外部资源压力闸门（反馈式规避，设计第十四节）。

        唯一事实来源是外部不足事件流（exit 75）——不建模资源身份/配额。
        多任务同时不足（密度×广度越阈）→ 全局限停新准入（在途不动，靠
        退避自然衰减）；密度回落持续观察期 → 解除。
        嫌疑优先原则：外部资源相对稳定，突然不足优先怀疑稳定回看窗内
        新加入的任务——进入压力时自动压制其动态目标至单并发探索并导出
        嫌疑名单（状态事件 + 快照），供人工排查；解除后嫌疑最后恢复。
        """
        prev = self.etrend.state
        active_tasks = {m.task_id for m in self.meta.values()}
        self.etrend.sample(now, active_tasks)
        if self.etrend.state == prev:
            return
        snap = self.etrend.snapshot(now)
        if self.etrend.holding():
            for tid in self.etrend.suspects():
                ts = self.scaling.get(tid)
                ts.run_target = min(ts.run_target, 1)
                ts.mode = "explore"
            self._stats["external_holds"] = self._stats.get("external_holds", 0) + 1
            self._append_status({
                "ts": runner.now_iso(), "kind": "external_state",
                "state": scaling.ExternalTrend.HOLD,
                "suspects": snap["suspects"],
                "recent_tasks": snap["recent_tasks"],
                "recent_events": snap["recent_events"]})
        else:
            self._append_status({
                "ts": runner.now_iso(), "kind": "external_state",
                "state": scaling.ExternalTrend.NORMAL})

    def _tick_admit(self, now: float) -> None:
        # B9（设计第八节：准入核执行资源反馈）：资源趋势持续不健康时暂停新增
        # 准入，在途执行不受影响；滞回恢复健康后自动继续。这不是隐藏上限——
        # 门禁仅依赖实测资源趋势（CPU/内存采样 + OOM/快速增长证据），
        # 任务动态目标不受影响，健康后立即按目标继续准入。
        if not self.trend.healthy_enough():
            self._healthy_since_mono = 0.0
            return
        # 外部压力闸门（反馈式）：多任务同时报外部不足 → 全局限停新准入。
        # 与本地资源门禁同语义（HOLD：在途不受影响，靠退避自然衰减）；
        # 不预设外部资源限制，密度回落并保持观察期后自动恢复。
        if self.etrend.holding():
            return
        interval = self._effective_start_interval(now)
        # 令牌制（设计第九节 L155"不锁死为每秒最多两个进程；缩短时避免积累
        # 大量启动令牌后一次性爆发"）：份额随时间累积，间隔小于主循环节拍时
        # 单轮可启动多个；份额上限为一个节拍，防积压后瞬间倾泻。
        if self._last_admit_tick_mono > 0.0:
            self._admit_tokens = min(
                self._admit_tokens + max(0.0, now - self._last_admit_tick_mono),
                max(interval, self.cfg.control.loop_tick_sec))
        else:
            self._admit_tokens = interval  # 冷启动先给一个份额
        self._last_admit_tick_mono = now
        if self._admit_tokens < interval:
            return
        candidates = self._candidates()
        for job in candidates:
            if self._admit_tokens < interval:
                break  # 份额耗尽：错开启动防突发（设计第九节）
            if self._admit_job(job):
                self._admit_tokens -= interval
                self._last_start_mono = now

    # 每任务每轮取的 FIFO 头部条数（轮转切片，防单任务占满启动份额）
    ADMIT_HEAD_PER_TASK = 8

    def _candidates(self) -> list[dict]:
        """准入候选：任务优先级降序 → 同优先级多任务轮转 → 任务内 FIFO
        → 合并后按作业优先级降序（设计第五、十节）。

        审查问题一修复：按任务取数（queued_head_jobs 索引直取头部），
        取代全局 receive_seq 前 500 窗口——积压规模下早期任务不再独占
        候选、同级任务真正轮转，run_target=0 的任务恢复冷启动探测
        （设计第九节：恢复探测另允许最小探索机会）。
        轮转发生在任务优先级分组内；组内合并后按作业优先级稳定排序，
        作业级优先级（仅允许低于任务优先级）仍保持全局降序语义。
        """
        prot_pri = self.protect.current_protected_priority()
        groups: dict[int, list[str]] = {}
        for tid, spec in self.specs.items():
            if spec.enabled:
                groups.setdefault(spec.priority, []).append(tid)
        out: list[dict] = []
        for pri in sorted(groups, reverse=True):
            tasks = sorted(groups[pri])
            if len(tasks) > 1:
                # 同级轮转：每次评估轮换起始任务（桶内保持 FIFO）
                off = self._adm_rr.get(pri, 0) % len(tasks)
                self._adm_rr[pri] = (off + 1) % len(tasks)
                tasks = tasks[off:] + tasks[:off]
            for tid in tasks:
                for j in self.db.queued_head_jobs(tid, self.ADMIT_HEAD_PER_TASK):
                    # 保护期间：立即停止比保护对象低优先级的新增（设计第十节）
                    if prot_pri and int(j["priority"]) < prot_pri:
                        continue
                    if self.db.live_jobs_for_job(j["job_id"]):
                        continue
                    if _now_mono() < self._backoff_until.get(j["job_id"], 0.0):
                        continue
                    out.append(j)
        # 作业优先级全局降序（稳定排序：轮转次序为同优先级内的决胜）
        out.sort(key=lambda j: -int(j["priority"]))
        return out

    def _admit_job(self, job: dict) -> bool:
        task_id = job["task_id"]
        live = self._live_count(task_id)
        scale = self.scaling.get(task_id)
        # 冷启动给 1，否则不超过动态目标
        why = "扩容补位"
        if scale.run_target == 0 and live == 0:
            scale.run_target = 1
            self.scaling.grew_at(task_id, _now_mono())
            self._persist_scale(task_id)
            why = "冷启动探索"
        limit = max(1, scale.run_target)
        # 审查修复（设计第十节 L177"每次只恢复少量运行机会并观察 A"）：
        # 保护轮放回观察中的任务受临时准入上限约束，防止未被暂停过的
        # QUEUED 积压在保护结束后按原目标一次性拉满
        cap = self.protect.resume_cap(task_id, _now_mono(), scale.run_target)
        if cap is not None:
            limit = min(limit, cap)
            why = f"保护后逐步放回（上限 {cap}）"
        if live >= limit:
            return False
        spec = self.specs[task_id]
        return self._launch(job, spec, why=why)

    def _launch(self, job: dict, spec: config.TaskSpec,
                why: str = "") -> bool:
        attempt_id = runner.new_id("att")
        token = runner.new_id("tok")
        seq = self.db.next_attempt_seq(job["job_id"])
        now = runner.now_iso()
        # 先持久化 attempt 意图（STARTING），再握手启动；持久化失败时作业保持
        # QUEUED 下轮重试（数据库瞬时不可写不得误判为业务失败）
        try:
            self.db.create_attempt(attempt_id, job["job_id"], seq, token, now,
                                   cgroup=f"{job['task_id']}/{attempt_id}")
        except Exception as e:
            log.warning("create_attempt failed for job %s: %s", job["job_id"], e)
            return False
        # 跨 attempt 检查点继承（设计文档第七节）：新 run_dir 是空的，若不把旧检查点
        # 搬运过来，worker 的 resume_data() 将读到 None 而从零执行——重试/暂停恢复/
        # 重启重放全部失去断点。此处把该作业最近一次已提交检查点整体复制进新 run_dir。
        resumed = self._restore_checkpoint(job["job_id"], attempt_id)
        if resumed:
            # 恢复早期豁免（审查修复，设计第十一节 L191"恢复早期……排除在退化
            # 比较之外"）：断点重放启动的速率天然低于稳态基线（缓存冷启动等），
            # 在一个扩容观察期内不参与变慢判定
            self.health.note_resumed(job["task_id"], _now_mono(),
                                     self.scaling.get(job["task_id"]).grow_observe_sec)
        try:
            import json as _json
            aspec = runner.AttemptSpec(
                attempt_id=attempt_id, job_id=job["job_id"], task_id=job["task_id"],
                token=token, task_root=config.task_dir(job["task_id"]),
                executor_entry=spec.executor_entry,
                entry_args=_json.loads(job["entry_args_json"]),
                workdir=job["workdir"],
                env=_json.loads(job["env_json"]),
                progress_semver=job["progress_semver"],
            )
            a = runner.launch(aspec, _json.loads(job["input_refs_json"]))
        except Exception as e:
            # B10：启动失败（解释器/入口缺失、工作目录损坏等）是基础设施问题，
            # 不是业务失败——attempt 落 FAILED 留证据，作业保持 QUEUED 并给
            # 短退避后重试；不得终态化 FAILED 让可重试的拉起故障永沉。
            log.warning("launch failed for job %s: %s", job["job_id"], e)
            self.db.update_attempt(attempt_id, status=JobStatus.FAILED,
                                   reason=f"启动失败：{e}",
                                   finished_at=runner.now_iso())
            self._backoff_until[job["job_id"]] = _now_mono() + 5.0
            self._append_status({"ts": runner.now_iso(), "kind": "launch_failed",
                                 "job_id": job["job_id"], "attempt_id": attempt_id,
                                 "reason": str(e)})
            return False
        meta = AttMeta(attempt_id, job["job_id"], job["task_id"], int(job["priority"]), token)
        meta.grown_at_mono = _now_mono()
        meta.feedback_interval = spec.feedback_interval_sec or 0.0
        self.attempts[attempt_id] = a
        self.meta[attempt_id] = meta
        # 外部压力嫌疑判定基准：登记任务首次活跃时刻（稳定回看窗内首次
        # 活跃的任务在压力爆发时优先被怀疑——外部资源相对稳定原则）
        self.etrend.note_active(str(job["task_id"]), _now_mono())
        self.db.update_attempt(attempt_id, pid=a.pid, pgid=a.pgid,
                               start_monotonic=a.start_time,
                               pid_start_time=a.start_time)
        self.db.set_job_status(job["job_id"], JobStatus.STARTING, ts=now)
        # 启动延迟监控：排队 → 拉起的墙钟耗时
        try:
            queued_at = datetime.fromisoformat(job["queued_at"])
            self._start_lat.append(max(0.0, time.time() - queued_at.timestamp()))
        except (KeyError, ValueError, TypeError, OSError):
            pass
        self._append_status({"ts": now, "kind": "attempt_launched", "attempt_id": attempt_id,
                             "job_id": job["job_id"], "task_id": job["task_id"],
                             "pid": a.pid, "why": why,
                             "resumed": resumed})
        return True

    def _restore_checkpoint(self, job_id: str, new_attempt_id: str) -> bool:
        """把该作业最近一次已提交检查点复制进新 attempt 的 run_dir（跨 attempt 恢复）。

        以旧 run_dir 的 checkpoint.json 为准：它由 worker 原子提交，可能领先于事件入库
        （调度器在文件落盘与事件消费之间崩溃时不丢断点）。调度器不理解业务内容，
        仅按 saved_at 选最近一份整体搬运，不解析、不修改。
        返回是否恢复了检查点。
        """
        best_raw: bytes | None = None
        best_at: datetime | None = None
        try:
            for att in self.db.list_attempts(job_id=job_id, limit=500):
                old_id = att.get("attempt_id")
                if not old_id or old_id == new_attempt_id:
                    continue
                p = config.run_dir(old_id) / "checkpoint.json"
                try:
                    raw = p.read_bytes()
                except OSError:
                    continue
                saved_at = None
                try:
                    obj = json.loads(raw.decode("utf-8"))
                    if isinstance(obj, dict):
                        v = str(obj.get("saved_at") or "")
                        if v:
                            saved_at = datetime.fromisoformat(v)
                except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
                    pass
                if saved_at is None:
                    try:
                        saved_at = datetime.fromtimestamp(
                            p.stat().st_mtime, timezone.utc)
                    except (OSError, ValueError):
                        saved_at = None
                if saved_at is None:
                    continue
                if best_at is None or saved_at >= best_at:
                    best_at = saved_at
                    best_raw = raw
        except Exception:
            log.exception("restore_checkpoint scan failed for job %s", job_id)
            return False
        if best_raw is None:
            return False
        try:
            new_dir = config.run_dir(new_attempt_id)
            new_dir.mkdir(parents=True, exist_ok=True)
            (new_dir / "checkpoint.json").write_bytes(best_raw)
        except OSError:
            log.warning("restore_checkpoint write failed for %s", new_attempt_id)
            return False
        log.info("restored checkpoint for attempt %s (job %s)", new_attempt_id, job_id)
        self._append_status({"ts": runner.now_iso(), "kind": "checkpoint_restored",
                             "job_id": job_id, "attempt_id": new_attempt_id})
        return True

    # ---------- 显式取消 / 查询 ----------
    def cancel_job(self, job_id: str, reason: str = "显式取消") -> bool:
        job = self.db.get_job(job_id)
        if job is None:
            return False
        for attempt_id, meta in list(self.meta.items()):
            if meta.job_id == job_id:
                a = self.attempts.get(attempt_id)
                meta.status = JobStatus.CANCELLED
                meta.cancel_deadline_mono = _now_mono() + \
                    self._pause_grace_sec(meta)
                if a is None:
                    self._finalize_missing(meta, runner.now_iso())
                else:
                    a.signal_pause()  # 非阻塞 TERM；超时由 _tick_attempts 强杀
        self._backoff_until.pop(job_id, None)
        # C6：取消立即持久化——调度器崩溃/重启后 _recover 依据落库的 CANCELLED
        # 终止旧进程并按取消收尾，不重放复活已取消作业（此前仅在无在途执行时
        # 落库，取消存活执行后崩溃会经 RECOVERING 重放路径复活）
        self.db.set_job_status(job_id, JobStatus.CANCELLED, reason, runner.now_iso())
        self._append_status({"ts": runner.now_iso(), "kind": "job_cancelled",
                             "job_id": job_id, "reason": reason})
        return True

    def cancel_attempt(self, attempt_id: str, reason: str = "显式取消") -> bool:
        meta = self.meta.get(attempt_id)
        if meta is None:
            att = None
            for row in self.db.live_attempts():
                if row["attempt_id"] == attempt_id:
                    att = row
                    break
            if att is None:
                return False
            pid = att.get("pid") or 0
            pst = att.get("pid_start_time") or 0.0
            if pid and pst:
                runner.terminate_external(pid, att.get("pgid") or pid, pst,
                                           self.cfg.control.pause_grace_base_sec,
                                           cgroup=str(att.get("cgroup") or ""))
            self.db.commit_transition(
                attempt_id,
                {"status": JobStatus.CANCELLED, "reason": reason,
                 "finished_at": runner.now_iso()},
                att["job_id"], JobStatus.CANCELLED, reason, runner.now_iso())
            return True
        meta.status = JobStatus.CANCELLED
        meta.cancel_deadline_mono = _now_mono() + self._pause_grace_sec(meta)
        a = self.attempts.get(attempt_id)
        if a is not None:
            a.signal_pause()  # 非阻塞 TERM；超时由 _tick_attempts 强杀
        # C6：取消立即持久化（崩溃/重启后按取消收尾，不重放复活）
        self.db.set_job_status(meta.job_id, JobStatus.CANCELLED, reason,
                               runner.now_iso())
        return True

    def requeue_job(self, job_id: str, reason: str = "人工重排") -> bool:
        """RECOVERY_BLOCKED 等不可解状态的解除出口（设计第十二节）。

        关闭遗留 attempt（进程仍在则先终止），作业回到 QUEUED。运行中
        （STARTING/RUNNING/PAUSING）或重启恢复核实中的作业拒绝重排。
        """
        job = self.db.get_job(job_id)
        if job is None:
            return False
        if job["status"] in (JobStatus.STARTING, JobStatus.RUNNING,
                             JobStatus.PAUSING):
            return False
        # 本实例内存中的在途 attempt（防御；正常路径不会出现）
        if any(m.job_id == job_id for m in self.meta.values()):
            return False
        # 重启恢复核实中（协作退出宽限）的作业不允许重排（避免与 _tick_recover 竞争）
        if any(att["job_id"] == job_id for att, _ in self._recover_wait.values()):
            return False
        # 关闭数据库中的遗留 attempt（RECOVERY_BLOCKED 的身份未知 attempt、
        # RECOVERING/PAUSED/BACKOFF 等未关闭行；进程仍在则先终止）
        for att in self.db.open_attempts_for_job(job_id):
            pid = att.get("pid") or 0
            pst = att.get("pid_start_time") or 0.0
            if pid and pst and runner.pid_alive_with_start(pid, pst):
                runner.terminate_external(pid, att.get("pgid") or pid, pst,
                                           self._pause_grace_base(),
                                           cgroup=str(att.get("cgroup") or ""))
            self.db.commit_transition(
                att["attempt_id"],
                {"status": JobStatus.CANCELLED, "reason": f"重排关闭：{reason}",
                 "finished_at": runner.now_iso()},
                job_id, JobStatus.QUEUED, reason, runner.now_iso())
        self._backoff_until.pop(job_id, None)
        self.db.set_job_status(job_id, JobStatus.QUEUED, reason, runner.now_iso())
        self._append_status({"ts": runner.now_iso(), "kind": "job_requeued",
                             "job_id": job_id, "reason": reason})
        return True

    def _pause_grace_base(self) -> float:
        """数据库侧 attempt（无内存 meta）的终止宽限：用配置基值。"""
        return self.cfg.control.pause_grace_base_sec

    def _finalize_missing(self, meta: AttMeta, now: str) -> None:
        """meta 存在但 AttemptRun 句柄缺失（不应发生）：直接落终态。"""
        self.db.commit_transition(
            meta.attempt_id,
            {"status": JobStatus.CANCELLED, "reason": "显式取消", "finished_at": now},
            meta.job_id, JobStatus.CANCELLED, "显式取消", now)
        self.meta.pop(meta.attempt_id, None)
        self._append_status({"ts": now, "kind": "job_cancelled",
                             "job_id": meta.job_id, "attempt_id": meta.attempt_id})

    def _write_snapshot(self) -> None:
        snap = {
            "ts": runner.now_iso(),
            "mono": _now_mono(),
            "tasks": len(self.specs),
            "queued": self.db.count_queued(),
            "live_attempts": len(self.attempts),
            "producers": self.guard.list(),
            "exec_cpu_usec": int(self._sample_cpu),
            "exec_mem_mb": int(self._sample_mem_mb),
            "exec_io_bytes": int(self._sample_io),
            "exec_pressure": self._sample_pressure,
            "observability_ok": self._observability_ok,
            "resource_unhealthy": self.trend.unhealthy(),
            "resource_state": self.trend.state,
            "external": self.etrend.snapshot(_now_mono()),
            "protected_task": self.protect.st.protected_task,
            "protect_reason": self.protect.st.reason,
            "yielded": sorted(self.protect.st.yielded),
            "pending_resume": sorted(self.protect.st.pending_resume),
            # 每个被退让任务可查询的恢复机会（设计第十节 L181）：已放回任务
            # 的临时准入上限 + 下一次放回允许时刻
            "resume_state": {
                "released": {t: self.protect.resume_cap(t, _now_mono(), 10**6)
                             for t in self.protect.st.released},
                "next_release_in_sec": round(max(
                    0.0, self.protect.st.next_release_mono - _now_mono()), 1),
            },
            "recovering": len(self._recover_wait),
            "scan_pending": dict(self._scan_pending),
            "attempt_resources": self._sample_attempt,
            "scale": {tid: {"run_target": s.run_target, "mode": s.mode}
                      for tid, s in self.scaling.scale_items()},
            # 监控统计（设计第十五节）：事件计数 + 关键路径平均延迟（最近 50 次滑动窗口）
            "stats": {
                "protect_released": self._stats["protect_released"],
                "protect_failed": self._stats["protect_failed"],
                "oom_events": self._stats["oom_events"],
                "recovery_blocked_total": self._stats["recovery_blocked_total"],
                "discovery_latency_avg_sec": self._avg(self._disc_lat),
                "start_latency_avg_sec": self._avg(self._start_lat),
                "pause_duration_avg_sec": self._avg(self._pause_dur),
                "checkpoint_latency_avg_sec": self._avg(self._ckpt_lat),
            },
            # 每任务监控视图：优先级/存活/排队/生产者积压/游标/最近重试
            "task_monitor": self._task_monitor(),
        }
        try:
            store.save_json(config.snapshot_file(), snap)
        except Exception:
            pass

    def _task_monitor(self) -> dict[str, dict]:
        """每任务监控视图（设计第十四节）。

        B2：在基础运行指标上补充——健康基线摘要（按业务阶段：样本数/速率/
        变慢判定）、不足退避原因、存活执行的暂停原因统计、生产者崩溃信息
        （持续报警在监控中始终可见，不依赖一次性日志）。
        """
        # 最近退避原因（每任务取一条最近 BACKOFF 作业）
        backoff_reasons: dict[str, str] = {}
        for j in self._jobs_in_status(JobStatus.BACKOFF):
            tid = j["task_id"]
            if tid not in backoff_reasons and (j.get("reason") or ""):
                backoff_reasons[tid] = str(j["reason"])
        # 存活执行的暂停原因统计（PAUSING 请求原因 → 计数）
        pausing_reasons: dict[str, dict[str, int]] = {}
        for m in self.meta.values():
            if m.status == JobStatus.PAUSING and m.pause_command:
                r = str(m.pause_command.get("reason") or "")
                if r:
                    d = pausing_reasons.setdefault(m.task_id, {})
                    d[r] = d.get(r, 0) + 1
        producers = {p["task_id"]: p for p in self.guard.list()}
        now_mono = _now_mono()
        out: dict[str, dict] = {}
        for task_id, spec in self.specs.items():
            pst = self._producer_state_brief(task_id)
            mon = {
                "priority": spec.priority,
                "enabled": spec.enabled,
                "live": self._live_count(task_id),
                "queued": self.db.count_queued(task_id),
                "unacked": pst["unacked"],
                "conflicted": pst["conflicted"],
                "cursor": pst["cursor"],
                "next_retry_at": self._next_retry_by_task.get(task_id, ""),
                "backoff_reason": backoff_reasons.get(task_id, ""),
            }
            stages = self.health.stages_summary(task_id, now_mono)
            if stages:
                mon["health"] = stages
            if task_id in pausing_reasons:
                mon["pausing_reasons"] = pausing_reasons[task_id]
            p = producers.get(task_id)
            if p is not None:
                mon["producer"] = {
                    "pid": p.get("pid"),
                    "alive": p.get("alive"),
                    "crash_count": int(p.get("crash_count") or 0),
                    "last_crash_wall": p.get("last_crash_wall") or "",
                }
            out[task_id] = mon
        return out

    def _producer_state_brief(self, task_id: str) -> dict:
        """生产者状态摘要：unacked 积压数 + conflicted 隔离数 + 发现游标
        （游标超 512 字节截断为占位）。"""
        p = config.producer_state_dir(task_id) / "state.json"
        try:
            obj = json.loads(p.read_text("utf-8"))
            if not isinstance(obj, dict):
                obj = {}
        except (OSError, json.JSONDecodeError):
            return {"unacked": 0, "conflicted": 0, "cursor": None}
        cursor = obj.get("cursor")
        try:
            blob = json.dumps(cursor, ensure_ascii=False)
            if len(blob) > 512:
                cursor = {"_truncated": True, "size": len(blob)}
        except (TypeError, ValueError):
            cursor = None
        return {"unacked": len(obj.get("unacked") or {}),
                "conflicted": len(obj.get("conflicted") or {}),
                "cursor": cursor}
