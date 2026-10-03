"""组装层：注册表 + 触发引擎 + 全局队列(WQ) + 准出/抢占 + 监测 + 执行(带 cgroup) + 持久化。

运行模型：
- 自动准入：tasks/ 下出现新任务目录即由后台线程串行跑五道闸（dry、无副作用），
  通过自动纳入调度；失败记录原因并在任务文件修改/冷却到期后自动重试，全程无需手动注册。
- WQ 所有待运行任务先入全局队列（持久化在 scheduler_state.json）。
- 准出 tick：按双轨排序扫描，固定资源（CPU/内存）账本允许即准出，spawn runner 线程。
- 抢占 tick：高优等固定资源时，选最低优先者发 preempt 事件 → runner SIGTERM → 任务 exit 99。
- 监测 tick：心跳超时/超限退让。
- finalize：唯一定点收敛终态，释放账本、记录画像、（中断/资源/抢占）触发恢复或退避重试。

代理并发/API 配额等外部资源不做静态容量池（调度器无法感知跨账号、跨机器的真实占用）：
任务遇到这类问题时以退出码 100/101 报错，调度器按错误码指数退避重试，
配合冷启动的渐进式提交（一次只放 1 项观察健康）动态避让。
"""

from __future__ import annotations

import fcntl
import logging
import os
import pwd
import shutil
import threading
import time
import weakref
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from . import (admission, cgroup, config, contract, errors, monitor, persist, preempt,
               registry, resources, runlog, runner, taskdoc)
from . import engine as _engine_mod
from .engine import TriggerEngine

log = logging.getLogger("sched.service")

TICK_SEC = 1.0


@dataclass
class RunningEntry:
    run: dict[str, Any]
    spec: runner.RunSpec
    est_cpu: float = 0.0
    est_mem_mb: int = 0
    thread: threading.Thread | None = None
    cgroup_name: str = ""
    started_monotonic: float = 0.0
    exempt_until_monotonic: float = 0.0


class SchedulerService:
    # 全部实例的弱引用集合：测试 fixture 在撤销 SCHED_HOME 前强制 join worker 线程，
    # 防止 daemon 线程的异步持久化把测试数据写进生产 data/ 目录
    _instances: "weakref.WeakSet[SchedulerService]" = weakref.WeakSet()

    def __init__(self, settings: config.Settings | None = None):
        self.settings = settings or config.load_settings()
        self.entries: dict[str, registry.JobEntry] = {}
        self.profiles = resources.ProfileStore()
        self.profiles.load()
        self.state = persist.load_scheduler_state()
        self.ledger = resources.Ledger()
        self.capacity = resources.Capacity()
        self.running: dict[str, RunningEntry] = {}
        self.engine = TriggerEngine(self.settings.timezone)
        self.monitor = monitor.Monitor(
            heartbeat_timeout_sec=self.settings.runner.heartbeat_timeout_sec,
            overload_ratio=self.settings.resource.overload_ratio,
            overload_sustain_ticks=self.settings.resource.overload_sustain_sec // self.settings.resource.sample_interval_sec,
            tick_interval_sec=self.settings.resource.sample_interval_sec,
        )
        self._lock = threading.RLock()
        self._stopping = threading.Event()
        self._loop_thread: threading.Thread | None = None
        self._web_thread: threading.Thread | None = None
        self._web_server = None
        self.started_at = ""
        # 单实例锁：防止同一 SCHED_HOME 双 serve 双跑任务
        self._instance_lock_fd: int = -1
        # 渐进式提交：有固定资源等待者且无运行中任务时，一次只准出 1 项，
        # 观察其健康后才放开后续准出。
        self._staged_run_id: str | None = None
        self._staged_started_monotonic: float = 0.0
        self._staged_healthy_since: float | None = None
        self._staged_released: bool = False
        self.staged_observe_sec = max(2.0, 2.0 * self.settings.resource.sample_interval_sec)
        # 抢占后的恢复保护期：被抢任务重入队后短期内不再被抢
        self.preempt_exempt_sec = float(self.settings.preempt_exempt_sec)
        # 数据风险（110）二刷上限：连续达上限仍 110 转人工
        self.data_risk_max_reschedules = int(self.settings.data_risk_max_reschedules)
        # 自动准入：发现 tasks/ 下未注册任务 → 后台串行跑五闸 → 通过即纳入调度；
        # 失败记录原因，任务文件变更或冷却到期后自动重试
        self.admission_retry_sec = float(self.settings.auto_admit_retry_sec)
        self._admission: dict[str, dict] = persist.load_admission_state()
        self._admission_lock = threading.Lock()
        self._admission_current: str | None = None
        self._admission_thread: threading.Thread | None = None
        SchedulerService._instances.add(self)

    # ================= 生命周期 =================
    def _acquire_instance_lock(self) -> bool:
        """runtime/scheduler.lock 独占 flock；同一 SCHED_HOME 只允许一个 serve 实例。"""
        lock_path = config.runtime_dir() / "scheduler.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            return False
        self._instance_lock_fd = fd
        return True

    def _release_instance_lock(self) -> None:
        if self._instance_lock_fd >= 0:
            try:
                fcntl.flock(self._instance_lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(self._instance_lock_fd)
                self._instance_lock_fd = -1

    def start(self) -> None:
        config.ensure_layout()
        if not self._acquire_instance_lock():
            raise RuntimeError("已有调度器实例在运行（runtime/scheduler.lock 被占用），拒绝双实例启动")
        # cgroup 不可用时显式告警（防打爆只剩账本预估，无硬边界）
        if not cgroup.ensure_scheduler_subtree():
            log.warning("cgroup v2 不可用：任务无内存/CPU 硬边界，仅账本预估防打爆（降级运行）")
            persist.append_decision({"ts": runlog.now_iso(), "act": "cgroup_degraded",
                                     "reason": "cgroup 不可用，无硬资源边界"})
        self.refresh()
        self._reconcile()
        self._recover_interrupted()
        self.started_at = runlog.now_iso()
        self._loop_thread = threading.Thread(target=self._loop, name="sched-loop", daemon=True)
        self._loop_thread.start()
        # tasks/ 下的新任务自动跑五闸准入（后台串行，不阻塞主循环）
        self._kick_auto_admission()
        # 内嵌管理台（settings.web.enabled 或 serve --web）；与调度循环共享本 service 实例
        if getattr(self, "_web_enabled", False):
            from . import web
            self._web_thread = web.start_in_thread(self)

    def stop(self) -> None:
        self._stopping.set()
        if self._loop_thread:
            self._loop_thread.join(timeout=5)
        if self._admission_thread and self._admission_thread.is_alive():
            self._admission_thread.join(timeout=10)
        # 优雅停机不直接 _finalize，而是对每个 worker 线程发 cancel → 等它自然收敛到
        # worker 里的 _finalize（终态唯一定点）；只有超时才兜底。
        with self._lock:
            live_workers: list[tuple[str, threading.Thread]] = [
                (rid, re.thread) for rid, re in list(self.running.items())
                if getattr(re, "thread", None) is not None
            ]
            for rid, re in list(self.running.items()):
                re.spec.cancel.set()
        for rid, t in live_workers:
            t.join(timeout=self.settings.runner.kill_grace_sec)
            if t.is_alive():
                # 线程 join 超时：活杀进程组，但终态仍交给 worker 线程（它随后会按 rc 判定）
                re = self.running.get(rid)
                if re:
                    pgid = getattr(re.spec, "pgid", None)
                    if pgid and runner.pgid_alive(int(pgid)):
                        runner.kill_process_group(int(pgid), self.settings.runner.kill_grace_sec)
        if self._web_server is not None:
            from . import web
            web.stop_in_thread(self)
        persist.save_scheduler_state(self.state)
        persist.save_active(self._active_snapshot())
        self._release_instance_lock()

    # ================= 注册/刷新 =================
    def refresh(self) -> dict:
        entries = registry.load_registered()
        with self._lock:
            self.entries = entries
            for jid in list(self.engine._next):
                e = entries.get(jid)
                if not e or not e.ok or not e.enabled or (e.doc and e.doc.contract_exempt):
                    self.engine.remove(jid)
            for jid, e in entries.items():
                if e.ok and e.enabled and e.doc and not e.doc.contract_exempt:
                    self.engine.upsert(jid, e.schedule())
        return {"jobs": len(entries), "ok": sum(1 for e in entries.values() if e.ok)}

    def register_job(self, name: str) -> dict:
        taskdoc.validate_task_id(name)
        task_dir = config.tasks_dir() / name
        if not task_dir.is_dir():
            raise taskdoc.TaskDocError(f"任务目录不存在：{task_dir}")
        doc = taskdoc.load_task_doc(task_dir)
        # run_user 不存在时给出可操作的部署指引（而非闸门深处 SubprocessError）
        run_user = (doc.runtime.user or "") if doc.runtime else ""
        run_user = run_user or (self.settings.run_user or "")
        if run_user:
            try:
                pw = pwd.getpwnam(run_user)
            except KeyError:
                raise taskdoc.TaskDocError(
                    f"运行用户 {run_user!r} 不存在：请先创建（useradd -r -m -s /usr/sbin/nologin {run_user}），"
                    f"或在 task.json 的 runtime.user / settings.json 的 run_user 中改用已有用户/置空")
            # serve 以 root 运行时，把任务目录树降权给执行用户（等价部署步骤
            # chown -R <user> tasks/<id>），保证闸门冒烟与正式运行能读代码、写 cache/results；
            # 非 root 部署下用户与任务同属主，无需 chown
            if os.geteuid() == 0:
                try:
                    self._chown_tree(task_dir, pw.pw_uid, pw.pw_gid)
                except OSError as exc:
                    raise taskdoc.TaskDocError(f"任务目录降权给 {run_user} 失败：{exc}")
        # 五道闸
        ok, results, profile = contract.run_gates(doc, task_dir, settings=self.settings)
        if not ok:
            msgs = [f"{r.gate}: {r.reason}" for r in results if not r.ok]
            raise taskdoc.TaskDocError(msgs)
        # 记录初始画像（冒烟实测峰值入画像第一级；CPU 样本为平均核数，非累计 CPU-秒）
        est_mem = int((profile.get("est_mem_mb") or doc.resources.memory_mb) or 0)
        est_cpu = float(profile.get("est_cpu") or doc.resources.cpu or 0.0)
        self.profiles.record(name, est_cpu, est_mem, doc.resources.cpu,
                             doc.resources.memory_mb, window=self.settings.resource.window_samples)
        self.profiles.save()
        registry.upsert_state(name, {"enabled": True})
        self.refresh()
        return {"job_id": name, "gates": [r.reason for r in results]}

    # ================= 自动准入 =================
    @staticmethod
    def _chown_tree(path: Path, uid: int, gid: int) -> None:
        """递归改属主；对符号链接只改链接本身，不跟随到目录外目标。"""
        os.chown(path, uid, gid, follow_symlinks=False)
        for root, dirs, files in os.walk(path):
            for name in dirs + files:
                os.chown(os.path.join(root, name), uid, gid, follow_symlinks=False)

    def admission_snapshot(self) -> dict[str, dict]:
        """未注册任务的自动准入状态（检查中/失败原因/下次重试），供管理台展示。"""
        with self._admission_lock:
            snap = {k: dict(v) for k, v in self._admission.items()}
        current = self._admission_current
        if current and current not in snap:
            snap[current] = {"status": "checking", "since": ""}
        return snap

    @staticmethod
    def _task_fingerprint(task_dir: Path) -> int:
        """任务代码/配置文件的最新修改时间（ns）。cache/results 是运行产物，不参与。"""
        latest = 0
        if not task_dir.is_dir():
            return latest
        for p in task_dir.rglob("*"):
            rel = p.relative_to(task_dir)
            if rel.parts and rel.parts[0] in ("cache", "results"):
                continue
            try:
                latest = max(latest, p.stat().st_mtime_ns)
            except OSError:
                continue
        return latest

    def _discover_unregistered(self) -> list[str]:
        """tasks/ 下含 task.json 但不在 jobs.json 的任务目录（按名排序，保证处理顺序稳定）。"""
        with self._lock:
            registered = set(self.entries)
        tdir = config.tasks_dir()
        if not tdir.is_dir():
            return []
        return [d.name for d in sorted(tdir.iterdir())
                if d.is_dir() and (d / "task.json").is_file() and d.name not in registered]

    def _admission_due(self, rec: dict, name: str) -> bool:
        """失败记录是否可以重试：任务文件改动后立即重试，否则等冷却到期。"""
        fp = self._task_fingerprint(config.tasks_dir() / name)
        if fp and fp != int(rec.get("fingerprint") or 0):
            return True
        after = rec.get("next_check_after") or ""
        try:
            return datetime.now().astimezone() >= datetime.fromisoformat(after)
        except ValueError:
            return True

    def _kick_auto_admission(self) -> None:
        """挑选一个待准入任务，后台串行跑五闸；同一时刻只有一个检查在跑。"""
        if self._stopping.is_set():
            return
        with self._admission_lock:
            if self._admission_current is not None:
                return
            candidate, attempts = None, 0
            for name in self._discover_unregistered():
                rec = self._admission.get(name)
                if rec is not None and rec.get("status") == "failed" and not self._admission_due(rec, name):
                    continue
                if rec is not None and rec.get("status") == "checking":
                    # 上次进程崩溃遗留的 checking：作为新一轮重试
                    attempts = int(rec.get("attempts") or 0)
                elif rec is not None:
                    attempts = int(rec.get("attempts") or 0)
                candidate = name
                break
            if candidate is None:
                return
            self._admission[candidate] = {"status": "checking", "since": runlog.now_iso(),
                                          "attempts": attempts}
            self._admission_current = candidate
            persist.save_admission_state(self._admission)
        t = threading.Thread(target=self._auto_admit_worker, args=(candidate,),
                             name=f"admit-{candidate}", daemon=True)
        self._admission_thread = t
        t.start()

    def _auto_admit_worker(self, name: str) -> None:
        try:
            self.register_job(name)
        except Exception as exc:  # 准入失败不允许炸掉后台线程：记录原因，冷却后/改完文件自动重试
            reason = self._admission_error_text(exc)
            with self._admission_lock:
                prev = self._admission.get(name) or {}
                attempts = int(prev.get("attempts") or 0) + 1
                not_before = (datetime.now().astimezone()
                              + timedelta(seconds=self.admission_retry_sec)).isoformat(timespec="seconds")
                self._admission[name] = {
                    "status": "failed",
                    "reason": reason,
                    "attempts": attempts,
                    "last_check": runlog.now_iso(),
                    "next_check_after": not_before,
                    "fingerprint": self._task_fingerprint(config.tasks_dir() / name),
                }
                persist.save_admission_state(self._admission)
            persist.append_decision({"ts": runlog.now_iso(), "act": "auto_admit_failed",
                                     "job_id": name, "reason": reason, "attempts": attempts})
        else:
            with self._admission_lock:
                self._admission.pop(name, None)
                persist.save_admission_state(self._admission)
            persist.append_decision({"ts": runlog.now_iso(), "act": "auto_admit_ok", "job_id": name})
        finally:
            with self._admission_lock:
                self._admission_current = None
            self._kick_auto_admission()  # 立刻检查下一个，不必等下一轮热加载

    @staticmethod
    def _admission_error_text(exc: Exception) -> str:
        if isinstance(exc, taskdoc.TaskDocError):
            return "；".join(exc.errors)
        return f"{type(exc).__name__}: {exc}"

    # ================= 触发/入队 =================
    def trigger(self, job_id: str, trigger: str = "manual", dry_run: bool = False, priority_override: int | None = None) -> dict:
        e = self.entries.get(job_id)
        if e is None:
            raise KeyError(job_id)
        if not e.ok or e.doc is None:
            raise ValueError(e.error or "任务不可用")
        return self._enqueue(e, trigger=trigger, dry_run=dry_run, priority_override=priority_override)

    def _enqueue(self, e: registry.JobEntry, trigger: str, dry_run: bool = False,
                 priority_override: int | None = None, resume_ref: bool = False) -> dict:
        doc = e.doc
        assert doc is not None
        run_id = runlog.new_run_id()
        priority = doc.priority if priority_override is None else priority_override
        rec = {
            "run_id": run_id,
            "job_id": e.job_id,
            "task_dir": str(e.task_dir),
            "priority": priority,
            "trigger": trigger,
            "dry_run": dry_run,
            "attempt": 1,
            "status": runlog.STATUS_WAITING,
            "exit_code": None,
            "reason": "",
            "scheduled_at": runlog.now_iso(),
            "queued_at": runlog.now_iso(),
            "enqueued_at": runlog.now_iso(),
            "started_at": "",
            "finished_at": "",
            "wait_ms": 0,
            "duration_ms": 0,
            "pid": None,
            "pgid": None,
            "stdout_log": "",
            "stderr_log": "",
            "outputs": [],
            "state_snapshot": {},
            "error": "",
            "resume_point_ref": resume_ref,
            "manual": trigger == "manual",
            "contract_exempt": bool(doc.contract_exempt),
        }
        with self._lock:
            self.state.enqueue(rec)
            # WQ 持久化前先落快照（先落快照后执行）
            persist.save_scheduler_state(self.state)
            persist.append_decision({"ts": runlog.now_iso(), "act": "enqueue", "run_id": run_id,
                                     "job_id": e.job_id, "priority": priority, "trigger": trigger})
        return rec

    # ================= 主循环 =================
    def _loop(self) -> None:
        last_refresh = time.monotonic()
        while not self._stopping.wait(TICK_SEC):
            try:
                # 热加载：周期性重读 jobs.json，serve 运行中注册/修改的任务即时生效
                # （engine.upsert 已保序，不会重置 interval 任务未到期的触发时刻）
                if time.monotonic() - last_refresh >= max(1.0, float(self.settings.hot_reload_sec)):
                    self.refresh()
                    if isinstance(self.capacity, resources.Capacity):
                        self.capacity.refresh()
                    self._kick_auto_admission()
                    last_refresh = time.monotonic()
                self._tick_triggers()
                self._tick_admission()
                self._tick_preempt()
                self._tick_monitor()
            except Exception:
                log.exception("scheduler loop iteration failed")

    def _tick_triggers(self) -> None:
        now = datetime.now(ZoneInfo(self.settings.timezone))
        for jid in self.engine.due(now):
            e = self.entries.get(jid)
            if not e or not e.ok or not e.enabled or e.doc is None:
                continue
            doc = e.doc
            if self._instances_in_flight(jid) >= doc.schedule.max_instances:
                if doc.schedule.overflow == "skip":
                    persist.append_decision({"ts": runlog.now_iso(), "act": "skip_trigger", "job_id": jid,
                                             "reason": f"在途实例已达 max_instances={doc.schedule.max_instances}"})
                    self.engine.advance(jid, e.schedule(), now)
                    continue
                # overflow=queue：照常入队，由准出侧的并发上限把关
            self._enqueue(e, trigger=("once" if doc.schedule.type == "once" else "interval"))
            self.engine.advance(jid, e.schedule(), now)
            if self.engine.next_run_time(jid) is None:
                # once 到期后 next_fire 为 None：从触发引擎摘除，防止重复到期
                self.engine.remove(jid)

    def _instances_in_flight(self, job_id: str) -> int:
        """同 job 在途实例数 = WQ 等待 + 运行中。"""
        with self._lock:
            waiting = sum(1 for it in self.state.wq if it.get("job_id") == job_id)
            running = sum(1 for re in self.running.values() if re.run.get("job_id") == job_id)
        return waiting + running

    def _tick_admission(self) -> None:
        with self._lock:
            # 冷却/退避期任务留在 WQ 但不参与本轮扫描：
            # 否则其虚拟占用会阻塞本轮其他任务，且每 tick 空转 pop/enqueue
            items = [it for it in self.state.wq
                     if not self._in_cooldown(it["job_id"]) and not self._in_backoff(it["job_id"])]
        if not items:
            self._staged_run_id = None
            return
        ordered = admission.order_waiting(items, self.settings.starvation_sec, runlog.now_iso())
        res = admission.admission_scan(
            ordered,
            get_declared=lambda jid: self._declared(jid),
            get_profile=lambda jid: self.profiles.get(jid, *self._declared(jid)),
            ledger=self.ledger,
            capacity=self.capacity,
            overshoot=self.settings.resource.overshoot,
            external_mem_margin_mb=self.settings.resource.external_safety_margin_mb,
        )
        admitted = list(res.admitted)
        # 渐进式提交：存在固定资源等待者时，冷启动一次只准出 1 项，
        # 观察其健康（运行超过观察期且无资源错误/超限）后放开后续准出。
        with self._lock:
            running_cnt = len(self.running)
            staged_in_running = self._staged_run_id is not None and self._staged_run_id in self.running
        if res.blocked:
            if self._staged_run_id is None:
                if running_cnt == 0:
                    admitted = admitted[:1]
                    if admitted:
                        self._staged_run_id = admitted[0]
                        self._staged_released = False
            elif not self._staged_released:
                if staged_in_running:
                    admitted = []  # 观察期内不放开后续准出
                else:
                    # 观察项已结束（快速成功/退出）→ 放开
                    self._staged_run_id = None
            # _staged_released=True → 观察期已过，放开后续准出
        else:
            self._staged_run_id = None
            self._staged_released = False
        for rid in admitted:
            self._admit(rid)

    def _admit(self, run_id: str) -> None:
        with self._lock:
            it = self.state.pop_run(run_id)
        if it is None:
            return
        if self._in_cooldown(it["job_id"]) or self._in_backoff(it["job_id"]):
            # 冷却/退避期内不重新准出，放回队列
            with self._lock:
                self.state.enqueue(it)
            return
        e = self.entries.get(it["job_id"])
        if e is None or not e.ok or e.doc is None:
            self._finalize_stub(it, runlog.STATUS_FAILED, f"任务不可用：{e.error if e else '未注册'}")
            return
        doc = e.doc
        # 并发上限：同 job 在途实例达 max_instances 时留在队列等待（不失效）
        if self._instances_in_flight(it["job_id"]) >= doc.schedule.max_instances:
            with self._lock:
                self.state.enqueue(it)
            return
        decl_cpu, decl_mem = self._declared(it["job_id"])
        prof = self.profiles.get(it["job_id"], decl_cpu, decl_mem)
        est_cpu, est_mem = admission.estimate_job(prof, decl_cpu, decl_mem,
                                                  self.settings.resource.overshoot, self.capacity)
        if est_mem <= 0:
            est_mem = max(decl_mem, self.capacity.total_memory_mb // 4)  # 保守默认
        if est_cpu <= 0:
            est_cpu = decl_cpu or 0.5

        self.ledger.reserve(run_id, est_cpu, est_mem)

        cgroup_name = f"{it['job_id']}/{run_id}"
        mem_high = int(est_mem * 1024 * 1024 * self.settings.resource.memory_high_ratio)
        mem_max = int(est_mem * 1024 * 1024 * self.settings.resource.memory_max_ratio)
        # 降权用户：settings.run_user 为空 = 全局明确禁用降权（测试环境）；
        # 否则任务级 runtime.user 优先，回落全局 settings.run_user（死字段接线）
        if not self.settings.run_user:
            run_user = ""
        else:
            run_user = (doc.runtime.user or "") if doc.runtime else ""
            run_user = run_user or self.settings.run_user
        spec = runner.RunSpec(
            job_id=it["job_id"], run_id=run_id, task_dir=Path(it["task_dir"]),
            entry_file=doc.entry.file, interpreter=doc.entry.interpreter, args=list(doc.entry.args),
            cwd=doc.entry.cwd, env=dict(doc.env), trigger=it["trigger"], attempt=int(it["attempt"]),
            scheduled_at=it["scheduled_at"], expect_outputs=list(doc.outputs.expect),
            dry_run=bool(it.get("dry_run")), run_user=run_user,
            cgroup_name=cgroup_name, memory_high=mem_high, memory_max=mem_max, cpu_quota=est_cpu,
            cancel_grace_sec=self.settings.runner.kill_grace_sec,
            preempt_grace_sec=self.settings.runner.grace_sec,
            error_rules={"resource_regex": doc.error_rules.resource_regex,
                         "logic_regex": doc.error_rules.logic_regex},
            max_log_bytes=int(self.settings.max_log_mb) * 1024 * 1024,
        )
        it["status"] = runlog.STATUS_RUNNING
        it["started_at"] = runlog.now_iso()
        it["wait_ms"] = int(runlog._iso_seconds_between(it["queued_at"], it["started_at"]) * 1000)
        it["resources"] = {"cpu": est_cpu, "memory_mb": est_mem}
        it["_started_monotonic"] = time.monotonic()
        re = RunningEntry(run=it, spec=spec, est_cpu=est_cpu, est_mem_mb=est_mem,
                          cgroup_name=cgroup_name, started_monotonic=it["_started_monotonic"],
                          exempt_until_monotonic=float(it.get("_exempt_until_monotonic") or 0.0))
        re.thread = threading.Thread(target=self._run_worker, args=(re,), name=f"run-{run_id}", daemon=True)
        with self._lock:
            self.running[run_id] = re
        if self._staged_run_id == run_id:
            self._staged_started_monotonic = it["_started_monotonic"]
            self._staged_healthy_since = time.monotonic()
        re.thread.start()
        persist.save_active(self._active_snapshot())
        persist.append_decision({"ts": runlog.now_iso(), "act": "admit", "run_id": run_id,
                                 "job_id": it["job_id"], "est_mem_mb": est_mem, "est_cpu": est_cpu})

    def _run_worker(self, re: RunningEntry) -> None:
        outcome = runner.execute(
            re.spec,
            on_started=lambda pid, pgid: self._on_started(re, pid, pgid),
        )
        re.run.update({
            "exit_code": outcome.exit_code, "pid": outcome.pid, "pgid": outcome.pgid,
            "started_at": outcome.started_at, "finished_at": outcome.finished_at,
            "duration_ms": outcome.duration_ms, "outputs": outcome.outputs,
            "stdout_log": outcome.stdout_log, "stderr_log": outcome.stderr_log,
            "state_snapshot": self._read_task_state(Path(re.run["task_dir"]), re.run.get("run_id", ""))[0],
            "error": outcome.reason,
        })
        self._finalize(re, outcome.status, outcome.reason, mem_peak_mb=outcome.memory_peak_mb,
                       cpu_sec=round(outcome.cpu_usage_usec / 1_000_000, 3), oom=outcome.oom_killed)

    def _on_started(self, re: RunningEntry, pid: int, pgid: int) -> None:
        re.run["pid"] = pid
        re.run["pgid"] = pgid
        re.spec.pid, re.spec.pgid = pid, pgid
        persist.save_active(self._active_snapshot())

    # ================= 抢占 =================
    def _tick_preempt(self) -> None:
        with self._lock:
            items = list(self.state.wq)
        if not items:
            return
        now_mono = time.monotonic()
        # 因固定资源被阻塞的等待项（按优先级降序逐个服务：P3 修复——次优等待者的
        # 可满足缺口同轮处理，而非只服务最高优者）
        blocked = [it for it in items if self._fixed_blocked(it)]
        if not blocked:
            return
        blocked.sort(key=lambda it: -int(it.get("priority") or 0))
        running_infos = [preempt.make_running_info(re.run, re.est_cpu, re.est_mem_mb)
                         for re in list(self.running.values())]
        chosen: set[str] = set()
        freed_cpu, freed_mem = 0.0, 0
        for top in blocked:
            need_cpu, need_mem = self._estimate_for(top)
            # 按真实缺口计算：需求 - 当前账本空闲 - 本轮已抢占将释放的量（部分空闲场景只补差额）
            free_cpu = self.capacity.total_cpu - self.ledger.reserved_cpu() + freed_cpu
            free_mem = self.capacity.total_memory_mb - self.ledger.reserved_mem_mb() + freed_mem
            gap_cpu = max(0.0, need_cpu - free_cpu)
            gap_mem = max(0, need_mem - free_mem)
            if gap_cpu <= 0 and gap_mem <= 0:
                continue  # 空闲已足够，无需抢占（等待可能是账本竞态，下轮准出即可）
            targets = preempt.select_preempt_targets(
                running_infos, gap_cpu, gap_mem,
                waiter_priority=int(top.get("priority") or 0))
            for t in targets:
                if t.run_id in chosen:
                    continue
                re = self.running.get(t.run_id)
                if re is None:
                    continue
                if re.exempt_until_monotonic > now_mono:
                    continue  # 豁免等待期内不动
                re.spec.preempt.set()
                chosen.add(t.run_id)
                freed_cpu += t.est_cpu
                freed_mem += t.est_mem_mb
                persist.append_decision({"ts": runlog.now_iso(), "act": "preempt", "run_id": t.run_id,
                                         "job_id": t.job_id, "for_job": top["job_id"]})

    def _fixed_blocked(self, it: dict) -> bool:
        decl_cpu, decl_mem = self._declared(it["job_id"])
        prof = self.profiles.get(it["job_id"], decl_cpu, decl_mem)
        est_cpu, est_mem = admission.estimate_job(prof, decl_cpu, decl_mem,
                                                  self.settings.resource.overshoot, self.capacity)
        ok, reason = admission.can_admit(est_cpu, est_mem, self.ledger, self.capacity,
                                         external_mem_margin_mb=self.settings.resource.external_safety_margin_mb)
        return not ok

    def _estimate_for(self, it: dict) -> tuple[float, int]:
        decl_cpu, decl_mem = self._declared(it["job_id"])
        prof = self.profiles.get(it["job_id"], decl_cpu, decl_mem)
        return admission.estimate_job(prof, decl_cpu, decl_mem,
                                      self.settings.resource.overshoot, self.capacity)

    # ================= 监测 =================
    def _tick_monitor(self) -> None:
        now = datetime.now(ZoneInfo(self.settings.timezone))
        # 窗口到点收口（每 tick 检查，不依赖是否有到期任务）：
        # allow_overrun=false 时终止过期窗口内的在途运行
        for e in self.entries.values():
            if e.ok and e.doc is not None:
                self._close_expired_windows(e.doc, now)
        obs = []
        with self._lock:
            running = list(self.running.values())
        for re in running:
            st, age = self._read_task_state(Path(re.run["task_dir"]), re.run.get("run_id", ""))
            if age >= 1e8:
                # 从未读到 state.json（Python 冷启动/import 期尚未首次 flush）：
                # 用进程存活年龄代替 1e9 哨兵——启动宽限等于完整心跳超时，
                # 绝不能在准出当 tick 就把任务误杀（同 tick 内 admission→monitor 的固有竞态）。
                age = max(0.0, time.monotonic() - re.started_monotonic)
            actual = self._cgroup_live_mem(re.cgroup_name)
            if actual is not None:
                # 账本实测值刷新（cpu 速率不逐 tick 采样，仅内存；status 用于对账）
                self.ledger.set_actual(re.run["run_id"], 0.0, actual)
            e = self.entries.get(re.run["job_id"])
            hb_timeout = None
            if e is not None and e.doc is not None:
                hb_timeout = e.doc.heartbeat.timeout_sec
                if re.run.get("manual"):
                    hb_timeout *= 2  # manual 任务放宽阈值但不豁免（防无限占用）
            obs.append(monitor.RunningObs(
                run_id=re.run["run_id"], job_id=re.run["job_id"], priority=re.run.get("priority", 0),
                est_mem_mb=re.est_mem_mb, actual_mem_mb=actual, heartbeat_age_sec=age,
                manual=bool(re.run.get("manual")), heartbeat_timeout_sec=hb_timeout,
            ))
        for act in self.monitor.tick(obs):
            re = self.running.get(act.run_id)
            if re is None:
                continue
            if act.kind == "heartbeat_kill":
                # 心跳超时：cancel 前写入原因，终态按 interrupted 语义重入队一次
                re.spec.cancel_reason = act.reason
                re.spec.cancel.set()
                persist.append_decision({"ts": runlog.now_iso(), "act": "heartbeat_kill", "run_id": act.run_id,
                                         "reason": act.reason})
                self._invalidate_staged(act.run_id)
            elif act.kind == "overload_preempt":
                re.spec.preempt.set()
                persist.append_decision({"ts": runlog.now_iso(), "act": "overload_preempt", "run_id": act.run_id,
                                         "reason": act.reason})
                self._invalidate_staged(act.run_id)
        # 渐进式提交观察期推进：观察项健康运行超过阈值后放开后续准出
        if self._staged_run_id is not None and not self._staged_released \
                and self._staged_healthy_since is not None:
            if time.monotonic() - self._staged_healthy_since >= self.staged_observe_sec:
                self._staged_released = True

    def _invalidate_staged(self, run_id: str) -> None:
        """观察项被杀/被抢占/资源错误终结 → 重置渐进门禁，下轮重新选首个准出项观察。"""
        if self._staged_run_id == run_id:
            self._staged_run_id = None
            self._staged_healthy_since = None
            self._staged_released = False

    # ================= 终态收敛 =================
    def _finalize(self, re: RunningEntry, status: str, reason: str = "", mem_peak_mb: int = 0,
                  cpu_sec: float = 0.0, oom: bool = False) -> None:
        rec = re.run
        with self._lock:
            self.running.pop(rec["run_id"], None)
        self.ledger.release(rec["run_id"])
        cgroup.Cgroup(re.cgroup_name).remove()

        rec["status"] = status
        if reason:
            rec["reason"] = reason
        if not rec.get("finished_at"):
            rec["finished_at"] = runlog.now_iso()
        rec.setdefault("state_snapshot", {})

        # 画像更新：正式 run（非 dry）采样本。
        # CPU 样本必须是「平均核数」（cpu_sec / 时长），而非累计 CPU-秒——
        # 否则长任务样本被当作核数，预估膨胀致永久饥饿。
        if not rec.get("dry_run") and mem_peak_mb > 0:
            duration_sec = max(1, int((rec.get("duration_ms") or 0) / 1000))
            avg_cpu_cores = round(cpu_sec / duration_sec, 3)
            self.profiles.record(rec["job_id"], avg_cpu_cores, mem_peak_mb,
                                 *self._declared(rec["job_id"]),
                                 window=self.settings.resource.window_samples)
            self.profiles.save()

        # 渐进式提交：观察项以资源错误/超限终结 → 门禁重置
        if status in (runlog.STATUS_RESOURCE, runlog.STATUS_PREEMPTED, runlog.STATUS_FAILED):
            self._invalidate_staged(rec["run_id"])

        try:
            runlog.append_run(rec)
        except OSError:
            pass
        persist.save_active(self._active_snapshot())

        # 后继调度
        self._after_finalize(rec, status)

    def _finalize_stub(self, it: dict, status: str, reason: str) -> None:
        it["status"] = status
        it["reason"] = reason
        it["finished_at"] = runlog.now_iso()
        it.setdefault("state_snapshot", {})
        try:
            runlog.append_run(it)
        except OSError:
            pass

    def _after_finalize(self, rec: dict, status: str) -> None:
        jid = rec["job_id"]
        if status == runlog.STATUS_SUCCESS:
            self._set_cooldown(jid, self.settings.runner.cooldown_sec)
            return
        if status == runlog.STATUS_PREEMPTED:
            self._set_cooldown(jid, self.settings.runner.cooldown_sec)
            self._requeue_resume(rec, exempt_sec=self.preempt_exempt_sec)
            return
        if status == runlog.STATUS_RESOURCE:
            cat = "resource"
            self._handle_retry(rec, cat)
            return
        if status == runlog.STATUS_TIMEOUT or status == runlog.STATUS_INTERRUPTED:
            self._requeue_resume(rec)
            return
        if status == runlog.STATUS_FAILED and rec.get("exit_code") == errors.EXIT_DATA_RISK:
            # 数据风险（110）二刷闭环：跳过项已落 pending_skipped，
            # 延迟重入队二刷；连续达上限仍 110 才转人工
            self._handle_data_risk(rec)
            return
        if status == runlog.STATUS_KILLED and self._stopping.is_set():
            # 优雅停机导致的 killed 属服务中断而非任务失败 → 断点重入队，不转人工
            self._requeue_resume(rec)
            return
        if status == runlog.STATUS_KILLED and "心跳超时" in (rec.get("error") or ""):
            # 心跳超时误判（如长步骤未刷新状态）按 interrupted 语义重入队一次；
            # 连续两次心跳超时才转人工（真正卡死的任务不会无限循环）
            rounds = int(rec.get("hb_requeues") or 0)
            if rounds < 1:
                new_rec = self._new_run_record(rec, hb_requeues=rounds + 1)
                with self._lock:
                    self.state.enqueue(new_rec)
                persist.save_scheduler_state(self.state)
                persist.append_decision({"ts": runlog.now_iso(), "act": "heartbeat_requeue",
                                         "run_id": new_rec["run_id"], "job_id": jid, "round": rounds + 1})
                return
        # failed / killed / preempt_failed → 逻辑错误或数据风险，转人工（不再自动重试）
        persist.append_decision({"ts": runlog.now_iso(), "act": "terminal", "run_id": rec["run_id"],
                                 "job_id": jid, "status": status, "reason": rec.get("reason", "")})

    def _new_run_record(self, rec: dict, trigger: str | None = None, attempt: int | None = None,
                        resume_ref: bool = False, data_risk_rounds: int | None = None,
                        hb_requeues: int | None = None) -> dict:
        """重入队统一工厂：清空上轮运行痕迹（pid/日志/耗时/错误等），只保留调度语义字段。

        原先各处理分支手工 dict(rec) 拷贝，error/stdout_log/wait_ms 等会残留上轮失败痕迹。
        """
        new_rec = dict(rec)
        new_rec["run_id"] = runlog.new_run_id()
        new_rec["status"] = runlog.STATUS_WAITING
        new_rec["queued_at"] = runlog.now_iso()
        new_rec["enqueued_at"] = runlog.now_iso()
        new_rec["scheduled_at"] = runlog.now_iso()
        new_rec["started_at"] = ""
        new_rec["finished_at"] = ""
        new_rec["exit_code"] = None
        new_rec["pid"] = None
        new_rec["pgid"] = None
        new_rec["reason"] = ""
        new_rec["error"] = ""
        new_rec["stdout_log"] = ""
        new_rec["stderr_log"] = ""
        new_rec["wait_ms"] = 0
        new_rec["duration_ms"] = 0
        new_rec["resume_point_ref"] = resume_ref
        new_rec.pop("_started_monotonic", None)
        new_rec.pop("_exempt_until_monotonic", None)
        if trigger is not None:
            new_rec["trigger"] = trigger
        if attempt is not None:
            new_rec["attempt"] = attempt
        if data_risk_rounds is not None:
            new_rec["data_risk_rounds"] = data_risk_rounds
        if hb_requeues is not None:
            new_rec["hb_requeues"] = hb_requeues
        return new_rec

    def _handle_data_risk(self, rec: dict) -> None:
        jid = rec["job_id"]
        rounds = int(rec.get("data_risk_rounds") or 0)
        if rounds >= self.data_risk_max_reschedules:
            persist.append_decision({"ts": runlog.now_iso(), "act": "terminal", "run_id": rec["run_id"],
                                     "job_id": jid, "status": "data_risk",
                                     "reason": f"连续 {rounds} 次数据风险（110），转人工"})
            return
        new_rec = self._new_run_record(rec, trigger="second_pass", data_risk_rounds=rounds + 1)
        with self._lock:
            self.state.enqueue(new_rec)
        persist.save_scheduler_state(self.state)
        persist.append_decision({"ts": runlog.now_iso(), "act": "data_risk_reschedule",
                                 "run_id": new_rec["run_id"], "job_id": jid, "round": rounds + 1})

    def _handle_retry(self, rec: dict, category: str) -> None:
        """资源错误退避重试（退让而非扑杀）。

        默认不再设重试次数上限——指数退避封顶 1h，任务在资源释放后自然获得
        准出（总目标：所有已提交任务最终都能被正确执行）。仅当运维显式配置
        settings.runner.max_resource_attempts > 0 时才封顶转人工（止损逃生舱）。
        """
        jid = rec["job_id"]
        attempts = int(rec.get("attempt") or 1)
        max_attempts = int(self.settings.runner.max_resource_attempts)
        if max_attempts > 0 and attempts >= max_attempts:
            persist.append_decision({"ts": runlog.now_iso(), "act": "giveup", "run_id": rec["run_id"],
                                     "job_id": jid,
                                     "reason": f"资源错误已达配置上限 {max_attempts} 次（max_resource_attempts），转人工"})
            return
        backoff = min(300 * (2 ** min(attempts - 1, 20)), 3600)
        not_before = (datetime.now().astimezone() + timedelta(seconds=backoff)).isoformat(timespec="seconds")
        new_rec = self._new_run_record(rec, attempt=attempts + 1, resume_ref=False)
        self.state.backoffs[jid] = {"not_before": not_before, "attempts": attempts}
        with self._lock:
            self.state.enqueue(new_rec)
        persist.save_scheduler_state(self.state)
        persist.append_decision({"ts": runlog.now_iso(), "act": "retry_enqueue", "run_id": new_rec["run_id"],
                                 "job_id": jid, "attempt": attempts + 1, "backoff_sec": backoff})

    def _requeue_resume(self, rec: dict, exempt_sec: float = 0.0) -> None:
        jid = rec["job_id"]
        e = self.entries.get(jid)
        if e is None or not e.ok or e.doc is None:
            return
        new_rec = self._new_run_record(rec, resume_ref=True)
        if exempt_sec > 0:
            # 恢复保护期：被抢占者重入队后短期内不再被抢（豁免等待期）
            new_rec["_exempt_until_monotonic"] = time.monotonic() + exempt_sec
        with self._lock:
            self.state.enqueue(new_rec)
        persist.save_scheduler_state(self.state)
        persist.append_decision({"ts": runlog.now_iso(), "act": "resume_enqueue", "run_id": new_rec["run_id"],
                                 "job_id": jid, "resume": True})

    # ================= 持久化/恢复 =================
    def _active_snapshot(self) -> list[dict]:
        return [
            {"run_id": re.run.get("run_id"), "job_id": re.run.get("job_id"), "task_dir": re.run.get("task_dir"),
             "pid": re.run.get("pid"), "pgid": re.run.get("pgid"), "status": re.run.get("status"),
             "trigger": re.run.get("trigger"), "scheduled_at": re.run.get("scheduled_at"),
             "queued_at": re.run.get("queued_at"), "started_at": re.run.get("started_at"),
             "priority": re.run.get("priority"), "attempt": re.run.get("attempt"),
             "manual": re.run.get("manual"), "contract_exempt": re.run.get("contract_exempt"),
             "stdout_log": re.run.get("stdout_log", ""), "stderr_log": re.run.get("stderr_log", "")}
            for re in self.running.values()
        ]

    def _reconcile(self) -> None:
        """崩溃恢复：清理上次在途 run（kill 进程组 + 落 interrupted），残留 WQ 保留。

        被中断的运行写入 recover_queue，供 _recover_interrupted 重新入队。
        """
        prev = persist.load_active()
        now = runlog.now_iso()
        recover: list[dict] = []
        for p in prev:
            if not isinstance(p, dict):
                continue
            pgid = p.get("pgid")
            task_dir = p.get("task_dir", "")
            if pgid and runner.pgid_alive(int(pgid)) and self._pid_belongs_to(p.get("pid"), task_dir):
                runner.kill_process_group(int(pgid), self.settings.runner.kill_grace_sec)
            # 清理上次运行遗留的 cgroup（含仍在组内的孤儿进程，经 cgroup.kill 安全终止）
            cg_name = p.get("cgroup_name") or f"{p.get('job_id')}/{p.get('run_id')}"
            if p.get("job_id") and p.get("run_id"):
                try:
                    cgroup.Cgroup(cg_name).remove()
                except Exception:
                    pass
            rec = {"exit_code": None, "outputs": [], "state_snapshot": {}, "error": "", "resources": {},
                   "wait_ms": 0, "duration_ms": 0}
            rec.update(p)
            rec.update({"status": runlog.STATUS_INTERRUPTED, "reason": "调度器重启，进程已中断", "finished_at": now})
            try:
                runlog.append_run(rec)
            except OSError:
                pass
            recover.append(rec)
        persist.save_active([])
        persist.save_recover_queue(recover)

    def _recover_interrupted(self) -> None:
        """把重启前被中断（interrupted）且可恢复的任务重新入队（崩溃恢复）。

        contract_exempt 任务不可断点恢复（只手动触发），仅记录待人工，不自动入队。
        """
        queue = persist.load_recover_queue()
        if not queue:
            return
        for rec in queue:
            if not isinstance(rec, dict):
                continue
            jid = rec.get("job_id")
            if rec.get("contract_exempt"):
                persist.append_decision({"ts": runlog.now_iso(), "act": "recover_manual", "job_id": jid,
                                         "run_id": rec.get("run_id"),
                                         "reason": "contract_exempt 任务不自动恢复，待人工"})
                continue
            e = self.entries.get(jid)
            if e is None or not e.ok or e.doc is None:
                persist.append_decision({"ts": runlog.now_iso(), "act": "recover_skip", "job_id": jid,
                                         "run_id": rec.get("run_id"), "reason": "任务不可用/未注册"})
                continue
            new_rec = self._new_run_record(rec, resume_ref=True)
            with self._lock:
                self.state.enqueue(new_rec)
            persist.append_decision({"ts": runlog.now_iso(), "act": "recover_enqueue",
                                     "run_id": new_rec["run_id"], "job_id": jid,
                                     "from_run": rec.get("run_id")})
        with self._lock:
            persist.save_scheduler_state(self.state)
        persist.save_recover_queue([])

    @staticmethod
    def _pid_belongs_to(pid: Any, task_dir: str) -> bool:
        if not pid or not task_dir:
            return False
        try:
            raw = Path(f"/proc/{int(pid)}/cmdline").read_bytes()
        except (OSError, ValueError):
            return False
        return task_dir.encode() in raw

    # ================= 工具 =================
    def _declared(self, job_id: str) -> tuple[float, int]:
        e = self.entries.get(job_id)
        if e is None or not e.ok or e.doc is None:
            return 0.0, 0
        r = e.doc.resources
        return float(r.cpu), int(r.memory_mb)

    def _read_task_state(self, task_dir: Path, run_id: str = "") -> tuple[dict, float]:
        """读任务实例级状态。F4：优先 cache/runs/<run_id>/state.json，回落 legacy。"""
        candidates = []
        if run_id:
            candidates.append(task_dir / "cache" / "runs" / run_id / "state.json")
        candidates.append(task_dir / "cache" / "state.json")
        for p in candidates:
            if not p.exists():
                continue
            try:
                import json
                st = json.loads(p.read_text("utf-8"))
                age = max(0.0, time.time() - p.stat().st_mtime)
                return (st if isinstance(st, dict) else {}), age
            except (json.JSONDecodeError, OSError):
                continue
        return {}, 1e9

    def _cgroup_live_mem(self, name: str) -> int | None:
        c = cgroup.Cgroup(name)
        cur = c.read_current_mem()
        return (cur // (1024 * 1024)) if cur is not None else None

    def _set_cooldown(self, job_id: str, sec: int) -> None:
        self.state.cooldowns[job_id] = (datetime.now().astimezone() + timedelta(seconds=sec)).isoformat(timespec="seconds")
        persist.save_scheduler_state(self.state)

    def _in_cooldown(self, job_id: str) -> bool:
        until = self.state.cooldowns.get(job_id)
        if not until:
            return False
        try:
            return datetime.now().astimezone() < datetime.fromisoformat(until)
        except ValueError:
            return False

    def _in_backoff(self, job_id: str) -> bool:
        b = self.state.backoffs.get(job_id)
        if not b:
            return False
        try:
            return datetime.now().astimezone() < datetime.fromisoformat(b.get("not_before") or "")
        except ValueError:
            return False

    def _close_expired_windows(self, doc: taskdoc.TaskDoc, now: datetime) -> None:
        sched = doc.schedule
        if sched.type != "interval" or not (sched.start_time and sched.end_time) or sched.allow_overrun:
            return
        with self._lock:
            running = list(self.running.values())
        for re in running:
            rec = re.run
            started = rec.get("started_at") or rec.get("scheduled_at")
            try:
                st = datetime.fromisoformat(started)
            except (ValueError, TypeError):
                continue
            win = _engine_mod.window_interval_containing(sched.model_dump(), st.astimezone(now.tzinfo))
            if win and now > win[1]:
                re.spec.cancel.set()

    # ================= 查询 =================
    def list_jobs(self) -> list[dict]:
        out = []
        for e in sorted(self.entries.values(), key=lambda x: x.job_id):
            out.append({
                "job_id": e.job_id, "name": e.doc.display_name() if e.doc else e.job_id,
                "ok": e.ok, "error": e.error, "enabled": e.enabled,
                "schedule": e.schedule(), "priority": e.doc.priority if e.doc else 0,
                "next_run_at": self.engine.next_run_time(e.job_id),
            })
        return out

    def status(self) -> dict:
        with self._lock:
            return {
                "jobs": len(self.entries),
                "waiting": len(self.state.wq),
                "running": len(self.running),
                "ledger_reserved_mem_mb": self.ledger.reserved_mem_mb(),
                "ledger_actual_mem_mb": self.ledger.actual_mem_mb(),
                "admission": {
                    "checking": sum(1 for v in self.admission_snapshot().values()
                                    if v.get("status") == "checking"),
                    "failed": sum(1 for v in self.admission_snapshot().values()
                                  if v.get("status") == "failed"),
                },
            }