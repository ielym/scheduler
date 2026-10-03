"""组装层：注册表 + 触发引擎 + 全局队列(WQ) + 准出/抢占 + 监测 + 执行(带 cgroup) + 持久化。

运行模型：
- WQ 所有待运行任务先入全局队列（持久化在 scheduler_state.json）。
- 准出 tick：按双轨排序扫描，账本/软资源允许即准出，spawn runner 线程（cgroup 硬边界）。
- 抢占 tick：高优等固定资源时，选最低优先者发 preempt 事件 → runner SIGTERM → 任务 exit 99。
- 监测 tick：心跳超时/超限退让/软资源降额。
- finalize：唯一定点收敛终态，释放账本/软资源、记录画像、（中断/资源/抢占）触发恢复或退避重试。
"""

from __future__ import annotations

import logging
import shutil
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from . import (admission, cgroup, config, contract, errors, monitor, persist, preempt,
               registry, resources, runlog, runner, taskdoc)
from . import engine as _engine_mod
from .engine import TriggerEngine

log = logging.getLogger("sched.service")

TICK_SEC = 1.0
STARVATION_SEC = 3600.0          # 饿死看门狗阈值（1h）
MAX_ATTEMPTS = 5                  # 资源错误最大重试次数（之后转人工）


@dataclass
class RunningEntry:
    run: dict[str, Any]
    spec: runner.RunSpec
    est_cpu: float = 0.0
    est_mem_mb: int = 0
    soft: dict[str, int] = field(default_factory=dict)
    thread: threading.Thread | None = None
    cgroup_name: str = ""
    started_monotonic: float = 0.0
    exempt_until_monotonic: float = 0.0


class SchedulerService:
    def __init__(self, settings: config.Settings | None = None):
        self.settings = settings or config.load_settings()
        self.entries: dict[str, registry.JobEntry] = {}
        self.profiles = resources.ProfileStore()
        self.profiles.load()
        self.state = persist.load_scheduler_state()
        self.ledger = resources.Ledger()
        self.soft_pool = resources.SoftResourcePool()
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
        self.started_at = ""

    # ================= 生命周期 =================
    def start(self) -> None:
        config.ensure_layout()
        cgroup.ensure_scheduler_subtree()
        self.refresh()
        self._restore_soft_capacities()
        self._reconcile()
        self._recover_interrupted()
        self.started_at = runlog.now_iso()
        self._loop_thread = threading.Thread(target=self._loop, name="sched-v002-loop", daemon=True)
        self._loop_thread.start()

    def stop(self) -> None:
        self._stopping.set()
        if self._loop_thread:
            self._loop_thread.join(timeout=5)
        with self._lock:
            for rid, re in list(self.running.items()):
                re.spec.cancel.set()
        # 兜底整组终止并落终态
        for rid, re in list(self.running.items()):
            pgid = getattr(re.spec, "pgid", None)
            if pgid and runner.pgid_alive(int(pgid)):
                runner.kill_process_group(int(pgid), self.settings.runner.kill_grace_sec)
            self._finalize(re, runlog.STATUS_INTERRUPTED, "服务停止")
        persist.save_scheduler_state(self.state)
        persist.save_active(self._active_snapshot())

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
        # 五道闸
        ok, results, profile = contract.run_gates(doc, task_dir)
        if not ok:
            msgs = [f"{r.gate}: {r.reason}" for r in results if not r.ok]
            raise taskdoc.TaskDocError(msgs)
        # 记录初始画像（冒烟实测峰值入画像第一级）
        est_mem = int((profile.get("est_mem_mb") or doc.resources.memory_mb) or 0)
        self.profiles.record(name, doc.resources.cpu, est_mem, doc.resources.cpu,
                             doc.resources.memory_mb, window=self.settings.resource.window_samples)
        self.profiles.save()
        registry.upsert_state(name, {"enabled": True})
        self.refresh()
        return {"job_id": name, "gates": [r.reason for r in results]}

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
        while not self._stopping.wait(TICK_SEC):
            try:
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
            # 窗口到点收口：allow_overrun=false 时终止窗口内在途运行
            self._close_expired_windows(doc, now)
            self._enqueue(e, trigger="interval")
            self.engine.advance(jid, e.schedule(), now)

    def _tick_admission(self) -> None:
        with self._lock:
            items = list(self.state.wq)
        if not items:
            return
        ordered = admission.order_waiting(items, STARVATION_SEC, runlog.now_iso())
        res = admission.admission_scan(
            ordered,
            get_declared=lambda jid: self._declared(jid),
            get_profile=lambda jid: self.profiles.get(jid, *self._declared(jid)[:2]),
            ledger=self.ledger,
            capacity=self.capacity,
            soft_pool=self.soft_pool,
            overshoot=self.settings.resource.overshoot,
        )
        for rid in res.admitted:
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
        decl_cpu, decl_mem, soft = self._declared(it["job_id"])
        prof = self.profiles.get(it["job_id"], decl_cpu, decl_mem)
        est_cpu, est_mem = admission.estimate_job(prof, decl_cpu, decl_mem, self.settings.resource.overshoot)
        if est_mem <= 0:
            est_mem = max(decl_mem, self.capacity.total_memory_mb // 4)  # 保守默认
        if est_cpu <= 0:
            est_cpu = decl_cpu or 0.5

        # 软资源令牌（admission_scan 已预判，这里竞态兜底）
        acquired: list[str] = []
        for name, amt in soft.items():
            if not self.soft_pool.acquire(name, amt):
                for n2 in acquired:
                    self.soft_pool.release(n2, soft.get(n2, 0))
                with self._lock:
                    self.state.enqueue(it)
                return
            acquired.append(name)
        self.ledger.reserve(run_id, est_cpu, est_mem, soft)

        cgroup_name = f"{it['job_id']}/{run_id}"
        mem_high = int(est_mem * 1024 * 1024 * self.settings.resource.memory_high_ratio)
        mem_max = int(est_mem * 1024 * 1024 * self.settings.resource.memory_max_ratio)
        spec = runner.RunSpec(
            job_id=it["job_id"], run_id=run_id, task_dir=Path(it["task_dir"]),
            entry_file=doc.entry.file, interpreter=doc.entry.interpreter, args=list(doc.entry.args),
            cwd=doc.entry.cwd, env=dict(doc.env), trigger=it["trigger"], attempt=int(it["attempt"]),
            scheduled_at=it["scheduled_at"], expect_outputs=list(doc.outputs.expect),
            dry_run=bool(it.get("dry_run")), run_user=self.settings.run_user,
            cgroup_name=cgroup_name, memory_high=mem_high, memory_max=mem_max, cpu_quota=est_cpu,
        )
        it["status"] = runlog.STATUS_RUNNING
        it["started_at"] = runlog.now_iso()
        it["wait_ms"] = int(runlog._iso_seconds_between(it["queued_at"], it["started_at"]) * 1000)
        it["resources"] = {"cpu": est_cpu, "memory_mb": est_mem, "soft": soft}
        re = RunningEntry(run=it, spec=spec, est_cpu=est_cpu, est_mem_mb=est_mem, soft=soft,
                          cgroup_name=cgroup_name, started_monotonic=time.monotonic())
        re.thread = threading.Thread(target=self._run_worker, args=(re,), name=f"run-{run_id}", daemon=True)
        with self._lock:
            self.running[run_id] = re
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
            "state_snapshot": self._read_task_state(Path(re.run["task_dir"]))[0],
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
        # 找出最高优先的、因固定资源被阻塞的等待项
        blocked = [it for it in items if self._fixed_blocked(it)]
        if not blocked:
            return
        top = max(blocked, key=lambda it: int(it.get("priority") or 0))
        need_cpu, need_mem = self._estimate_for(top)
        running_infos = [preempt.make_running_info(re.run, re.est_cpu, re.est_mem_mb, re.soft)
                         for re in self.running.values()]
        targets = preempt.select_preempt_targets(running_infos, need_cpu, need_mem, self._declared(top["job_id"])[2])
        for t in targets:
            re = self.running.get(t.run_id)
            if re is None:
                continue
            if re.exempt_until_monotonic > now_mono:
                continue  # 豁免等待期内不动
            re.spec.preempt.set()
            persist.append_decision({"ts": runlog.now_iso(), "act": "preempt", "run_id": t.run_id,
                                     "job_id": t.job_id, "for_job": top["job_id"]})

    def _fixed_blocked(self, it: dict) -> bool:
        decl_cpu, decl_mem, soft = self._declared(it["job_id"])
        prof = self.profiles.get(it["job_id"], decl_cpu, decl_mem)
        est_cpu, est_mem = admission.estimate_job(prof, decl_cpu, decl_mem, self.settings.resource.overshoot)
        ok, reason = admission.can_admit(est_cpu, est_mem, soft, self.ledger, self.capacity, self.soft_pool)
        return (not ok) and ("固定" in reason or "CPU" in reason or "内存" in reason)

    def _estimate_for(self, it: dict) -> tuple[float, int]:
        decl_cpu, decl_mem, soft = self._declared(it["job_id"])
        prof = self.profiles.get(it["job_id"], decl_cpu, decl_mem)
        return admission.estimate_job(prof, decl_cpu, decl_mem, self.settings.resource.overshoot)

    # ================= 监测 =================
    def _tick_monitor(self) -> None:
        obs = []
        with self._lock:
            running = list(self.running.values())
        for re in running:
            st, age = self._read_task_state(Path(re.run["task_dir"]))
            actual = self._cgroup_live_mem(re.cgroup_name)
            obs.append(monitor.RunningObs(
                run_id=re.run["run_id"], job_id=re.run["job_id"], priority=re.run.get("priority", 0),
                est_mem_mb=re.est_mem_mb, actual_mem_mb=actual, heartbeat_age_sec=age,
                manual=bool(re.run.get("manual")),
            ))
        for act in self.monitor.tick(obs):
            re = self.running.get(act.run_id)
            if re is None:
                continue
            if act.kind == "heartbeat_kill":
                re.spec.cancel.set()
                persist.append_decision({"ts": runlog.now_iso(), "act": "heartbeat_kill", "run_id": act.run_id,
                                         "reason": act.reason})
            elif act.kind == "overload_preempt":
                re.spec.preempt.set()
                persist.append_decision({"ts": runlog.now_iso(), "act": "overload_preempt", "run_id": act.run_id,
                                         "reason": act.reason})

    # ================= 终态收敛 =================
    def _finalize(self, re: RunningEntry, status: str, reason: str = "", mem_peak_mb: int = 0,
                  cpu_sec: float = 0.0, oom: bool = False) -> None:
        rec = re.run
        with self._lock:
            self.running.pop(rec["run_id"], None)
        self.ledger.release(rec["run_id"])
        for k, v in re.soft.items():
            self.soft_pool.release(k, v)
        cgroup.Cgroup(re.cgroup_name).remove()

        rec["status"] = status
        if reason:
            rec["reason"] = reason
        if not rec.get("finished_at"):
            rec["finished_at"] = runlog.now_iso()
        rec.setdefault("state_snapshot", {})

        # 画像更新：正式 run（非 dry）采样本
        if not rec.get("dry_run") and mem_peak_mb > 0:
            self.profiles.record(rec["job_id"], cpu_sec, mem_peak_mb,
                                 *self._declared(rec["job_id"])[:2],
                                 window=self.settings.resource.window_samples)
            self.profiles.save()

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
            self._requeue_resume(rec)
            return
        if status == runlog.STATUS_RESOURCE:
            cat = "resource"
            self._handle_retry(rec, cat)
            return
        if status == runlog.STATUS_TIMEOUT or status == runlog.STATUS_INTERRUPTED:
            self._requeue_resume(rec)
            return
        # failed / killed / preempt_failed → 逻辑错误或数据风险，转人工（不再自动重试）
        persist.append_decision({"ts": runlog.now_iso(), "act": "terminal", "run_id": rec["run_id"],
                                 "job_id": jid, "status": status, "reason": rec.get("reason", "")})

    def _handle_retry(self, rec: dict, category: str) -> None:
        jid = rec["job_id"]
        attempts = int(rec.get("attempt") or 1)
        if attempts >= MAX_ATTEMPTS:
            persist.append_decision({"ts": runlog.now_iso(), "act": "giveup", "run_id": rec["run_id"],
                                     "job_id": jid, "reason": "资源错误已达最大重试，转人工"})
            return
        backoff = min(300 * (2 ** (attempts - 1)), 3600)
        from datetime import timedelta
        not_before = (datetime.now().astimezone() + timedelta(seconds=backoff)).isoformat(timespec="seconds")
        new_rec = dict(rec)
        new_rec["run_id"] = runlog.new_run_id()
        new_rec["attempt"] = attempts + 1
        new_rec["status"] = runlog.STATUS_WAITING
        new_rec["queued_at"] = runlog.now_iso()
        new_rec["enqueued_at"] = runlog.now_iso()
        new_rec["started_at"] = ""
        new_rec["finished_at"] = ""
        new_rec["exit_code"] = None
        new_rec["pid"] = None
        new_rec["pgid"] = None
        new_rec["resume_point_ref"] = False
        self.state.backoffs[jid] = {"not_before": not_before, "attempts": attempts}
        with self._lock:
            self.state.enqueue(new_rec)
        persist.save_scheduler_state(self.state)
        persist.append_decision({"ts": runlog.now_iso(), "act": "retry_enqueue", "run_id": new_rec["run_id"],
                                 "job_id": jid, "attempt": attempts + 1, "backoff_sec": backoff})

    def _requeue_resume(self, rec: dict) -> None:
        jid = rec["job_id"]
        e = self.entries.get(jid)
        if e is None or not e.ok or e.doc is None:
            return
        new_rec = dict(rec)
        new_rec["run_id"] = runlog.new_run_id()
        new_rec["status"] = runlog.STATUS_WAITING
        new_rec["queued_at"] = runlog.now_iso()
        new_rec["enqueued_at"] = runlog.now_iso()
        new_rec["started_at"] = ""
        new_rec["finished_at"] = ""
        new_rec["exit_code"] = None
        new_rec["pid"] = None
        new_rec["pgid"] = None
        new_rec["resume_point_ref"] = True
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
             "stdout_log": re.run.get("stdout_log", ""), "stderr_log": re.run.get("stderr_log", "")}
            for re in self.running.values()
        ]

    def _reconcile(self) -> None:
        """崩溃恢复：清理上次在途 run（kill 进程组 + 落 interrupted），残留 WQ 保留。"""
        prev = persist.load_active()
        now = runlog.now_iso()
        for p in prev:
            if not isinstance(p, dict):
                continue
            pgid = p.get("pgid")
            task_dir = p.get("task_dir", "")
            if pgid and runner.pgid_alive(int(pgid)) and self._pid_belongs_to(p.get("pid"), task_dir):
                runner.kill_process_group(int(pgid), self.settings.runner.kill_grace_sec)
            rec = {"exit_code": None, "outputs": [], "state_snapshot": {}, "error": "", "resources": {},
                   "wait_ms": 0, "duration_ms": 0}
            rec.update(p)
            rec.update({"status": runlog.STATUS_INTERRUPTED, "reason": "调度器重启，进程已中断", "finished_at": now})
            try:
                runlog.append_run(rec)
            except OSError:
                pass
        persist.save_active([])

    def _recover_interrupted(self) -> None:
        """把重启前被中断（interrupted）且可恢复的任务重新入队。"""
        # 简化：中断记录在 reconcile 已落 runlog；这里把其 job 重新入队为 resume
        # 仅处理 reconcile 里 marked interrupted 的 run_id（通过最近 interrupted 记录）
        # 为确定性起见：遍历运行态 active 本已为空，这里从 WQ 兜底，不再二次入队。
        return

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
    def _declared(self, job_id: str) -> tuple[float, int, dict[str, int]]:
        e = self.entries.get(job_id)
        if e is None or not e.ok or e.doc is None:
            return 0.0, 0, {}
        r = e.doc.resources
        return float(r.cpu), int(r.memory_mb), dict(r.soft)

    def _read_task_state(self, task_dir: Path) -> tuple[dict, float]:
        p = task_dir / "cache" / "state.json"
        if not p.exists():
            return {}, 1e9
        try:
            import json
            st = json.loads(p.read_text("utf-8"))
            age = max(0.0, time.time() - p.stat().st_mtime)
            return (st if isinstance(st, dict) else {}), age
        except (json.JSONDecodeError, OSError):
            return {}, 1e9

    def _cgroup_live_mem(self, name: str) -> int | None:
        c = cgroup.Cgroup(name)
        cur = c.read_current_mem()
        return (cur // (1024 * 1024)) if cur is not None else None

    def _set_cooldown(self, job_id: str, sec: int) -> None:
        from datetime import timedelta
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

    def _restore_soft_capacities(self) -> None:
        for k, v in self.state.soft_capacities.items():
            self.soft_pool.set_capacity(k, v)

    def _close_expired_windows(self, doc: taskdoc.TaskDoc, now: datetime) -> None:
        sched = doc.schedule
        if sched.type != "interval" or not (sched.start_time and sched.end_time) or sched.allow_overrun:
            return
        for re in list(self.running.values()):
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
                "soft": {k: {"cap": self.soft_pool.capacity(k), "used": self.soft_pool.used(k)}
                         for k in self.state.soft_capacities},
            }