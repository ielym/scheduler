"""调度器主循环：扫描→准出→抢占→监测→终结收敛。

组装层：manifest(注册表) + scanner(意图) + admission + preempt + monitor + runner + control。
与 v1 的根本区别：
  1. 没有 engine/trigger（interval 由意图文件的写入表达）
  2. 退出码终结决策由 control.decide_from_exit 统一处理
  3. 进程协调（stop/no_retry/reschedule）走 control 段，不靠删改意图文件
"""

from __future__ import annotations

import fcntl
import logging
import os
import pwd
import threading
import time
import weakref
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import (admission, cgroup, config, control, errors, manifest, monitor,
               persist, preempt, registry, resources, runlog, runner, store)
from . import scanner as _scanner

log = logging.getLogger("sched.service")

TICK_SEC = 1.0


@dataclass
class RunningIntent:
    """运行中进程的运行态。"""
    run: dict[str, Any]
    spec: runner.RunSpec
    est_cpu: float = 0.0
    est_mem_mb: int = 0
    thread: threading.Thread | None = None
    cgroup_name: str = ""
    started_monotonic: float = 0.0
    # 抢占豁免
    exempt_until_monotonic: float = 0.0
    # 动态画像采样
    prev_cpu_usec: int = 0
    prev_cpu_monotonic: float = 0.0
    live_cpu_cores: float = 0.0
    live_mem_mb: int = 0
    last_profile_rec_monotonic: float = 0.0


class SchedulerService:
    _instances: "weakref.WeakSet[SchedulerService]" = weakref.WeakSet()

    def __init__(self, settings: config.Settings | None = None):
        self.settings = settings or config.load_settings()
        self.entries: dict[str, registry.JobEntry] = {}
        self.profiles = resources.ProfileStore()
        self.profiles.load()
        self.state = persist.load_scheduler_state()
        self.ledger = resources.Ledger()
        self.capacity = resources.Capacity()
        self.running: dict[str, RunningIntent] = {}
        self._lock = threading.RLock()
        self._stopping = threading.Event()
        self._loop_thread: threading.Thread | None = None
        self._web_thread: threading.Thread | None = None
        self._web_server = None
        self.started_at = ""
        self._instance_lock_fd: int = -1

        # 抢占后恢复保护期（被抢者重入队后短期内不再被抢）
        self.preempt_exempt_sec = float(self.settings.preempt_exempt_sec)

        # 运行中动态画像采样节流间隔
        self.profile_live_interval_sec = max(5.0, float(self.settings.resource.profile_live_interval_sec))
        self._last_profile_save_monotonic = 0.0

        # 已注册子任务目录的指纹（用于 auto 触发驱动进程的自动准入）
        self._admission: dict[str, dict] = persist.load_admission_state()
        self._admission_lock = threading.Lock()
        self._admission_current: str | None = None

        # 监测器
        self.monitor = monitor.Monitor(
            heartbeat_timeout_sec=self.settings.runner.heartbeat_timeout_sec,
            overload_ratio=self.settings.resource.overload_ratio,
            overload_sustain_ticks=self.settings.resource.overload_sustain_sec //
                                    max(1, int(self.settings.resource.sample_interval_sec)),
            tick_interval_sec=self.settings.resource.sample_interval_sec,
        )

        SchedulerService._instances.add(self)

    # ================= 生命周期 =================

    def _acquire_instance_lock(self) -> bool:
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
            raise RuntimeError("已有调度器实例在运行")
        if not cgroup.ensure_scheduler_subtree():
            log.warning("cgroup v2 不可用：降级运行（无硬资源边界）")
            persist.append_decision({"ts": runlog.now_iso(), "act": "cgroup_degraded",
                                     "reason": "cgroup 不可用"})
        self.refresh()
        self._recover_running()
        self.started_at = runlog.now_iso()
        self._loop_thread = threading.Thread(target=self._loop, name="sched-loop", daemon=True)
        self._loop_thread.start()
        if getattr(self, "_web_enabled", False):
            from . import web
            self._web_thread = web.start_in_thread(self)

    def stop(self) -> None:
        self._stopping.set()
        if self._loop_thread:
            self._loop_thread.join(timeout=5)
        # 优雅停机：cancel 所有运行中进程
        with self._lock:
            live = [(rid, re) for rid, re in list(self.running.items())
                    if getattr(re.thread, "is_alive", lambda: False)()]
            for rid, re in live:
                re.spec.cancel_reason = "调度器停机"
                re.spec.cancel.set()
        for rid, re in live:
            if re.thread:
                re.thread.join(timeout=self.settings.runner.kill_grace_sec)
        if self._web_server is not None:
            from . import web
            web.stop_in_thread(self)
        persist.save_scheduler_state(self.state)
        persist.save_active(self._active_snapshot())
        self._release_instance_lock()

    # ================= 注册表 =================

    def refresh(self) -> dict:
        entries = registry.load_registered()
        with self._lock:
            self.entries = entries
        return {"subtasks": len(entries), "ok": sum(1 for e in entries.values() if e.ok)}

    def _recover_running(self) -> None:
        """重启恢复：上次 running 的意图，其进程已随 serve 退出而消失。

        把它们重置回 pending 并重新入队（trigger=resume），配合 SDK 断点续跑不重复工作。
        重置 pending 让下次 scan 不会被当成新意图，重新入队让它们能继续被准出。
        """
        state = _scanner.load_intents_state()
        recovered = 0
        for stid, rid_map in state.items():
            for rid, st in rid_map.items():
                if st.get("status") != "running":
                    continue
                st["status"] = "pending"
                st["run_id"] = ""
                recovered += 1
                try:
                    self._enqueue_intent(stid, rid, trigger="resume", resume_ref=True)
                except Exception:
                    log.warning("recover intent %s/%s failed", stid, rid)
        if recovered:
            _scanner.save_intents_state(state)
            log.info("recovered %d running intents after restart", recovered)

    def register_subtask(self, name: str) -> dict:
        """导入一个子任务目录（必须含 manifest.json）。"""
        manifest.validate_task_id(name)
        task_dir = config.tasks_dir() / name
        if not task_dir.is_dir():
            raise manifest.ManifestError(f"任务目录不存在：{task_dir}")
        doc = manifest.load_manifest(task_dir)
        run_user = (doc.runtime.user or "") if doc.runtime else ""
        run_user = run_user or (self.settings.run_user or "")
        if run_user:
            try:
                pw = pwd.getpwnam(run_user)
            except KeyError:
                raise manifest.ManifestError(
                    f"运行用户 {run_user!r} 不存在：请先创建（useradd -r -m -s /usr/sbin/nologin {run_user}）")
            if os.geteuid() == 0:
                self._chown_tree(task_dir, pw.pw_uid, pw.pw_gid)

        # 跑双闸：schema + 契约可执行（dry 跑通 exit 0）
        self._check_gates(doc, task_dir)

        # 写入基线画像
        est_mem = int(doc.resources.memory_mb or 0)
        est_cpu = float(doc.resources.cpu or 0.0)
        if est_mem > 0 or est_cpu > 0:
            self.profiles.record(name, est_cpu, est_mem, est_cpu, est_mem,
                                 window=self.settings.resource.window_samples)
        self.profiles.save()

        registry.upsert_state(name, {"enabled": doc.run_mode == "auto"})
        self.refresh()
        return {"subtask_id": name, "run_mode": doc.run_mode}

    @staticmethod
    def _chown_tree(path: Path, uid: int, gid: int) -> None:
        os.chown(path, uid, gid, follow_symlinks=False)
        for root, dirs, files in os.walk(path):
            for name in dirs + files:
                os.chown(os.path.join(root, name), uid, gid, follow_symlinks=False)

    def _check_gates(self, doc: manifest.TaskManifest, task_dir: Path) -> None:
        """双闸校验：闸1 schema + entry 存在 + 路径约束（parse_manifest 做）
        闸2 契约可执行（dry 跑通 exit 0）
        """
        from . import contract as _contract
        gates = _contract.ContractGates(self.settings)
        ok, results = gates.run_all_gates(doc, task_dir)
        if not ok:
            msgs = [f"{r.gate}: {r.reason}" for r in results if not r.ok]
            raise manifest.ManifestError(msgs)

    # ================= 主循环 =================

    def _loop(self) -> None:
        last_refresh = time.monotonic()
        while not self._stopping.wait(TICK_SEC):
            try:
                if time.monotonic() - last_refresh >= max(1.0, float(self.settings.hot_reload_sec)):
                    self.refresh()
                    if isinstance(self.capacity, resources.Capacity):
                        self.capacity.refresh()
                    last_refresh = time.monotonic()
                self._tick_scan()
                self._tick_admit()
                self._tick_preempt()
                self._tick_monitor()
            except Exception:
                log.exception("scheduler loop iteration failed")

    # ================= 扫描 → WQ =================

    def _tick_scan(self) -> None:
        """扫描所有已启用子任务的 requests/，新意图入队。"""
        now_iso = runlog.now_iso()
        all_states = _scanner.load_intents_state()
        for stid, entry in self.entries.items():
            if not entry.ok or not entry.enabled:
                continue
            persisted = all_states.get(stid, {})
            try:
                diff = _scanner.scan_subtask(stid, entry.task_dir, persisted)
            except Exception as e:
                log.warning("scan_subtask %s failed: %s", stid, e)
                continue
            # 新意图入 WQ
            for r in diff.new_requests:
                self._enqueue_intent(stid, r["request_id"], trigger="auto", save=False)
            # 改写违规 → reject（仅审计，不入队）
            for r in diff.modified_requests:
                persist.append_decision({"ts": now_iso, "act": "intent_modified_rejected",
                                         "subtask_id": stid, "request_id": r["request_id"],
                                         "reason": r["reason"]})
            # 已消失（running 态）→ 进程 stop 由 cancel 触发；pending 态直接终结
            for r in diff.disappeared_requests:
                self._drain_gone_request(stid, r["request_id"], r["reason"])
        _scanner.save_intents_state(all_states)
        persist.save_scheduler_state(self.state)

    def _drain_gone_request(self, subtask_id: str, request_id: str, reason: str) -> None:
        # WQ 里还有这条 → 踢掉
        with self._lock:
            kept = []
            for it in self.state.wq:
                if it.get("subtask_id") == subtask_id and it.get("request_id") == request_id:
                    persist.append_decision({"ts": runlog.now_iso(), "act": "intent_gone_drain",
                                             "subtask_id": subtask_id, "request_id": request_id, "reason": reason})
                    continue
                kept.append(it)
            if len(kept) != len(self.state.wq):
                self.state.wq = kept
                persist.save_scheduler_state(self.state)
        # running 态 → 发 cancel
        for rid, re in list(self.running.items()):
            if re.run.get("subtask_id") == subtask_id and re.run.get("request_id") == request_id:
                re.spec.cancel_reason = "意图文件消失（子任务结束）"
                re.spec.cancel.set()

    def _enqueue_intent(self, subtask_id: str, request_id: str, trigger: str = "auto",
                        priority_override: int | None = None, attempt: int = 1,
                        resume_ref: bool = False, not_before: str = "",
                        save: bool = True) -> dict:
        e = self.entries.get(subtask_id)
        if e is None or not e.ok or e.doc is None:
            raise KeyError(subtask_id)

        # 读取意图文件取覆盖值
        try:
            intents = _scanner.INTENT_FILENAME_RE  # 防循环 import
            intent_path = e.task_dir / "requests" / (request_id + ".json")
            from . import intent as intent_mod
            doc = intent_mod.load_intent(intent_path, e.task_dir)
            priority = priority_override if priority_override is not None else (
                doc.priority if doc.priority is not None else e.doc.priority)
            cpu = float(doc.resources.get("cpu") or e.doc.resources.cpu)
            mem = int(doc.resources.get("memory_mb") or e.doc.resources.memory_mb)
        except Exception as exc:
            log.warning("enqueue_intent %s/%s load failed: %s; fallback to manifest", subtask_id, request_id, exc)
            priority = priority_override if priority_override is not None else e.doc.priority
            cpu = float(e.doc.resources.cpu)
            mem = int(e.doc.resources.memory_mb)

        run_id = runlog.new_run_id()
        rec = {
            "run_id": run_id,
            "subtask_id": subtask_id,
            "request_id": request_id,
            "task_dir": str(e.task_dir),
            "priority": priority,
            "trigger": trigger,
            "attempt": attempt,
            "scheduled_at": runlog.now_iso(),
            "queued_at": runlog.now_iso(),
            "enqueued_at": runlog.now_iso(),
            "started_at": "",
            "finished_at": "",
            "wait_ms": 0,
            "duration_ms": 0,
            "pid": None, "pgid": None,
            "exit_code": None,
            "reason": "",
            "status": "waiting",
            "resume_ref": resume_ref,
            "not_before": not_before,
            "resources": {"cpu": cpu, "memory_mb": mem},
            "error": "",
            "rejected_reason": "",
        }
        with self._lock:
            self.state.enqueue(rec)
            if save:
                persist.save_scheduler_state(self.state)
            persist.append_decision({"ts": runlog.now_iso(), "act": "enqueue_intent",
                                     "run_id": run_id, "subtask_id": subtask_id,
                                     "request_id": request_id, "trigger": trigger,
                                     "priority": priority})
        return rec

    # ================= 准出 =================

    def _tick_admit(self) -> None:
        """准出：排序 → admission_scan 虚拟账本 → 拉起进程。"""
        with self._lock:
            items = [it for it in self.state.wq if not self._not_before_pending(it)]
        if not items:
            return

        ordered = admission.order_waiting(
            items, self.settings.starvation_sec, runlog.now_iso(),
            get_duration_sec=self.profiles.estimate_duration_sec,
        )

        def _declared(sid: str) -> tuple[float, int]:
            e = self.entries.get(sid)
            if e is not None and e.doc is not None:
                return float(e.doc.resources.cpu), int(e.doc.resources.memory_mb)
            return 0.0, 0

        def _profile(sid: str):
            e = self.entries.get(sid)
            mcpu = float(e.doc.resources.cpu if e and e.doc else 0)
            mmem = int(e.doc.resources.memory_mb if e and e.doc else 0)
            return self.profiles.get(sid, mcpu, mmem)

        scanned = admission.admission_scan(
            ordered, get_declared=_declared, get_profile=_profile,
            ledger=self.ledger, capacity=self.capacity,
            overshoot=self.settings.resource.overshoot,
            external_mem_margin_mb=self.settings.resource.external_safety_margin_mb,
        )
        # 并发上限：CPU/内存再空闲，也限制最大 running 进程数，防文件描述符/进程耗尽
        budget = max(0, int(self.settings.resource.max_running) - len(self.running))
        for rid in scanned.admitted[:budget]:
            self._admit(rid)

    def _admit(self, run_id: str) -> None:
        with self._lock:
            it = self.state.pop_run(run_id)
        if it is None:
            return
        if self._not_before_pending(it):
            with self._lock:
                self.state.enqueue(it)
            return

        stid = it["subtask_id"]
        rid = it["request_id"]
        e = self.entries.get(stid)
        if e is None or not e.ok or e.doc is None:
            return

        declared_cpu = float((it.get("resources") or {}).get("cpu") or e.doc.resources.cpu)
        declared_mem = int((it.get("resources") or {}).get("memory_mb") or e.doc.resources.memory_mb)
        prof = self.profiles.get(stid, float(e.doc.resources.cpu), int(e.doc.resources.memory_mb))
        # 准入预估：有画像用画像（真实消耗），无画像才用声明兜底；声明不再作硬下界
        est_cpu, est_mem = admission.estimate_job(prof, declared_cpu, declared_mem,
                                                   self.settings.resource.overshoot, self.capacity)
        if est_mem <= 0:
            est_mem = declared_mem
        if est_cpu <= 0:
            est_cpu = declared_cpu or 0.1

        self.ledger.reserve(run_id, est_cpu, est_mem)
        # cgroup 硬限用声明值做保护下限：画像再低也不把任务限太死（防正常抖动被节流/OOM）
        limit_cpu = max(est_cpu, declared_cpu)
        limit_mem = max(est_mem, declared_mem)

        # 意图文件覆盖 manifest 里的 entry
        entry_file = e.doc.entry.file
        interpreter = e.doc.entry.interpreter
        args = list(e.doc.entry.args)
        cwd = e.doc.entry.cwd

        from . import intent as intent_mod
        try:
            intent_path = e.task_dir / "requests" / (rid + ".json")
            d = intent_mod.load_intent(intent_path, e.task_dir)
            if d.entry.file:
                entry_file = d.entry.file
            if d.entry.interpreter is not None:
                interpreter = d.entry.interpreter
            args += list(d.entry.args)
            if d.entry.cwd and d.entry.cwd != ".":
                cwd = d.entry.cwd
        except Exception:
            pass

        # config 路径（相对任务目录）
        config_rel = ""
        try:
            intent_path = e.task_dir / "requests" / (rid + ".json")
            d = intent_mod.load_intent(intent_path, e.task_dir)
            config_rel = d.config or ""
        except Exception:
            pass
        config_abs = str((e.task_dir / config_rel).resolve()) if config_rel else ""

        run_user = (e.doc.runtime.user or "") if e.doc.runtime else ""
        run_user = run_user or (self.settings.run_user or "")

        cg_name = f"{stid}/{rid}"
        mem_high = int(limit_mem * 1024 * 1024 * self.settings.resource.memory_high_ratio)
        mem_max = int(limit_mem * 1024 * 1024 * self.settings.resource.memory_max_ratio)

        spec = runner.RunSpec(
            subtask_id=stid, request_id=rid, run_id=run_id,
            task_dir=e.task_dir,
            entry_file=entry_file, interpreter=interpreter, args=args, cwd=cwd,
            env=dict(e.doc.env if hasattr(e.doc, "env") else {}),
            trigger=it.get("trigger", "auto"), attempt=int(it.get("attempt") or 1),
            scheduled_at=it.get("scheduled_at", runlog.now_iso()),
            config=config_rel or None,
            run_user=run_user,
            cgroup_name=cg_name,
            memory_high=mem_high, memory_max=mem_max, cpu_quota=limit_cpu,
            cancel_grace_sec=self.settings.runner.kill_grace_sec,
            preempt_grace_sec=self.settings.runner.grace_sec,
            error_rules={"resource_regex": e.doc.error_rules.resource_regex,
                         "logic_regex": e.doc.error_rules.logic_regex},
            max_log_bytes=int(self.settings.max_log_mb) * 1024 * 1024,
        )

        it["status"] = "running"
        it["started_at"] = runlog.now_iso()
        it["wait_ms"] = int(runlog._iso_seconds_between(it["queued_at"], it["started_at"]) * 1000)
        now_mono = time.monotonic()
        re = RunningIntent(run=it, spec=spec, est_cpu=est_cpu, est_mem_mb=est_mem,
                           cgroup_name=cg_name, started_monotonic=now_mono)
        re.exempt_until_monotonic = now_mono + self.preempt_exempt_sec

        re.thread = threading.Thread(target=self._run_worker, args=(re,),
                                     name=f"run-{run_id}", daemon=True)
        with self._lock:
            self.running[run_id] = re
        re.thread.start()
        persist.save_active(self._active_snapshot())
        # 标记意图 running
        _scanner.mark_running(stid, rid, run_id)
        persist.append_decision({"ts": runlog.now_iso(), "act": "admit_intent",
                                 "run_id": run_id, "subtask_id": stid, "request_id": rid,
                                 "est_cpu": est_cpu, "est_mem_mb": est_mem})

    def _run_worker(self, re: RunningIntent) -> None:
        outcome = runner.execute(re.spec)
        re.run.update({
            "exit_code": outcome.exit_code, "pid": outcome.pid, "pgid": outcome.pgid,
            "started_at": outcome.started_at, "finished_at": outcome.finished_at,
            "duration_ms": outcome.duration_ms, "error": outcome.reason,
        })
        self._finalize(re, outcome)

    # ================= 抢占 =================

    def _tick_preempt(self) -> None:
        with self._lock:
            items = list(self.state.wq)
        if not items:
            return
        now_mono = time.monotonic()

        def _fixed_blocked(it: dict) -> bool:
            cpu = float((it.get("resources") or {}).get("cpu") or 0)
            mem = int((it.get("resources") or {}).get("memory_mb") or 0)
            prof = self.profiles.get(it["subtask_id"], 0.0, 0)
            e = self.entries.get(it["subtask_id"])
            mcpu = float(e.doc.resources.cpu if e and e.doc else 0)
            mmem = int(e.doc.resources.memory_mb if e and e.doc else 0)
            ecpu, emem = admission.estimate_job(prof, cpu or mcpu, mem or mmem,
                                                  self.settings.resource.overshoot, self.capacity)
            ok, _ = admission.can_admit(ecpu, emem, self.ledger, self.capacity,
                                         external_mem_margin_mb=self.settings.resource.external_safety_margin_mb)
            return not ok

        blocked = [it for it in items if _fixed_blocked(it)]
        if not blocked:
            return
        blocked.sort(key=lambda it: -int(it.get("priority") or 0))

        running_infos = [
            preempt.make_running_info(re.run, re.est_cpu, re.est_mem_mb,
                                       now_monotonic=now_mono,
                                       preempt_exempt_sec=int(self.settings.preempt_exempt_sec))
            for re in list(self.running.values())
        ]
        chosen: set[str] = set()
        freed_cpu, freed_mem = 0.0, 0

        for top in blocked:
            cpu = float((top.get("resources") or {}).get("cpu") or 0)
            mem = int((top.get("resources") or {}).get("memory_mb") or 0)
            e = self.entries.get(top["subtask_id"])
            mcpu = float(e.doc.resources.cpu if e and e.doc else 0)
            mmem = int(e.doc.resources.memory_mb if e and e.doc else 0)
            prof = self.profiles.get(top["subtask_id"], mcpu, mmem)
            ecpu, emem = admission.estimate_job(prof, cpu or mcpu, mem or mmem,
                                                self.settings.resource.overshoot, self.capacity)
            free_cpu = self.capacity.total_cpu - self.ledger.reserved_cpu() + freed_cpu
            free_mem = self.capacity.total_memory_mb - self.ledger.reserved_mem_mb() + freed_mem
            gap_cpu = max(0.0, ecpu - free_cpu)
            gap_mem = max(0, emem - free_mem)
            if gap_cpu <= 0 and gap_mem <= 0:
                continue
            targets = preempt.select_preempt_targets(
                running_infos, gap_cpu, gap_mem,
                waiter_priority=int(top.get("priority") or 0),
                now_monotonic=now_mono)
            for t in targets:
                if t.run_id in chosen:
                    continue
                re = self.running.get(t.run_id)
                if re is None:
                    continue
                if re.exempt_until_monotonic > now_mono:
                    continue
                re.spec.preempt.set()
                chosen.add(t.run_id)
                freed_cpu += t.est_cpu
                freed_mem += t.est_mem_mb
                persist.append_decision({"ts": runlog.now_iso(), "act": "preempt",
                                         "run_id": t.run_id, "subtask_id": t.subtask_id,
                                         "request_id": t.request_id,
                                         "for_subtask": top["subtask_id"],
                                         "for_request": top["request_id"]})

    # ================= 监测 =================

    def _tick_monitor(self) -> None:
        now = datetime.now().astimezone()
        obs = []
        with self._lock:
            running = list(self.running.values())
        for re in running:
            st, age = self._read_task_state(Path(re.run["task_dir"]), re.run.get("run_id", ""))
            if age >= 1e8:
                age = max(0.0, time.monotonic() - re.started_monotonic)
            self._sample_live_cpu(re)
            actual = self._cgroup_live_mem(re.cgroup_name)
            if actual is not None:
                re.live_mem_mb = actual
            if actual is not None or re.live_cpu_cores > 0:
                self.ledger.set_actual(re.run["run_id"], re.live_cpu_cores, actual or 0)
            self._maybe_record_live_profile(re)
            e = self.entries.get(re.run["subtask_id"])
            hb_timeout = None
            if e is not None and e.doc is not None:
                hb_timeout = e.doc.heartbeat.timeout_sec
            obs.append(monitor.RunningObs(
                run_id=re.run["run_id"], subtask_id=re.run["subtask_id"],
                priority=re.run.get("priority", 0),
                est_mem_mb=re.est_mem_mb, actual_mem_mb=actual,
                heartbeat_age_sec=age, heartbeat_timeout_sec=hb_timeout,
            ))
        for act in self.monitor.tick(obs):
            re = self.running.get(act.run_id)
            if re is None:
                continue
            if act.kind == "heartbeat_kill":
                re.spec.cancel_reason = act.reason
                re.spec.cancel.set()
                persist.append_decision({"ts": runlog.now_iso(), "act": "heartbeat_kill",
                                         "run_id": act.run_id, "reason": act.reason})
            elif act.kind == "overload_preempt":
                re.spec.preempt.set()
                persist.append_decision({"ts": runlog.now_iso(), "act": "overload_preempt",
                                         "run_id": act.run_id, "reason": act.reason})

    def _cgroup_live_mem(self, name: str) -> int | None:
        c = cgroup.Cgroup(name)
        cur = c.read_current_mem()
        return (cur // (1024 * 1024)) if cur is not None else None

    def _cgroup_cpu_usec(self, name: str) -> int | None:
        return cgroup.Cgroup(name).read_cpu_usage_usec()

    def _sample_live_cpu(self, re: RunningIntent) -> None:
        usec = self._cgroup_cpu_usec(re.cgroup_name)
        now_mono = time.monotonic()
        if usec is None:
            return
        if re.prev_cpu_monotonic > 0 and usec >= re.prev_cpu_usec:
            dt = max(1e-6, now_mono - re.prev_cpu_monotonic)
            inst = (usec - re.prev_cpu_usec) / 1_000_000.0 / dt
            re.live_cpu_cores = inst if re.live_cpu_cores <= 0 else 0.7 * re.live_cpu_cores + 0.3 * inst
        re.prev_cpu_usec, re.prev_cpu_monotonic = usec, now_mono

    def _maybe_record_live_profile(self, re: RunningIntent) -> None:
        now_mono = time.monotonic()
        if now_mono - re.last_profile_rec_monotonic < self.profile_live_interval_sec:
            return
        if re.live_mem_mb <= 0 and re.live_cpu_cores <= 0:
            return
        re.last_profile_rec_monotonic = now_mono
        stid = re.run["subtask_id"]
        e = self.entries.get(stid)
        mcpu = float(e.doc.resources.cpu if e and e.doc else 0)
        mmem = int(e.doc.resources.memory_mb if e and e.doc else 0)
        # mem 读不到（cgroup memory 降级）时用声明值兜底，避免 0 污染画像
        mem_sample = re.live_mem_mb if re.live_mem_mb > 0 else mmem
        self.profiles.record_live(stid, round(re.live_cpu_cores, 3), mem_sample,
                                   mcpu, mmem, window=self.settings.resource.window_samples)
        if now_mono - self._last_profile_save_monotonic >= self.profile_live_interval_sec:
            self._last_profile_save_monotonic = now_mono
            self.profiles.save()

    def _read_task_state(self, task_dir: Path, run_id: str = "") -> tuple[dict, float]:
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

    @staticmethod
    def _not_before_pending(it: dict) -> bool:
        nb = it.get("not_before") or ""
        if not nb:
            return False
        try:
            return datetime.now().astimezone() < datetime.fromisoformat(nb)
        except ValueError:
            return False

    # ================= 终结收敛 =================

    def _finalize(self, re: RunningIntent, outcome: runner.RunOutcome) -> None:
        rec = re.run
        with self._lock:
            self.running.pop(rec["run_id"], None)
        self.ledger.release(rec["run_id"])
        cgroup.Cgroup(re.cgroup_name).remove()

        rec["status"] = outcome.status
        rec["reason"] = outcome.reason
        if not rec.get("finished_at"):
            rec["finished_at"] = outcome.finished_at
        rec.setdefault("state_snapshot", {})

        # 画像更新：时长只要有真实运行就记（成功/失败/抢占都算），让 SJF 有数据可用
        duration_sec = max(1, int((rec.get("duration_ms") or 0) / 1000))
        e = self.entries.get(rec.get("subtask_id"))
        mcpu = float(e.doc.resources.cpu if e and e.doc else 0)
        mmem = int(e.doc.resources.memory_mb if e and e.doc else 0)
        if outcome.memory_peak_mb > 0 or outcome.cpu_usage_usec > 0:
            # 有真实资源用量才记资源样本（避免 0 污染画像）
            avg_cpu = (round(outcome.cpu_usage_usec / 1_000_000 / duration_sec, 3)
                       if outcome.cpu_usage_usec > 0 else mcpu)
            self.profiles.record(rec["subtask_id"], avg_cpu,
                                 outcome.memory_peak_mb if outcome.memory_peak_mb > 0 else mmem,
                                 mcpu, mmem, window=self.settings.resource.window_samples,
                                 duration_sec=duration_sec)
        else:
            # 无资源样本（如进程很快失败/被 SIGKILL），仍记时长支撑 SJF
            self.profiles.record_duration(rec["subtask_id"], duration_sec,
                                          window=self.settings.resource.window_samples)
        self.profiles.save()

        # run 记录落盘
        runlog.append_run(rec)
        persist.save_active(self._active_snapshot())

        # 后继调度：读控制 + 决策
        self._after_finalize(rec, outcome)

    def _after_finalize(self, rec: dict, outcome: runner.RunOutcome) -> None:
        stid = rec.get("subtask_id")
        rid = rec.get("request_id")
        e = self.entries.get(stid)

        # 先读运行时 control 段
        state_snapshot = {}
        try:
            from . import control as ctrl_mod
            d = ctrl_mod.read_state_file(Path(rec["task_dir"]), rec.get("run_id") or "")
            directive = ctrl_mod.read_control_directive(d)
            state_snapshot = d
        except Exception:
            directive = control.ControlDirective(action="none")

        # 取意图的 lifetime.retry 配置
        lifetime_retry: dict | None = None
        try:
            from . import intent as intent_mod
            intent_path = Path(rec["task_dir"]) / "requests" / (rid + ".json")
            if intent_path.exists():
                d = intent_mod.load_intent(intent_path, Path(rec["task_dir"]))
                lifetime_retry = d.lifetime.retry
        except Exception:
            pass

        decision = control.decide_from_exit(
            outcome.exit_code, directive, lifetime_retry,
            now=datetime.now().astimezone(),
        )

        # 画像里保留 state_snapshot（retry_hint 等未来扩展用）
        rec["state_snapshot"] = state_snapshot

        persist.append_decision({
            "ts": runlog.now_iso(), "act": "exit_decision",
            "run_id": rec.get("run_id"), "subtask_id": stid, "request_id": rid,
            "exit_code": outcome.exit_code, "status": outcome.status,
            "decision_action": decision.action, "decision_terminal": decision.terminal,
            "directive_action": directive.action,
        })

        if decision.terminal:
            # 意图终结（done/archived）
            _scanner.mark_terminal(stid, rid, "done",
                                   reason=f"{outcome.status}: {outcome.reason}")
            return

        # 非终结 → 续跑/重试
        resume_ref = decision.action == "resume"
        nb = decision.not_before

        # second_pass：特殊处理（可选二刷，这里简化为重入队）
        if decision.action == "second_pass":
            # 若 intent 文件存在（子任务没删）就继续
            from . import intent as intent_mod
            intent_path = Path(rec["task_dir"]) / "requests" / (rid + ".json")
            if not intent_path.exists():
                _scanner.mark_terminal(stid, rid, "done", reason="意图文件消失（second_pass 跳过）")
                return
            self._enqueue_intent(stid, rid, trigger="second_pass",
                                 resume_ref=resume_ref, not_before=nb)
            _scanner.mark_retry_same(stid, rid)
            return

        if decision.action in ("resume", "retry", "skip"):
            from . import intent as intent_mod
            intent_path = Path(rec["task_dir"]) / "requests" / (rid + ".json")
            if not intent_path.exists():
                # 意图文件消失 = 子任务不想跑了
                _scanner.mark_terminal(stid, rid, "done", reason="意图文件消失")
                return
            self._enqueue_intent(stid, rid, trigger=("resume" if resume_ref else "retry"),
                                 resume_ref=resume_ref, not_before=nb)
            _scanner.mark_retry_same(stid, rid)
            return

        # 兜底终结
        _scanner.mark_terminal(stid, rid, "done", reason=f"未覆盖的决策 action={decision.action}")

    # ================= 查询 / 辅助 =================

    def _active_snapshot(self) -> list[dict]:
        return [
            {"run_id": re.run.get("run_id"),
             "subtask_id": re.run.get("subtask_id"),
             "request_id": re.run.get("request_id"),
             "task_dir": re.run.get("task_dir"),
             "pid": re.run.get("pid"), "pgid": re.run.get("pgid"),
             "priority": re.run.get("priority"),
             "status": re.run.get("status"),
             "started_at": re.run.get("started_at"),
             "queued_at": re.run.get("queued_at")}
            for re in self.running.values()
        ]

    def list_subtasks(self) -> list[dict]:
        out = []
        for e in sorted(self.entries.values(), key=lambda x: x.subtask_id):
            doc = e.doc
            out.append({
                "subtask_id": e.subtask_id,
                "name": doc.display_name() if doc else e.subtask_id,
                "ok": e.ok, "error": e.error, "enabled": e.enabled,
                "run_mode": (doc.run_mode if doc else "auto"),
                "priority": (doc.priority if doc else 0),
                "resources": (doc.resources.model_dump() if doc else {"cpu": 0, "memory_mb": 0}),
                "estimate_duration_sec": self.profiles.estimate_duration_sec(e.subtask_id),
            })
        return out

    def status(self) -> dict:
        with self._lock:
            return {
                "subtasks": len(self.entries),
                "waiting": len(self.state.wq),
                "running": len(self.running),
                "ledger_reserved_mem_mb": self.ledger.reserved_mem_mb(),
                "ledger_reserved_cpu": self.ledger.reserved_cpu(),
            }

    def stop_run(self, run_id: str, reason: str = "管理员手动停止") -> dict:
        with self._lock:
            re = self.running.get(run_id)
        if re is None:
            raise KeyError(run_id)
        re.spec.cancel_reason = reason
        re.spec.cancel.set()
        return {"run_id": run_id, "subtask_id": re.run.get("subtask_id"),
                "request_id": re.run.get("request_id")}

    def delete_subtask(self, name: str, delete_dir: bool = False) -> dict:
        """删除子任务：停所有运行中进程 + 清调度状态 + 可选删除本地任务目录。

        delete_dir=False（默认）只移除调度器内的记录（注册表/队列/意图状态/运行历史/日志），
        保留 tasks/<name>/ 本地目录与意图文件；delete_dir=True 连本地目录一起删。
        """
        # 1) 停该任务所有 running 进程
        with self._lock:
            run_ids = [rid for rid, re in self.running.items()
                       if re.run.get("subtask_id") == name]
        killed = []
        for rid in run_ids:
            try:
                self.stop_run(rid, reason=f"子任务 {name} 被删除")
                killed.append(rid)
            except KeyError:
                pass

        # 2) 删注册表条目
        registry.drop_state(name)

        # 3) 清 WQ 中该任务项
        st = persist.load_scheduler_state()
        before = len(st.wq)
        st.wq = [r for r in st.wq if r.get("subtask_id") != name]
        removed_wq = before - len(st.wq)
        persist.save_scheduler_state(st)

        # 4) 清意图消费状态
        intents = scanner.load_intents_state()
        intents.pop(name, None)
        scanner.save_intents_state(intents)

        # 5) 清运行历史 jsonl + 日志目录
        import shutil
        runs_dir = config.runs_dir()
        hist = runs_dir / f"{name}.jsonl"
        logs_dir = runs_dir / name
        removed_files = 0
        if hist.exists():
            hist.unlink()
            removed_files += 1
        if logs_dir.is_dir():
            removed_files += sum(1 for f in logs_dir.rglob("*") if f.is_file())
            shutil.rmtree(logs_dir, ignore_errors=True)

        # 6) 可选删除本地任务目录
        deleted_dir = False
        if delete_dir:
            task_dir = config.tasks_dir() / name
            if task_dir.is_dir():
                shutil.rmtree(task_dir, ignore_errors=True)
                deleted_dir = True

        self.refresh()
        return {"ok": True, "subtask_id": name, "killed_running": killed,
                "removed_wq": removed_wq, "removed_files": removed_files,
                "deleted_dir": deleted_dir}

    def delete_run(self, run_id: str, reason: str = "进程删除", delete_files: bool = False) -> dict:
        """删除单个进程：停（若运行中）+ 标 done；delete_files=true 才删日志/缓存。

        无论 delete_files 与否，都从界面移除（intent 标 done），不影响任务与其他进程。
        """
        with self._lock:
            re = self.running.get(run_id)
        subtask_id = request_id = None
        if re is not None:
            subtask_id = re.run.get("subtask_id")
            request_id = re.run.get("request_id")
            re.spec.cancel_reason = reason
            re.spec.cancel.set()
        else:
            rec = runlog.find_run(run_id)
            if rec:
                subtask_id = rec.get("subtask_id")
                request_id = rec.get("request_id")

        # 可选删除日志文件 + 缓存目录（只按 run_id 定位，不动意图文件和任务目录）
        if delete_files and subtask_id:
            import shutil
            log_dir = config.runs_dir() / subtask_id
            for suffix in (".stdout.log", ".stderr.log"):
                f = log_dir / f"{run_id}{suffix}"
                if f.exists():
                    f.unlink()
            cache_run = config.tasks_dir() / subtask_id / "cache" / "runs" / run_id
            if cache_run.is_dir():
                shutil.rmtree(cache_run, ignore_errors=True)

        # 该 run 对应的 intent 标 done（若仍指向该 run），不影响其他进程
        if subtask_id and request_id:
            intents = scanner.load_intents_state()
            st = intents.get(subtask_id, {}).get(request_id)
            if st and st.get("run_id") == run_id:
                st["status"] = "done"
                st["run_id"] = ""
                st["finished_at"] = runlog.now_iso()
                st["rejected_reason"] = f"进程 {run_id} 被删除"
                scanner.save_intents_state(intents)

        return {"ok": True, "run_id": run_id, "subtask_id": subtask_id,
                "request_id": request_id, "deleted_files": delete_files}