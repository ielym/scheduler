"""调度器主循环（设计文档第十四、十五节）。

启动先取单实例锁、恢复执行数据库与进程身份、独立确认所有生产者常驻。
每轮：处理退出/心跳/进展/不足/检查点/控制确认 → 提交状态 → 确认进程树退出；
持续接收 outbox 通知与增量扫描 → 去重索引 → 发布回执；
更新资源趋势、健康基线与动态目标，先处理高优先级新需求/明确不足/持续变慢，
推进累计退让与实例取消/暂停超时/保护截止；
选择恢复探针与普通候选，按优先级/同级轮转/任务内顺序准入并握手启动。
"""

from __future__ import annotations

import fcntl
import logging
import os
import threading
import time
from datetime import datetime, timedelta
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
        self.had_shortage = False
        self.shortage_recent = False
        self.grown_recently = False
        self.pause_command: dict | None = None
        self.pause_deadline_mono = 0.0


class SchedulerService:
    def __init__(self, cfg: config.SchedulerConfig | None = None):
        self.cfg = cfg or config.load_config()
        self.db = dbmod.DB()
        self.guard = prodguard.ProducerGuard(self.cfg)
        self.scaling = scaling.Scaling(self.cfg)
        self.health = scaling.HealthTracker(
            slow_confidence_sec=self.cfg.control.slow_confidence_sec)
        self.protect = scaling.Protect(self.cfg)
        self.specs: dict[str, config.TaskSpec] = {}
        self.attempts: dict[str, runner.AttemptRun] = {}
        self.meta: dict[str, AttMeta] = {}
        self._backoff_until: dict[str, float] = {}   # job_id -> 不足探测截止(monotonic)
        self._last_start_mono = 0.0
        self._last_sample_mono = 0.0
        self._sample_cpu = 0.0
        self._sample_mem_mb = 0
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
        # 优雅停机：显式整套服务关闭由外部生命周期管理；此处只撤进程树
        for a in list(self.attempts.values()):
            try:
                a.terminate(self.cfg.control.pause_grace_base_sec)
            except Exception:
                pass
        self.guard.stop_all()
        self.db.close()
        if self._lock_fd >= 0:
            try:
                os.close(self._lock_fd)
            except OSError:
                pass
            self._lock_fd = -1

    def _recover(self) -> None:
        """启动恢复：执行数据库 + 进程身份，不清空状态。"""
        self.refresh_specs()
        for task_id, spec in self.specs.items():
            if spec.enabled:
                self.guard.launch(task_id, spec)
        # 上次在途 attempt → 作业置 RECOVERING，等待新一轮事件/进程核实后接管或恢复
        self.db.recover_inflight(runner.now_iso())

    def refresh_specs(self) -> dict[str, config.TaskSpec]:
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
        self.specs = specs
        return specs

    # ---------- 主循环 ----------
    def _loop(self) -> None:
        last_refresh = time.monotonic()
        while not self._stopping.wait(self.cfg.control.loop_tick_sec):
            try:
                now = _now_mono()
                if now - last_refresh >= 5.0:
                    self.refresh_specs()
                    self._tick_ensure_producers()
                    last_refresh = now
                self._tick_receive(runner.now_iso())
                self._tick_attempts()
                self._tick_resource(now)
                self._tick_control(now)
                self._tick_admit(now)
            except Exception:
                log.exception("scheduler loop iteration failed")

    def _tick_ensure_producers(self) -> None:
        for task_id, spec in self.specs.items():
            if not spec.enabled:
                continue
            self.guard.ensure(task_id, spec)

    def _tick_receive(self, now: str) -> None:
        receiver.receive_all(self.db, now, self.specs)

    # ---------- 执行尝试事件/退出 ----------
    def _tick_attempts(self) -> None:
        now = runner.now_iso()
        for attempt_id in list(self.attempts.keys()):
            a = self.attempts.get(attempt_id)
            meta = self.meta.get(attempt_id)
            if a is None or meta is None:
                continue
            self._ingest_events(a, meta)

            if not a.is_alive():
                self._finalize(a, meta, now)
            elif meta.status == JobStatus.PAUSING and \
                    _now_mono() >= meta.pause_deadline_mono:
                # 暂停宽限超时 → 强撤进程树，靠上次已提交检查点恢复
                rc = a.terminate(self.cfg.control.pause_grace_max_sec)
                log.warning("pause grace exceeded for attempt %s (rc=%s)", attempt_id, rc)
                self._finalize(a, meta, now)

    def _ingest_events(self, a: runner.AttemptRun, meta: AttMeta) -> None:
        for ev in a.read_events(meta.after_seq):
            seq = int(ev.get("seq") or 0)
            meta.after_seq = max(meta.after_seq, seq)
            kind = ev.get("kind")
            if kind == EventKind.STARTED:
                if meta.status == JobStatus.STARTING:
                    self._transition(meta, JobStatus.RUNNING)
            elif kind == EventKind.HEARTBEAT:
                pass
            elif kind == EventKind.PROGRESS:
                self.health.record_progress(
                    meta.task_id, _now_mono(),
                    int(ev.get("completed_units") or 0),
                    bool(ev.get("has_unfinished")),
                    str(ev.get("stage") or ""),
                    ev.get("active_duration_sec"),
                    ev.get("in_flight_duration_sec"),
                    ev.get("work_units"),
                )
                self.db.append_event(meta.attempt_id, seq, kind, ev, runner.now_iso())
            elif kind == EventKind.SHORTAGE:
                meta.had_shortage = True
                meta.shortage_recent = True
                self.db.append_event(meta.attempt_id, seq, kind, ev, runner.now_iso())
                self._on_explicit_shortage(meta)
            elif kind == EventKind.CONTROL_ACK:
                self.db.append_event(meta.attempt_id, seq, kind, ev, runner.now_iso())
            elif kind == EventKind.CHECKPOINT:
                self.db.record_checkpoint(meta.attempt_id, int(ev.get("commit_seq") or 0),
                                          str(ev.get("input_ref") or ""), runner.now_iso())
                self.db.append_event(meta.attempt_id, seq, kind, ev, runner.now_iso())
            elif kind == EventKind.COMPLETED:
                meta.had_completed = True
                self.db.append_event(meta.attempt_id, seq, kind, ev, runner.now_iso())
            else:
                self.db.append_event(meta.attempt_id, seq, kind, ev, runner.now_iso())

    def _transition(self, meta: AttMeta, status: str) -> None:
        meta.status = status
        self.db.update_attempt(meta.attempt_id, status=status)
        self.db.set_job_status(meta.job_id, status, ts=runner.now_iso())

    def _on_explicit_shortage(self, meta: AttMeta) -> None:
        # 高优先级明确不足：把该任务设为保护对象，立即启动低优先级退让
        self.protect.start_round(meta.task_id, meta.priority, _now_mono())

    def _finalize(self, a: runner.AttemptRun, meta: AttMeta, now: str) -> None:
        # 确认整棵进程树退出
        a.proc.wait(timeout=1)
        exit_code = a.proc.returncode
        status_ctx = JobStatus.PAUSING if meta.status == JobStatus.PAUSING else \
            (JobStatus.CANCELLED if meta.status == JobStatus.CANCELLED else "")
        outcome = runner.decide_outcome(status_ctx, exit_code, meta.had_completed,
                                        meta.had_shortage)
        self.db.update_attempt(
            meta.attempt_id, status=outcome.status, exit_code=exit_code,
            reason=outcome.reason, finished_at=now,
            pid=a.pid, pgid=a.pgid,
        )
        self.db.set_job_status(meta.job_id, outcome.status, outcome.reason, now)
        a.cleanup()

        self.attempts.pop(meta.attempt_id, None)
        self.meta.pop(meta.attempt_id, None)

        stid = meta.task_id
        if outcome.status == JobStatus.SUCCEEDED:
            self.scaling.on_success(stid, _now_mono())
        elif outcome.status == JobStatus.BACKOFF:
            self.scaling.on_shortage(stid, _now_mono(),
                                     self._live_count(stid))
            delay = self.cfg.control.shortage_retry_base_sec
            self._backoff_until[meta.job_id] = _now_mono() + delay
        elif outcome.status == JobStatus.PAUSED:
            # 主动退让不计为任务错误
            pass
        elif outcome.status == JobStatus.FAILED:
            self.scaling.on_shortage(stid, _now_mono(), self._live_count(stid))

        self._append_status({
            "ts": now, "kind": "attempt_finalized", "attempt_id": meta.attempt_id,
            "job_id": meta.job_id, "task_id": stid, "exit_code": exit_code,
            "status": outcome.status, "reason": outcome.reason,
        })

    def _append_status(self, obj: dict) -> None:
        try:
            store.append_jsonl(config.status_log(), obj)
        except Exception:
            pass

    # ---------- 资源观测（只观测执行进程） ----------
    def _tick_resource(self, now: float) -> None:
        if now - self._last_sample_mono < self.cfg.control.sample_interval_sec:
            return
        self._last_sample_mono = now
        cpu = mem = 0.0
        for a in self.attempts.values():
            usec = a.cgrp.read_cpu_usage_usec()
            if usec:
                cpu += usec
            m = a.cgrp.read_current_mem()
            if m:
                mem += m
        self._sample_cpu = cpu
        self._sample_mem_mb = mem

    def _resource_healthy(self) -> bool:
        total_cpu = _read_cpu_count()
        total_mem = _read_total_mem_mb()
        cpu_ratio = self._sample_cpu / 1_000_000.0 / max(1, total_cpu)
        mem_ratio = self._sample_mem_mb / max(1, total_mem)
        return cpu_ratio < 0.9 and mem_ratio < 0.9

    # ---------- 控制：动态目标 / 保护 / 退让 ----------
    def _tick_control(self, now: float) -> None:
        # 1) 保护截止 / 恢复
        if self.protect.st.protected_task and self.protect.deadline_passed(now):
            self.protect.end_round(now)

        # 2) 动态目标：健康任务平缓扩容
        for task_id in self.specs:
            live = self._live_count(task_id)
            has_progress = self.health._samples.get(task_id) is not None
            if self.scaling.may_grow(task_id, now, live, has_progress,
                                     self._resource_healthy()):
                self.scaling.apply_grow(task_id, now)

        # 3) 无报错变慢 → 温和退让（置信度低于明确不足）
        if not self.protect.st.protected_task:
            for meta in list(self.meta.values()):
                if self.health.slow(meta.task_id, now):
                    self.protect.start_round(meta.task_id, meta.priority, now)
                    break

        # 4) 保护进行中：选择退让候选
        if self.protect.st.protected_task and not self.protect.in_backoff(now):
            running = self._running_infos()
            cand = self.protect.choose_yield_candidate(running, now)
            if cand:
                self._request_pause(cand, reason="高优先级保护退让")
            else:
                # 无更低优先级可退让 → 本轮保护无改善，结束并节流恢复
                if self.protect.st.yielded:
                    self.protect.end_round(now)

    def _running_infos(self) -> list[dict]:
        out = []
        for attempt_id, meta in self.meta.items():
            if meta.status not in (JobStatus.RUNNING, JobStatus.STARTING):
                continue
            out.append({
                "attempt_id": attempt_id,
                "task_id": meta.task_id,
                "priority": meta.priority,
                "shortage_recent": meta.shortage_recent,
                "grown_recently": meta.grown_recently,
            })
        return out

    def _request_pause(self, attempt_id: str, reason: str) -> None:
        a = self.attempts.get(attempt_id)
        meta = self.meta.get(attempt_id)
        if a is None or meta is None:
            return
        if meta.status != JobStatus.RUNNING:
            return
        cmd_id = runner.new_id("pause")
        deadline = (datetime.now().astimezone() +
                    timedelta(seconds=self.cfg.control.pause_grace_base_sec)).isoformat()
        a.write_pause(cmd_id, reason, deadline)
        meta.pause_command = a.pause_command
        meta.pause_deadline_mono = _now_mono() + self.cfg.control.pause_grace_base_sec
        self._transition(meta, JobStatus.PAUSING)
        a.signal_pause()
        self.protect.note_yielded(attempt_id)
        self._append_status({"ts": runner.now_iso(), "kind": "pause_requested",
                             "attempt_id": attempt_id, "job_id": meta.job_id,
                             "task_id": meta.task_id, "reason": reason})

    def _live_count(self, task_id: str) -> int:
        return sum(1 for m in self.meta.values()
                   if m.task_id == task_id and
                   m.status in (JobStatus.STARTING, JobStatus.RUNNING, JobStatus.PAUSING))

    # ---------- 准入 ----------
    def _tick_admit(self, now: float) -> None:
        if now - self._last_start_mono < self.cfg.control.start_interval_sec:
            return
        candidates = self._candidates()
        started = 0
        for job in candidates:
            if started >= 1:
                break  # 每个启动间隔只启动一个，防突发
            if self._admit_job(job):
                started += 1
                self._last_start_mono = now

    def _candidates(self) -> list[dict]:
        jobs = self.db.queued_jobs()
        out = []
        for j in jobs:
            task_id = j["task_id"]
            spec = self.specs.get(task_id)
            if spec is None or not spec.enabled:
                continue
            if self.db.live_jobs_for_job(j["job_id"]):
                continue
            if _now_mono() < self._backoff_until.get(j["job_id"], 0.0):
                continue
            out.append(j)
        return out

    def _admit_job(self, job: dict) -> bool:
        task_id = job["task_id"]
        live = self._live_count(task_id)
        scale = self.scaling.get(task_id)
        # 冷启动给 1，否则不超过动态目标
        if scale.run_target == 0 and live == 0:
            scale.run_target = 1
            scale.grew_at(task_id, _now_mono())
        if live >= max(1, scale.run_target):
            return False
        spec = self.specs[task_id]
        return self._launch(job, spec)

    def _launch(self, job: dict, spec: config.TaskSpec) -> bool:
        attempt_id = runner.new_id("att")
        token = runner.new_id("tok")
        seq = self.db.next_attempt_seq(job["job_id"])
        now = runner.now_iso()
        # 先持久化 attempt 意图（STARTING），再握手启动
        self.db.create_attempt(attempt_id, job["job_id"], seq, token, now,
                               cgroup=f"{job['task_id']}/{attempt_id}")
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
            self.db.update_attempt(attempt_id, status=JobStatus.FAILED,
                                   reason=f"启动失败：{e}", finished_at=runner.now_iso())
            self.db.set_job_status(job["job_id"], JobStatus.FAILED, f"启动失败：{e}",
                                   runner.now_iso())
            return False
        meta = AttMeta(attempt_id, job["job_id"], job["task_id"], int(job["priority"]), token)
        meta.grown_recently = True
        self.attempts[attempt_id] = a
        self.meta[attempt_id] = meta
        self.db.update_attempt(attempt_id, pid=a.pid, pgid=a.pgid,
                               start_monotonic=a.start_time)
        self.db.set_job_status(job["job_id"], JobStatus.STARTING, ts=now)
        self._append_status({"ts": now, "kind": "attempt_launched", "attempt_id": attempt_id,
                             "job_id": job["job_id"], "task_id": job["task_id"],
                             "pid": a.pid})
        return True

    # ---------- 显式取消 / 查询 ----------
    def cancel_job(self, job_id: str, reason: str = "显式取消") -> bool:
        for attempt_id, meta in list(self.meta.items()):
            if meta.job_id == job_id:
                a = self.attempts.get(attempt_id)
                self._transition(meta, JobStatus.CANCELLED)
                if a is not None:
                    a.terminate(self.cfg.control.pause_grace_base_sec)
                return True
        self.db.set_job_status(job_id, JobStatus.CANCELLED, reason, runner.now_iso())
        return True

    def status(self) -> dict:
        return {
            "tasks": len(self.specs),
            "queued": self.db.count_queued(),
            "live_attempts": len(self.attempts),
            "producers": self.guard.list(),
            "exec_cpu_usec": int(self._sample_cpu),
            "exec_mem_mb": int(self._sample_mem_mb),
            "protected_task": self.protect.st.protected_task,
            "yielded": sorted(self.protect.st.yielded),
            "scale": {tid: {"run_target": s.run_target, "mode": s.mode}
                      for tid, s in self.scaling._tasks.items()},
        }