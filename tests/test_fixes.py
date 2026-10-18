"""针对本轮修复点的单元测试（不含真实进程的端到端，见 test_integration.py）。

覆盖：
1. decide_outcome 归因重排序（不足后完成、退出 75 无事件）
2. Protect 优先级防劫持 / 失败轮退避 / 截止延长 / 逐步放回
3. ResourceTrend 滞回
4. 接收器时间片 + 作业级优先级钳制 + 扫描顺序
5. DB 新列与原子事务迁移
6. BACKOFF→QUEUED / PAUSED→QUEUED 闭环 + 重启冷启动重排（内存态）
7. 指数退避 + 抖动 + 封顶
8. 准入保护门禁（保护期间低优先级不新增）
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from sched import config, db as dbmod, receiver, runner, scaling
from sched.config import JobStatus
from sched.scheduler import SchedulerService


# ---------- 1. decide_outcome ----------
class TestDecideOutcome:
    def test_shortage_then_complete_is_success(self):
        # 退出 0 + 完成记录优先（运行期曾报不足事件不改变归因）
        o = runner.decide_outcome("", 0, had_completed=True)
        assert o.status == JobStatus.SUCCEEDED

    def test_exit75_without_event_is_backoff(self):
        o = runner.decide_outcome("", 75, had_completed=False)
        assert o.status == JobStatus.BACKOFF

    def test_exit0_no_completed_requeues(self):
        # 退出 0 但未提交完成记录 → 作业回 QUEUED 重跑（attempt 以 FAILED 收尾）
        o = runner.decide_outcome("", 0, had_completed=False)
        assert o.status == JobStatus.QUEUED
        assert o.attempt_status == JobStatus.FAILED

    def test_pause_ctx_completed_wins(self):
        # 暂停请求到达前已完成（done）→ 完成证据优先于暂停意图，不误标 PAUSED 重跑
        o = runner.decide_outcome(JobStatus.PAUSING, 0, True)
        assert o.status == JobStatus.SUCCEEDED

    def test_pause_ctx_no_completed_is_paused(self):
        # 未完成时协作暂停 → PAUSED（断点已保存）
        o = runner.decide_outcome(JobStatus.PAUSING, 0, False)
        assert o.status == JobStatus.PAUSED

    def test_cancel_ctx_wins(self):
        # 未完成时显式取消生效
        o = runner.decide_outcome(JobStatus.CANCELLED, -15, False)
        assert o.status == JobStatus.CANCELLED

    def test_cancel_but_completed_wins(self):
        # B1：取消请求到达前完成记录已持久化 → 完成证据优先于取消意图
        o = runner.decide_outcome(JobStatus.CANCELLED, -15, True)
        assert o.status == JobStatus.SUCCEEDED


# ---------- 2. Protect ----------
class TestProtect:
    def _cfg(self):
        return config.SchedulerConfig()

    def test_lower_priority_cannot_hijack(self):
        # 修复前：低优先级 shortage 可直接替换保护对象（N1）
        p = scaling.Protect(self._cfg())
        assert p.start_round("hi", 90, 0.0) is True
        assert p.start_round("lo", 10, 1.0) is False
        assert p.st.protected_task == "hi"

    def test_higher_priority_can_switch(self):
        p = scaling.Protect(self._cfg())
        p.start_round("hi", 90, 0.0)
        assert p.start_round("top", 95, 1.0) is True
        assert p.st.protected_task == "top"

    def test_round_backoff_blocks_same_or_lower_priority(self):
        p = scaling.Protect(self._cfg())
        p.start_round("hi", 90, 0.0)
        p.end_round(10.0)  # 失败轮，进入退避
        now = 20.0  # 退避窗口内
        assert p.in_backoff(now)
        assert p.start_round("hi", 90, now) is False   # 同级不能立即重开
        assert p.start_round("lo", 10, now) is False   # 更低不能
        assert p.start_round("top", 95, now) is True   # 更高可以

    def test_note_yielded_extends_deadline(self):
        p = scaling.Protect(self._cfg())
        p.start_round("hi", 90, 0.0)
        d0 = p.st.deadline_mono
        p.note_yielded("att-1", "lo_task", 10)
        assert p.st.deadline_mono > d0
        assert p.st.yielded_tasks == {"lo_task": 10}

    def test_gradual_release_one_at_a_time(self):
        p = scaling.Protect(self._cfg())
        p.start_round("hi", 90, 0.0)
        p.note_yielded("a1", "lo_a", 20)
        p.note_yielded("a2", "lo_b", 50)
        p.end_round(10.0)
        assert set(p.st.pending_resume) == {"lo_a", "lo_b"}
        first = p.release_ready(10.0)
        assert first == "lo_b"  # 优先级高者先放回
        assert "lo_b" not in p.st.pending_resume
        assert p.release_ready(11.0) is None  # recovery_probe_sec 节奏内不再放回
        second = p.release_ready(10.0 + self._cfg().control.recovery_probe_sec + 1)
        assert second == "lo_a"

    def test_no_release_while_round_active(self):
        p = scaling.Protect(self._cfg())
        p.start_round("hi", 90, 0.0)
        p.note_yielded("a1", "lo_a", 20)
        # 保护轮进行中不放回
        assert p.release_ready(1000.0) is None


# ---------- 3. ResourceTrend ----------
class TestResourceTrend:
    def test_hysteresis_requires_consecutive_samples(self):
        t = scaling.ResourceTrend(0.9, 0.8, 3)
        t.sample(0.95, 0.1, True)
        t.sample(0.95, 0.1, True)
        assert not t.unhealthy()          # 2 个还不够
        t.sample(0.95, 0.1, True)
        assert t.unhealthy()              # 连续 3 个 → 不健康
        t.sample(0.5, 0.1, True)
        assert t.unhealthy()              # 单次回落不恢复（滞回）
        t.sample(0.5, 0.1, True)
        t.sample(0.5, 0.1, True)
        assert not t.unhealthy()          # 连续 3 个低 → 恢复

    def test_middle_band_holds_state(self):
        t = scaling.ResourceTrend(0.9, 0.8, 3)
        t.sample(0.95, 0.1, True)
        t.sample(0.95, 0.1, True)
        t.sample(0.95, 0.1, True)
        assert t.unhealthy()
        t.sample(0.85, 0.1, True)         # 中间带 0.8~0.9
        assert t.unhealthy()              # 维持原判定

    def test_oom_marks_unhealthy_immediately(self):
        t = scaling.ResourceTrend(0.9, 0.8, 3)
        t.sample(0.1, 0.1, True)
        t.mark_oom()
        assert t.unhealthy()
        t.sample(0.1, 0.1, True)
        t.sample(0.1, 0.1, True)
        assert t.unhealthy()  # OOM 后需重新累计低样本
        t.sample(0.1, 0.1, True)
        assert not t.unhealthy()

    def test_not_observed_flag(self):
        t = scaling.ResourceTrend(0.9, 0.8, 3)
        t.sample(0.1, 0.1, observed=False)
        assert t.observed is False


# ---------- 4. 接收器 ----------
class TestReceiver:
    def _mk_task(self, home, task_id="t1", priority=50):
        td = home / "tasks" / task_id
        config.outbox_dir(task_id).mkdir(parents=True, exist_ok=True)
        spec = config.TaskSpec(
            schema_version=1, task_id=task_id, priority=priority,
            producer_entry=config.EntrySpec(file="producer.py"),
            executor_entry=config.EntrySpec(file="run.py"))
        return td, spec

    def _put(self, td, job_id, pri=None, task_id="t1"):
        obj = {"schema_version": 1, "job_id": job_id, "task_id": task_id,
               # 幂等键必填（审查修复后协议基础）：缺失的文件被接收器拒绝
               "idempotency_key": f"idem-{job_id}"}
        if pri is not None:
            obj["priority"] = pri
        (config.outbox_dir(task_id) / f"{job_id}.json").write_text(json.dumps(obj))

    def test_batch_time_slice(self, home):
        td, spec = self._mk_task(home)
        for i in range(5):
            self._put(td, f"j{i}")
        d = dbmod.DB()
        # B3：新文件优先 + 新旧配额（batch_size=2 → 新 1 + 旧 1；首轮全为新文件）
        s1 = receiver.receive_outbox("t1", spec, d, "now", batch_size=2)
        assert s1["seen"] == 1 and s1["deferred"] == 4
        assert d.count_queued() == 1
        s2 = receiver.receive_outbox("t1", spec, d, "now", batch_size=10)
        assert s2["seen"] == 4 and s2["deferred"] == 0
        assert d.count_queued() == 5

    def test_job_priority_clamped_to_task(self, home):
        # 修复前：无作业级优先级（Q4）；现在可设但不得越过任务优先级
        td, spec = self._mk_task(home, priority=50)
        self._put(td, "j_hi", pri=99)
        self._put(td, "j_lo", pri=10)
        self._put(td, "j_eq", pri=50)
        d = dbmod.DB()
        receiver.receive_outbox("t1", spec, d, "now")
        assert d.get_job("j_hi")["priority"] == 50  # 钳制
        assert d.get_job("j_lo")["priority"] == 10
        assert d.get_job("j_eq")["priority"] == 50

    def test_receive_all_respects_order(self, home):
        for tid, pri in (("t_hi", 90), ("t_lo", 10)):
            td, _ = self._mk_task(home, tid, pri)
            self._put(td, f"{tid}-j", task_id=tid)
        specs = {}
        for tid, pri in (("t_hi", 90), ("t_lo", 10)):
            specs[tid] = config.TaskSpec(
                schema_version=1, task_id=tid, priority=pri,
                producer_entry=config.EntrySpec(file="producer.py"),
                executor_entry=config.EntrySpec(file="run.py"))
        d = dbmod.DB()
        receiver.receive_all(d, "now", specs, order=["t_lo", "t_hi"])
        rows = d.list_jobs()
        # list_jobs 按 receive_seq DESC：t_hi 后接收 → 排最前（order 生效：t_lo 先处理）
        assert rows[0]["task_id"] == "t_hi"
        assert rows[1]["task_id"] == "t_lo"


# ---------- 5. DB 新列 / 迁移 / 原子事务 ----------
class TestDB:
    def test_new_columns_exist(self, home):
        d = dbmod.DB()
        cols = {r[1] for r in d._conn.execute("PRAGMA table_info(jobs)")}
        assert {"retry_count", "next_retry_at"} <= cols
        acols = {r[1] for r in d._conn.execute("PRAGMA table_info(attempts)")}
        assert "pid_start_time" in acols

    def test_migration_from_old_schema(self, home):
        # 构造旧 v3 库（缺 retry_count/next_retry_at/pid_start_time）→ 重开后自动迁移
        import sqlite3
        p = config.db_file()
        p.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(p)
        conn.executescript("""
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL,
            entry_args_json TEXT NOT NULL DEFAULT '[]',
            workdir TEXT NOT NULL DEFAULT '.',
            input_refs_json TEXT NOT NULL DEFAULT '[]',
            idempotency_key TEXT NOT NULL DEFAULT '',
            progress_semver TEXT NOT NULL DEFAULT '1.0.0',
            status TEXT NOT NULL DEFAULT 'QUEUED',
            reason TEXT NOT NULL DEFAULT '',
            queued_at TEXT NOT NULL,
            started_at TEXT NOT NULL DEFAULT '',
            finished_at TEXT NOT NULL DEFAULT '',
            receive_seq INTEGER NOT NULL DEFAULT 0,
            priority INTEGER NOT NULL DEFAULT 0,
            content_hash TEXT NOT NULL DEFAULT '',
            env_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE TABLE attempts (
            attempt_id TEXT PRIMARY KEY,
            job_id TEXT NOT NULL,
            seq INTEGER NOT NULL,
            token TEXT NOT NULL,
            pid INTEGER,
            pgid INTEGER,
            start_monotonic REAL,
            cgroup TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'STARTING',
            started_at TEXT NOT NULL DEFAULT '',
            finished_at TEXT NOT NULL DEFAULT '',
            exit_code INTEGER,
            reason TEXT NOT NULL DEFAULT ''
        );
        """)
        conn.execute("INSERT INTO jobs (job_id, task_id, queued_at) VALUES ('old-1','t1','now')")
        conn.commit()
        conn.close()
        d = dbmod.DB()
        cols = {r[1] for r in d._conn.execute("PRAGMA table_info(jobs)")}
        assert "retry_count" in cols and "next_retry_at" in cols
        acols = {r[1] for r in d._conn.execute("PRAGMA table_info(attempts)")}
        assert "pid_start_time" in acols
        assert d.get_job("old-1")["task_id"] == "t1"  # 旧数据保留

    def test_commit_transition_atomic(self, home):
        d = dbmod.DB()
        d.upsert_job(_ready("j-1", "t1"), "h1", "now", 50)
        d.create_attempt("att-1", "j-1", 1, "tok", "now")
        d.commit_transition("att-1", {"status": JobStatus.SUCCEEDED, "exit_code": 0},
                            "j-1", JobStatus.SUCCEEDED, "完成", "now")
        assert d.get_job("j-1")["status"] == JobStatus.SUCCEEDED
        assert d.get_attempt("att-1")["status"] == JobStatus.SUCCEEDED

    def test_set_job_backoff_roundtrip(self, home):
        d = dbmod.DB()
        d.upsert_job(_ready("j-1", "t1"), "h1", "now", 50)
        d.set_job_backoff("j-1", 3, "2026-01-01T00:00:00+08:00")
        j = d.get_job("j-1")
        assert j["retry_count"] == 3 and j["next_retry_at"] == "2026-01-01T00:00:00+08:00"

    def test_has_event_kind(self, home):
        d = dbmod.DB()
        d.upsert_job(_ready("j-1", "t1"), "h1", "now", 50)
        d.create_attempt("att-1", "j-1", 1, "tok", "now")
        assert not d.has_event_kind("att-1", "completed")
        d.append_event("att-1", 1, "completed", {}, "now")
        assert d.has_event_kind("att-1", "completed")

    def test_status_counts(self, home):
        d = dbmod.DB()
        d.upsert_job(_ready("j-1", "t1"), "h1", "now", 50)
        d.upsert_job(_ready("j-2", "t1"), "h2", "now", 50)
        d.set_job_status("j-2", JobStatus.SUCCEEDED, "done", "now")
        assert d.status_counts() == {"QUEUED": 1, "SUCCEEDED": 1}


def _ready(job_id, task_id):
    return config.ReadyJob(schema_version=1, job_id=job_id, task_id=task_id,
                           idempotency_key=f"idem-{job_id}")


# ---------- 6/7/8. SchedulerService 状态闭环（DB 层，不起进程） ----------
class TestStateClosures:
    def _svc(self, home):
        return SchedulerService()

    def test_backoff_requeue_when_due(self, home):
        svc = self._svc(home)
        d = svc.db
        d.upsert_job(_ready("j-1", "t1"), "h1", "now", 50)
        d.set_job_status("j-1", JobStatus.BACKOFF, "不足", "now")
        # 过去时间 → 立即到期
        d.set_job_backoff("j-1", 1,
                          (datetime.now().astimezone() - timedelta(seconds=1)).isoformat())
        svc._tick_backoff(0.0)
        assert d.get_job("j-1")["status"] == JobStatus.QUEUED

    def test_backoff_not_requeued_before_due(self, home):
        svc = self._svc(home)
        d = svc.db
        d.upsert_job(_ready("j-1", "t1"), "h1", "now", 50)
        d.set_job_status("j-1", JobStatus.BACKOFF, "不足", "now")
        d.set_job_backoff("j-1", 1,
                          (datetime.now().astimezone() + timedelta(hours=1)).isoformat())
        svc._tick_backoff(0.0)
        assert d.get_job("j-1")["status"] == JobStatus.BACKOFF

    def test_paused_requeued_when_no_protection(self, home):
        svc = self._svc(home)
        d = svc.db
        d.upsert_job(_ready("j-1", "t1"), "h1", "now", 50)
        d.set_job_status("j-1", JobStatus.PAUSED, "协作暂停", "now")
        svc._tick_release(0.0)
        assert d.get_job("j-1")["status"] == JobStatus.QUEUED

    def test_paused_pending_resume_holds(self, home):
        # 待逐步放回的任务在放回节奏之外保持 PAUSED
        svc = self._svc(home)
        d = svc.db
        d.upsert_job(_ready("j-1", "lo"), "h1", "now", 10)
        d.set_job_status("j-1", JobStatus.PAUSED, "退让", "now")
        svc.protect.st.pending_resume = {"lo": 10}
        svc.protect.st.next_release_mono = 1000.0  # 节奏未到
        svc._tick_release(0.0)
        assert d.get_job("j-1")["status"] == JobStatus.PAUSED

    def test_reinitialize_requeues_unfinished_and_clears_attempts(self, home):
        # 冷启动：未终结 attempt 与工件被清除，作业一律回 QUEUED（不核实完成、不接管）
        svc = self._svc(home)
        d = svc.db
        d.upsert_job(_ready("j-1", "t1"), "h1", "now", 50)
        d.create_attempt("att-1", "j-1", 1, "tok", "now", cgroup="t1/att-1")
        d.update_attempt("att-1", pid=999999999, pgid=999999999,
                         pid_start_time=12345.0)
        d.append_event("att-1", 1, "done", {"ok": True}, "now")  # 有完成记录也不核实
        d.set_job_status("j-1", JobStatus.RUNNING, "", "now")
        svc._reinitialize()
        assert d.get_job("j-1")["status"] == JobStatus.QUEUED
        assert d.get_attempt("att-1") is None  # 旧 attempt 已删除

    def test_reinitialize_keeps_terminal_jobs(self, home):
        # 终态作业（成功/取消/失败）不重排；非终态一律回 QUEUED
        svc = self._svc(home)
        d = svc.db
        d.upsert_job(_ready("j-done", "t1"), "h", "now", 50)
        d.set_job_status("j-done", JobStatus.SUCCEEDED, "", "now")
        d.upsert_job(_ready("j-run", "t1"), "h2", "now", 50)
        d.set_job_status("j-run", JobStatus.RUNNING, "", "now")
        svc._reinitialize()
        assert d.get_job("j-done")["status"] == JobStatus.SUCCEEDED
        assert d.get_job("j-run")["status"] == JobStatus.QUEUED

    def test_reinitialize_resets_scheduler_state(self, home):
        # 调度学习态冷启动：动态目标清零、task_state 清空
        svc = self._svc(home)
        svc.db.set_task_state("t1", run_target=5, mode="steady")
        svc._reinitialize()
        assert svc.db.all_task_states() == []
        assert svc.scaling.get("t1").run_target == 0

    def test_backoff_retry_schedule_exponential_with_jitter(self, home):
        svc = self._svc(home)
        d = svc.db
        d.upsert_job(_ready("j-1", "t1"), "h1", "now", 50)
        delays = []
        prev_retry = 0
        for _ in range(5):
            t0 = datetime.now().astimezone()
            svc._schedule_backoff_retry("j-1")
            j = d.get_job("j-1")
            assert j["retry_count"] == prev_retry + 1
            prev_retry = j["retry_count"]
            nr = datetime.fromisoformat(j["next_retry_at"])
            delays.append((nr, t0))  # 即时配对测量，排除后续循环的 DB 写耗时
        base = svc.cfg.control.shortage_retry_base_sec
        cap = svc.cfg.control.shortage_retry_cap_sec
        # 第 n 次期望 ~ base*2^(n-1)*(1+jitter∈[1,1.25])，封顶 cap
        for n, (nr, t0) in enumerate(delays, start=1):
            expect = min(cap, base * (2 ** (n - 1)))
            actual = (nr - t0).total_seconds()
            assert expect * 0.9 <= actual <= expect * 1.3 + 0.1, \
                f"retry {n}: expect~{expect}, actual {actual}"

    def test_admission_gate_during_protection(self, home):
        # 保护期间：低于保护对象优先级的排队作业不被准入（修复第 4 类问题）
        svc = self._svc(home)
        d = svc.db
        d.upsert_job(_ready("j-low", "lo_task"), "h1", "now", 10)
        d.upsert_job(_ready("j-high", "hi_task"), "h2", "now", 90)
        svc.specs = {
            "lo_task": _spec("lo_task", 10),
            "hi_task": _spec("hi_task", 90),
        }
        svc.protect.start_round("hi_task", 90, 0.0, reason="test")
        cands = svc._candidates()
        ids = [c["job_id"] for c in cands]
        assert "j-low" not in ids
        assert "j-high" in ids
        # 无保护时两者都可准入
        svc.protect.end_round(1.0)
        ids2 = [c["job_id"] for c in svc._candidates()]
        assert set(ids2) == {"j-low", "j-high"}


def _spec(task_id, priority):
    return config.TaskSpec(
        schema_version=1, task_id=task_id, priority=priority,
        producer_entry=config.EntrySpec(file="producer.py"),
        executor_entry=config.EntrySpec(file="run.py"))
