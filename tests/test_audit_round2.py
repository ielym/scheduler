"""第二轮审计修复的专项回归（A2/A5/A7/B3/B4/B7/B9/B10/B13/B14/C2/C5/C6）。

与 test_fixes / test_repair_fixes 互补：这里只放本轮新语义的直接单测，
不依赖真实子进程（端到端由 test_integration.py 覆盖）。
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

ROOT = str(Path(__file__).resolve().parent.parent)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import pytest  # noqa: E402

from sched import config, db as dbmod, runner, scaling  # noqa: E402
from sched.config import JobStatus  # noqa: E402
from sched.scheduler import AttMeta, SchedulerService  # noqa: E402
from sched_task_sdk import control as sdk_control  # noqa: E402
from sched_task_sdk import events as sdk_events  # noqa: E402
from sched_task_sdk.executor import ExecutorTask, Shortage, _clean_reason  # noqa: E402


def _write_task(home, task_id, priority=50):
    td = home / "tasks" / task_id
    td.mkdir(parents=True, exist_ok=True)
    (td / "producer.py").write_text("")
    (td / "run.py").write_text("")
    from sched import store
    store.save_json(td / "task.json", {
        "schema_version": 1, "task_id": task_id, "enabled": True,
        "priority": priority,
        "producer_entry": {"file": "producer.py"},
        "executor_entry": {"file": "run.py"},
        "recovery": {"replay": "outbox", "retain": "测试保留", "replay_note": "测试重放"},
    })
    return td


def _mk_job(db, job_id, task_id="t", priority=50):
    ready = config.parse_ready({
        "schema_version": 1, "job_id": job_id, "task_id": task_id,
        "entry_args": [], "env": {}, "idempotency_key": job_id,
    })
    db.upsert_job(ready, f"hash-{job_id}", "2026-10-12T00:00:00+00:00", priority)
    return job_id


def _svc(home):
    (home / "runtime").mkdir(parents=True, exist_ok=True)
    return SchedulerService()


# ---------- A5 reason 纯净化 ----------

class TestCleanReason:
    def test_keeps_error_type_and_resource_id(self):
        """R4：错误类型（异常类名/HTTP 状态码）与资源标识（URL/主机）随
        reason 保留上浮，供调度器与运维归因；仅清理不可见字符/空白并截断。"""
        raw = ("ConnectionError: HTTP 503 at https://api.example.com/x?y=1 "
               "TimeoutError 后重试")
        out = _clean_reason(raw)
        assert "ConnectionError" in out and "503" in out
        assert "https://api.example.com/x?y=1" in out
        assert len(out) <= 300

    def test_shortage_reason_keeps_fingerprint(self):
        s = Shortage("download failed HTTPStatusError https://arxiv.org/pdf/1 404")
        assert "arxiv" in s.reason and "404" in s.reason


# ---------- A7 启动许可门 ----------

class TestPermitGate:
    def _task(self, tmp_path, monkeypatch, permit_body):
        pf = tmp_path / "permit.json"
        pf.write_text(permit_body)
        monkeypatch.setenv("SCHED_PERMIT_FILE", str(pf))
        monkeypatch.setenv("SCHED_ATTEMPT_ID", "att-1")
        monkeypatch.setenv("SCHED_JOB_ID", "j1")
        monkeypatch.setenv("SCHED_EVENT_FILE", str(tmp_path / "ev.jsonl"))
        monkeypatch.setenv("SCHED_CONTROL_FILE", str(tmp_path / "ctl.json"))
        return ExecutorTask("t", permit_timeout_sec=0.3)

    def test_mismatched_permit_times_out_as_shortage(self, tmp_path, monkeypatch):
        t = self._task(tmp_path, monkeypatch,
                       json.dumps({"action": "permit", "attempt_id": "att-OTHER"}))
        with pytest.raises(Shortage):
            t._wait_for_permit()

    def test_matching_permit_releases(self, tmp_path, monkeypatch):
        t = self._task(tmp_path, monkeypatch,
                       json.dumps({"action": "permit", "attempt_id": "att-1"}))
        t._wait_for_permit()  # 立即返回不超时


# ---------- B3 暂停命令身份校验 ----------

class TestPauseIdentity:
    def test_stale_command_for_other_attempt_ignored(self, tmp_path, monkeypatch):
        f = tmp_path / "ctl.json"
        f.write_text(json.dumps({"command_id": "c1", "action": "pause",
                                 "attempt_id": "att-OLD", "token": "tok-OLD"}))
        monkeypatch.setenv("SCHED_CONTROL_FILE", str(f))
        monkeypatch.setenv("SCHED_ATTEMPT_ID", "att-NEW")
        monkeypatch.setenv("SCHED_TOKEN", "tok-NEW")
        assert sdk_control.current_pause() is None

    def test_matching_command_honored(self, tmp_path, monkeypatch):
        f = tmp_path / "ctl.json"
        f.write_text(json.dumps({"command_id": "c1", "action": "pause",
                                 "attempt_id": "att-NEW", "token": "tok-NEW"}))
        monkeypatch.setenv("SCHED_CONTROL_FILE", str(f))
        monkeypatch.setenv("SCHED_ATTEMPT_ID", "att-NEW")
        monkeypatch.setenv("SCHED_TOKEN", "tok-NEW")
        p = sdk_control.current_pause()
        assert p is not None and p.command_id == "c1"


# ---------- C2 事件写失败不向上抛 ----------

def test_emit_failure_waits_then_gives_up_without_raising(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_JOB_ID", "j1")
    monkeypatch.setenv("SCHED_ATTEMPT_ID", "a1")
    # 目录不存在且父路径不可创建 → 每次重试都失败
    monkeypatch.setenv("SCHED_EVENT_FILE", "/proc/definitely/not/writable/ev.jsonl")
    monkeypatch.setattr(sdk_events.EventWriter, "_RETRY_DELAYS",
                        (0.01, 0.01, 0.01))
    seq = sdk_events.emit("error")
    assert seq >= 1  # 不抛异常，返回 seq（stderr 告警）


# ---------- B7 成功后重试计数复位 ----------

def test_reset_job_retry(home):
    svc = _svc(home)
    _mk_job(svc.db, "j1")
    svc.db.set_job_backoff("j1", 3, "2026-10-12T00:00:00+00:00")
    svc.db.reset_job_retry("j1")
    row = svc.db.get_job("j1")
    assert int(row["retry_count"] or 0) == 0


# ---------- B9 资源不健康时停止准入 ----------

def test_admit_blocked_when_trend_unhealthy(home):
    svc = _svc(home)
    _write_task(home, "t9")
    _mk_job(svc.db, "j9", task_id="t9")
    svc.trend.mark_oom()
    assert svc.trend.healthy_enough() is False
    svc._tick_admit(0.0)
    assert svc.db.get_job("j9")["status"] == JobStatus.QUEUED  # 未被拉起
    assert not svc.attempts
    # 恢复健康后放行
    for _ in range(3):
        svc.trend.sample(0.1, 0.1, True)
    assert svc.trend.healthy_enough() is True


# ---------- B10 启动失败：attempt 留证据、作业保持 QUEUED ----------

def test_launch_infra_failure_keeps_job_queued(home, monkeypatch):
    svc = _svc(home)
    _write_task(home, "t10")
    _mk_job(svc.db, "j10", task_id="t10")

    def boom(aspec, refs):
        raise OSError("解释器不存在")

    monkeypatch.setattr(runner, "launch", boom)
    job = svc.db.jobs_in_status(JobStatus.QUEUED, limit=5)[0]
    assert svc._launch(job, config.load_task("t10")) is False
    # 作业保持 QUEUED（基础设施问题不得终态化 FAILED）
    assert svc.db.get_job("j10")["status"] == JobStatus.QUEUED
    assert svc._backoff_until.get("j10", 0.0) > 0.0  # 短退避后重试
    att = svc.db.list_attempts(job_id="j10")[0]
    assert att["status"] == JobStatus.FAILED  # attempt 留证据
    lines = config.status_log().read_text("utf-8").splitlines()
    assert any('"launch_failed"' in ln for ln in lines)


# ---------- B13 终态历史清理 ----------

def test_stale_terminal_attempts_cleanup(home):
    svc = _svc(home)
    _mk_job(svc.db, "jold")
    _mk_job(svc.db, "jnew")
    old = "2026-01-01T00:00:00+00:00"
    recent = "2026-10-12T00:00:00+00:00"
    for jid, fin in (("jold", old), ("jnew", recent)):
        svc.db.create_attempt(f"att-{jid}", jid, 1, "tok", "t0")
        svc.db.update_attempt(f"att-{jid}", status=JobStatus.SUCCEEDED,
                              finished_at=fin)
        svc.db.set_job_status(jid, JobStatus.SUCCEEDED, "完成", fin)
    cutoff = "2026-10-01T00:00:00+00:00"
    ids = svc.db.stale_terminal_attempts(cutoff)
    assert set(ids) == {"att-jold"}
    svc.db.delete_attempt_artifacts("att-jold")
    assert svc.db.list_attempts(job_id="jold") == []
    assert svc.db.list_attempts(job_id="jnew")  # 新终态不动


# ---------- C5 jobs_in_status 分页游标 ----------

def test_jobs_in_status_pagination(home):
    svc = _svc(home)
    for i in range(7):
        _mk_job(svc.db, f"pq-{i}")
    first = svc.db.jobs_in_status(JobStatus.QUEUED, limit=3)
    second = svc.db.jobs_in_status(JobStatus.QUEUED, limit=3, offset=3)
    third = svc.db.jobs_in_status(JobStatus.QUEUED, limit=3, offset=6)
    ids = {j["job_id"] for j in first + second + third}
    assert len(ids) == 7
    assert all(j["job_id"] not in {x["job_id"] for x in second} for j in first)
    assert len(third) == 1


# ---------- C6 取消立即持久化 ----------

def test_cancel_job_persists_immediately(home):
    svc = _svc(home)
    _mk_job(svc.db, "jc")
    svc.db.create_attempt("att-jc", "jc", 1, "tok", "t0")
    svc.db.set_job_status("jc", JobStatus.RUNNING, "run", "2026-10-12T00:00:00+00:00")
    meta = AttMeta("att-jc", "jc", "t", 50, "tok")

    class _GoneAttempt:
        pid = pgid = 0
        start_time = 0.0
        spec = config  # placeholder

        def is_alive(self):
            return False

        def signal_pause(self):
            pass

    svc.meta["att-jc"] = meta
    svc.attempts["att-jc"] = _GoneAttempt()
    assert svc.cancel_job("jc") is True
    # 立即落库——不等宽限/进程退出（崩溃后 _recover 按 CANCELLED 收尾不复活）
    assert svc.db.get_job("jc")["status"] == JobStatus.CANCELLED


# ---------- B14 暂停宽限推导 ----------

def test_pause_grace_uses_feedback_interval(home):
    svc = _svc(home)
    _write_task(home, "tg")
    svc.cfg.control.pause_grace_max_sec = 300.0  # 不让封顶干扰断言
    meta = AttMeta("a1", "j1", "tg", 50, "tok")
    undeclared = svc._pause_grace_sec(meta)
    # 喂事件让反馈间隔 EMA = 40s → 宽限抬升为 40×4=160
    svc.timing.note_event("tg", 0.0)
    svc.timing.note_event("tg", 40.0)
    declared = svc._pause_grace_sec(meta)
    assert declared == pytest.approx(160.0)
    assert declared >= undeclared


# ---------- A2 撤销放回 ----------

def test_revoke_released_pauses_lower_priority(home):
    svc = _svc(home)
    _write_task(home, "hi", priority=90)
    _write_task(home, "lo", priority=50)
    _mk_job(svc.db, "j-lo", task_id="lo", priority=50)
    svc.protect.st.released["lo"] = 50  # 模拟"lo 已被放回"
    blocked = AttMeta("a-hi", "j-hi", "hi", 90, "tok")
    svc._revoke_released(blocked, dict(svc.protect.st.released))
    # QUEUED 作业回退 PAUSED，pending_resume 回灌
    assert svc.db.get_job("j-lo")["status"] == JobStatus.PAUSED
    assert svc.protect.st.pending_resume.get("lo") == 50
    lines = config.status_log().read_text("utf-8").splitlines()
    assert any('"release_revoked"' in ln for ln in lines)
