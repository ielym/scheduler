"""本轮重构（结构去重 + 缺陷修复）的回归测试。

覆盖：
- Web 批量控制命令接纳 pause_attempt（此前遗漏导致"停止任务"恒失败）
- 归档窗口对账改为单批时间片（满批推进游标、不足批复位）
- 已删除作业从 CLI/web 口径中排除（status_counts / list_jobs）
- 同状态多消费方扫描使用独立游标（避免互踩漏检）
- 进程身份核实统一委托 sched.proc（runner/producer 共用）
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = str(Path(__file__).resolve().parent.parent)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from sched import config, proc, runner, web  # noqa: E402
from sched.config import JobStatus  # noqa: E402
from sched.scheduler import SchedulerService  # noqa: E402


def _svc(home):
    (home / "runtime").mkdir(parents=True, exist_ok=True)
    return SchedulerService()


def _mk_job(db, job_id, task_id="t", priority=50):
    ready = config.parse_ready({
        "schema_version": 1, "job_id": job_id, "task_id": task_id,
        "entry_args": [], "env": {}, "idempotency_key": job_id,
    })
    db.upsert_job(ready, f"hash-{job_id}", runner.now_iso(), priority)
    return job_id


# ---------- Web 批量命令接纳 pause_attempt ----------

def test_web_batch_accepts_pause_attempt(home):
    # serve 不在线：不该被判"未知命令类型"，而应进入逐条离线处理分支
    obj, code = web.api_command_batch("pause_attempt", ["att-1"], "任务级停止")
    assert code == 200
    assert obj["results"][0]["ok"] is False
    assert "serve" in obj["results"][0]["detail"]


def test_web_batch_rejects_unknown_kind(home):
    obj, code = web.api_command_batch("not_a_command", ["x"], "r")
    assert code == 400 and "error" in obj


def test_web_task_stop_no_live_attempts(home):
    _svc(home)  # 初始化运行时数据库
    obj, code = web.api_task_stop("t")
    assert code == 200 and obj["ok"] is True and obj["stopped"] == 0


# ---------- 归档对账单批时间片 ----------

def test_reconcile_archive_single_batch_per_call(home, monkeypatch):
    monkeypatch.setattr(SchedulerService, "RECONCILE_BATCH", 2)
    svc = _svc(home)
    for jid in ("j1", "j2", "j3"):
        _mk_job(svc.db, jid)
    ad = home / "runtime" / "archive" / "t"
    ad.mkdir(parents=True)
    (ad / "j1.json").write_text("{}")
    (ad / "j2.json").write_text("{}")  # j3 缺归档
    statuses: list[dict] = []
    svc._append_status = statuses.append

    # 第一次：只核对一页（j1/j2），j3 未入批 → 无告警；满批 → 游标推进
    svc._reconcile_archive()
    assert svc._reconcile_offset == 2
    assert not [s for s in statuses if s.get("kind") == "archive_missing"]

    # 第二次：核对剩余页（j3，缺归档）→ 告警；不足批 → 游标复位
    svc._reconcile_archive()
    assert svc._reconcile_offset == 0
    assert {s["job_id"] for s in statuses
            if s.get("kind") == "archive_missing"} == {"j3"}


# ---------- 已删除作业排除 ----------

def test_deleted_jobs_excluded_from_listing_and_counts(home):
    svc = _svc(home)
    _mk_job(svc.db, "keep")
    _mk_job(svc.db, "gone")
    svc.db.set_job_status("gone", JobStatus.SUCCEEDED, ".", runner.now_iso())
    svc.db._conn.execute("UPDATE jobs SET deleted=1 WHERE job_id='gone'")
    assert {j["job_id"] for j in svc.db.list_jobs()} == {"keep"}
    assert svc.db.status_counts() == {JobStatus.QUEUED: 1}


# ---------- 状态扫描独立游标 ----------

def test_jobs_in_status_independent_scan_key(home):
    svc = _svc(home)
    for jid in ("b1", "b2", "b3"):
        _mk_job(svc.db, jid)
        svc.db.set_job_status(jid, JobStatus.BACKOFF, "测试", runner.now_iso())
    head = [j["job_id"] for j in svc._jobs_in_status(JobStatus.BACKOFF, limit=2)]
    assert len(head) == 2
    # 默认游标已被上一行推进；用独立 scan_key 的消费方仍应从头部开始
    monitor = [j["job_id"] for j in svc._jobs_in_status(
        JobStatus.BACKOFF, limit=2, scan_key="BACKOFF:monitor")]
    assert monitor == head


# ---------- 进程身份核实统一 ----------

def test_pid_alive_delegates_to_proc(home):
    import os
    pid = os.getpid()
    assert runner.pid_alive_with_start(pid, proc.start_time(pid)) is True
    assert runner.pid_alive_with_start(pid, proc.start_time(pid) + 1.0) is False
    assert proc.alive(pid) is True
