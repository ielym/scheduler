"""目录重构回归：tasks 只放定义、运行态全在 runtime、env/契约/回收。

覆盖：
- 新路径布局（state/ipc/logs/artifacts/tasks）与 ensure_layout；
- task.json 的 params（原任务级 config.json 并入）；
- entry.cwd / workdir 相对**运行态**目录校验（越界拒绝）；
- 注入 env（SCHED_TASK_DIR/STATE_DIR/ATTEMPT_DIR/ATTEMPT_WORK_DIR、SCHED_WORKSPACE 重指向）；
- attempt 级临时区随 attempt 回收；
- 孤儿运行态目录 GC。
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace

ROOT = str(Path(__file__).resolve().parent.parent)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import pytest  # noqa: E402

from sched import config, runner, store  # noqa: E402
from sched.config import JobStatus  # noqa: E402
from sched.scheduler import AttMeta, SchedulerService  # noqa: E402


def _write_task(home, task_id, params=None, workdir="."):
    td = home / "tasks" / task_id
    td.mkdir(parents=True, exist_ok=True)
    (td / "producer.py").write_text("")
    (td / "run.py").write_text("")
    doc = {
        "schema_version": 1, "task_id": task_id, "enabled": True, "priority": 50,
        "producer_entry": {"file": "producer.py"},
        "executor_entry": {"file": "run.py"},
        "workdir": workdir,
        "recovery": {"replay": "outbox", "retain": "r", "replay_note": "n"},
    }
    if params is not None:
        doc["params"] = params
    store.save_json(td / "task.json", doc)
    return td


# ---------- 布局与 ensure_layout ----------

def test_layout_paths(home):
    config.ensure_layout()
    assert config.state_dir() == home / "runtime" / "state"
    assert config.db_file() == config.state_dir() / "exec.db"
    assert config.snapshot_file() == config.state_dir() / "snapshot.json"
    assert config.timing_file() == config.state_dir() / "timing.json"
    assert config.ipc_dir() == home / "runtime" / "ipc"
    assert config.commands_file() == config.ipc_dir() / "commands.jsonl"
    assert config.status_log() == home / "runtime" / "logs" / "status.jsonl"
    assert config.web_metrics_file() == home / "runtime" / "logs" / "web_metrics.jsonl"
    assert config.artifacts_dir() == home / "runtime" / "artifacts"
    assert config.archive_root() == config.artifacts_dir() / "archive"
    assert config.isolation_root() == config.artifacts_dir() / "isolation"
    assert config.runs_root() == config.artifacts_dir() / "runs"
    assert config.task_dir("t") == home / "tasks" / "t"
    assert config.task_state_dir("t") == home / "runtime" / "tasks" / "t"
    assert config.outbox_dir("t") == config.task_state_dir("t") / "outbox"
    assert config.receipts_dir("t") == config.task_state_dir("t") / "receipts"
    assert config.producer_state_dir("t") == \
        config.task_state_dir("t") / "producer_state"
    for d in (config.state_dir(), config.ipc_dir(), config.logs_dir(),
              config.artifacts_dir(), config.tasks_state_root()):
        assert d.is_dir(), d


# ---------- task.json params ----------

def test_static_params_live_in_task_json(home):
    _write_task(home, "t", params={"topic": "X", "query": "cat:cs.AI",
                                   "start_date": "2026-01-01"})
    spec = config.load_task("t")
    assert spec.params == {"topic": "X", "query": "cat:cs.AI",
                           "start_date": "2026-01-01"}
    # tasks/ 下不再有任务级 config.json
    assert not (home / "tasks" / "t" / "config.json").exists()


def test_task_json_defaults_empty_params(home):
    _write_task(home, "t")
    assert config.load_task("t").params == {}


# ---------- cwd/workdir 相对运行态目录校验 ----------

def test_workdir_validated_against_state_dir(home):
    _write_task(home, "t")
    assert config.load_task("t").workdir == "."      # 默认合法
    _write_task(home, "u", workdir="../escape")
    with pytest.raises(config.TaskError):
        config.load_task("u")
    _write_task(home, "v", workdir="/abs")
    with pytest.raises(config.TaskError):
        config.load_task("v")


def test_ready_workdir_validated_against_state_dir(home):
    _write_task(home, "t")
    ok = config.parse_ready({"schema_version": 1, "job_id": "t-a", "task_id": "t",
                             "idempotency_key": "k"})
    assert ok.workdir == "."
    with pytest.raises(config.ReadyError):
        config.parse_ready({"schema_version": 1, "job_id": "t-b", "task_id": "t",
                            "idempotency_key": "k", "workdir": "../../x"})


# ---------- env 注入 ----------

def _spec(home, tid, att="att-1"):
    return runner.AttemptSpec(
        attempt_id=att, job_id=f"{tid}-a", task_id=tid, token="tok",
        task_root=config.task_dir(tid),
        executor_entry=config.EntrySpec(file="run.py"),
        entry_args=[], workdir=".", env={})


def test_build_env_separates_code_and_state(home):
    _write_task(home, "t")
    env = runner.build_env(_spec(home, "t"))
    assert env["SCHED_TASK_DIR"] == str(config.task_dir("t"))
    assert env["SCHED_TASK_STATE_DIR"] == str(config.task_state_dir("t"))
    assert env["SCHED_ATTEMPT_DIR"] == str(config.run_dir("att-1"))
    assert env["SCHED_ATTEMPT_WORK_DIR"] == str(config.attempt_work_dir("att-1"))
    # 兼容旧写法：SCHED_WORKSPACE 指向任务运行态（可写），不再是 tasks/
    assert env["SCHED_WORKSPACE"] == str(config.task_state_dir("t"))


def test_launch_creates_dirs_under_runtime(home):
    _write_task(home, "t")
    run = runner.launch(_spec(home, "t", att="att-l"))
    try:
        assert config.attempt_work_dir("att-l").is_dir()
        assert (config.run_dir("att-l") / "worker.log").exists()
        # 工作目录落在运行态目录（任务级），而非 tasks/
        assert config.task_state_dir("t").is_dir()
    finally:
        try:
            run.proc.kill()
        except Exception:  # noqa: BLE001
            pass
        run.cleanup()


# ---------- attempt 级临时区回收 ----------

class _FakeProc:
    def __init__(self, returncode=0):
        self.returncode = returncode

    def wait(self, timeout=None):
        return 0


class _FakeCgroup:
    available = False

    def procs(self):
        return []

    def kill_all(self):
        pass


class _FakeAttempt:
    pid = 11
    pgid = 11
    start_time = 1.0

    def __init__(self, returncode=0):
        self.proc = _FakeProc(returncode)
        self.cgrp = _FakeCgroup()
        self.spec = SimpleNamespace(attempt_id="att-1")

    def is_alive(self):
        return False

    def read_events(self, after_seq: int = 0):
        return []

    def cleanup(self):
        pass


def test_finalize_reclaims_attempt_work_dir(home):
    svc = SchedulerService()
    _write_task(home, "t")
    work = config.attempt_work_dir("att-1")
    (work / "downloads").mkdir(parents=True, exist_ok=True)
    (work / "downloads" / "tmp.pdf").write_text("x")
    svc.db.create_attempt("att-1", "t-a", 1, "tok", "t0")
    meta = AttMeta("att-1", "t-a", "t", 50, "tok")
    meta.status = JobStatus.RUNNING
    svc._finalize(_FakeAttempt(returncode=0), meta, runner.now_iso())
    assert not work.exists()                       # 随 attempt 回收


# ---------- 孤儿运行态目录 GC ----------

def test_orphan_gc_removes_deleted_task_dirs(home):
    svc = SchedulerService()
    config.ensure_layout()
    _write_task(home, "live")
    for root in (config.archive_root(), config.isolation_root(),
                 config.logs_dir(), config.tasks_state_root()):
        d = root / "gone"
        d.mkdir(parents=True, exist_ok=True)
        (d / "f").write_text("1")
    (config.archive_root() / "live").mkdir(parents=True, exist_ok=True)
    svc._tick_orphan_gc(10 ** 9)
    for root in (config.archive_root(), config.isolation_root(),
                 config.logs_dir(), config.tasks_state_root()):
        assert not (root / "gone").exists(), root
    assert (config.archive_root() / "live").exists()   # 现存任务保留


# ---------- 运行态卫生：命令队列清理 + .lock 残影 GC ----------

def test_tick_commands_removes_idless_command(home):
    """无 id 的畸形命令必须被移除：此前仅按 id 匹配会每轮重复执行、ack 累积。"""
    svc = SchedulerService()
    config.ensure_layout()
    store.append_jsonl(config.commands_file(),
                       {"kind": "cancel_job", "target": "nonexistent"})  # 无 id
    svc._tick_commands()
    svc._tick_commands()          # 第二轮：命令应已不在队列，不得再次执行
    assert store.read_jsonl(config.commands_file()) == []
    assert len(store.read_jsonl(config.commands_ack_file())) == 1


def test_lock_gc_removes_stale_unlocked_locks(home):
    """GC 只删陈旧且未被持有的 .lock；持久锁与新鲜锁保留。"""
    svc = SchedulerService()
    config.ensure_layout()
    old = time.time() - 7200.0
    d = config.receipts_dir("t")
    d.mkdir(parents=True, exist_ok=True)
    stale = d / "job1.json.lock"
    stale.write_text("")
    os.utime(stale, (old, old))
    fresh = d / "job2.json.lock"
    fresh.write_text("")
    keep = config.runtime_dir() / "scheduler.lock"   # 持久锁按名跳过
    keep.write_text("")
    os.utime(keep, (old, old))
    svc._tick_lock_gc(10 ** 9)
    assert not stale.exists()
    assert fresh.exists()
    assert keep.exists()
