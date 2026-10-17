"""心跳（heartbeat）存活机制回归测试。

覆盖恢复心跳后的关键语义：
1. EventKind.HEARTBEAT 在 SDK 与调度器两侧镜像。
2. SDK 心跳间隔从环境变量读取（默认 5s）。
3. ExecutorTask 运行期间后台自动发 heartbeat 事件（与进度分离）。
4. 调度器 ingest heartbeat 只刷新存活时间戳，不喂进度节奏 EMA。
5. TaskTiming 进度节奏 EMA 的持久化 snapshot/restore 往返。
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

ROOT = str(Path(__file__).resolve().parent.parent)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from sched import config, scaling  # noqa: E402
from sched.config import EventKind  # noqa: E402
from sched.scheduler import AttMeta, SchedulerService  # noqa: E402


def _mk_job(db, job_id, task_id="t", priority=50):
    ready = config.parse_ready({
        "schema_version": 1, "job_id": job_id, "task_id": task_id,
        "entry_args": [], "env": {}, "idempotency_key": job_id,
    })
    db.upsert_job(ready, f"hash-{job_id}", "2026-10-11T00:00:00+00:00", priority)
    return job_id


def _svc(home):
    (home / "runtime").mkdir(parents=True, exist_ok=True)
    return SchedulerService()


def test_eventkind_heartbeat_mirror():
    from sched_task_sdk import errors as sdk_errors
    assert config.EventKind.HEARTBEAT == "heartbeat"
    assert sdk_errors.EventKind.HEARTBEAT == "heartbeat"


def test_heartbeat_interval_from_env(monkeypatch):
    from sched_task_sdk.executor import (
        DEFAULT_HEARTBEAT_INTERVAL_SEC, _heartbeat_interval_from_env)
    monkeypatch.delenv("SCHED_HEARTBEAT_INTERVAL_SEC", raising=False)
    assert _heartbeat_interval_from_env() == DEFAULT_HEARTBEAT_INTERVAL_SEC
    monkeypatch.setenv("SCHED_HEARTBEAT_INTERVAL_SEC", "3.5")
    assert _heartbeat_interval_from_env() == 3.5
    monkeypatch.setenv("SCHED_HEARTBEAT_INTERVAL_SEC", "bad")
    assert _heartbeat_interval_from_env() == DEFAULT_HEARTBEAT_INTERVAL_SEC


def test_executor_emits_heartbeat(monkeypatch, tmp_path):
    from sched_task_sdk.executor import EXIT_OK, ExecutorTask, run_task
    ev = tmp_path / "events.jsonl"
    monkeypatch.setenv("SCHED_EVENT_FILE", str(ev))
    monkeypatch.setenv("SCHED_CHECKPOINT_FILE", str(tmp_path / "checkpoint.json"))
    monkeypatch.setenv("SCHED_ATTEMPT_ID", "att-1")
    monkeypatch.setenv("SCHED_HEARTBEAT_INTERVAL_SEC", "0.05")
    monkeypatch.delenv("SCHED_PERMIT_FILE", raising=False)
    monkeypatch.delenv("SCHED_CONTROL_FILE", raising=False)

    class T(ExecutorTask):
        def execute(self):
            time.sleep(0.15)
            self.complete({"ok": 1})

    assert run_task(T(name="hb")) == EXIT_OK
    lines = [json.loads(l) for l in ev.read_text("utf-8").splitlines()]
    kinds = [l["kind"] for l in lines]
    assert kinds[0] == "started"
    assert "heartbeat" in kinds
    assert "done" in kinds


def test_ingest_heartbeat_updates_liveness_not_ema(home):
    svc = _svc(home)
    _mk_job(svc.db, "j-hb")
    svc.db.create_attempt("a-hb", "j-hb", 1, "tok", "now")
    meta = AttMeta("a-hb", "j-hb", "t", 50, "tok")

    class FakeRun:
        def read_events(self, after_seq):
            return [{"seq": 1, "kind": EventKind.HEARTBEAT}]

    svc._ingest_events(FakeRun(), meta)
    assert meta.last_heartbeat_mono is not None
    # heartbeat 不喂进度节奏 EMA：无业务事件，feedback_interval 仍为默认
    assert svc.timing._last_event_mono.get("t") is None
    assert svc.timing.feedback_interval("t") == pytest.approx(
        svc.cfg.control.feedback_interval_init_sec)


def test_timing_snapshot_restore_roundtrip():
    t = scaling.TaskTiming(default_interval_sec=5.0)
    t.note_event("a", 0.0)
    t.note_event("a", 2.0)
    t.note_event("b", 0.0)
    t.note_event("b", 10.0)
    t2 = scaling.TaskTiming(default_interval_sec=5.0)
    t2.restore(t.snapshot())
    assert t2.feedback_interval("a") == pytest.approx(2.0)
    assert t2.feedback_interval("b") == pytest.approx(10.0)
    assert t2.feedback_interval("c") == pytest.approx(5.0)  # 未持久化的任务用默认