"""scanner 单元测试：扫描、去重、消费状态、append-only 违规检测。"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from sched import config, manifest, scanner as scan_mod
from sched.manifest import TaskManifest
from sched.scanner import DiffResult


def _min_manifest() -> dict:
    return {
        "schema_version": 3, "name": "t", "description": "t",
        "run_mode": "auto",
        "entry": {"file": "run.py", "interpreter": "python3", "args": [], "cwd": "."},
        "priority": 50, "resources": {"cpu": 0.1, "memory_mb": 16},
        "error_rules": {"resource_regex": [], "logic_regex": []},
        "heartbeat": {"timeout_sec": 60}, "runtime": {"user": ""},
    }


def _setup_subtask(tmp: Path, sid: str = "hello") -> Path:
    tasks = tmp / "tasks" / sid
    (tasks / "requests").mkdir(parents=True)
    (tasks / "cache").mkdir(parents=True)
    (tasks / "manifest.json").write_text(json.dumps(_min_manifest()), encoding="utf-8")
    (tasks / "run.py").write_text("import sys; sys.exit(0)", encoding="utf-8")
    (tmp / "data" / "runtime").mkdir(parents=True)
    return tasks


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_HOME", str(tmp_path))
    yield tmp_path


class TestScanSubtask:
    def test_discovers_new_intent(self, env):
        tasks = _setup_subtask(env)
        intent = {"schema_version": 1, "request_id": "abc123", "kind": "oneshot",
                  "config": "", "priority": 50, "resources": {"cpu": 0.1, "memory_mb": 16},
                  "lifetime": {"completion": "terminal", "retry": {}}}
        (tasks / "requests" / "abc123.json").write_text(json.dumps(intent), encoding="utf-8")
        diff = scan_mod.scan_subtask("hello", tasks)
        assert len(diff.new_requests) == 1
        assert diff.new_requests[0]["request_id"] == "abc123"
        assert diff.total_visible == 1

    def test_persists_pending_state(self, env):
        tasks = _setup_subtask(env)
        intent = {"schema_version": 1, "request_id": "r1", "kind": "oneshot",
                  "config": "", "priority": 50, "resources": {"cpu": 0.1, "memory_mb": 16}}
        (tasks / "requests" / "r1.json").write_text(json.dumps(intent), encoding="utf-8")
        diff = scan_mod.scan_subtask("hello", tasks)
        scan_mod.save_intents_state({"hello": {
            "r1": {"status": "pending", "fingerprint": diff.new_requests[0]["fingerprint"],
                   "run_id": "", "first_seen": "2026-10-01", "finished_at": "", "rejected_reason": ""}
        }})
        loaded = scan_mod.load_intents_state()
        assert loaded["hello"]["r1"]["status"] == "pending"

    def test_disappeared_pending_marks_done(self, env):
        tasks = _setup_subtask(env)
        intent = {"schema_version": 1, "request_id": "gone", "kind": "oneshot",
                  "config": "", "priority": 50, "resources": {"cpu": 0.1, "memory_mb": 16}}
        (tasks / "requests" / "gone.json").write_text(json.dumps(intent), encoding="utf-8")
        diff = scan_mod.scan_subtask("hello", tasks)
        assert len(diff.new_requests) == 1
        scan_mod.save_intents_state({"hello": {
            "gone": {"status": "pending", "fingerprint": diff.new_requests[0]["fingerprint"],
                     "run_id": "", "first_seen": "t", "finished_at": "", "rejected_reason": ""}
        }})
        (tasks / "requests" / "gone.json").unlink()
        diff2 = scan_mod.scan_subtask("hello", tasks)
        assert len(diff2.disappeared_requests) == 1
        assert diff2.disappeared_requests[0]["request_id"] == "gone"

    def test_modified_intent_flagged_as_rejected(self, env):
        tasks = _setup_subtask(env)
        intent = {"schema_version": 1, "request_id": "mut", "kind": "oneshot",
                  "config": "", "priority": 50, "resources": {"cpu": 0.1, "memory_mb": 16}}
        p = tasks / "requests" / "mut.json"
        p.write_text(json.dumps(intent), encoding="utf-8")
        diff = scan_mod.scan_subtask("hello", tasks)
        scan_mod.save_intents_state({"hello": {
            "mut": {"status": "pending", "fingerprint": diff.new_requests[0]["fingerprint"],
                    "run_id": "", "first_seen": "t"}
        }})
        # 改写
        intent["priority"] = 80
        p.write_text(json.dumps(intent), encoding="utf-8")
        diff2 = scan_mod.scan_subtask("hello", tasks)
        assert len(diff2.modified_requests) == 1
        assert diff2.modified_requests[0]["request_id"] == "mut"

    def test_pending_items_exposes_requests(self, env):
        tasks = _setup_subtask(env)
        intent = {"schema_version": 1, "request_id": "p1", "kind": "oneshot",
                  "config": "", "priority": 50, "resources": {"cpu": 0.1, "memory_mb": 16}}
        (tasks / "requests" / "p1.json").write_text(json.dumps(intent), encoding="utf-8")
        scan_mod.scan_subtask("hello", tasks)
        scan_mod.save_intents_state({"hello": {
            "p1": {"status": "pending", "fingerprint": "fp", "run_id": "", "first_seen": "t"}
        }})
        out = scan_mod.pending_items("hello")
        assert len(out) == 1
        assert out[0]["request_id"] == "p1"

    def test_ignore_non_json_files(self, env):
        tasks = _setup_subtask(env)
        (tasks / "requests" / "not_json.txt").write_text("garbage")
        (tasks / "requests" / "manifest.json").write_text("{}")  # 忽略
        diff = scan_mod.scan_subtask("hello", tasks)
        assert diff.total_visible == 0


class TestMarkTransitions:
    def test_mark_running_then_terminal(self, env):
        tasks = _setup_subtask(env)
        intent = {"schema_version": 1, "request_id": "trans", "kind": "oneshot",
                  "config": "", "priority": 50, "resources": {"cpu": 0.1, "memory_mb": 16}}
        (tasks / "requests" / "trans.json").write_text(json.dumps(intent), encoding="utf-8")
        scan_mod.scan_subtask("hello", tasks)
        scan_mod.mark_running("hello", "trans", "run-123")
        st = scan_mod.load_intents_state()["hello"]["trans"]
        assert st["status"] == "running"
        assert st["run_id"] == "run-123"
        scan_mod.mark_terminal("hello", "trans", "done")
        st2 = scan_mod.load_intents_state()["hello"]["trans"]
        assert st2["status"] == "done"
        assert st2["finished_at"]

    def test_mark_retry_same_resets_status(self, env):
        tasks = _setup_subtask(env)
        intent = {"schema_version": 1, "request_id": "rt", "kind": "oneshot",
                  "config": "", "priority": 50, "resources": {"cpu": 0.1, "memory_mb": 16}}
        (tasks / "requests" / "rt.json").write_text(json.dumps(intent), encoding="utf-8")
        scan_mod.scan_subtask("hello", tasks)
        scan_mod.mark_running("hello", "rt", "run-rt")
        scan_mod.mark_retry_same("hello", "rt")
        st = scan_mod.load_intents_state()["hello"]["rt"]
        assert st["status"] == "pending"
        assert st["run_id"] == ""


class TestPruneOrphanIntents:
    def test_prunes_unregistered_keeps_registered_and_disabled(self, env):
        scan_mod.save_intents_state({
            "hello": {"r1": {"status": "done", "finished_at": "t", "rejected_reason": "x"}},
            "gone": {"r2": {"status": "done", "finished_at": "t", "rejected_reason": "x"}},
            "disabled": {"r3": {"status": "pending", "fingerprint": "fp"}},
        })
        removed = scan_mod.prune_orphan_intents({"hello", "disabled"})
        assert removed == 1  # 只有 gone 是孤儿
        st = scan_mod.load_intents_state()
        assert set(st) == {"hello", "disabled"}

    def test_no_orphan_no_write(self, env):
        scan_mod.save_intents_state({"hello": {"r1": {"status": "pending"}}})
        state_file = env / "data" / "runtime" / "intents_state.json"
        before = state_file.stat().st_mtime_ns
        removed = scan_mod.prune_orphan_intents({"hello"})
        assert removed == 0
        assert state_file.stat().st_mtime_ns == before  # 无孤儿时不落盘
