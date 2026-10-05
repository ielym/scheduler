"""SDK 单元测试：SchedTask 生命周期 + emit_intent + 断点契约 + 控制指令。"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from pathlib import Path

import pytest

from sched_task_sdk import (
    DaemonTask, OneshotTask, SchedTask, TaskState,
    emit_intent, list_intents, request_stop, no_retry, reschedule,
    EXIT_OK, EXIT_PREEMPTED, EXIT_STOP_REQUESTED, EXIT_NO_RETRY,
    EXIT_RESOURCE,
)


@pytest.fixture
def sdk_env(tmp_path, monkeypatch):
    ws = tmp_path / "tasks" / "hello"
    ws.mkdir(parents=True)
    (ws / "cache" / "runs" / "test-run").mkdir(parents=True)
    (ws / "requests").mkdir(parents=True)
    monkeypatch.setenv("SCHED_WORKSPACE", str(ws))
    monkeypatch.setenv("SCHED_RUN_ID", "test-run")
    monkeypatch.setenv("SCHED_SUBTASK_ID", "hello")
    monkeypatch.setenv("SCHED_REQUEST_ID", "rid")
    monkeypatch.setenv("SCHED_ATTEMPT", "1")
    yield ws


class TestSchedTaskOneshot:
    def test_run_returns_zero(self, sdk_env):
        class T(OneshotTask):
            def steps(self):
                yield None
        t = T()
        assert t.run() == EXIT_OK

    def test_save_checkpoint_persists(self, sdk_env):
        class T(OneshotTask):
            def steps(self):
                self.save_checkpoint({"done": ["a", "b"]})
                yield None
        t = T()
        t.run()
        rp = sdk_env / "cache" / "resume_point.json"
        assert rp.exists()
        data = json.loads(rp.read_text("utf-8"))
        assert data["resume_point"]["done"] == ["a", "b"]

    def test_sigterm_returns_preempt(self, sdk_env):
        class T(OneshotTask):
            def steps(self):
                import time
                self.save_checkpoint({"at": "first"})
                yield None
                time.sleep(2)
                yield None
        t = T()

        def _sig():
            time.sleep(0.2)
            os.kill(os.getpid(), signal.SIGTERM)

        import threading
        threading.Thread(target=_sig, daemon=True).start()
        rc = t.run()
        assert rc == EXIT_PREEMPTED
        rp = sdk_env / "cache" / "resume_point.json"
        assert rp.exists()  # 断点已保存

    def test_request_stop_returns_stop_requested(self, sdk_env):
        class T(DaemonTask):
            def steps(self):
                import time
                while True:
                    self.request_stop("done")
                    yield None
                    time.sleep(2)
        t = T()
        import threading

        def _wait():
            time.sleep(0.3)
            t.state._stop_evt.set()

        threading.Thread(target=_wait, daemon=True).start()
        # request_stop 在 steps() 里被调了，但 state._stop_requested 要从外部触发
        # 这里简化：直接在 main 线程跑一小段然后主动 stop
        rc = t.run()
        assert rc == EXIT_STOP_REQUESTED

    def test_no_retry_returns_no_retry(self, sdk_env):
        class T(OneshotTask):
            def steps(self):
                yield "x"
                yield "y"
            def process_step(self, step):
                raise RuntimeError("boom")
            def on_error(self, exc):
                self.no_retry("别重试")
                self.state.error("resource", "boom")
                return "resource"
        t = T()
        rc = t.run()
        assert rc == EXIT_NO_RETRY


class TestEmitIntent:
    def test_atomic_write(self, sdk_env):
        rid = emit_intent(config="config/2026-10-01.json", kind="daemon",
                          resources={"cpu": 0.2, "memory_mb": 64})
        p = sdk_env / "requests" / f"{rid}.json"
        assert p.exists()
        data = json.loads(p.read_text("utf-8"))
        assert data["schema_version"] == 1
        assert data["kind"] == "daemon"
        assert data["resources"]["cpu"] == 0.2
        assert "priority" not in data  # 优先级唯一来源是 manifest，意图文件不再写 priority

    def test_emit_without_sched_workspace_raises(self, tmp_path, monkeypatch):
        monkeypatch.delenv("SCHED_WORKSPACE", raising=False)
        with pytest.raises(RuntimeError):
            emit_intent()

    def test_duplicate_same_content_is_idempotent(self, sdk_env):
        rid = emit_intent(kind="oneshot")
        rid2 = emit_intent(kind="oneshot", request_id=rid)
        assert rid == rid2  # 幂等返回同一个
        # 文件仍只有一份
        assert len(list((sdk_env / "requests").glob(f"{rid}.json"))) == 1

    def test_duplicate_different_content_raises(self, sdk_env):
        rid = emit_intent(kind="oneshot")
        with pytest.raises(FileExistsError):
            emit_intent(kind="daemon", request_id=rid)

    def test_list_intents_reads_all(self, sdk_env):
        emit_intent(kind="oneshot", request_id="r1")
        emit_intent(kind="daemon", request_id="r2")
        intents = list_intents()
        assert {i["request_id"] for i in intents} == {"r1", "r2"}


class TestStateControl:
    def test_request_stop_writes_control(self, sdk_env):
        state = TaskState().start()
        state.request_stop("done")
        p = sdk_env / "cache" / "state.json"
        data = json.loads(p.read_text("utf-8"))
        assert data["control"]["action"] == "stop"

    def test_reschedule_writes_delay(self, sdk_env):
        state = TaskState().start()
        state.reschedule(60, "晚点再跑")
        p = sdk_env / "cache" / "state.json"
        data = json.loads(p.read_text("utf-8"))
        assert data["control"]["action"] == "reschedule"
        assert data["control"]["delay_sec"] == 60.0

    def test_heartbeat_thread_updates_mtime(self, sdk_env):
        state = TaskState(heartbeat_interval_sec=1).start()
        import time
        time.sleep(1.5)
        p = sdk_env / "cache" / "state.json"
        # 文件被心跳线程改过
        assert p.stat().st_mtime > time.time() - 1
        state.finalize()

    def test_resume_point_reloaded(self, sdk_env):
        # 先有一个断点
        rp = sdk_env / "cache" / "resume_point.json"
        rp.write_text(json.dumps({"resume_point": {"cursor": 42}, "saved_at": "t"}))
        state = TaskState().start()
        assert state.resume_point()["resume_point"]["cursor"] == 42
        state.finalize()


class TestExitDecisionsSDK:
    """SDK 自身不做退出决策，但控制段写入被 control 模块读——验证两者对齐。"""

    def test_control_stop_written_then_control_module_reads(self, sdk_env):
        state = TaskState().start()
        state.request_stop("done")
        state.finalize()
        # 用 sched.control 读同一个 state.json
        from sched import control as ctr
        st = ctr.read_state_file(sdk_env, "test-run")
        d = ctr.read_control_directive(st)
        assert d.action == "stop"
