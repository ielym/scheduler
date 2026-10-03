"""自动准入：tasks/ 下出现新任务目录即后台跑五闸，通过自动纳入调度；
失败记录原因并持久化，任务文件修改后立即重试（否则冷却到期再试），全程无手动注册。"""

from __future__ import annotations

import time

from sched import registry
from sched.service import SchedulerService

from conftest import write_task


def _wait_for(pred, timeout=40.0, interval=0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


def test_valid_task_auto_admitted(home, make_settings):
    write_task(home, "auto_ok", real_sleep=0.01)
    svc = SchedulerService(make_settings(run_user=""))
    assert svc._discover_unregistered() == ["auto_ok"]

    svc._kick_auto_admission()  # 后台线程串行跑五闸，不阻塞调用方

    # 五道闸通过 → 自动落库纳入调度；准入状态清空、无正在检查项
    assert _wait_for(lambda: "auto_ok" in registry.load_state())
    svc.refresh()
    assert "auto_ok" in svc.entries
    assert svc.admission_snapshot() == {}
    assert svc._admission_current is None


def test_bad_task_failed_then_retried_after_fix(home, make_settings):
    task_dir = write_task(home, "auto_bad", real_sleep=0.01)
    # 破坏入口：run.py 语法错误 → 闸门必然失败
    (task_dir / "run.py").write_text("def broken(:\n", encoding="utf-8")
    svc = SchedulerService(make_settings(run_user="", auto_admit_retry_sec=3600))

    svc._kick_auto_admission()
    assert _wait_for(
        lambda: (svc.admission_snapshot().get("auto_bad") or {}).get("status") == "failed")

    rec = svc.admission_snapshot()["auto_bad"]
    assert rec["attempts"] == 1
    assert rec["reason"]            # 失败原因必须可展示给用户
    assert rec["next_check_after"]
    assert "auto_bad" not in registry.load_state()

    # 冷却未到且文件未改 → 不重试
    assert svc._admission_due(rec, "auto_bad") is False

    # 修好文件 → fingerprint 变化 → 立即允许重试，无需等冷却
    write_task(home, "auto_bad", real_sleep=0.01)
    assert svc._admission_due(rec, "auto_bad") is True
    svc._kick_auto_admission()
    assert _wait_for(lambda: "auto_bad" in registry.load_state())
    assert "auto_bad" not in svc.admission_snapshot()


def test_failed_state_persisted_across_restart(home, make_settings):
    task_dir = write_task(home, "auto_bad2", real_sleep=0.01)
    (task_dir / "run.py").write_text("def broken(:\n", encoding="utf-8")
    svc = SchedulerService(make_settings(run_user="", auto_admit_retry_sec=3600))
    svc._kick_auto_admission()
    assert _wait_for(
        lambda: (svc.admission_snapshot().get("auto_bad2") or {}).get("status") == "failed")

    # 重新启动一个调度器实例：失败状态从 admission.json 恢复，页面仍能展示原因
    svc2 = SchedulerService(make_settings(run_user="", auto_admit_retry_sec=3600))
    rec2 = svc2.admission_snapshot()["auto_bad2"]
    assert rec2["status"] == "failed"
    assert rec2["reason"]


def test_registered_task_not_rediscovered(home, make_settings):
    write_task(home, "auto_reg", real_sleep=0.01)
    svc = SchedulerService(make_settings(run_user=""))
    svc._kick_auto_admission()
    assert _wait_for(lambda: "auto_reg" in registry.load_state())
    svc.refresh()
    assert svc._discover_unregistered() == []
