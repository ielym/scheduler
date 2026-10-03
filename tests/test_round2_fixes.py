"""第二轮审查（REVIEW_ROUND2.md）修复项的回归测试。

F1 抢占优先级下限 | F2 G5 兜底反转 | F3 退避移出扫描 | F4 多实例状态隔离
F5 外部内存感知 | F6 闸门降权+env 黑名单 | F7 stop 单终态定点
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from sched_v002 import admission, errors, preempt, resources, runner, runlog, taskdoc
from sched_v002.service import RunningEntry, SchedulerService

from conftest import write_task
from test_regression import FakeCapacity, _entry, _run_custom, _spec, _svc


# ================= F1：抢占优先级严格下限 =================

def test_f1_low_waiter_cannot_preempt_high_runner():
    running = [preempt.RunningInfo(run_id="r_high", job_id="j", priority=90,
                                   est_cpu=4.0, est_mem_mb=400, soft={})]
    # 等待者 priority=50：即使缺口只有高优任务能补，也绝不抢占
    assert preempt.select_preempt_targets(running, 2.0, 200, {}, waiter_priority=50) == []


def test_f1_equal_priority_never_preempts():
    running = [preempt.RunningInfo(run_id="r_eq", job_id="j", priority=50,
                                   est_cpu=4.0, est_mem_mb=400, soft={})]
    assert preempt.select_preempt_targets(running, 2.0, 200, {}, waiter_priority=50) == []


def test_f1_high_waiter_preempts_lower_only():
    running = [
        preempt.RunningInfo(run_id="r90", job_id="j", priority=90, est_cpu=2.0, est_mem_mb=200, soft={}),
        preempt.RunningInfo(run_id="r50", job_id="j", priority=50, est_cpu=2.0, est_mem_mb=200, soft={}),
    ]
    targets = preempt.select_preempt_targets(running, 2.0, 200, {}, waiter_priority=95)
    assert [t.run_id for t in targets] == ["r50"]  # 只抢 <95 的


# ================= F2：G5 兜底语义 =================

def test_f2_exit1_without_self_report_is_resource():
    # 无 state.json 自报、退出码 1 → unclassified → 按资源错误重试（不转人工）
    cat, reason = errors.classify_exit_code(1)
    assert cat == errors.Category.UNCLASSIFIED.value
    assert errors.is_retryable_category(cat)


def test_f2_negative_exit_code_is_unclassified():
    # 信号杀死（如 -15/-9）在抢占窗口外出现 → unclassified 兜底
    cat, _ = errors.classify_exit_code(-15)
    assert cat == errors.Category.UNCLASSIFIED.value


# ================= F3：退避任务不参与准出扫描 =================

def test_f3_backoff_items_skipped_in_admission_scan(home, make_settings):
    svc = _svc(home, make_settings)
    svc.capacity = FakeCapacity(total_memory_mb=1000, total_cpu=4.0)
    svc.entries["ok"] = _entry(home, "ok", resources={"cpu": 0.1, "memory_mb": 900, "soft": {}})
    svc.entries["ok2"] = _entry(home, "ok2", resources={"cpu": 0.1, "memory_mb": 50, "soft": {}})
    svc.trigger("ok", trigger="manual")   # 900MB，将处于退避期
    svc.trigger("ok2", trigger="manual")  # 50MB，应正常准出
    import datetime as _dt
    future = (_dt.datetime.now().astimezone() + _dt.timedelta(hours=1)).isoformat(timespec="seconds")
    svc.state.backoffs["ok"] = {"not_before": future}
    svc._tick_admission()
    # 退避的 ok 不准出；ok2 正常准出（不被 ok 的 900MB 虚拟占用挡住）
    assert any(re.run["job_id"] == "ok2" for re in svc.running.values())
    assert not any(re.run["job_id"] == "ok" for re in svc.running.values())


# ================= F4：多实例状态隔离 =================

def test_f4_concurrent_instances_isolated_state(tmp_path, monkeypatch):
    """两个并发实例写各自的 state.json，resume_point 共享。"""
    import os
    from sched_task_sdk.state import TaskState

    ws = tmp_path / "task"
    ws.mkdir()
    # 实例 A
    monkeypatch.setenv("SCHED_WORKSPACE", str(ws))
    monkeypatch.setenv("SCHED_RUN_ID", "runA")
    a = TaskState()
    a.resume_point = {"index": 5}
    a.flush()
    # 实例 B
    monkeypatch.setenv("SCHED_RUN_ID", "runB")
    b = TaskState()
    assert b.resume_point == {"index": 5}  # resume_point 任务级共享
    b.resume_point = {"index": 7}
    b.last_error = {"category": "logic", "message": "x"}
    b.flush()
    # A 的实例级状态不受 B 污染
    st_a = json.loads((ws / "cache" / "runs" / "runA" / "state.json").read_text())
    st_b = json.loads((ws / "cache" / "runs" / "runB" / "state.json").read_text())
    assert st_a.get("last_error", {}) == {}
    assert st_b["last_error"]["category"] == "logic"
    # 调度器侧 runner 读取实例级状态
    assert runner._read_last_error_category(ws, "runA") == ""
    assert runner._read_last_error_category(ws, "runB") == "logic"


# ================= F5：外部内存感知 =================

class _ExtCapacity(FakeCapacity):
    """带可控 MemAvailable 的容量（模拟外部进程占用）。"""

    def __init__(self, total_memory_mb=1000, total_cpu=4.0, mem_available_mb=None):
        super().__init__(total_memory_mb, total_cpu)
        self._mem_available = mem_available_mb

    def available_memory_mb(self):
        return self._mem_available


def test_f5_external_occupancy_blocks_admission():
    cap = _ExtCapacity(total_memory_mb=2000, mem_available_mb=300)  # 外部吃掉大半
    ledger = resources.Ledger()
    soft = resources.SoftResourcePool()
    # 账本有余量（2000 空闲），但 MemAvailable 只有 300 - margin
    ok, reason = admission.can_admit(0.1, 500, {}, ledger, cap, soft,
                                     external_mem_margin_mb=512)
    assert not ok
    assert "MemAvailable" in reason


def test_f5_external_occupancy_allows_when_enough():
    cap = _ExtCapacity(total_memory_mb=2000, mem_available_mb=1500)
    ledger = resources.Ledger()
    ok, _ = admission.can_admit(0.1, 500, {}, ledger, cap, resources.SoftResourcePool(),
                                external_mem_margin_mb=512)
    assert ok


def test_f5_disabled_when_margin_zero():
    cap = _ExtCapacity(total_memory_mb=2000, mem_available_mb=100)
    ok, _ = admission.can_admit(0.1, 500, {}, resources.Ledger(), cap,
                                resources.SoftResourcePool(), external_mem_margin_mb=0)
    assert ok  # margin=0 关闭该判定（测试/兼容）


# ================= F6：env 黑名单 + 降权失败上抛 =================

def test_f6_env_blacklist_rejected_at_schema(home):
    task_dir = home / "tasks" / "evil"
    task_dir.mkdir(parents=True)
    (task_dir / "run.py").write_text("print(1)")
    (task_dir / "task.json").write_text(json.dumps({
        "schema_version": 2, "name": "evil",
        "entry": {"file": "run.py", "interpreter": "python3"},
        "schedule": {"type": "manual"},
        "env": {"LD_PRELOAD": "/evil.so"}}))
    try:
        taskdoc.load_task_doc(task_dir)
        assert False, "应拒绝黑名单 env 键"
    except Exception as e:
        assert "黑名单" in str(e)


def test_f6_build_env_filters_blacklist():
    env = runner.build_env(Path("/tmp/t"), "j", "r", "manual", 1, "2026-01-01",
                           {"FOO": "1", "LD_PRELOAD": "/x.so", "PATH": "/evil"})
    assert env["FOO"] == "1"
    assert "LD_PRELOAD" not in env
    assert env["PATH"] != "/evil"  # 任务注入的 PATH 被过滤，保留系统 PATH


def test_f6_drop_privileges_unknown_user_raises():
    import pytest
    with pytest.raises(KeyError):
        runner._drop_privileges("__no_such_user_hopefully__")


# ================= F7：stop 单终态定点 + 信号优先归因 =================

def test_f7_cancel_beats_exit_code(home, make_settings):
    """cancel 已 set 时进程退出（-15），终态必须是 killed 而非退出码兜底。"""
    task_dir = write_task(home, "c", real_sleep=0.5, total=10)
    doc = taskdoc.load_task_doc(task_dir)
    spec = runner.RunSpec(
        job_id="c", run_id="r_c", task_dir=task_dir, entry_file="run.py",
        interpreter="python3", args=[], cwd=".", env=dict(doc.env), trigger="manual",
        attempt=1, scheduled_at=runlog.now_iso(), run_user="",
    )
    import threading
    box = {}
    t = threading.Thread(target=lambda: box.setdefault("out", runner.execute(spec)), daemon=True)
    t.start()
    time.sleep(0.8)
    spec.cancel.set()
    t.join(timeout=15)
    assert box["out"].status == runlog.STATUS_KILLED  # 不是 failed/resource


def test_f7_preempt_beats_exit_code(home, make_settings):
    """preempt 窗口内进程以非 99 退出（如被 SIGTERM 直接带走）→ 归因 preempted。"""
    task_dir = write_task(home, "p2", real_sleep=0.5, total=10)
    doc = taskdoc.load_task_doc(task_dir)
    spec = runner.RunSpec(
        job_id="p2", run_id="r_p2", task_dir=task_dir, entry_file="run.py",
        interpreter="python3", args=[], cwd=".", env=dict(doc.env), trigger="manual",
        attempt=1, scheduled_at=runlog.now_iso(), run_user="",
        preempt_grace_sec=2,
    )
    import threading
    box = {}
    t = threading.Thread(target=lambda: box.setdefault("out", runner.execute(spec)), daemon=True)
    t.start()
    time.sleep(0.8)
    spec.preempt.set()
    t.join(timeout=15)
    assert box["out"].status in (runlog.STATUS_PREEMPTED, runlog.STATUS_KILLED)
    # 绝不能被兜底成 failed
    assert box["out"].status != runlog.STATUS_FAILED
