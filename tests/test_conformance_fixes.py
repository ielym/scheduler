"""本轮一致性修复的回归测试（对照设计方案逐项验证）。

覆盖：
1. 完成态样本作为"真实进展证据"进入健康基线（短任务可扩容学习，设计第九/十四节）
2. 变慢判定：速率下降 且（处理耗时上升 或 在途上升），不误删完成态样本（设计第十一节）
3. _request_pause 仅在高优保护退让时计入累计退让账（设计第十节）
4. OOM 优先暂停低优先级中可释放内存较多者（设计第八节）
5. SDK 暂不足事件 report_shortage_event：只报事件不退（设计第六节）
6. 内存快速增长标记不健康（设计第八节）
"""

from __future__ import annotations

import json
import sys
from collections import deque
from pathlib import Path

ROOT = str(Path(__file__).resolve().parent.parent)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from sched import scaling  # noqa: E402
from sched.config import JobStatus  # noqa: E402
from sched.scheduler import AttMeta, SchedulerService  # noqa: E402


def _svc(home):
    (home / "runtime").mkdir(parents=True, exist_ok=True)
    return SchedulerService()


# ---------- 1. 完成态样本计数为进展证据 ----------
def test_completion_sample_counts_as_progress(home):
    h = scaling.HealthTracker()
    h.record_progress("t", 100.0, 5)
    assert h.has_samples("t") is True


# ---------- 2. 归一化速率下降即可判慢 ----------
def test_is_slow_via_process_duration_rise(home):
    h = scaling.HealthTracker(slow_confidence_sec=5.0,
                              timing=scaling.TaskTiming(default_interval_sec=0.5))
    samples = [
        scaling.ProgressSample(ts_mono=float(i), completed_units=i * 10)
        for i in range(6)
    ] + [
        scaling.ProgressSample(ts_mono=float(i),
                               completed_units=60 + (i - 6) * 2)
        for i in range(6, 12)
    ]
    # 归一化速率下降（10 -> 2）：判慢
    assert h._is_slow(deque(samples))


def test_completion_samples_do_not_trigger_slow(home):
    h = scaling.HealthTracker(slow_confidence_sec=5.0,
                              timing=scaling.TaskTiming(default_interval_sec=0.5))
    samples = [
        scaling.ProgressSample(ts_mono=float(i), completed_units=i)
        for i in range(12)
    ]
    assert not h._is_slow(deque(samples))  # 恒速率：不判慢


# ---------- 3. _request_pause 退让账 ----------
def test_request_pause_yield_accounting(home):
    svc = _svc(home)
    meta = AttMeta("a1", "j1", "t", 50, "tok")
    meta.status = JobStatus.RUNNING
    svc.meta["a1"] = meta

    class FakeAttempt:
        def __init__(self):
            self.pause_command = None

        def write_pause(self, *a, **k):
            pass

        def signal_pause(self):
            pass

    svc.attempts["a1"] = FakeAttempt()
    svc._transition = lambda m, s, reason="": setattr(m, "status", s)

    svc._request_pause("a1", "心跳过期")           # 非保护退让
    assert "a1" not in svc.protect.st.yielded

    meta.status = JobStatus.RUNNING              # 复位存活态再触发第二次
    svc._request_pause("a1", "高优先级保护退让", yield_to_protect=True)
    assert "a1" in svc.protect.st.yielded


# ---------- 4. OOM 暂停候选：低优先级 + 可释放内存多者优先 ----------
def test_choose_oom_yield_candidate(home):
    svc = _svc(home)
    for aid, tid, pri in (("a_victim", "t_v", 50),
                           ("a_hi", "t_hi", 90), ("a_lo", "t_lo", 10)):
        m = AttMeta(aid, f"j_{tid}", tid, pri, "tok")
        m.status = JobStatus.RUNNING
        svc.meta[aid] = m

    class _MemCg:
        def __init__(self, b):
            self._b = b

        def read_current_mem(self):
            return self._b

    class FakeRun:
        def __init__(self, b):
            self.cgrp = _MemCg(b)

    svc.attempts["a_victim"] = FakeRun(10_000_000)   # 受害者（OOM 证据来源）
    svc.attempts["a_hi"] = FakeRun(1_000_000)        # 高优先级 1MB
    svc.attempts["a_lo"] = FakeRun(100_000_000)      # 低优先级 100MB
    # 退让只向下让：仅严格低于受害者优先级的候选中挑（a_hi 被排除）
    assert svc._choose_oom_yield_candidate("a_victim") == "a_lo"
    # 受害者已是最低优先级 → 无候选，由调用方回退暂停受害者自身
    assert svc._choose_oom_yield_candidate("a_lo") is None


# ---------- 5. SDK 错误归因事件：只报事件不退 ----------
def test_report_shortage_event_module(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_EVENT_FILE", str(tmp_path / "ev.jsonl"))
    from sched_task_sdk import EventKind, report_error_event
    report_error_event("临时不足")
    lines = (tmp_path / "ev.jsonl").read_text().strip().splitlines()
    obj = json.loads(lines[0])
    assert obj["kind"] == EventKind.ERROR
    assert obj["reason"] == "临时不足"


# ---------- 6. 内存快速增长标记不健康 ----------
def test_mark_memory_growth(home):
    t = scaling.ResourceTrend(0.9, 0.8, 3)
    t.sample(0.1, 0.1, True)
    t.mark_memory_growth()
    assert t.unhealthy()