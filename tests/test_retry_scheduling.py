"""讨论一整改专项测试：任务自治 retry 策略、时效截止、SJF、时长预估、动态画像与端到端场景。"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta

import pytest
from pydantic import ValidationError

from sched import admission, config, runlog, store, taskdoc
from sched.registry import JobEntry
from sched.service import RunningEntry, SchedulerService

from conftest import write_task


def _wait(pred, timeout=40.0, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


def _wait_terminal(job_id, n=1, timeout=40.0):
    assert _wait(lambda: len(runlog.query_runs(job_id=job_id, limit=100)) >= n, timeout), \
        f"{job_id} 未在 {timeout}s 内出现 {n} 条终态"


def _decisions():
    return store.read_jsonl(config.decisions_log())


class FakeCapacity:
    def __init__(self, total_memory_mb=1000, total_cpu=4.0):
        self.total_memory_mb = total_memory_mb
        self.total_cpu = total_cpu


# ================= RetrySpec schema =================

def test_retry_spec_defaults():
    r = taskdoc.RetrySpec()
    assert r.delay_sec == 0.0 and r.max_attempts == 0 and r.not_after == ""


def test_retry_spec_rejects_bad_values():
    with pytest.raises(ValidationError):
        taskdoc.RetrySpec(delay_sec=-1)
    with pytest.raises(ValidationError):
        taskdoc.RetrySpec(max_attempts=-2)
    with pytest.raises(ValidationError):
        taskdoc.RetrySpec(not_after="下班以后")


def test_retry_spec_accepts_iso_and_daily_not_after():
    assert taskdoc.RetrySpec(not_after="2026-10-04T18:00:00+08:00").not_after
    assert taskdoc.RetrySpec(not_after="18:00").not_after
    assert taskdoc.RetrySpec(not_after=" 18:00:30 ").not_after == "18:00:30"


# ================= retry_deadline / resolve_retry =================

def test_retry_deadline_daily_and_absolute():
    now = datetime(2026, 10, 4, 12, 0, tzinfo=None)
    dl = taskdoc.retry_deadline("18:30", now)
    assert dl == datetime(2026, 10, 4, 18, 30)
    dl_past = taskdoc.retry_deadline("09:00", now)
    assert now > dl_past  # 今日 09:00 已过 → 截止已到
    iso = "2026-10-05T08:00:00+08:00"
    dl2 = taskdoc.retry_deadline(iso, now.astimezone())
    assert dl2 == datetime.fromisoformat(iso)
    assert taskdoc.retry_deadline("", now) is None


def test_resolve_retry_static_then_hint_override():
    doc = taskdoc.RetrySpec(delay_sec=10, max_attempts=0, not_after="")
    # 包成最小 doc 供 resolve_retry 使用
    class _D:
        retry = doc
    now = datetime.now().astimezone()
    pol = taskdoc.resolve_retry(_D(), None, now)
    assert pol["delay_sec"] == 10.0 and pol["max_attempts"] == 0 and pol["deadline"] is None
    # 运行时 hint 收紧三要素
    pol2 = taskdoc.resolve_retry(_D(), {"retry_hint": {
        "delay_sec": 2, "max_attempts": 3, "not_after": "2020-01-01T00:00:00+08:00"}}, now)
    assert pol2["delay_sec"] == 2.0 and pol2["max_attempts"] == 3
    assert pol2["deadline"] is not None and now >= pol2["deadline"]


def test_resolve_retry_bad_hint_falls_back_to_static():
    class _D:
        retry = taskdoc.RetrySpec(delay_sec=9, max_attempts=7, not_after="12:00")
    now = datetime.now().astimezone()
    pol = taskdoc.resolve_retry(_D(), {"retry_hint": {"delay_sec": -999}}, now)
    assert pol["delay_sec"] == 9 and pol["max_attempts"] == 7 and pol["not_after"] == "12:00"
    # 非 dict hint / doc=None 均安全
    assert taskdoc.resolve_retry(None, {"retry_hint": "x"}, now)["max_attempts"] == 0


# ================= 时长画像 + 动态采样 =================

def _svc_entry(home, make_settings, name="ok", **kw):
    write_task(home, name, **kw)
    task_dir = home / "tasks" / name
    doc = taskdoc.load_task_doc(task_dir)
    svc = SchedulerService(make_settings(run_user=""))
    svc.entries[name] = JobEntry(job_id=name, task_dir=task_dir, doc=doc, enabled=True)
    return svc


def test_duration_samples_only_on_success(home, make_settings):
    svc = _svc_entry(home, make_settings, real_sleep=0.05, total=3)
    # 成功终态：采时长样本（≥1s 取整）
    re_ok = RunningEntry(
        run={"run_id": "r-ok", "job_id": "ok", "dry_run": False, "duration_ms": 1200,
             "queued_at": runlog.now_iso(), "status": "running", "trigger": "manual", "attempt": 1},
        spec=None, est_cpu=0.1, est_mem_mb=64, cgroup_name="ok/r-ok")
    svc._finalize(re_ok, runlog.STATUS_SUCCESS, mem_peak_mb=50, cpu_sec=0.6)
    assert svc.profiles.estimate_duration_sec("ok") is not None
    # 资源错误终态：残缺耗时不入时长样本
    re_bad = RunningEntry(
        run={"run_id": "r-bad", "job_id": "ok", "dry_run": False, "duration_ms": 9000,
             "queued_at": runlog.now_iso(), "status": "resource", "trigger": "manual", "attempt": 1},
        spec=None, est_cpu=0.1, est_mem_mb=64, cgroup_name="ok/r-bad")
    svc._finalize(re_bad, runlog.STATUS_RESOURCE, mem_peak_mb=50, cpu_sec=0.5)
    prof = svc.profiles.get("ok", 0.1, 64)
    assert prof.durations == [1.0]  # 9s 的残缺值未进窗
    assert prof.p50_duration_sec() == 1.0


def test_duration_p50_median_window():
    from sched.resources import Profile
    p = Profile(declared_cpu=0.1, declared_memory_mb=64)
    for d in (1, 2, 100, 3, 4):
        p.record_duration(d, window=8)
    assert p.p50_duration_sec() == 3  # 中位数，抗长尾
    assert p.estimate_duration_sec() == 3
    # 滑窗截断
    for d in range(10):
        p.record_duration(10 + d, window=8)
    assert len(p.durations) == 8


def test_live_profile_recording_updates_during_run(home, make_settings):
    svc = _svc_entry(home, make_settings)
    svc.profile_live_interval_sec = 0.0  # 测试不等 60s
    re = RunningEntry(
        run={"run_id": "r-live", "job_id": "ok", "dry_run": False, "trigger": "manual"},
        spec=None, est_cpu=0.1, est_mem_mb=64, cgroup_name="ok/r-live")
    re.live_cpu_cores = 0.35
    re.live_mem_mb = 77
    svc._maybe_record_live_profile(re)
    prof = svc.profiles.get("ok", 0.1, 64)
    assert prof.samples[-1] == (0.35, 77)  # 运行中样本已入画像窗
    # 节流：间隔未到不重复采样
    n = len(prof.samples)
    svc.profile_live_interval_sec = 1000.0
    svc._maybe_record_live_profile(re)
    assert len(svc.profiles.get("ok", 0.1, 64).samples) == n


# ================= SJF 排序 =================

def test_order_waiting_sjf_same_priority():
    items = [
        {"run_id": "long", "job_id": "j-long", "priority": 50, "enqueued_at": "2026-10-04T10:00:00+08:00"},
        {"run_id": "short", "job_id": "j-short", "priority": 50, "enqueued_at": "2026-10-04T10:00:01+08:00"},
        {"run_id": "unknown", "job_id": "j-new", "priority": 50, "enqueued_at": "2026-10-04T10:00:02+08:00"},
    ]
    dur = {"j-long": 100.0, "j-short": 1.0}  # 新任务无样本 → +inf
    out = admission.order_waiting(items, starvation_sec=3600,
                                  now_iso="2026-10-04T10:00:03+08:00",
                                  get_duration_sec=lambda j: dur.get(j))
    assert [x["run_id"] for x in out] == ["short", "long", "unknown"]
    # 高优先级仍压过 SJF
    items[0]["priority"] = 90
    out2 = admission.order_waiting(items, 3600, "2026-10-04T10:00:03+08:00",
                                   get_duration_sec=lambda j: dur.get(j))
    assert out2[0]["run_id"] == "long"


def test_sjf_admission_picks_short_job_first(home, make_settings):
    # 容量只够一个 600MB 任务；两个同优先级任务，画像显示一短一长 → 短的先准出
    svc = SchedulerService(make_settings(run_user=""))
    svc.capacity = FakeCapacity(total_memory_mb=1000, total_cpu=4.0)
    for name in ("sjf_long", "sjf_short"):
        write_task(home, name, resources={"cpu": 0.1, "memory_mb": 600}, real_sleep=0.01)
        svc.register_job(name)
        svc.entries[name].enabled = True
    svc.profiles.record_duration("sjf_long", 100)
    svc.profiles.record_duration("sjf_short", 1)
    # 长作业先入队（若按 FIFO 会长的先跑），短作业后入队
    svc.trigger("sjf_long", trigger="manual")
    svc.trigger("sjf_short", trigger="manual")
    svc._tick_admission()
    running = [re.run["job_id"] for re in svc.running.values()]
    assert running == ["sjf_short"]
    waiting = [it["job_id"] for it in svc.state.wq]
    assert waiting == ["sjf_long"]


# ================= 端到端：真实子任务场景 =================

def test_e2e_success_run(home, make_settings):
    write_task(home, "e2e_ok", real_sleep=0.02)
    svc = SchedulerService(make_settings(run_user=""))
    svc.register_job("e2e_ok")
    svc.trigger("e2e_ok", trigger="manual")
    svc._tick_admission()
    _wait_terminal("e2e_ok")
    rec = runlog.query_runs(job_id="e2e_ok", limit=1)[0]
    assert rec["status"] == "success"
    assert svc.state.wq == []  # 成功不冷却、不重排队


def test_e2e_exit101_immediate_requeue_then_giveup_at_cap(home, make_settings):
    # 任务声明最多尝试 2 次：101 → 立即重排队 → 再 101 → giveup 转人工
    write_task(home, "e2e_101", extra_env={"SCHED_TASK_EXIT_CODE": "101"},
               retry={"max_attempts": 2}, real_sleep=0.01)
    svc = SchedulerService(make_settings(run_user=""))
    svc.register_job("e2e_101")
    svc.trigger("e2e_101", trigger="manual")
    svc._tick_admission()
    _wait_terminal("e2e_101", n=1)
    assert len(svc.state.wq) == 1 and svc.state.wq[0]["attempt"] == 2
    assert svc.state.wq[0]["not_before"] == ""  # 立即重排队，无退避
    svc._tick_admission()
    _wait_terminal("e2e_101", n=2)
    assert _wait(lambda: not svc.state.wq and not svc.running)
    runs = runlog.query_runs(job_id="e2e_101", limit=10)
    assert [r["status"] for r in runs] == ["resource", "resource"]
    assert any(d.get("act") == "giveup" for d in _decisions())


def test_e2e_exit100_delay_sec_blocks_admission_until_due(home, make_settings):
    write_task(home, "e2e_delay", extra_env={"SCHED_TASK_EXIT_CODE": "100"},
               retry={"delay_sec": 3600}, real_sleep=0.01)
    svc = SchedulerService(make_settings(run_user=""))
    svc.register_job("e2e_delay")
    svc.trigger("e2e_delay", trigger="manual")
    svc._tick_admission()
    _wait_terminal("e2e_delay", n=1)
    assert len(svc.state.wq) == 1
    it = svc.state.wq[0]
    assert it["attempt"] == 2 and it["not_before"]
    # 未到 not_before：再怎么 tick 都不准出
    svc._tick_admission()
    assert not svc.running
    assert len(svc.state.wq) == 1
    # 到期后正常准出
    it["not_before"] = (datetime.now().astimezone() - timedelta(seconds=1)).isoformat(timespec="seconds")
    svc._tick_admission()
    assert _wait(lambda: bool(svc.running))


def test_e2e_not_after_expired_via_runtime_hint(home, make_settings):
    # 任务运行时按当天情况写 retry_hint（绝对时刻已过期）→ 101 后直接放弃
    hint = json.dumps({"not_after": "2020-01-01T00:00:00+08:00"})
    write_task(home, "e2e_expired", extra_env={"SCHED_TASK_EXIT_CODE": "101",
                                               "SCHED_TASK_RETRY_HINT": hint},
               real_sleep=0.01)
    svc = SchedulerService(make_settings(run_user=""))
    svc.register_job("e2e_expired")
    svc.trigger("e2e_expired", trigger="manual")
    svc._tick_admission()
    _wait_terminal("e2e_expired", n=1)
    assert _wait(lambda: not svc.state.wq)
    assert any(d.get("act") == "giveup_expired" for d in _decisions())


def test_e2e_daily_not_after_expired(home, make_settings):
    # 每日时刻 00:00：任何晚于零点的运行都已过当日截止
    hint = json.dumps({"not_after": "00:00"})
    write_task(home, "e2e_daily", extra_env={"SCHED_TASK_EXIT_CODE": "101",
                                             "SCHED_TASK_RETRY_HINT": hint},
               real_sleep=0.01)
    svc = SchedulerService(make_settings(run_user=""))
    svc.register_job("e2e_daily")
    svc.trigger("e2e_daily", trigger="manual")
    svc._tick_admission()
    _wait_terminal("e2e_daily", n=1)
    assert _wait(lambda: not svc.state.wq)
    assert any(d.get("act") == "giveup_expired" for d in _decisions())


def test_e2e_heartbeat_timeout_requeues_once(home, make_settings):
    # 卡死（不刷心跳）→ 心跳超时 SIGTERM → 按 interrupted 语义重入队一次并打标 hb_requeues
    write_task(home, "e2e_hb", extra_env={"SCHED_TASK_HB_STALL": "1"},
               heartbeat_timeout=5, real_sleep=0.01)
    svc = SchedulerService(make_settings(run_user=""))
    svc.register_job("e2e_hb")
    # 用 interval 触发：手动触发心跳阈值翻倍，会拖长测试
    svc.trigger("e2e_hb", trigger="interval")
    svc._tick_admission()
    assert _wait(lambda: bool(svc.running), timeout=20)

    def _saw_kill():
        svc._tick_monitor()
        return any(d.get("act") == "heartbeat_kill" for d in _decisions())

    assert _wait(_saw_kill, timeout=30), "心跳超时应被 monitor 检出并杀"
    assert _wait(lambda: len(svc.state.wq) == 1 and not svc.running, timeout=30)
    it = svc.state.wq[0]
    assert it["hb_requeues"] == 1
    # 第二次再心跳超时即转人工（hb_requeues 已达 1，下面直接验证判定）
    rec2 = {"job_id": "e2e_hb", "run_id": it["run_id"], "status": "killed",
            "error": "心跳超时（99s > 5s）", "hb_requeues": 1}
    svc._after_finalize(rec2, runlog.STATUS_KILLED)
    assert svc.state.wq == [it]  # 未再产生新等待项（转人工）


def test_e2e_logic_failure_goes_terminal_no_requeue(home, make_settings):
    # 逻辑错误（RuntimeError，非资源码）→ failed 转人工，不自动重排队
    write_task(home, "e2e_logic", extra_env={"SCHED_TASK_FAIL": "1"},
               logic_regex=["timeout simulated"], real_sleep=0.01)
    svc = SchedulerService(make_settings(run_user=""))
    svc.register_job("e2e_logic")
    svc.trigger("e2e_logic", trigger="manual")
    svc._tick_admission()
    _wait_terminal("e2e_logic")
    rec = runlog.query_runs(job_id="e2e_logic", limit=1)[0]
    assert rec["status"] == "failed"
    assert svc.state.wq == []
    assert any(d.get("act") == "terminal" for d in _decisions())
