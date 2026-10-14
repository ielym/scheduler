"""设计方案对齐修复的专项回归（对应审计报告修复 1-11）。

每项修复与设计条款的对应：
1. 启动间隔稳定证据下平缓缩短 + 令牌制（设计第九节 L155）；
2. 内存快速增长触发低优先级退让（设计第八节 L137）；
3. outbox 分目录接收/归档/隔离（设计第二、五节）；
4. 重启后保护退让节流恢复（设计第十节 L179）；
5. 生产者崩溃恢复每轮独立维护（设计第十五节 L265）；
6. 变慢起因退让间隔加倍（设计第十一节 L195）；
7. 退让候选排序含暂停成本（设计第十节 L169）；
8. 保护截止按反馈周期/暂停耗时推导且有总上限（设计第十节 L181）；
9. 正常无输入等待不算变慢（设计第六节 L95）；
10. 控制节奏随任务反馈周期缩放（设计第十四节 L239）；
11. /proc/meminfo 不可读时内存占比不爆表（设计第八节观测不足语义）。
"""

from __future__ import annotations

import json
import sys
import threading
import time
from collections import deque
from pathlib import Path
from types import SimpleNamespace

ROOT = str(Path(__file__).resolve().parent.parent)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import pytest  # noqa: E402

from sched import config, receiver, runner, scaling  # noqa: E402
from sched.config import JobStatus  # noqa: E402
from sched.scheduler import AttMeta, SchedulerService, _read_total_mem_mb  # noqa: E402


def _write_task(home, task_id, priority=50, raw_extra=None):
    td = home / "tasks" / task_id
    td.mkdir(parents=True, exist_ok=True)
    (td / "producer.py").write_text("")
    (td / "run.py").write_text("")
    raw = {
        "schema_version": 1, "task_id": task_id, "enabled": True,
        "priority": priority,
        "producer_entry": {"file": "producer.py"},
        "executor_entry": {"file": "run.py"},
        "recovery": {"replay": "outbox", "retain": "测试保留", "replay_note": "测试重放"},
    }
    if raw_extra:
        raw.update(raw_extra)
    from sched import store
    store.save_json(td / "task.json", raw)
    return td


def _svc(home):
    (home / "runtime").mkdir(parents=True, exist_ok=True)
    return SchedulerService()


def _ready(job_id, task_id="t1", priority=None):
    doc = {
        "schema_version": 1, "job_id": job_id, "task_id": task_id,
        "entry_args": [], "env": {}, "idempotency_key": job_id,
    }
    if priority is not None:
        doc["priority"] = priority
    return config.parse_ready(doc)


class _FakePauseAttempt:
    """_request_pause / _tick_control / _tick_resource 所需的最小执行句柄。"""
    pid = 11
    pgid = 11
    start_time = 1.0

    def __init__(self):
        self.pause_command = None
        # _tick_resource 遍历 attempts 时访问 a.cgrp：默认无内存观测
        self.cgrp = _FakeMemCgroup(0)

    def write_pause(self, cmd_id, reason, deadline):
        self.pause_command = {"command_id": cmd_id, "reason": reason,
                              "deadline": deadline}

    def signal_pause(self):
        pass


class _FakeMemCgroup:
    """内存增长路径所需的 cgroup 假对象：只报内存。"""
    available = True

    def __init__(self, mem_mb):
        self._mem = mem_mb

    def read_cpu_usage_usec(self):
        return None

    def read_current_mem(self):
        return self._mem * 1024 * 1024

    def read_io_bytes(self):
        return None

    def read_oom_kill_count(self):
        return None


class _FakeMemAttempt:
    pid = 21
    pgid = 21
    start_time = 1.0

    def __init__(self, mem_mb):
        self.cgrp = _FakeMemCgroup(mem_mb)


# ---------- 修复 1：启动间隔阶梯缩短 + 令牌制 ----------

class TestStartIntervalLadder:
    def test_ladder_shortens_with_stable_health(self, home):
        svc = _svc(home)
        base = svc.cfg.control.start_interval_sec
        # 时间用正数基准：_healthy_since_mono<=0 视为未锚定，now=0 会反复重新锚定
        assert svc._effective_start_interval(1000.0) == base  # 冷启动锚定
        assert svc._effective_start_interval(1059.0) == base  # 未满 60s
        assert svc._effective_start_interval(1061.0) == pytest.approx(base * 0.5)
        assert svc._effective_start_interval(1181.0) == pytest.approx(base * 0.25)
        assert svc._effective_start_interval(1301.0) == pytest.approx(
            max(0.125, base * 0.125))

    def test_unhealthy_or_shortage_resets_to_base(self, home):
        svc = _svc(home)
        base = svc.cfg.control.start_interval_sec
        svc._effective_start_interval(1000.0)
        assert svc._effective_start_interval(1301.0) < base  # 已缩短
        svc.trend.mark_oom()
        assert svc._effective_start_interval(1302.0) == base  # 不健康立即回弹
        assert svc._healthy_since_mono == 0.0
        # 观测不足同样回弹（设计第八节：观测不足减慢探索）
        svc._observability_ok = False
        svc._effective_start_interval(1303.0)
        svc._observability_ok = True
        svc._effective_start_interval(1304.0)
        svc._on_explicit_shortage(AttMeta("a", "j", "t", 50, "tok"))
        assert svc._healthy_since_mono == 0.0

    def test_token_admission_multiple_starts_per_tick(self, home, monkeypatch):
        svc = _svc(home)
        _write_task(home, "t")
        svc.refresh_specs()
        svc.scaling.get("t").run_target = 10
        svc.scaling.get("t").mode = "steady"
        for i in range(8):
            svc.db.upsert_job(_ready(f"j-{i}", "t"), f"h{i}", "now", 50)

        class _Launch:
            pid = 1
            pgid = 1
            start_time = 1.0

        monkeypatch.setattr(runner, "launch", lambda aspec, refs: _Launch())
        t0 = 1000.0
        # 持续健康 400s → 有效间隔 0.125s；上一节拍距今 1s → 份额 1.0
        svc._healthy_since_mono = t0 - 400.0
        svc._last_admit_tick_mono = t0 - 1.0
        svc._admit_tokens = 0.0
        svc._tick_admit(t0)
        assert len(svc.attempts) == 8  # 单轮补齐一个节拍内的 8 个份额

    def test_token_admission_single_start_at_base(self, home, monkeypatch):
        svc = _svc(home)
        _write_task(home, "t")
        svc.refresh_specs()
        svc.scaling.get("t").run_target = 10
        for i in range(8):
            svc.db.upsert_job(_ready(f"j-{i}", "t"), f"h{i}", "now", 50)

        class _Launch:
            pid = 1
            pgid = 1
            start_time = 1.0

        monkeypatch.setattr(runner, "launch", lambda aspec, refs: _Launch())
        t0 = 1000.0
        svc._last_admit_tick_mono = t0 - 1.0  # 首次评估：冷启动一个份额
        svc._admit_tokens = 0.0
        svc._tick_admit(t0)
        assert len(svc.attempts) == 1  # 间隔=配置值：仍然每间隔只启动一个


# ---------- 修复 2：内存快速增长触发低优先级退让 ----------

class TestMemoryGrowthYield:
    def _setup(self, home):
        svc = _svc(home)
        svc.db.upsert_job(_ready("j-hi", "thi", priority=None), "hh", "now", 90)
        svc.db.upsert_job(_ready("j-lo", "tlo"), "hl", "now", 10)
        svc.db.upsert_job(_ready("j-lo2", "tlo2"), "hl2", "now", 10)
        for aid, jid in (("a-hi", "j-hi"), ("a-lo", "j-lo"), ("a-lo2", "j-lo2")):
            svc.db.create_attempt(aid, jid, 1, "tok", "t0")
        hi = AttMeta("a-hi", "j-hi", "thi", 90, "tok")
        lo = AttMeta("a-lo", "j-lo", "tlo", 10, "tok")
        lo2 = AttMeta("a-lo2", "j-lo2", "tlo2", 10, "tok")
        for m in (hi, lo, lo2):
            m.status = JobStatus.RUNNING
            svc.meta[m.attempt_id] = m
            svc.attempts[m.attempt_id] = _FakePauseAttempt()
        return svc, hi, lo, lo2

    def test_growth_yields_low_priority(self, home, monkeypatch):
        svc, hi, lo, lo2 = self._setup(home)
        monkeypatch.setattr("sched.scheduler._read_total_mem_mb", lambda: 1000)
        t1, t2 = 100.0, 103.0
        # 第一采样：100MB（建立 prev 基线）
        svc.attempts["a-hi"].cgrp = _FakeMemCgroup(100)
        svc._tick_resource(t1)
        # 第二采样：850MB（占比 0.85 ≥ 0.8，增长 750MB > 50MB 容量占比阈值）
        svc.attempts["a-hi"].cgrp = _FakeMemCgroup(850)
        svc._tick_resource(t2)
        assert svc.trend.unhealthy()  # 停止新增（既有语义保持）
        assert lo.status == JobStatus.PAUSING  # 退让低优先级（新增语义）
        assert svc.attempts["a-lo"].pause_command["reason"].startswith("执行内存快速增长")
        assert hi.status == JobStatus.RUNNING  # 高优先级不动

    def test_growth_yield_throttled(self, home, monkeypatch):
        svc, hi, lo, lo2 = self._setup(home)
        monkeypatch.setattr("sched.scheduler._read_total_mem_mb", lambda: 1000)
        svc.attempts["a-hi"].cgrp = _FakeMemCgroup(100)
        svc._tick_resource(100.0)
        svc.attempts["a-hi"].cgrp = _FakeMemCgroup(850)
        svc._tick_resource(103.0)
        assert lo.status == JobStatus.PAUSING
        # 节流窗口内再次增长：不再发起第二次退让
        svc.attempts["a-hi"].cgrp = _FakeMemCgroup(950)
        svc._tick_resource(106.0)
        assert lo2.status == JobStatus.RUNNING
        # 超过节流窗口（≥ max(3×采样间隔, 30s)）后恢复退让
        # 1010-950=60MB > 50MB 阈值；140-103=37s ≥ 30s 节流窗口
        svc.attempts["a-hi"].cgrp = _FakeMemCgroup(1010)
        svc._tick_resource(140.0)
        assert lo2.status == JobStatus.PAUSING


# ---------- 修复 3：outbox 分目录 ----------

class TestOutboxSubdirectories:
    def _spec(self, home, task_id="t1"):
        _write_task(home, task_id)
        return config.load_task(task_id)

    def test_receive_subdir_files_and_archive_by_job_id(self, home):
        spec = self._spec(home)
        out = home / "tasks" / "t1" / "outbox"
        (out / "batch-01").mkdir(parents=True)
        (out / "batch-02").mkdir(parents=True)
        doc_a = {"schema_version": 1, "job_id": "j-sub-a", "task_id": "t1",
                 "entry_args": [], "env": {}, "idempotency_key": "j-sub-a"}
        doc_b = {"schema_version": 1, "job_id": "j-sub-b", "task_id": "t1",
                 "entry_args": [], "env": {}, "idempotency_key": "j-sub-b"}
        (out / "batch-01" / "f.json").write_text(json.dumps(doc_a))
        (out / "batch-02" / "f.json").write_text(json.dumps(doc_b))  # 同名不同分目录
        s = receiver.receive_outbox("t1", spec, SchedulerService().db,
                                    "2026-10-12T00:00:00+00:00")
        assert s["created"] == 2  # 分目录内文件均被发现（设计第二节 L23）
        assert not list(out.rglob("*.json"))
        # 归档按 job_id 平铺（与归档对账路径一致），同名文件不冲突
        arch = home / "runtime" / "archive" / "t1"
        assert (arch / "j-sub-a.json").exists()
        assert (arch / "j-sub-b.json").exists()

    def test_isolate_preserves_subdir_structure(self, home):
        spec = self._spec(home)
        out = home / "tasks" / "t1" / "outbox" / "bad-batch"
        out.mkdir(parents=True)
        (out / "broken.json").write_text("{not json")
        s = receiver.receive_outbox("t1", spec, SchedulerService().db,
                                    "2026-10-12T00:00:00+00:00")
        assert s["invalid"] == 1
        iso = home / "runtime" / "isolation" / "t1" / "bad-batch" / "broken.json"
        assert iso.exists()

    def test_seen_tracked_by_relative_path(self, home):
        spec = self._spec(home)
        out = home / "tasks" / "t1" / "outbox"
        (out / "s1").mkdir(parents=True)
        (out / "s2").mkdir(parents=True)
        doc_a = {"schema_version": 1, "job_id": "j-x1", "task_id": "t1",
                 "entry_args": [], "env": {}, "idempotency_key": "j-x1"}
        doc_b = {"schema_version": 1, "job_id": "j-x2", "task_id": "t1",
                 "entry_args": [], "env": {}, "idempotency_key": "j-x2"}
        (out / "s1" / "a.json").write_text(json.dumps(doc_a))
        (out / "s2" / "a.json").write_text(json.dumps(doc_b))  # 同名不同分目录
        db = SchedulerService().db
        receiver.receive_outbox("t1", spec, db, "t0", batch_size=1)
        # 半数配额给新文件：只处理了一个，另一个下轮继续（相对路径区分）
        assert len(db.list_jobs("t1", limit=1000)) == 1
        receiver.receive_outbox("t1", spec, db, "t1")
        assert len(db.list_jobs("t1", limit=1000)) == 2
        # outbox 清空后再扫一轮：seen 记忆随本轮扫描结果清空（归档文件不滞留）
        receiver.receive_outbox("t1", spec, db, "t2")
        assert receiver._seen_files["t1"] == set()


# ---------- 修复 4：重启后保护退让节流恢复 ----------

class TestRestartThrottledResume:
    def test_recover_injects_yield_paused_into_pending_resume(self, home):
        svc = _svc(home)
        now = "2026-10-12T00:00:00+00:00"
        svc.db.upsert_job(_ready("j-y1", "lo"), "h1", now, 10)
        svc.db.upsert_job(_ready("j-y2", "lo"), "h2", now, 10)
        svc.db.upsert_job(_ready("j-y3", "lo2", priority=20), "h3", now, 20)
        svc.db.upsert_job(_ready("j-hb", "t"), "h4", now, 50)
        for jid in ("j-y1", "j-y2", "j-y3"):
            svc.db.set_job_status(jid, JobStatus.PAUSED,
                                  "高优先级保护退让（hi）；统一外部不足退出", now)
            svc.db.create_attempt(f"att-{jid}", jid, 1, "tok", now)
            svc.db.update_attempt(f"att-{jid}", status=JobStatus.PAUSED,
                                  finished_at=now)
        svc.db.set_job_status("j-hb", JobStatus.PAUSED, "心跳过期，疑似卡住", now)
        svc._recover()
        st = svc.protect.st
        # 退让任务按任务分组注入逐步放回队列；心跳过期作业不注入
        assert set(st.pending_resume) == {"lo", "lo2"}
        assert st.protected_task == ""  # 保护轮不重建（重启即视为本轮结束）
        assert st.pending_resume["lo2"] == 20
        # 节流：首个放回也要等一个 recovery_probe_sec
        assert st.next_release_mono > time.monotonic()

    def test_released_one_task_per_probe_interval(self, home):
        svc = _svc(home)
        now = "2026-10-12T00:00:00+00:00"
        svc.db.upsert_job(_ready("j-y1", "lo"), "h1", now, 10)
        svc.db.upsert_job(_ready("j-y2", "lo2", priority=20), "h2", now, 20)
        for jid, tid in (("j-y1", "lo"), ("j-y2", "lo2")):
            svc.db.set_job_status(jid, JobStatus.PAUSED,
                                  "撤销放回：hi 再次受阻，等待重新逐步放回", now)
            svc.db.create_attempt(f"att-{jid}", jid, 1, "tok", now)
            svc.db.update_attempt(f"att-{jid}", status=JobStatus.PAUSED,
                                  finished_at=now)
        svc._recover()
        st = svc.protect.st
        # 未到 next_release：不放回（重启不一次性倾泻）
        svc._tick_release(st.next_release_mono - 1.0)
        assert svc.db.get_job("j-y1")["status"] == JobStatus.PAUSED
        assert svc.db.get_job("j-y2")["status"] == JobStatus.PAUSED
        # 到点后每次只放回一个任务（优先级高者先）
        svc._tick_release(st.next_release_mono + 0.1)
        statuses = {svc.db.get_job(j)["status"] for j in ("j-y1", "j-y2")}
        assert statuses == {JobStatus.QUEUED, JobStatus.PAUSED}
        assert svc.db.get_job("j-y2")["status"] == JobStatus.QUEUED  # pri 20 先放


# ---------- 修复 5：生产者守护每轮独立维护 ----------

def test_producer_guard_ensured_every_tick(home, monkeypatch):
    svc = _svc(home)
    _write_task(home, "t")
    svc.refresh_specs()
    svc.cfg.control.loop_tick_sec = 0.05
    calls = []
    monkeypatch.setattr(svc.guard, "ensure",
                        lambda tid, spec: calls.append(tid) or False)
    t = threading.Thread(target=svc._loop, daemon=True)
    t.start()
    try:
        time.sleep(0.4)
    finally:
        svc._stopping.set()
        t.join(timeout=3)
    # 每 50ms 节拍调用一次（5s 节流只会得到 1 次）
    assert len(calls) >= 4


# ---------- 修复 6：变慢退让间隔加倍 ----------

class TestSlowYieldGap:
    def _setup(self, home):
        svc = _svc(home)
        svc.db.upsert_job(_ready("j-hi", "thi"), "h1", "now", 90)
        svc.db.upsert_job(_ready("j-lo", "tlo"), "h2", "now", 10)
        svc.db.create_attempt("a-hi", "j-hi", 1, "tok", "t0")
        svc.db.create_attempt("a-lo", "j-lo", 1, "tok", "t0")
        hi = AttMeta("a-hi", "j-hi", "thi", 90, "tok")
        lo = AttMeta("a-lo", "j-lo", "tlo", 10, "tok")
        hi.status = lo.status = JobStatus.RUNNING
        svc.meta["a-hi"] = hi
        svc.meta["a-lo"] = lo
        svc.attempts["a-lo"] = _FakePauseAttempt()
        return svc, lo

    def test_slow_cause_doubles_yield_gap(self, home):
        svc, lo = self._setup(home)
        t0 = 1000.0
        assert svc.protect.start_round("thi", 90, t0, reason="无报错持续变慢",
                                       cause="slow")
        svc.protect.note_step_mono(t0)  # 上次退让/控制动作时刻
        # yield_observe_sec=10：t0+15 处于 2×观察窗内 → 不再退让
        svc._tick_control(t0 + 15.0)
        assert lo.status == JobStatus.RUNNING
        # 明确不足起因（1×观察窗）在同一时刻已可退让
        svc.protect.st.cause = "shortage"
        svc._tick_control(t0 + 15.0)
        assert lo.status == JobStatus.PAUSING


# ---------- 修复 7：退让候选排序含暂停成本 ----------

class TestPauseCost:
    def test_pause_cost_ema(self, home):
        sc = scaling.Scaling(config.SchedulerConfig())
        assert sc.pause_cost("t") == 0.0
        sc.note_pause_cost("t", 10.0)
        assert sc.pause_cost("t") == pytest.approx(10.0)
        sc.note_pause_cost("t", 20.0)
        assert sc.pause_cost("t") == pytest.approx(10.0 * 0.7 + 20.0 * 0.3)

    def test_choose_yield_candidate_prefers_low_pause_cost(self, home):
        svc = _svc(home)
        svc.protect.start_round("hi", 90, 0.0, cause="shortage")
        running = [
            {"attempt_id": "a1", "task_id": "b1", "priority": 10,
             "shortage_recent": False, "grown_recently": False,
             "pause_cost": 5.0},
            {"attempt_id": "a2", "task_id": "b2", "priority": 10,
             "shortage_recent": False, "grown_recently": False,
             "pause_cost": 1.0},
        ]
        assert svc.protect.choose_yield_candidate(running, 1.0) == "a2"

    def test_running_infos_and_finalize_record_pause_cost(self, home):
        svc = _svc(home)
        svc.db.upsert_job(_ready("j1", "t"), "h1", "now", 50)
        svc.db.create_attempt("a1", "j1", 1, "tok", "t0")
        meta = AttMeta("a1", "j1", "t", 50, "tok")
        meta.status = JobStatus.RUNNING
        svc.meta["a1"] = meta
        svc.scaling.note_pause_cost("t", 7.0)
        infos = svc._running_infos()
        assert infos[0]["pause_cost"] == 7.0

        # _finalize PAUSED：真实暂停耗时进入 EMA
        meta.status = JobStatus.PAUSING
        meta.pause_command = {"reason": "高优先级保护退让（hi）"}
        meta.pause_requested_mono = time.monotonic() - 4.0

        class _FA:
            pid = 1
            pgid = 1
            start_time = 1.0

            def __init__(self):
                from sched.runner import AttemptRun  # noqa: F401
                self.proc = SimpleNamespace(returncode=0, wait=lambda timeout=None: 0)
                self.cgrp = SimpleNamespace(available=False, procs=lambda: [],
                                            kill_all=lambda: None)
                self.spec = SimpleNamespace(attempt_id="a1")

            def is_alive(self):
                return False

            def read_events(self, after_seq=0):
                return []

            def cleanup(self):
                pass

        svc._finalize(_FA(), meta, runner.now_iso())
        assert svc.scaling.pause_cost("t") >= 4.0


# ---------- 修复 8：保护截止推导 ----------

class TestProtectDeadlineDerivation:
    def test_window_from_feedback_interval(self, home):
        svc = _svc(home)
        cfg_deadline = svc.cfg.control.protect_deadline_sec
        assert svc.protect.start_round("A", 90, 0.0, cause="shortage")
        assert svc.protect.st.deadline_mono == pytest.approx(cfg_deadline)
        # 反馈周期 100s → 窗口 = max(300, 4×100) = 400
        assert svc.protect.start_round("B", 95, 10.0, cause="shortage",
                                       feedback_interval_sec=100.0)
        st = svc.protect.st
        assert st.deadline_mono == pytest.approx(10.0 + 400.0)
        assert st.deadline_cap_mono == pytest.approx(10.0 + 800.0)

    def test_yield_extension_uses_pause_cost_and_caps(self, home):
        svc = _svc(home)
        svc.protect.start_round("A", 90, 0.0, cause="shortage",
                                feedback_interval_sec=100.0)  # 窗口 400，上限 800
        st = svc.protect.st
        svc.protect.note_yielded("att-1", "lo", 10, pause_cost_sec=30.0)
        assert st.deadline_mono == pytest.approx(430.0)  # 按实际暂停宽限延长
        svc.protect.note_yielded("att-2", "lo2", 20, pause_cost_sec=400.0)
        assert st.deadline_mono == pytest.approx(800.0)  # 受总上限约束
        svc.protect.note_yielded("att-3", "lo3", 20, pause_cost_sec=100.0)
        assert st.deadline_mono == pytest.approx(800.0)  # 不无限续期

    def test_yield_extension_defaults_to_grace_max(self, home):
        svc = _svc(home)
        svc.protect.start_round("A", 90, 0.0, cause="shortage")
        grace_max = svc.cfg.control.pause_grace_max_sec
        before = svc.protect.st.deadline_mono
        svc.protect.note_yielded("att-1", "lo", 10)  # 未传成本：回退宽限上限
        assert svc.protect.st.deadline_mono == pytest.approx(before + grace_max)


# ---------- 修复 9：正常无输入等待不算变慢 ----------

class TestWaitingNotSlow:
    def _samples(self, waiting_back):
        samples = []
        for i in range(10):
            back = i >= 5
            samples.append(scaling.ProgressSample(
                ts_mono=float(i * 30), completed_units=i + 1,
                work_units=float(i * 100 if not back else 400 + (i - 5) * 10),
                stage="s", active_duration_sec=2.0 if back else 1.0,
                in_flight_duration_sec=50.0 if back else 10.0,
                has_unfinished=True,
                waiting=(45.0 if back else None) if waiting_back else None))
        return deque(samples)

    def test_slow_without_waiting_signal(self):
        h = scaling.HealthTracker()
        assert h._is_slow(self._samples(waiting_back=False)) is True

    def test_not_slow_when_back_half_waiting_dominates(self):
        h = scaling.HealthTracker()
        # 后半窗口 waiting 225 ≥ 0.8×在途 250：判为等待输入，非变慢
        assert h._is_slow(self._samples(waiting_back=True)) is False


# ---------- 修复 10：控制节奏随反馈周期缩放 ----------

class TestControlRhythmScaling:
    def test_grow_observe_clamped_to_feedback_interval(self, home):
        svc = _svc(home)
        _write_task(home, "t-fast")  # 无申报 → 保持配置值
        _write_task(home, "t-mid", raw_extra={"feedback_interval_sec": 20.0})
        _write_task(home, "t-long", raw_extra={"feedback_interval_sec": 200.0})
        svc.refresh_specs()
        assert svc.scaling.get("t-fast").grow_observe_sec == \
            svc.cfg.control.grow_observe_sec
        assert svc.scaling.get("t-mid").grow_observe_sec == 80.0   # 4×20
        assert svc.scaling.get("t-long").grow_observe_sec == 600.0  # 封顶

    def test_backoff_cap_scaled_by_feedback_interval(self, home):
        svc = _svc(home)
        _write_task(home, "t-long", raw_extra={"feedback_interval_sec": 200.0})
        svc.refresh_specs()
        svc.db.upsert_job(_ready("j-1", "t-long"), "h1", "now", 50)
        svc.db.set_job_backoff("j-1", 30, "")  # 历史计数已高 → 直接触顶
        from datetime import datetime, timezone
        t0 = datetime.now(timezone.utc)
        svc._schedule_backoff_retry("j-1")
        nr = datetime.fromisoformat(svc.db.get_job("j-1")["next_retry_at"])
        delay = (nr - t0).total_seconds()
        # cap = min(3600, max(900, 8×200)) = 1600（含抖动 ≤ ×1.25）
        assert 1600.0 <= delay <= 1600.0 * 1.25 + 0.1


# ---------- 修复 11：/proc/meminfo 不可读不爆表 ----------

def test_meminfo_unavailable_does_not_block_admission(home, monkeypatch):
    svc = _svc(home)
    monkeypatch.setattr("sched.scheduler._read_total_mem_mb", lambda: 0)
    meta = AttMeta("a1", "j1", "t", 50, "tok")
    meta.status = JobStatus.RUNNING
    svc.meta["a1"] = meta
    svc.attempts["a1"] = _FakeMemAttempt(500)  # 500MB 执行内存
    svc._tick_resource(100.0)
    # total_mem=0 → mem_ratio=0（未知而非爆表）：趋势保持健康，准入不被误停
    assert svc.trend.healthy_enough() is True
    assert svc._tick_admit(100.5) is None  # 不因读数失败炸出异常
    # 对照：真实 meminfo 可读时同量内存占比远超阈值
    assert _read_total_mem_mb() > 0
