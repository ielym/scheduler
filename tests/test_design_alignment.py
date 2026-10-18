"""设计方案对齐修复的专项回归（对应审计报告修复 1-11）。

每项修复与设计条款的对应：
1. 启动节奏改为反馈轴限速（方案 A′）：并发预算 + 采样周期硬下界，
   取代"健康时长阶梯 + 令牌制"；
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


# ---------- 方案 A′：反馈轴限速（并发预算 + 采样周期下界，取代令牌制） ----------

class _FakeLaunch:
    pid = 1
    pgid = 1
    start_time = 1.0


def _prep_admit(home, monkeypatch, tasks=("ta", "tb"), jobs=4):
    svc = _svc(home)
    for tid in tasks:
        _write_task(home, tid)
    svc.refresh_specs()
    for tid in tasks:
        svc.scaling.get(tid).run_target = 10
        svc.scaling.get(tid).mode = "steady"
        for i in range(jobs):
            svc.db.upsert_job(_ready(f"j{tid}-{i}", tid), f"h{tid}{i}", "now", 50)
    monkeypatch.setattr(runner, "launch", lambda aspec, refs: _FakeLaunch())
    return svc


class TestAdmitPacingFeedbackAxis:
    def test_cold_start_budget_one_then_additive_growth(self, home, monkeypatch):
        """冷启动预算=1：只放 1 个新启动；完成一步（最高置信证据）释放预算位，
        下一轮加法 +1（一次只 +1），不靠压缩启动间隔提速。"""
        svc = _prep_admit(home, monkeypatch, tasks=("ta",), jobs=8)
        t0 = 1000.0
        assert svc._admit_budget == 1
        svc._tick_admit(t0)
        assert len(svc.attempts) == 1 and svc._pending_count() == 1
        # 该新执行完成一步 → 解除任务逐个拉取占位 + 释放全局预算位
        aid = next(iter(svc.meta))
        svc.tbackoff.note_progress("ta", aid, t0 + 0.1)
        svc._release_pending_verified(aid)
        assert svc._pending_count() == 0
        svc._tick_admit(t0 + 2.0)
        assert svc._admit_budget == 2          # 加法增长（A）
        assert len(svc.attempts) == 2

    def test_sample_interval_hard_lower_bound(self, home, monkeypatch):
        """硬时间下界：两次启动间隔 ≥ 采样周期（观测能力下界，不可压到采样以下）。"""
        svc = _prep_admit(home, monkeypatch, tasks=("ta", "tb"))
        svc._admit_budget = 5                  # 预算充足，隔离预算门
        t0 = 1000.0
        svc._tick_admit(t0)
        assert len(svc.attempts) == 1
        svc._tick_admit(t0 + 0.5)              # 采样周期内：不再启动
        assert len(svc.attempts) == 1
        svc._tick_admit(t0 + 2.0)              # 越过一个采样周期：放行
        assert len(svc.attempts) == 2

    def test_unhealthy_stops_admission(self, home, monkeypatch):
        svc = _prep_admit(home, monkeypatch, tasks=("ta",))
        svc.trend.mark_oom()
        svc._tick_admit(1000.0)
        assert not svc.attempts                  # 主闸：资源不健康停新增

    def test_budget_multiplicative_shrink_and_recover(self, home):
        """乘法收缩（M）：资源越线 → max(1, BUDGET//2)；不健康期只缩一次。"""
        svc = _svc(home)
        svc._admit_budget = 8
        svc.trend.mark_oom()
        svc._update_admit_budget(1000.0)
        assert svc._admit_budget == 4
        svc._update_admit_budget(1001.0)        # episode 去重：不连缩
        assert svc._admit_budget == 4
        for _ in range(4):
            svc.trend.sample(0.1, 0.1, True)    # 连续低占用 → 恢复健康
        assert svc.trend.healthy_enough()
        svc._update_admit_budget(1002.0)
        svc.trend.mark_oom()
        svc._update_admit_budget(1003.0)
        assert svc._admit_budget == 2

    def test_pending_secondary_evidence_releases_budget(self, home):
        """次级验证：存活 ≥1 采样周期 + 资源连续不增长 + 本任务无近期 75
        → 释放预算位，长单步任务不必久等首步。"""
        svc = _svc(home)
        meta = AttMeta("a1", "j1", "t", 50, "tok")
        meta.status = JobStatus.RUNNING
        meta.started_mono = 100.0
        svc.meta["a1"] = meta
        svc._pending_new["a1"] = {"task_id": "t", "started_mono": 100.0}
        svc._mem_hist.extend([20.0, 20.0, 20.0])   # 连续不增长
        svc._release_verified_pending(104.0)       # 存活 4s ≥ 采样周期 2s
        assert "a1" not in svc._pending_new
        assert svc._verified_since_grow == 1

    def test_pending_secondary_blocked_by_recent_shortage(self, home):
        svc = _svc(home)
        meta = AttMeta("a1", "j1", "t", 50, "tok")
        meta.status = JobStatus.RUNNING
        meta.started_mono = 100.0
        svc.meta["a1"] = meta
        svc._pending_new["a1"] = {"task_id": "t", "started_mono": 100.0}
        svc._mem_hist.extend([20.0, 20.0, 20.0])
        svc._last_shortage_mono["t"] = 102.0       # 近期新 75
        svc._release_verified_pending(104.0)       # 一个观察窗口内 → 不释放
        assert "a1" in svc._pending_new


class TestTaskBackoffRestore:
    def test_restore_needs_consecutive_steps(self):
        tb = scaling.TaskBackoff(probe_base_sec=5.0, probe_cap_sec=10.0,
                                 restore_steps=2)
        tb.set_state("t", tb.VERIFY)
        tb.note_progress("t", "a1", 1.0)
        assert tb.state_of("t") == tb.VERIFY     # 一步不足以恢复
        tb.note_progress("t", "a1", 2.0)
        assert tb.state_of("t") == tb.NORMAL     # 连续两步 → 恢复

    def test_resource_error_resets_step_streak(self):
        tb = scaling.TaskBackoff(probe_base_sec=5.0, probe_cap_sec=10.0,
                                 restore_steps=2)
        tb.set_state("t", tb.VERIFY)
        tb.note_progress("t", "a1", 1.0)          # 第 1 步
        tb.clear_attempt("t", "a1", resource_error=True)  # 又报 75：连续步清零
        tb.note_progress("t", "a2", 2.0)          # 重新只算第 1 步
        assert tb.state_of("t") == tb.VERIFY

    def test_may_grow_observe_is_evidence_driven(self):
        class Cfg:
            class control:
                grow_observe_sec = 30.0
        s = scaling.Scaling(Cfg())
        s.get("t").run_target = 2
        s.grew_at("t", 10.0)                      # mode=steady, last_grow=10
        # 观察期内：无新证据 → 不放行
        assert not s.may_grow("t", 15.0, live_count=2, has_progress=True,
                              healthy_resource=True)
        # 观察期内：有新进展证据 → 证据驱动放行（不等墙钟）
        assert s.may_grow("t", 15.0, live_count=2, has_progress=True,
                          healthy_resource=True, evidence_since_grow=True)
        # 越过安全上限秒兜底 → 无证据也放行（防探索停摆）
        assert s.may_grow("t", 45.0, live_count=2, has_progress=True,
                          healthy_resource=True)


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
        out = config.outbox_dir("t1")
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
        arch = config.archive_root() / "t1"
        assert (arch / "j-sub-a.json").exists()
        assert (arch / "j-sub-b.json").exists()

    def test_isolate_preserves_subdir_structure(self, home):
        spec = self._spec(home)
        out = config.outbox_dir("t1") / "bad-batch"
        out.mkdir(parents=True)
        (out / "broken.json").write_text("{not json")
        s = receiver.receive_outbox("t1", spec, SchedulerService().db,
                                    "2026-10-12T00:00:00+00:00")
        assert s["invalid"] == 1
        iso = config.isolation_root() / "t1" / "bad-batch" / "broken.json"
        assert iso.exists()

    def test_seen_tracked_by_relative_path(self, home):
        spec = self._spec(home)
        out = config.outbox_dir("t1")
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

class TestRestartColdStart:
    def test_restart_requeues_all_unfinished_and_no_protect_restore(self, home):
        # 重启冷启动：不重建保护轮、不注入逐步放回；全部未完结作业一律回 QUEUED
        svc = _svc(home)
        now = "2026-10-12T00:00:00+00:00"
        svc.db.upsert_job(_ready("j-y1", "lo"), "h1", now, 10)
        svc.db.upsert_job(_ready("j-y2", "lo2", priority=20), "h2", now, 20)
        svc.db.upsert_job(_ready("j-hb", "t"), "h4", now, 50)
        for jid in ("j-y1", "j-y2"):
            svc.db.set_job_status(jid, JobStatus.PAUSED,
                                  "高优先级保护退让（hi）；统一外部不足退出", now)
            svc.db.create_attempt(f"att-{jid}", jid, 1, "tok", now)
        svc.db.set_job_status("j-hb", JobStatus.PAUSED, "心跳超时，疑似卡死", now)
        svc._reinitialize()
        st = svc.protect.st
        assert st.pending_resume == {}
        assert st.protected_task == ""
        for jid in ("j-y1", "j-y2", "j-hb"):
            assert svc.db.get_job(jid)["status"] == JobStatus.QUEUED


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


# ---------- 修复 10：控制节奏随反馈周期（调度器自统计 EMA）缩放 ----------

class TestControlRhythmScaling:
    def test_backoff_cap_scaled_by_feedback_interval(self, home):
        svc = _svc(home)
        _write_task(home, "t-long")
        svc.refresh_specs()
        svc.db.upsert_job(_ready("j-1", "t-long"), "h1", "now", 50)
        svc.db.set_job_backoff("j-1", 30, "")  # 历史计数已高 → 直接触顶
        # 喂事件让反馈间隔 EMA 升到 200s（调度器自统计，任务不再声明间隔）
        svc.timing.note_event("t-long", 0.0)
        svc.timing.note_event("t-long", 200.0)
        assert svc.timing.feedback_interval("t-long") == pytest.approx(200.0)
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
