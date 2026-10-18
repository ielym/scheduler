"""审计修复回归测试（对应审查报告 19 项问题的逐项验证）。

编号与审查报告一致：
#1  reap_orphans exclude 保护协作退出 worker
#2  _is_slow 速率差分（累计值不重复求和）
#3  requeue_job（人工重排）
#4  生产者空事件轮次游标持久化
#5  准入同级任务轮转
#6  反馈间隔契约（STARTED 申报 → 心跳超时/暂停宽限推导）
#8  监控统计（task_monitor / 生产者状态摘要）
#9  events.jsonl 增量读 + 轮转重置；SDK 事件文件 16MB 轮转
#10 jobs_in_status 过滤与 LIMIT
#11 now_iso 统一 UTC
#12 生产者退避语义（一次死亡只计一次崩溃）
#13 排序信号时间戳化（shortage_recent 窗口衰减）
#14 SDK 就绪载荷保留作业级 priority
#15 PAUSING 中以 75 退出 → BACKOFF
#17 has_progress_since 改善判定
#18 全局配置无效时告警（不静默回退）
#19 create_attempt 失败时作业保持 QUEUED
"""

from __future__ import annotations

import json
import sys
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = str(Path(__file__).resolve().parent.parent)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from sched import config, db as dbmod, receiver, runner, scaling, store  # noqa: E402
from sched import cgroup as cg  # noqa: E402
from sched.config import EventKind, JobStatus  # noqa: E402
from sched.scheduler import AttMeta, SchedulerService  # noqa: E402


def _write_task(home, task_id, priority=50, enabled=True):
    td = home / "tasks" / task_id
    td.mkdir(parents=True, exist_ok=True)
    (td / "producer.py").write_text("")
    (td / "run.py").write_text("")
    store.save_json(td / "task.json", {
        "schema_version": 1, "task_id": task_id, "enabled": enabled,
        "priority": priority,
        "producer_entry": {"file": "producer.py"},
        "executor_entry": {"file": "run.py"},
        "recovery": {"replay": "outbox", "retain": "测试保留", "replay_note": "测试重放"},
    })
    return td


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


def _spec(task_id, priority=50, **kw):
    return config.TaskSpec(task_id=task_id, priority=priority,
                           producer_entry={"file": "producer.py"},
                           executor_entry={"file": "run.py"}, **kw)


# ---------- #1 reap_orphans exclude ----------
def test_reap_orphans_excludes_protected(home, monkeypatch):
    root = home / "fakecg"
    for rel in ("orphanA", "keep", "keep/attempt", "keep/attempt/child"):
        (root / rel).mkdir(parents=True)
    (root / "orphanA" / "cgroup.procs").write_text("111 222\n")
    (root / "keep" / "cgroup.procs").write_text("333\n")            # 祖先
    (root / "keep" / "attempt" / "cgroup.procs").write_text("444\n")  # 自身
    (root / "keep" / "attempt" / "child" / "cgroup.procs").write_text("555\n")  # 后代
    monkeypatch.setattr(cg, "FALLBACK_ROOT", root)
    monkeypatch.setattr(cg, "_cgroup_fs_mounted", lambda: True)
    n = cg.reap_orphans(exclude={"keep/attempt"})
    assert n == 2  # 仅 orphanA 的 2 个进程被回收
    assert (root / "orphanA" / "cgroup.kill").exists()
    assert not (root / "keep" / "cgroup.kill").exists()
    assert not (root / "keep" / "attempt" / "cgroup.kill").exists()
    assert not (root / "keep" / "attempt" / "child" / "cgroup.kill").exists()
    # 无 exclude 时：受保护目录同样被回收（keep 3 + orphanA 2）
    assert cg.reap_orphans() == 5
    assert (root / "keep" / "attempt" / "cgroup.kill").exists()
    assert (root / "keep" / "attempt" / "child" / "cgroup.kill").exists()


# ---------- #2 _is_slow 速率差分 ----------
def _sample(ts, cum):
    return scaling.ProgressSample(ts_mono=ts, completed_units=int(cum))


def test_is_slow_uses_rate_differential(home):
    # 反馈间隔 EMA 初值调小，避免 6×反馈间隔的观察窗门槛把短跨度样本提前判为非慢
    h = scaling.HealthTracker(slow_confidence_sec=5.0,
                              timing=scaling.TaskTiming(default_interval_sec=0.5))
    # 恒速但样本为累计值：差分速率恒定 → 不判慢（旧实现按样本求和会误判）
    dq = deque(_sample(float(i), float(i * 10)) for i in range(12))
    assert not h._is_slow(dq)
    # 真变慢：后半速率降为 1/5 → 判慢
    dq2 = deque([_sample(float(i), float(i * 10)) for i in range(6)]
                + [_sample(float(i), float(60 + (i - 6) * 2)) for i in range(6, 12)])
    assert h._is_slow(dq2)


# ---------- #3 requeue_job（人工重排） ----------
def test_requeue_job_unit(home):
    svc = _svc(home)
    d = svc.db
    _mk_job(d, "j-rq")
    d.create_attempt("a1", "j-rq", 1, "tok", "now")
    d.set_job_status("j-rq", JobStatus.PAUSED, "test", "now")
    d.update_attempt("a1", status=JobStatus.PAUSED)
    assert svc.requeue_job("j-rq", "测试重排") is True
    assert d.get_job("j-rq")["status"] == JobStatus.QUEUED
    atts = d.list_attempts(job_id="j-rq")
    assert atts and atts[0]["status"] == JobStatus.CANCELLED
    assert "重排关闭" in (atts[0].get("reason") or "")
    # FAILED 终态作业也可人工重排（无遗留 attempt）
    _mk_job(d, "j-fail")
    d.set_job_status("j-fail", JobStatus.FAILED, "业务失败", "now")
    assert svc.requeue_job("j-fail") is True
    assert d.get_job("j-fail")["status"] == JobStatus.QUEUED
    # 不存在的作业
    assert svc.requeue_job("nope") is False
    # 运行中拒绝
    _mk_job(d, "j-run")
    d.set_job_status("j-run", JobStatus.RUNNING, "test", "now")
    assert svc.requeue_job("j-run") is False
    assert d.get_job("j-run")["status"] == JobStatus.RUNNING


# ---------- #4 生产者空事件轮次游标持久化 ----------
def test_run_producer_persists_empty_advance(home, monkeypatch):
    monkeypatch.setenv("SCHED_TASK_ID", "t")
    monkeypatch.setenv("SCHED_WORKSPACE", str(config.task_state_dir("t")))
    from sched_task_sdk.producer import Producer, run_producer

    class P(Producer):
        def discover(self, cursor):
            if cursor is None:
                return {"done": True}, []
            return cursor, []

    assert run_producer(P(), tick_sec=0.1, replay_only=True) == 0
    state = json.loads(
        (config.producer_state_dir("t") / "state.json").read_text())
    assert state["cursor"] == {"done": True}  # 空事件轮次游标也持久化


# ---------- #5 准入同级任务轮转 ----------
def test_admission_round_robin(home):
    svc = _svc(home)
    svc.specs = {"ta": _spec("ta"), "tb": _spec("tb")}
    _mk_job(svc.db, "ja", task_id="ta")
    _mk_job(svc.db, "jb", task_id="tb")
    seq1 = [c["task_id"] for c in svc._candidates()]
    seq2 = [c["task_id"] for c in svc._candidates()]
    assert set(seq1) == {"ta", "tb"}
    assert seq1 != seq2  # 同优先级多任务轮转，两次评估起始任务不同


# ---------- #6 反馈间隔契约（调度器自统计 EMA） ----------
def test_started_event_sets_feedback_interval(home):
    svc = _svc(home)
    _mk_job(svc.db, "j-fb")
    svc.db.create_attempt("a-fb", "j-fb", 1, "tok", "now")
    meta = AttMeta("a-fb", "j-fb", "t", 50, "tok")

    class FakeRun:
        def read_events(self, after_seq):
            return [{"seq": 1, "kind": EventKind.STARTED, "name": "t"}]

    svc._ingest_events(FakeRun(), meta)
    # 事件到达即喂入调度器反馈间隔 EMA（任务不再声明间隔）
    assert svc.timing._last_event_mono.get("t") is not None
    assert meta.status == JobStatus.RUNNING


def test_heartbeat_timeout_and_pause_grace(home):
    svc = _svc(home)
    c = svc.cfg.control
    c.heartbeat_interval_sec = 5.0
    c.heartbeat_stale_multiplier = 3.0
    meta = AttMeta("a", "j", "t", 50, "tok")
    # 存活阈值 = 心跳间隔 × 倍数，与业务进展节奏无关（首步/并发/重启不影响）
    assert svc._heartbeat_timeout() == pytest.approx(15.0)
    # 暂停宽限仍按业务进展节奏 EMA 推导：无观测 → 默认反馈间隔 × 4
    assert svc._pause_grace_sec(meta) == pytest.approx(
        max(c.pause_grace_base_sec, c.feedback_interval_init_sec * 4))
    # 观测到 100s 反馈间隔 → 宽限抬升并封顶
    svc.timing = scaling.TaskTiming(default_interval_sec=c.feedback_interval_init_sec)
    svc.timing.note_event("t", 0.0)
    svc.timing.note_event("t", 100.0)
    assert svc._pause_grace_sec(meta) == pytest.approx(c.pause_grace_max_sec)


# ---------- #8 监控统计：task_monitor / 生产者状态摘要 ----------
def test_task_monitor_and_cursor_truncation(home):
    svc = _svc(home)
    svc.specs["t"] = _spec("t", priority=42)
    _mk_job(svc.db, "jm", task_id="t")
    psd = config.producer_state_dir("t")
    psd.mkdir(parents=True, exist_ok=True)
    store.save_json(psd / "state.json",
                    {"cursor": {"x": 1}, "unacked": {"a": {}, "b": {}}})
    tm = svc._task_monitor()["t"]
    assert tm["priority"] == 42 and tm["enabled"] is True
    assert tm["queued"] == 1 and tm["unacked"] == 2
    assert tm["cursor"] == {"x": 1}
    # 超长游标截断为占位摘要
    store.save_json(psd / "state.json", {"cursor": "z" * 600, "unacked": {}})
    tm2 = svc._task_monitor()["t"]
    assert tm2["cursor"] == {"_truncated": True, "size": 602}
    assert tm2["unacked"] == 0


# ---------- #9 events.jsonl 增量读 + 轮转；SDK 事件文件轮转 ----------
def _bare_run(tmp_path):
    a = runner.AttemptRun.__new__(runner.AttemptRun)
    a.run_dir = tmp_path
    a._read_offset = 0
    a._read_seq = 0
    return a


def test_read_events_incremental_and_rotation(home, tmp_path):
    p = tmp_path / "events.jsonl"
    a = _bare_run(tmp_path)
    p.write_text('{"seq":1,"kind":"started"}\n{"seq":2,"kind":"error"}\n')
    evs = a.read_events(0)
    assert [e["seq"] for e in evs] == [1, 2]
    assert a._read_offset == p.stat().st_size  # 偏移推进
    # 追加一行 → 只读新增
    with open(p, "a") as f:
        f.write('{"seq":3,"kind":"progress"}\n')
    assert [e["seq"] for e in a.read_events(2)] == [3]
    # 半行不消费
    with open(p, "a") as f:
        f.write('{"seq":4,"ki')
    assert a.read_events(3) == []
    with open(p, "a") as f:
        f.write('nd":"error"}\n')
    assert [e["seq"] for e in a.read_events(3)] == [4]
    # 轮转/截断 → size < offset → 重置游标从头读
    p.write_text('{"seq":1,"kind":"started"}\n')
    assert [e["seq"] for e in a.read_events(0)] == [1]


def test_sdk_event_writer_rotation(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_EVENT_FILE", str(tmp_path / "events.jsonl"))
    from sched_task_sdk.events import EventWriter
    p = tmp_path / "events.jsonl"
    p.write_text("x" * 150)  # 预置超限内容
    w = EventWriter()
    monkeypatch.setattr(EventWriter, "_ROTATE_BYTES", 120)
    w.emit("error")
    # 旧内容原子轮转到 .1，新事件从空文件续写
    assert (tmp_path / "events.jsonl.1").read_text() == "x" * 150
    lines = p.read_text().strip().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["kind"] == "error"


# ---------- #10 jobs_in_status 过滤与 LIMIT ----------
def test_jobs_in_status_filter_and_limit(tmp_path):
    d = dbmod.DB(tmp_path / "e.db")
    for i in range(5):
        _mk_job(d, f"j{i}")
    d.set_job_status("j0", JobStatus.RUNNING, "t", "now")
    d.set_job_status("j1", JobStatus.BACKOFF, "t", "now")
    assert {j["job_id"] for j in d.jobs_in_status(JobStatus.QUEUED)} == \
        {"j2", "j3", "j4"}
    assert len(d.jobs_in_status(JobStatus.QUEUED, limit=2)) == 2
    _mk_job(d, "k9", task_id="other")
    rows = d.jobs_in_status(JobStatus.QUEUED, task_id="other")
    assert [j["job_id"] for j in rows] == ["k9"]


# ---------- #11 now_iso 统一 UTC ----------
def test_now_iso_is_utc():
    dt = datetime.fromisoformat(runner.now_iso())
    assert dt.utcoffset() == timedelta(0)
    assert abs((datetime.now(timezone.utc) - dt).total_seconds()) < 5


# ---------- #12 生产者退避语义 ----------
def test_producer_backoff_counts_crash_once(home, monkeypatch):
    from sched import producer as prod
    guard = prod.ProducerGuard(config.SchedulerConfig())
    calls: list[str] = []
    monkeypatch.setattr(guard, "launch", lambda tid, spec: calls.append(tid))
    pp = prod.ProducerProc(task_id="t", proc=None, pid=999_999_999,
                           start_time=0.0)
    guard._procs["t"] = pp
    spec = _spec("t")
    guard.ensure("t", spec)  # 首次观测死亡：计一次崩溃，排定退避
    assert pp.crash_count == 1 and calls == []
    guard.ensure("t", spec)  # 退避窗口内：不重复计数、不重启
    assert pp.crash_count == 1 and calls == []
    pp.restart_not_before_mono = 0.0
    guard.ensure("t", spec)  # 窗口结束 → 重启
    assert calls == ["t"]


# ---------- #13 排序信号时间戳化 ----------
def test_running_infos_shortage_window(home):
    svc = _svc(home)
    m1 = AttMeta("a1", "j1", "tA", 10, "tok")
    m1.status = JobStatus.RUNNING
    m1.shortage_at_mono = time.monotonic()  # 刚发生
    m2 = AttMeta("a2", "j2", "tB", 20, "tok")
    m2.status = JobStatus.RUNNING
    m2.shortage_at_mono = (time.monotonic()
                           - svc.cfg.control.slow_confidence_sec * 2)  # 已过期
    svc.meta.update({"a1": m1, "a2": m2})
    infos = {i["attempt_id"]: i for i in svc._running_infos()}
    assert infos["a1"]["shortage_recent"] is True
    assert infos["a2"]["shortage_recent"] is False


# ---------- #14 SDK 就绪载荷保留作业级 priority ----------
def test_sdk_payload_priority(home, monkeypatch):
    monkeypatch.setenv("SCHED_TASK_ID", "t")
    monkeypatch.setenv("SCHED_WORKSPACE", str(config.task_state_dir("t")))
    from sched_task_sdk.producer import Producer

    class P(Producer):
        def discover(self, cursor):
            return cursor, []

    p = P()
    assert p._ready_payload("j1", {"event_id": "e", "priority": 80})["priority"] == 80
    assert "priority" not in p._ready_payload("j2", {"event_id": "e"})


# ---------- #15 PAUSING 中以 75 退出 → BACKOFF ----------
def test_pausing_with_shortage_exit_is_backoff():
    out = runner.decide_outcome(JobStatus.PAUSING, 75, False)
    assert out.status == JobStatus.BACKOFF
    out2 = runner.decide_outcome(JobStatus.PAUSING, 0, False)
    assert out2.status == JobStatus.PAUSED


# ---------- #17 has_progress_since ----------
def test_has_progress_since():
    h = scaling.HealthTracker()
    h.record_progress("t", 100.0, 5)
    assert h.has_progress_since("t", 99.0) is True
    assert h.has_progress_since("t", 101.0) is False
    assert h.has_progress_since("other", 0.0) is False


# ---------- #18 全局配置无效时告警 ----------
def test_load_config_warns_on_invalid(home, capsys):
    store.save_json(config.config_file(),
                    {"schema_version": 1, "timezone": "Not/AZone"})
    cfg = config.load_config()
    assert cfg.timezone == "Asia/Shanghai"  # 回退默认
    assert "全局配置" in capsys.readouterr().err


# ---------- #19 create_attempt 失败时作业保持 QUEUED ----------
def test_launch_create_attempt_failure_keeps_queued(home, monkeypatch):
    svc = _svc(home)
    _mk_job(svc.db, "jX", task_id="t")
    svc.specs["t"] = _spec("t")
    job = svc.db.jobs_in_status(JobStatus.QUEUED, limit=10)[0]
    assert job["job_id"] == "jX"

    def boom(*a, **k):
        raise RuntimeError("db down")

    monkeypatch.setattr(svc.db, "create_attempt", boom)
    assert svc._launch(job, svc.specs["t"]) is False
    # 数据库瞬时不可写 → 作业保持 QUEUED 下轮重试，不误标 FAILED
    assert svc.db.get_job("jX")["status"] == JobStatus.QUEUED
    assert not svc.db.live_jobs_for_job("jX")


# ---------- 接收发现延迟监控（#8 配套） ----------
def test_receiver_reports_discovery_latency(home):
    _write_task(home, "demo")
    out = config.outbox_dir("demo")
    out.mkdir(parents=True)
    (out / "demo-abc.json").write_text(json.dumps({
        "schema_version": 1, "job_id": "demo-abc", "task_id": "demo",
        "entry_args": [], "input_refs": [], "idempotency_key": "k", "env": {}}))
    d = dbmod.DB(home / "runtime" / "e.db")
    summary = receiver.receive_outbox("demo", config.load_task("demo"), d, "now")
    assert summary["created"] == 1
    lats = summary.get("discovery_latencies") or []
    assert lats and all(x >= 0.0 for x in lats)
