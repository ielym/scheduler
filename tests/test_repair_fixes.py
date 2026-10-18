"""本轮修复的回归测试（编号与审查报告对应）。

P0-2  explore→steady 恢复链：on_success 恢复 steady；on_shortage 后持久化动态目标
P1-3  consume_receipts 仅 status==received 才 ack（conflict/invalid 不误删）
P1-4  Producer.advance 先持久化再推进内存游标（持久化失败不丢事件）
P1-5  心跳超时暂停的作业恢复排队前冷却（防每秒重启循环）；_finalize 保留暂停发起原因
P2-7  SDK 事件每行自带 job_id/attempt_id 归属
P2-8  归档对账：archive 文件数 vs 库内作业数不一致告警
P2-9  保护起因 cause 标签：变慢退让放回间隔加倍
P2-10 PAUSING 中非 0/75 退出：保守归因 PAUSED（断点可能未保存）
P2-16 切换保护对象：旧退让账合并进 pending_resume，不丢失
P2-17 主循环内非阻塞终止状态机：PAUSING/CANCELLED/STARTING 超时路径不再
     同步 wait(grace)，单个卡死 worker 不冻结控制平面；D 态兜底按未知码收尾
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

ROOT = str(Path(__file__).resolve().parent.parent)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from sched import config, runner, scaling, store  # noqa: E402
from sched.config import JobStatus  # noqa: E402
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
    # queued_at 用当前时刻：对账窗口类测试（48h 归档对账）随日历推移不失效
    db.upsert_job(ready, f"hash-{job_id}",
                  datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                  priority)
    return job_id


def _svc(home):
    (home / "runtime").mkdir(parents=True, exist_ok=True)
    return SchedulerService()


def _spec(task_id, priority=50, **kw):
    return config.TaskSpec(task_id=task_id, priority=priority,
                           producer_entry={"file": "producer.py"},
                           executor_entry={"file": "run.py"}, **kw)


class _FakeProc:
    def __init__(self, returncode=0):
        self.returncode = returncode

    def wait(self, timeout=None):
        return 0


class _FakeCgroup:
    """B6 _ensure_tree_gone 所需的最小 cgroup 假对象：树已空。"""
    available = False

    def procs(self):
        return []

    def kill_all(self):
        pass


class _FakeAttempt:
    pid = 11
    pgid = 11
    start_time = 1.0

    def __init__(self, returncode=0):
        self.proc = _FakeProc(returncode)
        self.cgrp = _FakeCgroup()
        self.spec = SimpleNamespace(attempt_id="att-fake")

    def is_alive(self):
        return False

    def read_events(self, after_seq: int = 0):
        return []

    def cleanup(self):
        pass


# ---------- P0-2 explore→steady 恢复链 ----------

def test_on_success_restores_steady_after_shortage(home):
    cfg = config.SchedulerConfig()
    sc = scaling.Scaling(cfg)
    ts = sc.get("t")
    ts.run_target = 3
    ts.mode = "steady"
    sc.on_shortage("t", 1.0, 3)
    # on_shortage 只记证据（不收缩目标——退避由 TaskBackoff 承担）
    assert ts.mode == "explore" and ts.run_target == 3 and ts.shortage_streak == 1
    # 恢复唯一入口不再是冷启动分支：一次真实成功即恢复 steady
    sc.on_success("t", 2.0)
    assert ts.mode == "steady" and ts.shortage_streak == 0


def test_finalize_backoff_persists_scale(home):
    svc = _svc(home)
    _mk_job(svc.db, "j1")
    svc.db.create_attempt("a1", "j1", 1, "tok", "t0")
    svc.scaling.get("t").run_target = 3
    svc.scaling.get("t").mode = "steady"
    meta = AttMeta("a1", "j1", "t", 50, "tok")
    meta.status = JobStatus.RUNNING
    svc._finalize(_FakeAttempt(returncode=75), meta, runner.now_iso())
    st = next(s for s in svc.db.all_task_states() if s["task_id"] == "t")
    # on_shortage 不再收缩目标（退避由 TaskBackoff 承担）：目标保持 3，
    # 但 explore（退避期不扩容）仍持久化，重启不回退
    assert st["run_target"] == 3 and st["mode"] == "explore"


def test_finalize_backoff_shrinks_admit_budget(home):
    """方案 A′：任一任务 exit 75 → 并发预算乘法收缩 max(1, BUDGET//2)。"""
    svc = _svc(home)
    _mk_job(svc.db, "j1")
    svc.db.create_attempt("a1", "j1", 1, "tok", "t0")
    svc._admit_budget = 6
    meta = AttMeta("a1", "j1", "t", 50, "tok")
    meta.status = JobStatus.RUNNING
    svc._finalize(_FakeAttempt(returncode=75), meta, runner.now_iso())
    assert svc._admit_budget == 3


def test_finalize_backoff_suppressed_when_step_normal(home):
    """单步耗时与自身基线相当（偶发 75、节奏正常）→ 不抽人、不减预算。"""
    svc = _svc(home)
    _mk_job(svc.db, "j1")
    svc.db.create_attempt("a1", "j1", 1, "tok", "t0")
    svc._admit_budget = 6
    for _ in range(20):                # 基线≈当前≈2s → 偏离≈1 < K
        svc.step_stats.note_step("t", 2.0)
    meta = AttMeta("a1", "j1", "t", 50, "tok")
    meta.status = JobStatus.RUNNING
    svc._finalize(_FakeAttempt(returncode=75), meta, runner.now_iso())
    assert svc._admit_budget == 6      # 未减半
    assert svc.tbackoff.state_of("t") == svc.tbackoff.NORMAL   # 未步降


def test_finalize_backoff_deviated_when_step_slow(home):
    """单步耗时严重偏离自身基线（真的走不动）→ 照常抽人 + 预算减半。"""
    svc = _svc(home)
    _mk_job(svc.db, "j1")
    svc.db.create_attempt("a1", "j1", 1, "tok", "t0")
    svc._admit_budget = 6
    for _ in range(50):                # 基线≈2s
        svc.step_stats.note_step("t", 2.0)
    for _ in range(3):                 # 近期尖峰 20s → 偏离 ≥ K（默认 slow_ema+fast_ema）
        svc.step_stats.note_step("t", 20.0)
    meta = AttMeta("a1", "j1", "t", 50, "tok")
    meta.status = JobStatus.RUNNING
    svc._finalize(_FakeAttempt(returncode=75), meta, runner.now_iso())
    assert svc._admit_budget == 3


# ---------- TaskBackoff VERIFY 死区（步降暂停最后一个执行→自锁） ----------

def test_verify_drained_transitions_to_empty(home):
    """死锁修复：VERIFY 态的唯一在途执行以 PAUSED 终态退出时，
    任务必须转 EMPTY 探测——否则 VERIFY 无出口（无人可上报进展/
    不可能再报 75）且封锁准入，状态机自锁、任务永久停摆。"""
    svc = _svc(home)
    _mk_job(svc.db, "j1")
    svc.db.create_attempt("a1", "j1", 1, "tok", "t0")
    meta = AttMeta("a1", "j1", "t", 50, "tok")
    meta.status = JobStatus.PAUSING   # 被步降暂停的执行
    svc.tbackoff.set_state("t", svc.tbackoff.VERIFY)
    svc._finalize(_FakeAttempt(returncode=0), meta, runner.now_iso())
    # 零在途 + VERIFY → EMPTY（任务级退避，按间隔探测放行）
    assert svc.tbackoff.state_of("t") == svc.tbackoff.EMPTY
    # EMPTY 后探测间隔到期即允许准入（不再被 verify 永久封锁）
    ok, why = svc.tbackoff.admission_allowed("t", _now_mono_far())
    assert ok and why == ""


def test_verify_kept_when_live_remains(home):
    """负向：VERIFY 但任务仍有其他在途执行时保持 VERIFY——
    步降验证语义不变（等剩余执行完成正确一步）。"""
    svc = _svc(home)
    _mk_job(svc.db, "j1")
    _mk_job(svc.db, "j2")
    svc.db.create_attempt("a1", "j1", 1, "tok", "t0")
    svc.db.create_attempt("a2", "j2", 2, "tok", "t0")
    other = AttMeta("a2", "j2", "t", 50, "tok")
    other.status = JobStatus.RUNNING
    svc.meta["a2"] = other   # 剩余未退避的执行仍在途
    meta = AttMeta("a1", "j1", "t", 50, "tok")
    meta.status = JobStatus.PAUSING
    svc.tbackoff.set_state("t", svc.tbackoff.VERIFY)
    svc._finalize(_FakeAttempt(returncode=0), meta, runner.now_iso())
    assert svc.tbackoff.state_of("t") == svc.tbackoff.VERIFY


def test_reconcile_noop_in_normal(home):
    """幂等：任务处 NORMAL 时执行退出不触发任何迁移。"""
    svc = _svc(home)
    _mk_job(svc.db, "j1")
    svc.db.create_attempt("a1", "j1", 1, "tok", "t0")
    meta = AttMeta("a1", "j1", "t", 50, "tok")
    meta.status = JobStatus.PAUSING
    svc._finalize(_FakeAttempt(returncode=0), meta, runner.now_iso())
    assert svc.tbackoff.state_of("t") == svc.tbackoff.NORMAL


def _now_mono_far() -> float:
    """EMPTY 探测到期时刻（base 间隔 + 余量；tbackoff 用 time.monotonic 基准）。"""
    import time as _t
    from sched import config as _cfg
    return _t.monotonic() + _cfg.SchedulerConfig().control.task_backoff_probe_base_sec + 5.0


# ---------- P1-3 回执仅 received 才算 ack ----------

def test_consume_receipts_ignores_conflict_and_invalid(monkeypatch, tmp_path):
    monkeypatch.setenv("SCHED_TASK_ID", "t")
    monkeypatch.setenv("SCHED_WORKSPACE", str(tmp_path))
    from sched_task_sdk.producer import Producer, job_id_of
    p = Producer()
    p.advance(1, [{"event_id": "e1", "business_version": "v1"},
                  {"event_id": "e2", "business_version": "v1"}])
    assert len(p._state["unacked"]) == 2
    rd = tmp_path / "receipts"
    rd.mkdir()
    j1 = job_id_of("t", "e1", "v1")
    j2 = job_id_of("t", "e2", "v1")
    (rd / f"{j1}.json").write_text(json.dumps(
        {"job_id": j1, "status": "received"}))
    (rd / f"{j2}.json").write_text(json.dumps(
        {"job_id": j2, "status": "conflict", "reason": "内容冲突"}))
    assert p.consume_receipts() == 1
    assert j1 not in p._state["unacked"]   # 正常确认 → 清理
    assert j1 not in p._state["conflicted"]
    # C1：conflict 回执 → 移入 conflicted 隔离桶，停止无意义重发，留待人工处理
    assert j2 not in p._state["unacked"]
    assert j2 in p._state["conflicted"]
    disk = json.loads((tmp_path / "producer_state" / "state.json").read_text("utf-8"))
    assert j2 not in disk["unacked"] and j2 in disk["conflicted"]


# ---------- P1-4 advance 先持久化再推进内存 ----------

def test_advance_failure_keeps_memory_cursor(monkeypatch, tmp_path):
    monkeypatch.setenv("SCHED_TASK_ID", "t")
    monkeypatch.setenv("SCHED_WORKSPACE", str(tmp_path))
    import sched_task_sdk.producer as pp
    from sched_task_sdk.producer import job_id_of
    p = pp.Producer()
    p.advance(5, [{"event_id": "e1", "business_version": "v1"}])
    assert p.cursor == 5

    def boom(path, obj):
        raise OSError("disk full")

    monkeypatch.setattr(pp, "_atomic_write_json", boom)
    with_error = False
    try:
        p.advance(10, [{"event_id": "e2", "business_version": "v1"}])
    except OSError:
        with_error = True
    assert with_error
    # 持久化失败：内存与磁盘都停留在旧游标，e2 未被跳过
    assert p.cursor == 5
    assert job_id_of("t", "e2", "v1") not in p._state["unacked"]
    disk = json.loads((tmp_path / "producer_state" / "state.json").read_text("utf-8"))
    assert disk["cursor"] == 5
    assert job_id_of("t", "e2", "v1") not in disk["unacked"]


def test_advance_failure_then_retry_succeeds(monkeypatch, tmp_path):
    monkeypatch.setenv("SCHED_TASK_ID", "t")
    monkeypatch.setenv("SCHED_WORKSPACE", str(tmp_path))
    import sched_task_sdk.producer as pp
    orig_write = pp._atomic_write_json
    p = pp.Producer()
    p.advance(5, [{"event_id": "e1", "business_version": "v1"}])
    monkeypatch.setattr(pp, "_atomic_write_json",
                        lambda path, obj: (_ for _ in ()).throw(OSError("x")))
    try:
        p.advance(10, [{"event_id": "e2", "business_version": "v1"}])
    except OSError:
        pass
    monkeypatch.setattr(pp, "_atomic_write_json", orig_write)
    # 故障恢复后同轮结果可再次提交，不丢 e2
    p.advance(10, [{"event_id": "e2", "business_version": "v1"}])
    assert p.cursor == 10
    assert len(p._state["unacked"]) == 2


# ---------- P1-5 心跳超时恢复冷却 + 暂停原因保留 ----------

def test_tick_release_stale_cooldown(home):
    svc = _svc(home)
    _mk_job(svc.db, "jH")
    _mk_job(svc.db, "jN")
    hb_reason = "心跳超时，疑似卡死；协作暂停，断点已保存"
    svc.db.set_job_status("jH", JobStatus.PAUSED, hb_reason, ts=runner.now_iso())
    svc.db.set_job_status("jN", JobStatus.PAUSED, "高优先级保护退让；协作暂停，断点已保存",
                          ts=runner.now_iso())
    svc._tick_release(0.0)
    # 普通暂停立即恢复；心跳超时暂停在冷却窗口内保持 PAUSED
    assert svc.db.get_job("jN")["status"] == JobStatus.QUEUED
    assert svc.db.get_job("jH")["status"] == JobStatus.PAUSED
    # 冷却（默认 60s）过后恢复排队
    old = (datetime.now(timezone.utc) -
           timedelta(seconds=svc.cfg.control.stale_pause_cooldown_sec + 60)
           ).isoformat(timespec="seconds")
    svc.db.set_job_status("jH", JobStatus.PAUSED, hb_reason, ts=old)
    svc._tick_release(0.0)
    assert svc.db.get_job("jH")["status"] == JobStatus.QUEUED


def test_finalize_preserves_pause_initiator_reason(home):
    svc = _svc(home)
    _mk_job(svc.db, "j1")
    svc.db.create_attempt("a1", "j1", 1, "tok", "t0")
    meta = AttMeta("a1", "j1", "t", 50, "tok")
    meta.status = JobStatus.PAUSING
    meta.pause_command = {"reason": "心跳超时，疑似卡死"}
    svc._finalize(_FakeAttempt(returncode=0), meta, runner.now_iso())
    job = svc.db.get_job("j1")
    assert job["status"] == JobStatus.PAUSED
    assert "心跳超时" in job["reason"] and "协作暂停" in job["reason"]


# ---------- P2-7 事件归属字段 ----------

def test_event_writer_includes_job_and_attempt(monkeypatch, tmp_path):
    monkeypatch.setenv("SCHED_EVENT_FILE", str(tmp_path / "ev.jsonl"))
    monkeypatch.setenv("SCHED_JOB_ID", "job-7")
    monkeypatch.setenv("SCHED_ATTEMPT_ID", "att-7")
    from sched_task_sdk.events import EventWriter, EventKind
    w = EventWriter()
    w.emit(EventKind.PROGRESS, completed_units=1)
    obj = json.loads((tmp_path / "ev.jsonl").read_text("utf-8").splitlines()[0])
    assert obj["job_id"] == "job-7" and obj["attempt_id"] == "att-7"
    assert obj["kind"] == EventKind.PROGRESS


# ---------- P2-8 归档对账 ----------

def test_reconcile_archive_detects_mismatch(home, caplog):
    import logging
    svc = _svc(home)
    _write_task(home, "t")
    svc.refresh_specs()
    _mk_job(svc.db, "j1", task_id="t")
    arch = config.archive_root() / "t"
    arch.mkdir(parents=True)
    (arch / "j1.json").write_text("{}")
    with caplog.at_level(logging.WARNING, logger="sched.scheduler"):
        svc._reconcile_archive()  # 窗口内作业均有归档 → 无告警
        assert not any("archive reconciliation" in r.message for r in caplog.records)
        # C1：单向窗口对账（库→文件）——孤儿文件（文件有库无）不再告警，
        # 归档是接收最后一步，不存在"库无而文件在"的遗漏证据形态
        (arch / "ghost.json").write_text("{}")
        svc._reconcile_archive()
        assert not any("archive reconciliation" in r.message for r in caplog.records)
        # 窗口内作业的归档文件缺失 → 告警
        (arch / "j1.json").unlink()
        svc._reconcile_archive()
        assert any("archive reconciliation" in r.message for r in caplog.records)


# ---------- P2-9 变慢退让放回间隔加倍 ----------

def test_slow_cause_release_interval_doubled(home):
    svc = _svc(home)
    p = svc.protect
    assert p.start_round("A", 50, 0.0, reason="r", cause="slow")
    p.note_yielded("att1", "B", 10)
    p.end_round(10.0, success=True)
    t = 100.0
    assert p.release_ready(t) == "B"
    assert p.st.next_release_mono == t + 2 * svc.cfg.control.recovery_probe_sec


def test_shortage_cause_release_interval_normal(home):
    svc = _svc(home)
    p = svc.protect
    assert p.start_round("A", 50, 0.0, reason="r", cause="shortage")
    p.note_yielded("att1", "B", 10)
    p.end_round(10.0, success=True)
    t = 100.0
    assert p.release_ready(t) == "B"
    assert p.st.next_release_mono == t + svc.cfg.control.recovery_probe_sec


# ---------- P2-10 PAUSING 非 0/75 退出保守归因 ----------

def test_decide_outcome_pausing_crash_is_conservative():
    from sched.runner import decide_outcome
    o = decide_outcome(JobStatus.PAUSING, 1, False)
    assert o.status == JobStatus.PAUSED
    assert "重新排队" in o.reason
    o0 = decide_outcome(JobStatus.PAUSING, 0, False)
    assert o0.status == JobStatus.PAUSED and o0.reason == "协作暂停"
    o75 = decide_outcome(JobStatus.PAUSING, 75, False)
    assert o75.status == JobStatus.BACKOFF


# ---------- P2-16 切换保护对象不丢退让账 ----------

def test_start_round_switch_merges_yielded_into_pending_resume(home):
    svc = _svc(home)
    p = svc.protect
    assert p.start_round("A", 50, 0.0, reason="r", cause="shortage")
    p.note_yielded("att1", "B", 10)
    # 更高优先级 C 到达 → 切换保护对象，B 的退让账进待放回队列而非被清空
    assert p.start_round("C", 60, 5.0, reason="r2", cause="shortage")
    assert "B" in p.st.pending_resume and not p.st.yielded_tasks
    p.note_yielded("att2", "D", 20)
    p.end_round(10.0)
    assert set(p.st.pending_resume) == {"B", "D"}


# ---------- P2-17 主循环内非阻塞终止状态机 ----------

class _FakeCgrp:
    available = False

    def kill_all(self):
        pass

    def procs(self):
        return []

    def remove(self):
        pass


def _stuck_attempt(home, attempt_id, job_id):
    """真实 AttemptRun 包一个忽略 SIGTERM 的真实子进程（模拟卡死 worker）。"""
    proc = subprocess.Popen(
        ["python3", "-c",
         "import signal, sys, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
         "print('ready', flush=True); time.sleep(300)"],
        start_new_session=True, stdout=subprocess.PIPE)
    proc.stdout.readline()  # 等 handler 安装完成，避免 TERM 打在 exec 窗口上
    import types
    spec = types.SimpleNamespace(
        attempt_id=attempt_id, job_id=job_id, task_id="t", token="tok",
        task_root=home / "tasks" / "t", executor_entry=None, entry_args=[],
        workdir=".", env={}, progress_semver="1.0.0")
    run_dir = config.run_dir(attempt_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    return runner.AttemptRun(spec, proc, run_dir, _FakeCgrp(), [])


def test_terminate_state_machine_phases(home):
    a = _stuck_attempt(home, "att-sm", "j-sm")
    try:
        assert a.begin_terminate(100.0, 60.0) is True    # 首次发 TERM（进程忽略）
        assert a.begin_terminate(120.0, 5.0) is False    # 幂等：不重发、不改截止
        assert a.poll_terminate(159.9) == "waiting"
        assert a.is_alive()
        assert a.poll_terminate(160.0) == "killed"       # 到期升级 SIGKILL（只发一次）
        assert a.poll_terminate(164.9) == "killed"
        assert a.poll_terminate(165.1) == "gone"         # KILL 后超 KILL_WAIT_SEC 兜底
        for _ in range(40):                              # 真实进程被杀，等收尸
            if not a.is_alive():
                break
            time.sleep(0.05)
        assert not a.is_alive()
        assert a.proc.returncode == -signal.SIGKILL
    finally:
        try:
            os.killpg(a.pgid, signal.SIGKILL)
        except OSError:
            pass
        try:
            a.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass


def test_pausing_grace_exceeded_nonblocking(home):
    svc = _svc(home)
    _write_task(home, "t")
    _mk_job(svc.db, "j-stuck")
    svc.db.create_attempt("att-ps", "j-stuck", 1, "tok", "t")
    a = _stuck_attempt(home, "att-ps", "j-stuck")
    meta = AttMeta("att-ps", "j-stuck", "t", 50, "tok")
    meta.status = JobStatus.PAUSING
    meta.pause_deadline_mono = 100.0
    svc.attempts["att-ps"] = a
    svc.meta["att-ps"] = meta

    # 第一轮：宽限超时 → 发 TERM（被忽略）→ 不阻塞、不收尾
    t0 = time.monotonic()
    svc._tick_attempts(100.5)
    assert time.monotonic() - t0 < 2.0            # 未发生 grace 秒级同步等待
    assert "att-ps" in svc.attempts               # 进程未退出，保持监护

    # 后续轮：到期升级 KILL → 进程退出 → 收尾为 PAUSED（断点可能未保存）
    # KILL 升级截止 = 首次 grace 超时时刻 + 暂停宽限上限（随配置自适应）
    kill_at = 100.5 + svc.cfg.control.pause_grace_max_sec
    for t in (kill_at + 0.5, kill_at + 1.0, kill_at + 2.0,
              kill_at + 5.5, kill_at + 6.0, kill_at + 6.5):
        if "att-ps" not in svc.attempts:
            break
        time.sleep(0.05)
        svc._tick_attempts(t)
    assert "att-ps" not in svc.attempts
    assert svc.db.get_job("j-stuck")["status"] == JobStatus.PAUSED
    assert svc.db.get_attempt("att-ps")["exit_code"] == -signal.SIGKILL
    lines = config.status_log().read_text("utf-8").splitlines()
    assert any('"pause_grace_exceeded"' in ln for ln in lines)


def test_finalize_dstate_gone_does_not_block_or_raise(home):
    svc = _svc(home)
    _write_task(home, "t")
    _mk_job(svc.db, "j-d")
    svc.db.create_attempt("att-d", "j-d", 1, "tok", "t")

    class _DStateProc:
        pid = 99999
        returncode = None

        def poll(self):
            return None

        def wait(self, timeout=None):
            raise subprocess.TimeoutExpired(cmd="d", timeout=timeout)

    class _DStateAttempt(_FakeAttempt):
        def __init__(self):
            super().__init__(None)
            self.proc = _DStateProc()

        def is_alive(self):
            return True

        def begin_terminate(self, now_mono, grace_sec):
            return True

        def poll_terminate(self, now_mono):
            return "gone"

    a = _DStateAttempt()
    meta = AttMeta("att-d", "j-d", "t", 50, "tok")
    meta.status = JobStatus.PAUSING
    meta.pause_deadline_mono = 50.0
    svc.attempts["att-d"] = a
    svc.meta["att-d"] = meta
    # D 态：KILL 后仍不退出 → _ensure_tree_gone 有界等满 2s 杀窗后放行
    # （B6 设计语义），按未知退出码收尾，不抛 TimeoutExpired、不无限阻塞
    t0 = time.monotonic()
    svc._tick_attempts(200.0)
    assert time.monotonic() - t0 < 4.0
    assert "att-d" not in svc.attempts
    assert svc.db.get_job("j-d")["status"] == JobStatus.PAUSED
    assert svc.db.get_attempt("att-d")["exit_code"] is None
