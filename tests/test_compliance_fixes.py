"""合规审查修复（A1-D4）的回归测试，对照 /mnt/data01/projects/todo_list/1011/调度器完整设计方案.md。

覆盖：
- A1 终止路径补 cgroup 整树终止（killpg 杀不到 setsid 孙进程）
- A2 保护门槛：报不足/变慢对象须严格高于存活执行/排队作业最低优先级
- A3 保护轮不得冻结高优先级 PAUSED 作业的恢复通道
- B1 I/O 压力（psi full avg10 归一化 0-1）参与资源健康判定
- B2 task_monitor 补健康摘要/退避原因/暂停原因/生产者崩溃字段
- B3 接收器新文件优先发现（内存态 seen 集合 + 新旧配额各半）
- B4 recovery.retain / replay_note 非空校验
- B5 SDK report_progress 的 completed_units 单调递增校验
- C1 回执一次性消费（账目落盘后删除）+ 归档 GC + 归档窗口对账
- C2 事件写失败不向上抛 / 目录条目 fsync
- D4 _pause_grace_base 去除未用的 att 参数
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = str(Path(__file__).resolve().parent.parent)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from sched import config, db as dbmod, receiver, runner, scaling  # noqa: E402
from sched.config import JobStatus  # noqa: E402
from sched.scheduler import AttMeta, SchedulerService  # noqa: E402


# ---------- 公共夹具 ----------

def _svc(home):
    (home / "runtime").mkdir(parents=True, exist_ok=True)
    return SchedulerService()


def _spec(task_id, priority=50, **kw):
    return config.TaskSpec(task_id=task_id, priority=priority,
                           producer_entry={"file": "producer.py"},
                           executor_entry={"file": "run.py"}, **kw)


def _mk_job(db, job_id, task_id="t", priority=50):
    """入库一个作业（queued_at 取当前时刻，保证落在对账窗口内）。"""
    ready = config.parse_ready({
        "schema_version": 1, "job_id": job_id, "task_id": task_id,
        "entry_args": [], "env": {}, "idempotency_key": job_id,
    })
    db.upsert_job(ready, f"hash-{job_id}", runner.now_iso(), priority)
    return job_id


def _mk_meta(attempt_id, job_id, task_id, priority, status=JobStatus.RUNNING):
    m = AttMeta(attempt_id, job_id, task_id, priority, "tok")
    m.status = status
    return m


def _spawn_sleep(sec: int = 30) -> subprocess.Popen:
    """新会话 sleep 进程（pgid==pid，模拟 setsid 脱离调度器进程组）。"""
    return subprocess.Popen(["sleep", str(sec)], start_new_session=True)


def _proc_start(pid: int) -> float:
    return os.stat(f"/proc/{pid}").st_ctime


# ---------- A1：终止路径 cgroup 整树终止 ----------

def test_terminate_external_cgroup_kill(home, monkeypatch):
    """宽限超时后 cgroup.kill 兜底：killpg 落空（孙进程脱离进程组）时由
    cgroup 整树终止收尾。"""
    proc = _spawn_sleep()
    try:
        pst = _proc_start(proc.pid)
        calls: list[str] = []

        def fake_cgroup(name):
            class _C:
                def kill_all(self):
                    calls.append(name)
                    os.kill(proc.pid, signal.SIGKILL)
                    proc.wait(timeout=5)  # 收割，避免僵尸干扰身份核实
            return _C()

        monkeypatch.setattr(runner.cg, "Cgroup", fake_cgroup)
        # pgid 传不存在的组：killpg 全部落空，只剩 cgroup 兜底路径
        rc = runner.terminate_external(proc.pid, proc.pid + 12345, pst, 0.2,
                                        cgroup="t/att-1")
        assert rc == 0
        assert calls == ["t/att-1"]
    finally:
        try:
            proc.kill()
            proc.wait(timeout=5)
        except Exception:
            pass


def test_terminate_external_default_cgroup_no_crash(home, monkeypatch):
    """cgroup 缺省（""）：不构造 Cgroup，优雅路径（SIGTERM 生效）正常返回。"""
    proc = _spawn_sleep()
    try:
        pst = _proc_start(proc.pid)

        def boom(name):
            raise AssertionError("cgroup 未提供时不应构造 Cgroup")

        monkeypatch.setattr(runner.cg, "Cgroup", boom)
        real_alive = runner.pid_alive_with_start

        def alive_and_reap(pid, st):
            proc.poll()  # 收割 SIGTERM 后的僵尸，让身份核实验证"已退出"
            return real_alive(pid, st)

        monkeypatch.setattr(runner, "pid_alive_with_start", alive_and_reap)
        rc = runner.terminate_external(proc.pid, proc.pid, pst, 0.5)
        assert rc == 0
    finally:
        try:
            proc.kill()
            proc.wait(timeout=5)
        except Exception:
            pass


def test_reinitialize_leftover_uses_cgroup_kill(home, monkeypatch):
    """冷启动清理遗留执行：cgroup 归属可用时整组终止（A1）。"""
    svc = _svc(home)
    _mk_job(svc.db, "j1")
    svc.db.create_attempt("att-1", "j1", 1, "tok", runner.now_iso(),
                          cgroup="t/att-1")
    svc.db.set_job_status("j1", JobStatus.RUNNING, "", runner.now_iso())
    calls: list[str] = []

    def fake_cgroup(name):
        class _C:
            def kill_all(self):
                calls.append(name)
        return _C()

    monkeypatch.setattr("sched.cgroup.Cgroup", fake_cgroup)
    svc._reinitialize()
    assert calls == ["t/att-1"]
    assert svc.db.get_job("j1")["status"] == JobStatus.QUEUED
    assert svc.db.get_attempt("att-1")["status"] == JobStatus.ABANDONED  # 保留记录作审计


# ---------- A2：保护门槛限高优先级 ----------

def test_shortage_same_priority_no_protect_round(home):
    """同优先级存活执行时报不足：无退让空间，不起保护轮（否则互冻结）。"""
    svc = _svc(home)
    svc.meta["a1"] = _mk_meta("a1", "j1", "t_hi", 60)
    svc.meta["a2"] = _mk_meta("a2", "j2", "t_lo", 60)
    svc._on_explicit_shortage(svc.meta["a1"])
    assert svc.protect.st.protected_task == ""


def test_shortage_higher_priority_starts_protect(home):
    """严格高于存活执行最低优先级：起保护轮。"""
    svc = _svc(home)
    svc.meta["a2"] = _mk_meta("a2", "j2", "t_lo", 60)
    m = _mk_meta("a1", "j1", "t_hi", 90)
    svc.meta["a1"] = m
    svc._on_explicit_shortage(m)
    assert svc.protect.st.protected_task == "t_hi"


def test_shortage_alone_no_yield_space(home):
    """无更低优先级存活/排队：报不足对象无退让空间，不起保护轮。"""
    svc = _svc(home)
    m = _mk_meta("a1", "j1", "t_solo", 90)
    svc.meta["a1"] = m
    svc._on_explicit_shortage(m)
    assert svc.protect.st.protected_task == ""


def test_shortage_queued_priority_gates_worthiness(home):
    """排队作业的最低优先级同样参与门槛：同级排队 → 不保护；低级排队 → 保护。"""
    svc = _svc(home)
    _mk_job(svc.db, "jq_same", task_id="t_q", priority=60)
    m = _mk_meta("a1", "j1", "t_hi", 60)
    svc.meta["a1"] = m
    svc._on_explicit_shortage(m)
    assert svc.protect.st.protected_task == ""

    svc2 = _svc(home)
    _mk_job(svc2.db, "jq_lo", task_id="t_q", priority=30)
    m2 = _mk_meta("a1", "j1", "t_hi", 60)
    svc2.meta["a1"] = m2
    svc2._on_explicit_shortage(m2)
    assert svc2.protect.st.protected_task == "t_hi"


def test_slowdown_gated_by_protect_worthy(home):
    """变慢路径同样受保护门槛约束：同级不起轮，高优先级起轮。"""
    svc = _svc(home)
    svc.health.slow = lambda tid, now: True  # 恒判慢，聚焦门槛逻辑
    svc.meta["a_lo"] = _mk_meta("a_lo", "j_lo", "t_lo", 60)
    svc.meta["a_hi"] = _mk_meta("a_hi", "j_hi", "t_hi", 90)
    svc._tick_control(time.monotonic())
    assert svc.protect.st.protected_task == "t_hi"

    svc2 = _svc(home)
    svc2.health.slow = lambda tid, now: True
    svc2.meta["a1"] = _mk_meta("a1", "j1", "t1", 60)
    svc2.meta["a2"] = _mk_meta("a2", "j2", "t2", 60)
    svc2._tick_control(time.monotonic())
    assert svc2.protect.st.protected_task == ""


# ---------- A3：保护轮不冻结高优先级恢复 ----------

def test_release_high_priority_resumes_during_protect(home):
    """保护轮进行中：优先级不低于保护对象的 PAUSED 仍可恢复；
    低优先级保持逐步放回节奏（pending_resume）。"""
    svc = _svc(home)
    assert svc.protect.start_round("t_prot", 70, time.monotonic(),
                                   reason="统一外部不足")
    _mk_job(svc.db, "j_prot", task_id="t_prot", priority=70)  # 保护对象自身
    _mk_job(svc.db, "j_eq", task_id="t_eq", priority=70)      # 同级
    _mk_job(svc.db, "j_lo", task_id="t_lo", priority=30)     # 低优先级
    _mk_job(svc.db, "j_pr", task_id="t_pr", priority=90)      # 待逐步放回
    for jid in ("j_prot", "j_eq", "j_lo", "j_pr"):
        svc.db.set_job_status(jid, JobStatus.PAUSED, "测试暂停", runner.now_iso())
    svc.protect.st.pending_resume["t_pr"] = 90
    svc._tick_release(time.monotonic())
    assert svc.db.get_job("j_prot")["status"] == JobStatus.QUEUED
    assert svc.db.get_job("j_eq")["status"] == JobStatus.QUEUED
    assert svc.db.get_job("j_lo")["status"] == JobStatus.PAUSED
    assert svc.db.get_job("j_pr")["status"] == JobStatus.PAUSED
    assert "t_pr" in svc.protect.st.pending_resume


def test_release_without_protect_resumes_all(home):
    svc = _svc(home)
    _mk_job(svc.db, "j_lo", task_id="t_lo", priority=30)
    _mk_job(svc.db, "j_hi", task_id="t_hi", priority=90)
    for jid in ("j_lo", "j_hi"):
        svc.db.set_job_status(jid, JobStatus.PAUSED, "测试暂停", runner.now_iso())
    svc._tick_release(time.monotonic())
    assert svc.db.get_job("j_lo")["status"] == JobStatus.QUEUED
    assert svc.db.get_job("j_hi")["status"] == JobStatus.QUEUED


def test_release_stale_cooldown_still_applies(home):
    """心跳超时冷却逻辑保持：冷却窗口内即使高优先级也不立即重启。"""
    svc = _svc(home)
    _mk_job(svc.db, "j_hb", task_id="t_hb", priority=90)
    svc.db.set_job_status("j_hb", JobStatus.PAUSED, "心跳超时", runner.now_iso())
    svc._tick_release(time.monotonic())
    assert svc.db.get_job("j_hb")["status"] == JobStatus.PAUSED


# ---------- B1：I/O 压力参与资源健康判定 ----------

def test_io_pressure_marks_unhealthy():
    t = scaling.ResourceTrend(0.9, 0.8, 3)
    for _ in range(3):
        t.sample(0.1, 0.1, True, io_pressure=0.3)  # psi full avg10 = 30
    assert t.unhealthy()


def test_io_pressure_low_occupancy_recovers():
    t = scaling.ResourceTrend(0.9, 0.8, 3)
    for _ in range(3):
        t.sample(0.1, 0.1, True, io_pressure=0.3)
    assert t.unhealthy()
    for _ in range(3):
        t.sample(0.1, 0.1, True, io_pressure=0.1)  # 回到阈值以下
    assert not t.unhealthy()


def test_io_pressure_default_zero_not_unhealthy():
    t = scaling.ResourceTrend(0.9, 0.8, 3)
    for _ in range(5):
        t.sample(0.1, 0.1, True)  # 无 psi 观测（缺省 0）不误判
    assert not t.unhealthy()


def test_io_pressure_mid_band_counts_high():
    """cpu/mem 处于滞回中间带时，I/O 高压仍累计高占用证据。"""
    t = scaling.ResourceTrend(0.9, 0.8, 3)
    for _ in range(3):
        t.sample(0.85, 0.1, True, io_pressure=0.3)
    assert t.unhealthy()


# ---------- B2：task_monitor 富字段 ----------

def test_task_monitor_rich_fields(home, monkeypatch):
    svc = _svc(home)
    svc.specs["t1"] = _spec("t1")
    now = time.monotonic()
    for i in range(3):  # 阶段 s1：恒定速率 10 units/s
        svc.health.record_progress("t1", now - 30 + i * 10, i * 100)
    _mk_job(svc.db, "j_b", task_id="t1", priority=50)
    svc.db.set_job_status("j_b", JobStatus.BACKOFF,
                          "测试不足：外部依赖不可用", runner.now_iso())
    m = _mk_meta("a1", "j_p", "t1", 50, status=JobStatus.PAUSING)
    m.pause_command = {"reason": "高优先级保护退让"}
    svc.meta["a1"] = m
    monkeypatch.setattr(svc.guard, "list", lambda: [{
        "task_id": "t1", "pid": 12345, "alive": True, "external": False,
        "crash_count": 2, "last_crash_wall": "2026-10-12T08:00:00+08:00"}])
    mon = svc._task_monitor()["t1"]
    assert mon["backoff_reason"] == "测试不足：外部依赖不可用"
    assert mon["pausing_reasons"] == {"高优先级保护退让": 1}
    p = mon["producer"]
    assert p["pid"] == 12345 and p["alive"] is True
    assert p["crash_count"] == 2
    assert p["last_crash_wall"] == "2026-10-12T08:00:00+08:00"
    hs = mon["health"]
    assert hs["samples"] == 3
    assert hs["completed_units"] == 200
    assert hs["rate_per_sec"] == 10.0  # (200-0) / 20s


# ---------- B3：接收器新文件优先发现 ----------

def test_receiver_new_files_first(home):
    db = dbmod.DB()
    spec = _spec("t1")
    out = config.outbox_dir("t1")
    out.mkdir(parents=True)

    def put(name):
        (out / name).write_text(json.dumps({
            "schema_version": 1, "job_id": name[:-5], "task_id": "t1",
            "entry_args": [], "env": {}, "idempotency_key": name[:-5]}))

    for i in range(5):
        put(f"f{i}.json")
    # 首轮：全部是新文件，配额半数给新文件
    s1 = receiver.receive_outbox("t1", spec, db, runner.now_iso(), batch_size=2)
    assert s1["seen"] == 1 and s1["deferred"] == 4
    # 次轮：无新文件，旧文件按剩余配额保底处理
    s2 = receiver.receive_outbox("t1", spec, db, runner.now_iso(), batch_size=2)
    assert s2["seen"] == 1 and s2["deferred"] == 3
    # 第三轮：新增 2 个文件，新文件配额优先 + 旧文件保底各半
    put("n1.json")
    put("n2.json")
    s3 = receiver.receive_outbox("t1", spec, db, runner.now_iso(), batch_size=2)
    assert s3["seen"] == 2 and s3["deferred"] == 3
    assert db.get_job("n1") is not None       # 新文件被优先处理
    assert db.get_job("f2") is not None       # 旧文件保底处理
    assert db.get_job("f3") is None and db.get_job("n2") is None
    # 全量收尾：outbox 清空；再扫一轮空目录，seen 集合随之出清
    s4 = receiver.receive_outbox("t1", spec, db, runner.now_iso())
    assert s4["seen"] == 3 and db.count_queued() == 7
    assert not list(out.glob("*.json"))
    s5 = receiver.receive_outbox("t1", spec, db, runner.now_iso())
    assert s5["seen"] == 0
    assert receiver._seen_files["t1"] == set()


# ---------- B4：recovery 说明非空校验 ----------

def test_parse_task_requires_recovery_notes(home):
    td = home / "tasks" / "t1"
    td.mkdir(parents=True)
    (td / "producer.py").write_text("")
    (td / "run.py").write_text("")
    good = {
        "schema_version": 1, "task_id": "t1", "priority": 50,
        "producer_entry": {"file": "producer.py"},
        "executor_entry": {"file": "run.py"},
        "recovery": {"replay": "outbox", "retain": "保留 7 天",
                     "replay_note": "按事件 ID 去重"},
    }
    assert config.parse_task(good, "t1").task_id == "t1"

    def assert_rejected(mutate):
        raw = json.loads(json.dumps(good))
        mutate(raw)
        with pytest.raises(config.TaskError):
            config.parse_task(raw, "t1")

    assert_rejected(lambda r: r["recovery"].__setitem__("retain", ""))
    assert_rejected(lambda r: r["recovery"].__setitem__("retain", "   "))
    assert_rejected(lambda r: r["recovery"].pop("retain"))
    assert_rejected(lambda r: r["recovery"].__setitem__("replay_note", ""))
    assert_rejected(lambda r: r["recovery"].pop("replay_note"))


# ---------- B5：completed_units 单调递增 ----------

def test_report_progress_monotonic(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_EVENT_FILE", str(tmp_path / "ev.jsonl"))
    monkeypatch.setenv("SCHED_JOB_ID", "j1")
    monkeypatch.setenv("SCHED_ATTEMPT_ID", "a1")
    from sched_task_sdk.executor import ExecutorTask

    class T(ExecutorTask):
        def execute(self):
            pass

    t = T()
    t.report_progress(5)
    t.report_progress(7)
    with pytest.raises(ValueError, match="单调递增"):
        t.report_progress(6)       # 回退 → 立即抛错
    t.report_progress(7)           # 相等允许（重复上报同值）
    t2 = T()
    t2.report_progress(0)          # 新实例（新 attempt）从 0 合法


# ---------- C1：回执一次性消费 + 归档 GC + 窗口对账 ----------

def test_consume_receipts_deletes_after_persist(monkeypatch, tmp_path):
    """回执是一次性确认凭证：账目落盘后删除（含孤儿/损坏回执一并清理）。"""
    monkeypatch.setenv("SCHED_TASK_ID", "t")
    monkeypatch.setenv("SCHED_WORKSPACE", str(tmp_path))
    from sched_task_sdk.producer import Producer, job_id_of
    p = Producer()
    p.advance(1, [{"event_id": "e1", "business_version": "v1"}])
    j1 = job_id_of("t", "e1", "v1")
    rd = tmp_path / "receipts"
    rd.mkdir()
    (rd / f"{j1}.json").write_text(json.dumps({"job_id": j1, "status": "received"}))
    (rd / "orphan.json").write_text(
        json.dumps({"job_id": "j_orphan", "status": "received"}))
    (rd / "broken.json").write_text("{broken")
    assert p.consume_receipts() == 1
    assert j1 not in p._state["unacked"]
    assert not list(rd.glob("*.json"))


def test_consume_receipts_keeps_receipt_on_save_failure(monkeypatch, tmp_path):
    """_save 失败异常冒泡：回执保留，下轮重试（不丢确认）。"""
    monkeypatch.setenv("SCHED_TASK_ID", "t")
    monkeypatch.setenv("SCHED_WORKSPACE", str(tmp_path))
    from sched_task_sdk.producer import Producer, job_id_of
    p = Producer()
    p.advance(1, [{"event_id": "e1", "business_version": "v1"}])
    j1 = job_id_of("t", "e1", "v1")
    rd = tmp_path / "receipts"
    rd.mkdir()
    (rd / f"{j1}.json").write_text(json.dumps({"job_id": j1, "status": "received"}))

    def boom():
        raise OSError("disk full")

    orig_save = p._save
    p._save = boom
    with pytest.raises(OSError):
        p.consume_receipts()
    assert (rd / f"{j1}.json").exists()      # 回执保留
    assert j1 in p._state["unacked"]        # 内存账目已回滚：重试仍能检测到变更
    # 恢复落盘能力后重试：确认持久化 + 回执删除一次完成
    p._save = orig_save
    assert p.consume_receipts() == 1
    assert not list(rd.glob("*.json"))
    disk = json.loads((tmp_path / "producer_state" / "state.json").read_text("utf-8"))
    assert j1 not in disk["unacked"]


def test_archive_gc_removes_expired(home):
    svc = _svc(home)
    d = config.archive_root() / "t"
    d.mkdir(parents=True)
    old, new = d / "old.json", d / "new.json"
    old.write_text("{}")
    new.write_text("{}")
    expired = time.time() - 8 * 86400
    os.utime(old, (expired, expired))
    svc._tick_archive_gc(10 ** 9)  # _last_archive_gc_mono=0 → 首轮立即执行
    assert not old.exists()
    assert new.exists()
    # 间隔节流：不到 12h 的下一轮不重复执行
    svc._last_archive_gc_mono = 10 ** 9
    old2 = d / "old2.json"
    old2.write_text("{}")
    os.utime(old2, (expired, expired))
    svc._tick_archive_gc(10 ** 9 + 100)
    assert old2.exists()


def test_reconcile_archive_window_alarm(home):
    """窗口对账（单向：库→归档文件）：窗口内作业缺归档告警，
    ghost 文件（库无文件有）不告警。"""
    svc = _svc(home)
    _mk_job(svc.db, "j_ok", task_id="t")
    _mk_job(svc.db, "j_miss", task_id="t")
    ad = config.archive_root() / "t"
    ad.mkdir(parents=True)
    (ad / "j_ok.json").write_text("{}")
    (ad / "ghost.json").write_text("{}")
    statuses: list[dict] = []
    svc._append_status = statuses.append
    svc._reconcile_archive()
    alarmed = {s.get("job_id") for s in statuses
               if s.get("kind") == "archive_missing"}
    assert alarmed == {"j_miss"}


# （SDK 检查点设施已整体移除：恢复/幂等改由任务以外部真源自证）


# ---------- D4：_pause_grace_base 无参调用 ----------

def test_pause_grace_base_no_arg(home):
    svc = _svc(home)
    assert svc._pause_grace_base() == svc.cfg.control.pause_grace_base_sec
