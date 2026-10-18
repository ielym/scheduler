"""新架构核心单元测试：config / db / receiver / scaling / SDK(事件+检查点)。"""

import json
import os
import sys

import pytest

ROOT = str(__import__("pathlib").Path(__file__).resolve().parent.parent)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from sched import config, db as dbmod, receiver, scaling, store  # noqa: E402


@pytest.fixture()
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_HOME", str(tmp_path))
    (tmp_path / "tasks").mkdir()
    (tmp_path / "runtime").mkdir()
    return tmp_path


def _write_task(home, task_id, priority=50, enabled=True):
    td = home / "tasks" / task_id
    td.mkdir(parents=True, exist_ok=True)
    (td / "producer.py").write_text("")
    (td / "run.py").write_text("")
    (td / "task.json").write_text(json.dumps({
        "schema_version": 1, "task_id": task_id, "enabled": enabled, "priority": priority,
        "producer_entry": {"file": "producer.py", "interpreter": "python3", "args": [], "cwd": "."},
        "executor_entry": {"file": "run.py", "interpreter": "python3", "args": [], "cwd": "."},
        "workdir": ".",
        "recovery": {"replay": "outbox", "retain": "测试保留", "replay_note": "测试重放"},
    }))
    return td


# ---------- config ----------
def test_parse_task_ok(home):
    _write_task(home, "demo")
    spec = config.load_task("demo")
    assert spec.task_id == "demo" and spec.priority == 50


def test_parse_task_id_mismatch(home):
    td = _write_task(home, "demo")
    (td / "task.json").write_text(json.dumps({
        "schema_version": 1, "task_id": "other", "priority": 1,
        "producer_entry": {"file": "producer.py"}, "executor_entry": {"file": "run.py"},
    }))
    with pytest.raises(config.TaskError):
        config.load_task("demo")


def test_parse_ready(home):
    doc = config.parse_ready({"schema_version": 1, "job_id": "j1", "task_id": "demo",
                              "entry_args": ["a"], "idempotency_key": "k"})
    assert doc.job_id == "j1" and doc.entry_args == ["a"]


# ---------- db ----------
def test_db_upsert_dedup_conflict(tmp_path):
    d = dbmod.DB(tmp_path / "e.db")
    ready = config.parse_ready({"schema_version": 1, "job_id": "j1", "task_id": "t",
                                "env": {}, "entry_args": [], "idempotency_key": "k"})
    st, created = d.upsert_job(ready, "hashA", "now", 50)
    assert st == "created" and created
    st, created = d.upsert_job(ready, "hashA", "now2", 50)
    assert st == "existing" and not created
    st, created = d.upsert_job(ready, "hashB", "now3", 50)
    assert st == "conflict" and not created


def test_db_attempt_unique_live(tmp_path):
    d = dbmod.DB(tmp_path / "e.db")
    ready = config.parse_ready({"schema_version": 1, "job_id": "j1", "task_id": "t", "idempotency_key": "k"})
    d.upsert_job(ready, "hashA", "now", 50)
    d.create_attempt("a1", "j1", 1, "tok", "now")
    assert d.live_jobs_for_job("j1")
    # 第二个存活 attempt 被唯一索引禁止
    with pytest.raises(Exception):
        d.create_attempt("a2", "j1", 2, "tok2", "now")


# ---------- receiver ----------
def test_receiver_flow(home, tmp_path):
    _write_task(home, "demo")
    out = config.outbox_dir("demo")
    out.mkdir(parents=True)
    payload = {"schema_version": 1, "job_id": "demo-abc", "task_id": "demo",
               "entry_args": [], "input_refs": [], "idempotency_key": "k", "env": {}}
    (out / "demo-abc.json").write_text(json.dumps(payload))
    d = dbmod.DB(tmp_path / "e.db")
    spec = config.load_task("demo")
    summary = receiver.receive_outbox("demo", spec, d, "now")
    assert summary["created"] == 1
    assert d.get_job("demo-abc")["status"] == config.JobStatus.QUEUED
    # 回执写入
    assert (config.receipts_dir("demo") / "demo-abc.json").exists()
    # 归档
    assert not list(out.glob("*.json"))


def test_receiver_conflict_isolation(home, tmp_path):
    _write_task(home, "demo")
    out = config.outbox_dir("demo")
    out.mkdir(parents=True)
    d = dbmod.DB(tmp_path / "e.db")
    spec = config.load_task("demo")
    p1 = {"schema_version": 1, "job_id": "demo-x", "task_id": "demo", "env": {}, "idempotency_key": "k"}
    p2 = {"schema_version": 1, "job_id": "demo-x", "task_id": "demo", "env": {"a": "1"}, "idempotency_key": "k"}
    (out / "demo-x.json").write_text(json.dumps(p1))
    receiver.receive_outbox("demo", spec, d, "now")
    (out / "demo-x.json").write_text(json.dumps(p2))
    summary = receiver.receive_outbox("demo", spec, d, "now2")
    assert summary["conflict"] == 1
    assert (config.isolation_root() / "demo" / "demo-x.json").exists()
    # 原作业未被覆盖
    assert d.get_job("demo-x")["env_json"] == "{}"


# ---------- scaling ----------
def test_scaling_growth(tmp_path):
    class Cfg:
        class control:
            grow_observe_sec = 30.0
    s = scaling.Scaling(Cfg())
    s.get("t").run_target = 1
    s.get("t").mode = "steady"
    s.apply_grow("t", 10.0)  # run_target -> 2, last_grow_mono=10
    # 观察期内不允许再扩容
    assert not s.may_grow("t", 20.0, live_count=2, has_progress=True, healthy_resource=True)
    # 观察期过后、达到当前目标(live_count==run_target)时允许
    assert s.may_grow("t", 50.0, live_count=2, has_progress=True, healthy_resource=True)
    # 未达到目标（live<run_target）不允许
    assert not s.may_grow("t", 50.0, live_count=1, has_progress=True, healthy_resource=True)


def test_scaling_shortage_marks_evidence(tmp_path):
    """on_shortage 只记证据不收缩动态目标：资源退避由 TaskBackoff 承担
    （步降暂停同任务在途执行、逐步验证、全退避探测、逐个放回），
    恢复后按既有目标逐个拉回，无需重新学习。"""
    class Cfg:
        class control:
            grow_observe_sec = 30.0
    s = scaling.Scaling(Cfg())
    s.get("t").run_target = 4
    # 不足证据：streak 累计 + 切回 explore（阻止扩容探索），目标不变
    s.on_shortage("t", 0.0, live_count=3)
    assert s.get("t").run_target == 4
    assert s.get("t").shortage_streak == 1
    assert s.get("t").mode == "explore"
    s.on_shortage("t", 1.0, live_count=2)
    assert s.get("t").run_target == 4
    assert s.get("t").shortage_streak == 2


def test_task_backoff_state_machine():
    """TaskBackoff：同任务作业必共用资源 → 报错步降自己任务内的执行；
    跨任务独立；逐个拉取一步验证；全退避按递增间隔探测。"""
    tb = scaling.TaskBackoff(probe_base_sec=30.0, probe_cap_sec=600.0)
    # NORMAL：允许准入；新执行未完成首步 → 阻止放下一个（逐个拉取）
    assert tb.admission_allowed("t", 0.0) == (True, "")
    tb.note_started("t", "att-1")
    assert tb.admission_allowed("t", 1.0) == (False, "step_pending")
    # 正确一步 → 解除占位，恢复放行
    tb.note_progress("t", "att-1", 2.0)
    assert tb.admission_allowed("t", 3.0) == (True, "")
    # VERIFY（步降后等剩余执行验证）：阻止新准入；需连续 N 步（默认 2）且无新 75
    tb.set_state("t", tb.VERIFY)
    ok, why = tb.admission_allowed("t", 4.0)
    assert not ok and why == "verify"
    tb.note_progress("t", "att-1", 5.0)
    assert tb.state_of("t") == tb.VERIFY        # 一步不足以恢复（方案 A′）
    tb.note_progress("t", "att-1", 5.5)
    assert tb.state_of("t") == tb.NORMAL
    assert tb.admission_allowed("t", 6.0) == (True, "")
    # EMPTY（全退避）：探测间隔内阻止，到期放行；探测失败级别递增
    tb.set_state("t", tb.EMPTY, now_mono=100.0)
    ok, why = tb.admission_allowed("t", 110.0)
    assert not ok and why == "probe_wait"
    ok, why = tb.admission_allowed("t", 131.0)
    assert ok and why == ""            # 间隔到期：放一个探测执行
    tb.note_started("t", "att-2")      # 探测执行拉起
    tb.clear_attempt("t", "att-2")     # 它退出了且未产生正确一步
    tb.note_empty_probe_failed("t", 131.0)   # 探测失败：级别+1 → 间隔翻倍
    assert tb.probe_level_of("t") == 1
    ok, _ = tb.admission_allowed("t", 161.0)  # 30s 级别0间隔已过但级别1需60s
    assert not ok
    ok, _ = tb.admission_allowed("t", 192.0)  # 100+60+1 边界后放行
    assert ok
    # 探测执行连续 N 步 → NORMAL，级别清零（方案 A′：连续步才恢复）
    tb.note_progress("t", "att-3", 193.0)
    assert tb.state_of("t") == tb.EMPTY
    tb.note_progress("t", "att-3", 193.5)
    assert tb.state_of("t") == tb.NORMAL
    assert tb.probe_level_of("t") == 0
    # 任务间完全独立：t 的退避不影响 u
    assert tb.admission_allowed("u", 194.0) == (True, "")


def test_protect_yield_candidate():
    class Cfg:
        class control:
            protect_deadline_sec = 300.0
            protect_round_backoff_sec = 60.0
    p = scaling.Protect(Cfg())
    p.start_round("hi", 90, 0.0)
    running = [
        {"attempt_id": "a1", "task_id": "lo", "priority": 10, "shortage_recent": False, "grown_recently": True},
        {"attempt_id": "a2", "task_id": "mid", "priority": 50, "shortage_recent": True, "grown_recently": False},
    ]
    assert p.choose_yield_candidate(running, 1.0) == "a1"  # 优先级更低者先


# ---------- SDK ----------
def test_sdk_event_writer(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_EVENT_FILE", str(tmp_path / "events.jsonl"))
    from sched_task_sdk import events as ev
    from sched_task_sdk.events import EventWriter
    w = EventWriter()
    w.emit("started", name="x")
    w.emit("progress", completed_units=3)
    lines = (tmp_path / "events.jsonl").read_text().strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[1])["completed_units"] == 3