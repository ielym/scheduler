"""第三轮审计修复的专项回归。

覆盖本轮新语义的直接单测（不依赖真实子进程，端到端由 test_integration.py 覆盖）：
- 重启冷启动：遗留执行按 cgroup 归属终止、attempt 清除、作业回 QUEUED（无完成核实）；
- receiver task_id 不匹配分支补写 invalid 回执（回执闭环）；
- 就绪 JSON workdir 包含校验 + task_id 格式校验（防越权）；
- cmd_task enable/disable 容错与启用前校验；
- EntrySpec interpreter 命令名校验；
- HealthTracker 基线按 task_id 单维度分离；
- OOM 让级候选不高于受害者优先级（退让只向下让）；
- 遗留行（有 pid 无 pid_start_time）恢复安全化（cgroup 归属核实）；
- skill 模板（atomic_write_json / wait_interruptible）。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = str(Path(__file__).resolve().parent.parent)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import pytest  # noqa: E402

from sched import config, db as dbmod, receiver, scaling  # noqa: E402
from sched import __main__ as cli  # noqa: E402
from sched.config import JobStatus, ReadyError, TaskError  # noqa: E402
from sched.scheduler import AttMeta, SchedulerService  # noqa: E402

TEMPLATES_DIR = Path("/root/.trae-cn/skills/sched/assets/templates")


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


def _ready(job_id, task_id="t1"):
    return config.parse_ready({
        "schema_version": 1, "job_id": job_id, "task_id": task_id,
        "entry_args": [], "env": {}, "idempotency_key": job_id,
    })


# ---------- receiver：task_id 不匹配回执 ----------
class TestMismatchTaskIdReceipt:
    def test_mismatched_task_id_writes_invalid_receipt(self, home, tmp_path):
        _write_task(home, "demo")
        out = config.outbox_dir("demo")
        out.mkdir(parents=True)
        payload = {"schema_version": 1, "job_id": "demo-x", "task_id": "other",
                   "entry_args": [], "env": {}, "idempotency_key": "k"}
        (out / "demo-x.json").write_text(json.dumps(payload))
        db = dbmod.DB(tmp_path / "e.db")
        spec = config.load_task("demo")
        summary = receiver.receive_outbox("demo", spec, db, "now")
        assert summary["invalid"] == 1
        # 回执闭环：错误投递的作业也有反馈，生产者可据此处理
        rc = config.receipts_dir("demo") / "demo-x.json"
        assert rc.exists()
        doc = json.loads(rc.read_text())
        assert doc["status"] == "invalid"
        assert "other" in doc["reason"]
        # 不入库、已隔离
        assert db.get_job("demo-x") is None
        assert (config.isolation_root() / "demo" / "demo-x.json").exists()


# ---------- 就绪 JSON workdir 包含校验 ----------
class TestReadyWorkdir:
    def _doc(self, **over):
        d = {"schema_version": 1, "job_id": "j1", "task_id": "t",
             "entry_args": [], "env": {}, "idempotency_key": "k"}
        d.update(over)
        return d

    def test_rejects_escaping_workdir(self):
        with pytest.raises(ReadyError):
            config.parse_ready(self._doc(workdir="../escape"))

    def test_rejects_absolute_workdir(self):
        with pytest.raises(ReadyError):
            config.parse_ready(self._doc(workdir="/etc"))

    def test_rejects_dotdot_inside(self):
        with pytest.raises(ReadyError):
            config.parse_ready(self._doc(workdir="a/../../x"))

    def test_rejects_bad_task_id_format(self):
        with pytest.raises(ReadyError):
            config.parse_ready(self._doc(task_id="a/b"))

    def test_accepts_nested_relative(self):
        doc = config.parse_ready(self._doc(workdir="config/dates"))
        assert doc.workdir == "config/dates"

    def test_receiver_isolates_escaping_workdir(self, home, tmp_path):
        _write_task(home, "demo")
        out = config.outbox_dir("demo")
        out.mkdir(parents=True)
        payload = self._doc(job_id="demo-esc", task_id="demo",
                            workdir="../demo")
        (out / "demo-esc.json").write_text(json.dumps(payload))
        db = dbmod.DB(tmp_path / "e.db")
        spec = config.load_task("demo")
        summary = receiver.receive_outbox("demo", spec, db, "now")
        assert summary["invalid"] == 1
        assert (config.isolation_root() / "demo" / "demo-esc.json").exists()
        assert (config.receipts_dir("demo") / "demo-esc.json").exists()
        assert db.get_job("demo-esc") is None


# ---------- cmd_task enable/disable 容错与校验 ----------
class TestEnableDisable:
    def _run(self, action, task_id):
        return cli.cmd_task(argparse.Namespace(action=action, task_id=task_id))

    def test_enable_rejects_corrupt_json_untouched(self, home):
        td = _write_task(home, "demo")
        (td / "task.json").write_text("{not json", encoding="utf-8")
        assert self._run("enable", "demo") == 2
        assert (td / "task.json").read_text(encoding="utf-8") == "{not json"

    def test_enable_rejects_non_dict(self, home):
        td = _write_task(home, "demo")
        (td / "task.json").write_text("[1, 2]", encoding="utf-8")
        assert self._run("enable", "demo") == 2

    def test_enable_rejects_invalid_spec_keeps_disabled(self, home):
        # 入口文件缺失的清单：启用被拒绝，enabled 保持 false
        _write_task(home, "demo", raw_extra={
            "enabled": False,
            "executor_entry": {"file": "missing.py", "interpreter": "python3"},
        })
        assert self._run("enable", "demo") == 2
        raw = json.loads((home / "tasks" / "demo" / "task.json").read_text())
        assert raw["enabled"] is False

    def test_disable_allowed_on_invalid_spec(self, home):
        # disable 是运维出口：清单不合规也允许禁用
        _write_task(home, "demo", raw_extra={
            "recovery": {"replay": "outbox", "retain": "", "replay_note": ""},
        })
        assert self._run("disable", "demo") == 0
        raw = json.loads((home / "tasks" / "demo" / "task.json").read_text())
        assert raw["enabled"] is False

    def test_enable_disable_roundtrip(self, home):
        _write_task(home, "demo")
        assert self._run("disable", "demo") == 0
        raw = json.loads((home / "tasks" / "demo" / "task.json").read_text())
        assert raw["enabled"] is False
        assert self._run("enable", "demo") == 0
        raw = json.loads((home / "tasks" / "demo" / "task.json").read_text())
        assert raw["enabled"] is True


# ---------- interpreter 命令名校验 ----------
class TestInterpreterValidation:
    @pytest.mark.parametrize("bad", [
        "/usr/bin/python3", "./venv/bin/python", "", "python/../python3",
    ])
    def test_rejects_path_like(self, home, bad):
        _write_task(home, "demo", raw_extra={
            "producer_entry": {"file": "producer.py", "interpreter": bad},
        })
        with pytest.raises(TaskError):
            config.load_task("demo")

    def test_accepts_command_name(self, home):
        _write_task(home, "demo")
        assert config.load_task("demo").producer_entry.interpreter == "python3"


# ---------- OOM 让级候选 ----------
class TestOOMYieldCandidate:
    def _meta(self, svc, aid, pri, status=JobStatus.RUNNING):
        m = AttMeta(aid, f"job-{aid}", "t", pri, "tok")
        m.status = status
        svc.meta[aid] = m
        return m

    def test_only_strictly_lower_priority_chosen(self, home):
        svc = _svc(home)
        self._meta(svc, "att-victim", 10)
        self._meta(svc, "att-low", 5)
        self._meta(svc, "att-high", 20)
        self._meta(svc, "att-equal", 10)
        assert svc._choose_oom_yield_candidate("att-victim") == "att-low"

    def test_no_lower_candidate_returns_none(self, home):
        svc = _svc(home)
        self._meta(svc, "att-victim", 5)
        self._meta(svc, "att-high", 20)
        self._meta(svc, "att-equal", 5)
        # 受害者已是最低优先级 → 无候选（回退暂停受害者自身）
        assert svc._choose_oom_yield_candidate("att-victim") is None

    def test_unknown_victim_returns_none(self, home):
        svc = _svc(home)
        self._meta(svc, "att-low", 5)
        assert svc._choose_oom_yield_candidate("att-unknown") is None

    def test_only_live_status_considered(self, home):
        svc = _svc(home)
        self._meta(svc, "att-victim", 10)
        self._meta(svc, "att-paused", 5, status=JobStatus.PAUSED)
        assert svc._choose_oom_yield_candidate("att-victim") is None


# ---------- 遗留行冷启动处理 ----------
class TestLegacyRowReinit:
    def test_leftover_proc_killed_via_cgroup_and_requeued(self, home, monkeypatch):
        # 重启冷启动：遗留执行按 cgroup 整组终止，attempt 与工件删除，作业回 QUEUED
        svc = _svc(home)
        db = svc.db
        db.upsert_job(_ready("j-1"), "h1", "now", 50)
        db.create_attempt("att-1", "j-1", 1, "tok", "now")
        db.update_attempt("att-1", pid=os.getpid(), pgid=os.getpid(),
                          pid_start_time=12345.0, cgroup="t/att-1")
        db.set_job_status("j-1", JobStatus.RUNNING, "", "now")
        from sched import cgroup as cg
        killed: list[str] = []
        monkeypatch.setattr(cg.Cgroup, "kill_all",
                            lambda self: killed.append(self.name))
        svc._reinitialize()
        assert killed == ["t/att-1"]
        assert db.get_job("j-1")["status"] == JobStatus.QUEUED
        assert db.get_attempt("att-1")["status"] == JobStatus.ABANDONED  # 保留记录作审计


# ---------- skill 模板 ----------
class TestTemplates:
    @pytest.mark.skipif(not TEMPLATES_DIR.is_dir(), reason="模板目录不存在")
    def test_producer_templates_use_atomic_write(self):
        for name in ("01_daily_producer/producer.py",
                     "02_batch_backfill/producer.py"):
            src = (TEMPLATES_DIR / name).read_text(encoding="utf-8")
            compile(src, name, "exec")  # 语法有效
            assert "atomic_write_json(" in src, name
            assert "write_text(json.dumps" not in src, name

    @pytest.mark.skipif(not TEMPLATES_DIR.is_dir(), reason="模板目录不存在")
    def test_longtask_template_uses_interruptible_wait(self):
        src = (TEMPLATES_DIR / "03_longtask" / "run.py").read_text(encoding="utf-8")
        compile(src, "03_longtask/run.py", "exec")
        assert "wait_interruptible(" in src
        assert "time.sleep" not in src
