"""运行期契约回归测试。

锁定行为：
- jobs.json 热加载保序（不重置未到期触发）+ 热加载新任务即时生效
- SIGKILL(-9) 三态归因（外部 kill→failed / cgroup OOM→resource / 抢占超时→preempt_failed）
- error_rules 经 SCHED_ERROR_RULES 注入任务进程，SDK 启动时读取
- run_user 不存在时注册报错含 useradd 指引
- 闸5 运行期心跳契约（拒绝伪契约任务）+ 心跳误杀只重入队一次
- stdout 截断、jsonl 轮转、单实例锁 + serve 友好报错、重入队字段工厂
- 110 数据风险 → second_pass 二刷端到端闭环
"""

from __future__ import annotations

import fcntl
import json
import os
import pwd
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from sched import config, contract, errors, runlog, runner, store, taskdoc
from sched import cgroup as cgroup_mod
from sched import engine as engine_mod
from sched.registry import JobEntry
from sched.service import SchedulerService
from sched_task_sdk import SchedTask

from conftest import write_task


def _wait(pred, timeout=30.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.1)
    return False


def _svc_with_entry(home, make_settings, name="ok", **settings_kw):
    write_task(home, name, real_sleep=0.01)
    task_dir = home / "tasks" / name
    doc = taskdoc.load_task_doc(task_dir)
    svc = SchedulerService(make_settings(run_user="", **settings_kw))
    svc.entries[name] = JobEntry(job_id=name, task_dir=task_dir, doc=doc, enabled=True)
    return svc


def _make_spec(home, name, run_id="r1", code=None, **kw):
    task_dir = home / "tasks" / name
    task_dir.mkdir(parents=True, exist_ok=True)
    if code is not None:
        (task_dir / "run.py").write_text(code, encoding="utf-8")
    defaults = dict(
        job_id=name, run_id=run_id, task_dir=task_dir, entry_file="run.py",
        interpreter="python3", args=[], cwd=".", env={}, trigger="manual",
        attempt=1, scheduled_at=runlog.now_iso(), run_user="",
    )
    defaults.update(kw)
    return runner.RunSpec(**defaults)


# ================= upsert 保序 + 热加载 =================

class _FakeDT(datetime):
    """可控时钟：让 engine 内 datetime.now() 按偏移推进（继承以保留 fromisoformat 等）。"""

    offset = 0.0

    @classmethod
    def now(cls, tz=None):
        return cls(2026, 10, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai")) + timedelta(seconds=cls.offset)


def test_p1_c_upsert_preserves_earlier_pending_trigger(home, monkeypatch):
    """refresh 热加载不得把未到期的 interval 触发重置为 now+interval。"""
    monkeypatch.setattr(engine_mod, "datetime", _FakeDT)
    base = datetime(2026, 10, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    eng = engine_mod.TriggerEngine("Asia/Shanghai")
    sched = {"type": "interval", "interval": 3600}
    _FakeDT.offset = 0
    eng.upsert("j", sched)
    first = eng.next_run_time("j")
    assert first == (base + timedelta(seconds=3600)).isoformat(timespec="seconds")

    # 100 秒后重复 upsert：新算出的触发（now+3600）更晚 → 保留原时刻
    _FakeDT.offset = 100
    eng.upsert("j", sched)
    assert eng.next_run_time("j") == first

    # 新配置的触发更早（interval 缩短）→ 允许重置
    eng.upsert("j", {"type": "interval", "interval": 10})
    assert eng.next_run_time("j") == (base + timedelta(seconds=110)).isoformat(timespec="seconds")

    # 已失效 once 的 None 残留在刷新时清除
    eng._next["gone"] = None
    eng.upsert("gone", {"type": "once", "once_at": "2000-01-01T00:00:00+08:00"})
    assert "gone" not in eng._next


def test_p1_c_refresh_hot_loads_new_interval_job(home, make_settings):
    """serve 热加载：注册表新任务经 refresh 进入触发引擎，且重复刷新不重置触发。"""
    task_dir = write_task(home, "hot", real_sleep=0.01)
    tj = task_dir / "task.json"
    doc = json.loads(tj.read_text("utf-8"))
    doc["schedule"] = {"type": "interval", "interval": 3600, "max_instances": 1, "overflow": "queue"}
    tj.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")

    from sched import registry
    registry.upsert_state("hot", {"enabled": True})
    svc = SchedulerService(make_settings(run_user=""))
    svc.refresh()
    assert "hot" in svc.entries and svc.entries["hot"].ok
    nxt1 = svc.engine.next_run_time("hot")
    assert nxt1 is not None, "热加载应把新注册任务纳入触发引擎"

    svc.refresh()  # 热加载重复刷新不重置
    assert svc.engine.next_run_time("hot") == nxt1


# ================= SIGKILL(-9) 三态归因 =================

def test_p1_d_sigkill_external_is_failed(home):
    """外部 SIGKILL（rc=-9）：无 OOM、非调度器终结 → failed（转人工归因）。"""
    spec = _make_spec(home, "kext",
                      code="import os, signal, time; time.sleep(0.3); os.kill(os.getpid(), signal.SIGKILL)")
    out = runner.execute(spec)
    assert out.exit_code == -9
    assert out.status == runlog.STATUS_FAILED
    assert "外部" in out.reason
    assert not out.scheduler_signaled


def test_p1_d_sigkill_oom_is_resource(home, monkeypatch):
    """cgroup OOM 计数 > 0 时 rc=-9 → resource（资源不足可重试，不判失败）。"""
    monkeypatch.setattr(cgroup_mod.Cgroup, "read_oom_kill_count", lambda self: 1)
    spec = _make_spec(home, "koom",
                      code="import os, signal, time; time.sleep(0.3); os.kill(os.getpid(), signal.SIGKILL)")
    out = runner.execute(spec)
    assert out.exit_code == -9
    assert out.status == runlog.STATUS_RESOURCE
    assert "OOM" in out.reason
    assert out.oom_killed


def test_p1_d_sigkill_during_preempt_is_preempt_failed(home):
    """抢占宽限期内未保存断点被 SIGKILL → preempt_failed（数据风险）。"""
    spec = _make_spec(home, "kterm",
                      code="import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(10)",
                      preempt_grace_sec=1)
    t = threading.Timer(0.4, spec.preempt.set)
    t.daemon = True
    t.start()
    out = runner.execute(spec)
    assert out.exit_code == -9
    assert out.status == runlog.STATUS_PREEMPT_FAILED
    assert out.scheduler_signaled
    assert "断点未确认" in out.reason


# ================= error_rules 注入 =================

def test_p1_e_build_env_injects_error_rules_and_home(home):
    rules = {"resource_regex": ["conn.*timeout"], "logic_regex": ["assert.*fail"]}
    env = runner.build_env(Path("/tmp/t"), "j", "r", "manual", 1, "2026-01-01", {},
                           error_rules=rules, run_user="root")
    assert json.loads(env["SCHED_ERROR_RULES"]) == rules
    # P3：降权任务的 HOME 为目标用户家目录
    assert env["HOME"] == pwd.getpwnam("root").pw_dir
    # 未提供 error_rules 时不注入
    env2 = runner.build_env(Path("/tmp/t"), "j", "r", "manual", 1, "2026-01-01", {})
    assert "SCHED_ERROR_RULES" not in env2


def test_p1_e_sdk_loads_error_rules_from_env(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_ERROR_RULES",
                       json.dumps({"resource_regex": ["conn.*timeout"], "logic_regex": []}))
    task = SchedTask(workspace=tmp_path)
    assert task.resource_regex == ["conn.*timeout"]

    # 任务代码在 __init__ 之后赋值仍可覆盖注入值
    task.resource_regex = ["custom.*rule"]
    assert task.resource_regex == ["custom.*rule"]

    # 非法 JSON 静默忽略，不阻塞任务启动
    monkeypatch.setenv("SCHED_ERROR_RULES", "{not-json")
    task2 = SchedTask(workspace=tmp_path)
    assert task2.resource_regex == []


# ================= run_user 友好校验 =================

def test_p1_f_register_rejects_unknown_run_user(home, make_settings):
    task_dir = write_task(home, "u")
    tj = task_dir / "task.json"
    doc = json.loads(tj.read_text("utf-8"))
    doc["runtime"] = {"user": "no-such-user-xyz"}
    tj.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")

    svc = SchedulerService(make_settings(run_user=""))
    import pytest
    with pytest.raises(taskdoc.TaskDocError) as ei:
        svc.register_job("u")
    msg = str(ei.value)
    assert "no-such-user-xyz" in msg
    assert "useradd" in msg  # 给出可操作的部署指引


# ================= 闸5 心跳契约 + 心跳误杀重入队 =================

FAKE_CONTRACT_RUN_PY = '''\
import os, signal, sys, time


def main():
    rid = os.environ.get("SCHED_RUN_ID", "local")
    if os.environ.get("SCHED_CONTRACT_PROBE") == "1":
        # 伪契约：只在探针模式写 state.json，正式运行期不写任何状态
        import json
        p = os.path.join("cache", "runs", rid, "state.json")
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w") as f:
            json.dump({"schema_version": 2, "status": "success", "current_step": "probe",
                       "next_action": "", "progress": 100, "loop_count": 0, "resume_point": {},
                       "metrics": {}, "last_error": {},
                       "started_at": "2026-10-03T00:00:00+08:00",
                       "updated_at": "2026-10-03T00:00:00+08:00"}, f)
        return 0
    if rid == "ckpt1":
        signal.signal(signal.SIGTERM, lambda s, f: sys.exit(99))
        time.sleep(30)
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
'''


def test_p1_g_gate5_rejects_task_without_runtime_heartbeat(home, make_settings):
    """闸2 能过但运行期不写 state.json 的伪契约任务，必须被闸5 的心跳契约拒绝。"""
    task_dir = write_task(home, "fakec")
    (task_dir / "run.py").write_text(FAKE_CONTRACT_RUN_PY, encoding="utf-8")
    doc = taskdoc.load_task_doc(task_dir)

    ok, results, _ = contract.run_gates(doc, task_dir, settings=make_settings(run_user=""))
    assert not ok
    gate5 = [r for r in results if r.gate == "gate5_checkpoint"]
    assert gate5 and not gate5[0].ok
    assert "心跳契约" in gate5[0].reason


def test_p1_g_heartbeat_kill_requeued_once_then_terminal(home, make_settings):
    """心跳超时误杀：第一次按 interrupted 语义重入队一次，第二次转人工。"""
    svc = _svc_with_entry(home, make_settings)

    rec = {"job_id": "ok", "run_id": "r1", "status": "killed", "attempt": 1,
           "trigger": "manual", "error": "心跳超时（200s > 100s）",
           "queued_at": runlog.now_iso(), "scheduled_at": runlog.now_iso(),
           "priority": 50, "exit_code": -9}
    svc._after_finalize(rec, runlog.STATUS_KILLED)
    assert len(svc.state.wq) == 1
    new = svc.state.wq[0]
    assert new["run_id"] != "r1"
    assert new["status"] == runlog.STATUS_WAITING
    assert new["hb_requeues"] == 1
    assert new["error"] == ""  # 上轮心跳超时痕迹已清空

    # 第二次仍心跳超时 → 不再重入队，转人工
    rec2 = dict(rec, run_id="r2", hb_requeues=1)
    svc._after_finalize(rec2, runlog.STATUS_KILLED)
    assert len(svc.state.wq) == 1  # 无新增
    decisions = store.read_jsonl(config.decisions_log())
    assert any(d.get("act") == "terminal" and d.get("run_id") == "r2" for d in decisions)
    assert any(d.get("act") == "heartbeat_requeue" for d in decisions)


# ================= P2：stdout 截断 / jsonl 轮转 / 单实例锁 =================

def test_p2_stdout_truncated_beyond_limit(home):
    """stdout 超过 max_log_bytes：保留头部 + 截断标记，磁盘占用有界。"""
    code = "print('A' * 1000)\nprint('B' * 1000)\nprint('C' * 1000)\n"  # ~3003 字节
    spec = _make_spec(home, "big", code=code, max_log_bytes=1500)
    out = runner.execute(spec)
    assert out.status == runlog.STATUS_SUCCESS
    raw = runlog.stdout_path("big", "r1").read_bytes()
    assert raw.endswith(runner.TRUNCATE_HEAD)
    assert len(raw) == 1500 + len(runner.TRUNCATE_HEAD)


def test_p2_jsonl_rotation_bounded(home):
    p = home / "data" / "runtime" / "audit.jsonl"
    store.append_jsonl(p, {"n": 1}, max_bytes=1)
    store.append_jsonl(p, {"n": 2}, max_bytes=1)  # 超限 → 先轮转再追加
    assert store.read_jsonl(p) == [{"n": 2}]
    assert store.read_jsonl(p.with_name(p.name + ".1")) == [{"n": 1}]

    # 默认上限走模块常量（monkeypatch 后同样生效）
    import sched.store as store_mod
    home2 = home / "data" / "runtime"
    p2 = home2 / "audit2.jsonl"
    try:
        store_mod.ROTATE_MAX_BYTES = 1
        store_mod.append_jsonl(p2, {"n": 1})
        store_mod.append_jsonl(p2, {"n": 2})
        assert store_mod.read_jsonl(p2) == [{"n": 2}]
    finally:
        store_mod.ROTATE_MAX_BYTES = 16 * 1024 * 1024


def test_p2_instance_lock_mutual_exclusion(home, make_settings):
    """同一 SCHED_HOME 只允许一个持有者：flock 互斥，释放后可再获取。"""
    svc1 = SchedulerService(make_settings(run_user=""))
    svc2 = SchedulerService(make_settings(run_user=""))
    assert svc1._acquire_instance_lock()
    assert not svc2._acquire_instance_lock()  # 已被持有
    svc1._release_instance_lock()
    assert svc2._acquire_instance_lock()
    svc2._release_instance_lock()


def test_p2_cmd_serve_friendly_error_when_locked(home, monkeypatch, capsys):
    """cmd_serve 遇单实例锁冲突：stderr 友好报错退出码 1，而非 traceback。"""
    config.ensure_layout()
    lock_path = config.runtime_dir() / "scheduler.lock"
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        from sched import __main__ as main_mod
        rc = main_mod.main(["serve"])
        assert rc == 1
        assert "已有调度器实例" in capsys.readouterr().err
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


# ================= 重入队字段工厂 =================

def test_p2_new_run_record_clears_traces(home, make_settings):
    svc = _svc_with_entry(home, make_settings)
    rec = {"job_id": "ok", "run_id": "r1", "status": "failed", "error": "boom",
           "reason": "rr", "exit_code": 1, "pid": 9, "pgid": 9,
           "stdout_log": "a.log", "stderr_log": "b.log", "wait_ms": 123, "duration_ms": 456,
           "started_at": "s", "finished_at": "f", "_started_monotonic": 1.0,
           "attempt": 1, "trigger": "manual", "manual": False, "priority": 50}
    new = svc._new_run_record(rec, attempt=2, resume_ref=True)
    assert new["run_id"] != "r1"
    assert new["status"] == runlog.STATUS_WAITING
    assert new["error"] == "" and new["reason"] == ""
    assert new["stdout_log"] == "" and new["stderr_log"] == ""
    assert new["wait_ms"] == 0 and new["duration_ms"] == 0
    assert new["exit_code"] is None and new["pid"] is None
    assert "_started_monotonic" not in new
    assert new["resume_point_ref"] is True and new["attempt"] == 2
    # 调度语义字段保留
    assert new["job_id"] == "ok" and new["trigger"] == "manual"


# ================= 110 数据风险二刷端到端闭环 =================

SKIP110_RUN_PY = '''\
import json, os, sys
from pathlib import Path
from sched_task_sdk import SchedTask, Step, run_task


class Task(SchedTask):
    def steps(self, resume):
        if os.environ.get("SCHED_TRIGGER") != "second_pass":
            self.skip_item("item-1", "模拟数据风险")
            sys.exit(110)
        skipped = self.read_pending_skipped()
        assert [r.get("item") for r in skipped] == ["item-1"], f"二刷未读到跳过项：{skipped}"
        out = Path(self.workspace) / "results"
        out.mkdir(parents=True, exist_ok=True)
        (out / "final.json").write_text(json.dumps({"reworked": True}))
        yield Step("rework")


if __name__ == "__main__":
    sys.exit(run_task(Task()))
'''


def test_data_risk_110_second_pass_end_to_end(home, make_settings):
    """110 → 记 pending_skipped → 调度器二刷重入队 → 任务读跳过项补跑成功。"""
    task_dir = write_task(home, "skippy", real_sleep=0.01)
    (task_dir / "run.py").write_text(SKIP110_RUN_PY, encoding="utf-8")
    doc = taskdoc.load_task_doc(task_dir)
    svc = SchedulerService(make_settings(run_user=""))
    svc.entries["skippy"] = JobEntry(job_id="skippy", task_dir=task_dir, doc=doc, enabled=True)

    svc.trigger("skippy", trigger="manual")
    svc._tick_admission()
    assert _wait(lambda: runlog.query_runs(job_id="skippy"))  # 首跑结束（110→failed）
    first = runlog.query_runs(job_id="skippy")[0]
    assert first["status"] == runlog.STATUS_FAILED
    assert first["exit_code"] == errors.EXIT_DATA_RISK

    # 调度器以 second_pass 重入队
    assert _wait(lambda: any(it.get("trigger") == "second_pass" for it in svc.state.wq))
    decisions = store.read_jsonl(config.decisions_log())
    assert any(d.get("act") == "data_risk_reschedule" for d in decisions)

    svc._tick_admission()  # 二刷准出
    assert _wait(lambda: any(r["status"] == "success" and r.get("trigger") == "second_pass"
                            for r in runlog.query_runs(job_id="skippy")))
    assert (task_dir / "results" / "final.json").exists()
    # 首跑记录的跳过项
    skipped = store.read_jsonl(task_dir / "cache" / "pending_skipped.jsonl")
    assert [r.get("item") for r in skipped] == ["item-1"]


# ================= SDK：skip_item 去重 + read_pending_skipped =================

def test_sdk_skip_item_dedup_and_read(tmp_path):
    task = SchedTask(workspace=tmp_path)
    task.skip_item("a", "原因一")
    task.skip_item("a", "原因二")  # 重复 item 去重，只记一条
    task.skip_item("b", "原因三")
    recs = task.read_pending_skipped()
    assert [r.get("item") for r in recs] == ["a", "b"]
    assert recs[0]["reason"] == "原因一"
