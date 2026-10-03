"""回归测试：覆盖 REVIEW.md 中 P0（Bug1–5）与 P1（D1–D8）修复。"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta

from sched_v002 import admission, config, errors, persist, preempt, resources, runner, runlog, store, taskdoc
from sched_v002.registry import JobEntry
from sched_v002.service import RunningEntry, SchedulerService

from conftest import write_task


class FakeCapacity:
    """可控容量，模拟受限机器。"""

    def __init__(self, total_memory_mb: int = 1000, total_cpu: float = 4.0):
        self.total_memory_mb = total_memory_mb
        self.total_cpu = total_cpu


def _entry(home, name, **task_kw):
    write_task(home, name, **task_kw)
    task_dir = home / "tasks" / name
    doc = taskdoc.load_task_doc(task_dir)
    return JobEntry(job_id=name, task_dir=task_dir, doc=doc, enabled=True)


def _svc(home, make_settings):
    return SchedulerService(make_settings(run_user=""))


def _write_task_with_schedule(home, name, schedule, priority=50, resources=None):
    task_dir = write_task(home, name, priority=priority, resources=resources, real_sleep=0.01)
    doc = json.loads((task_dir / "task.json").read_text())
    doc["schedule"] = schedule
    (task_dir / "task.json").write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    return task_dir


def _spec(home, job_id, run_id):
    return runner.RunSpec(
        job_id=job_id, run_id=run_id, task_dir=home / "tasks" / job_id,
        entry_file="run.py", interpreter="python3", args=[], cwd=".", env={},
        trigger="manual", attempt=1, scheduled_at="2026-10-03T00:00:00+08:00",
    )


# ================= Bug1：once 过期不再重复触发 =================

def test_once_expired_next_fire_none():
    from sched_v002.engine import next_fire
    from zoneinfo import ZoneInfo

    tz = ZoneInfo("Asia/Shanghai")
    now = datetime(2026, 10, 3, 12, 0, 1, tzinfo=tz)
    sched = {"type": "once", "once_at": "2026-10-03T12:00:00+08:00"}
    assert next_fire(sched, now) is None  # 已过期 → None
    sched2 = {"type": "once", "once_at": "2026-10-03T13:00:00+08:00"}
    assert next_fire(sched2, now) is not None  # 未来 → 保留


def test_once_fires_once_then_removed(home, make_settings):
    past = (datetime.now().astimezone() - timedelta(seconds=5)).isoformat(timespec="seconds")
    _write_task_with_schedule(home, "onetask", {"type": "once", "once_at": past})
    svc = _svc(home, make_settings)
    svc.refresh()
    svc.entries["onetask"] = JobEntry(job_id="onetask", task_dir=home / "tasks" / "onetask",
                                      doc=taskdoc.load_task_doc(home / "tasks" / "onetask"), enabled=True)
    svc.engine.upsert("onetask", svc.entries["onetask"].schedule())
    # 过期的 once 不入触发引擎
    assert svc.engine.next_run_time("onetask") is None
    svc._tick_triggers()
    assert svc.state.wq == []

    # 模拟未过期时到期一次：触发一次后从引擎摘除，不再重复
    svc.engine._next["onetask"] = datetime.now().astimezone() - timedelta(seconds=1)
    svc._tick_triggers()
    assert len(svc.state.wq) == 1
    assert "onetask" not in svc.engine._next
    svc._tick_triggers()
    assert len(svc.state.wq) == 1  # 不再重复入队


# ================= Bug2：准出扫描同轮虚拟账本 =================

def test_admission_scan_same_round_no_overcommit():
    cap = FakeCapacity(total_memory_mb=1000, total_cpu=4.0)
    ledger = resources.Ledger()
    soft = resources.SoftResourcePool()
    items = [{"run_id": "r1", "job_id": "j1"}, {"run_id": "r2", "job_id": "j2"}]
    declared = {"j1": (1.0, 800, {}), "j2": (1.0, 800, {})}
    res = admission.admission_scan(
        items,
        get_declared=lambda jid: declared[jid],
        get_profile=lambda jid: resources.Profile(declared_cpu=1.0, declared_memory_mb=800),
        ledger=ledger, capacity=cap, soft_pool=soft, overshoot=1.0,
    )
    # 容量 1000MB：两个 800MB 只能准出一个（修复前两个同时准出 → 1600MB 打爆）
    assert res.admitted == ["r1"]
    assert "r2" in res.blocked_fixed


def test_admission_scan_soft_same_round():
    cap = FakeCapacity()
    ledger = resources.Ledger()
    soft = resources.SoftResourcePool()
    soft.set_capacity("proxy", 2)
    items = [{"run_id": "r1", "job_id": "j1"}, {"run_id": "r2", "job_id": "j2"}]
    declared = {"j1": (0.1, 10, {"proxy": 2}), "j2": (0.1, 10, {"proxy": 2})}
    res = admission.admission_scan(
        items,
        get_declared=lambda jid: declared[jid],
        get_profile=lambda jid: resources.Profile(declared_cpu=0.1, declared_memory_mb=10),
        ledger=ledger, capacity=cap, soft_pool=soft, overshoot=1.0,
    )
    assert res.admitted == ["r1"]
    assert "r2" in res.blocked_soft


# ================= Bug3：画像 CPU 样本为平均核数 =================

def test_finalize_records_avg_cpu_cores(home, make_settings):
    svc = _svc(home, make_settings)
    svc.entries["ok"] = _entry(home, "ok")
    re = RunningEntry(
        run={"run_id": "r1", "job_id": "ok", "dry_run": False, "duration_ms": 3_600_000,
             "queued_at": runlog.now_iso(), "status": "running", "trigger": "manual"},
        spec=None, est_cpu=1.0, est_mem_mb=100, cgroup_name="ok/r1",
    )
    # 2 核跑 1 小时 = 7200 CPU-秒；样本必须是 2.0 核而非 7200
    svc._finalize(re, runlog.STATUS_SUCCESS, mem_peak_mb=500, cpu_sec=7200.0)
    prof = svc.profiles.get("ok", 0.1, 64)
    assert prof.samples == [(2.0, 500)]
    # 长任务跑完后仍可再次准出（预估不超过机器核数）
    est_cpu, _ = admission.estimate_job(prof, 0.1, 64, 1.2, FakeCapacity(1000, 4.0))
    assert est_cpu <= 4.0


# ================= Bug4：预估钳制在机器上限 =================

def test_estimate_job_clamped_to_capacity():
    cap = FakeCapacity(total_memory_mb=1000, total_cpu=4.0)
    # 冒烟 800 合法，正式运行实测 900 → ×1.2 = 1080 > 上限 → 钳制到 1000（修复前永久饥饿）
    prof = resources.Profile(samples=[(1.0, 900)], declared_cpu=0.5, declared_memory_mb=800)
    est_cpu, est_mem = admission.estimate_job(prof, 0.5, 800, 1.2, cap)
    assert est_mem == 1000
    assert est_cpu <= 4.0
    # 无 capacity 时保持原行为（不上浮超限，供测试/对比）
    _, est_mem2 = admission.estimate_job(prof, 0.5, 800, 1.2)
    assert est_mem2 == 1080


# ================= Bug5：抢占按真实缺口 =================

def _preempt_setup(home, make_settings):
    svc = _svc(home, make_settings)
    svc.capacity = FakeCapacity(total_memory_mb=1000, total_cpu=4.0)
    svc.entries["low"] = _entry(home, "low", priority=10,
                                resources={"cpu": 1.0, "memory_mb": 600, "soft": {}})
    svc.entries["high"] = _entry(home, "high", priority=90,
                                 resources={"cpu": 1.0, "memory_mb": 800, "soft": {}})
    return svc


def test_preempt_uses_gap_not_total_need(home, make_settings):
    svc = _preempt_setup(home, make_settings)
    # 低优占 600MB（空闲 400MB），高优需 960（800×1.2 钳制后）→ 缺口 560 ≤ 600 可抢占
    svc.ledger.reserve("low_run", 1.0, 600, {})
    re = RunningEntry(
        run={"run_id": "low_run", "job_id": "low", "priority": 10,
             "manual": False, "contract_exempt": False},
        spec=_spec(home, "low", "low_run"), est_cpu=1.0, est_mem_mb=600,
    )
    svc.running["low_run"] = re
    svc.state.enqueue({"run_id": "h1", "job_id": "high", "priority": 90})
    svc._tick_preempt()
    assert re.spec.preempt.is_set(), "部分空闲场景应按缺口抢占（修复前返回空集）"


def test_preempt_no_gap_no_preempt(home, make_settings):
    svc = _preempt_setup(home, make_settings)
    # 空闲足够：高优需 960（800×1.2），仅占 40MB → 空闲 960 ≥ 需求 → 不应抢占
    svc.ledger.reserve("low_run", 1.0, 40, {})
    re = RunningEntry(
        run={"run_id": "low_run", "job_id": "low", "priority": 10,
             "manual": False, "contract_exempt": False},
        spec=_spec(home, "low", "low_run"), est_cpu=1.0, est_mem_mb=40,
    )
    svc.running["low_run"] = re
    svc.state.enqueue({"run_id": "h1", "job_id": "high", "priority": 90})
    svc._tick_preempt()
    assert not re.spec.preempt.is_set()


def test_preempt_exempt_period_protects_resumed(home, make_settings):
    svc = _preempt_setup(home, make_settings)
    svc.ledger.reserve("low_run", 1.0, 600, {})
    re = RunningEntry(
        run={"run_id": "low_run", "job_id": "low", "priority": 10,
             "manual": False, "contract_exempt": False},
        spec=_spec(home, "low", "low_run"), est_cpu=1.0, est_mem_mb=600,
        exempt_until_monotonic=time.monotonic() + 60,  # 豁免期内
    )
    svc.running["low_run"] = re
    svc.state.enqueue({"run_id": "h1", "job_id": "high", "priority": 90})
    svc._tick_preempt()
    assert not re.spec.preempt.is_set(), "豁免等待期内不应被再次抢占"


def test_end_to_end_preempt_high_wins(home, make_settings):
    """端到端：高优入队 → 低优被抢(exit 99) → 高优准出 → 低优恢复入队。"""
    svc = _svc(home, make_settings)
    svc.capacity = FakeCapacity(total_memory_mb=1000, total_cpu=4.0)
    svc.entries["low"] = _entry(home, "low", priority=10, real_sleep=0.8, total=10,
                                resources={"cpu": 1.0, "memory_mb": 600, "soft": {}})
    svc.entries["high"] = _entry(home, "high", priority=90, real_sleep=0.01, total=1,
                                 resources={"cpu": 1.0, "memory_mb": 800, "soft": {}})

    svc.trigger("low", trigger="interval")
    svc._tick_admission()
    deadline = time.time() + 10
    while "low" not in {re.run["job_id"] for re in svc.running.values()} and time.time() < deadline:
        time.sleep(0.05)
    assert svc.running, "low 应已运行"
    # 等 low 真正启动（写出 state.json = 已装 SIGTERM 处理器），避免启动竞态
    low_state = svc.entries["low"].task_dir / "cache" / "state.json"
    deadline = time.time() + 10
    while not low_state.exists() and time.time() < deadline:
        time.sleep(0.05)

    svc.trigger("high", trigger="manual")
    # 循环驱动：抢占 → low 退出 99 → 高优准出
    deadline = time.time() + 30
    while time.time() < deadline:
        svc._tick_admission()
        svc._tick_preempt()
        if any(re.run["job_id"] == "high" for re in svc.running.values()):
            break
        time.sleep(0.1)
    assert any(re.run["job_id"] == "high" for re in svc.running.values()), "高优应抢占成功并准出"

    # low 被抢占后以 resume 重入队（带豁免期）——抢占终结是异步的，等待重入队落盘
    deadline = time.time() + 15
    low_waiting = []
    while time.time() < deadline:
        low_waiting = [it for it in svc.state.wq if it["job_id"] == "low"]
        if low_waiting:
            break
        time.sleep(0.1)
    assert low_waiting and low_waiting[0]["resume_point_ref"] is True
    assert low_waiting[0].get("_exempt_until_monotonic", 0) > time.monotonic()

    # 收尾：等 high 跑完
    deadline = time.time() + 20
    while svc.running and time.time() < deadline:
        time.sleep(0.1)


# ================= D1：G5 错误兜底链 =================

UNCLASSIFIED_RUN_PY = '''\
import sys
from sched_task_sdk import SchedTask, Step, run_task

class Task(SchedTask):
    def steps(self, resume):
        raise ValueError("bad data format xyz")
        yield Step("noop")

if __name__ == "__main__":
    sys.exit(run_task(Task()))
'''

LOGIC_RUN_PY = '''\
import sys
from sched_task_sdk import SchedTask, Step, run_task

class Task(SchedTask):
    def steps(self, resume):
        sys.exit(self.fail("assertion failed", category="logic"))
        yield Step("noop")

if __name__ == "__main__":
    sys.exit(run_task(Task()))
'''


def _run_custom(home, name, code) -> runner.RunOutcome:
    task_dir = home / "tasks" / name
    task_dir.mkdir(parents=True, exist_ok=True)
    (task_dir / "run.py").write_text(code, encoding="utf-8")
    spec = runner.RunSpec(
        job_id=name, run_id="r1", task_dir=task_dir, entry_file="run.py",
        interpreter="python3", args=[], cwd=".", env={}, trigger="manual",
        attempt=1, scheduled_at="2026-10-03T00:00:00+08:00", run_user="",
    )
    return runner.execute(spec)


def test_g5_unclassified_treated_as_resource(home, make_settings):
    out = _run_custom(home, "unclass", UNCLASSIFIED_RUN_PY)
    assert out.exit_code == 1
    # G5：未分类错误一律按资源错误处理（可重试），而非转人工
    assert out.status == runlog.STATUS_RESOURCE
    st = json.loads((home / "tasks" / "unclass" / "cache" / "state.json").read_text())
    assert st["last_error"]["category"] == "unclassified"


def test_state_logic_category_is_failed(home, make_settings):
    out = _run_custom(home, "logictask", LOGIC_RUN_PY)
    assert out.exit_code == 1
    assert out.status == runlog.STATUS_FAILED  # 任务自报 logic → 不重试


# ================= D2：崩溃恢复 =================

def test_crash_recovery_requeues_interrupted(home, make_settings):
    config.ensure_layout()
    write_task(home, "ok", real_sleep=0.01)
    persist.save_active([{
        "run_id": "stale", "job_id": "ok", "task_dir": str(home / "tasks" / "ok"),
        "pid": 999999, "pgid": 999999, "status": "running", "trigger": "interval",
        "priority": 50, "attempt": 1, "manual": False, "contract_exempt": False,
        "scheduled_at": runlog.now_iso(), "queued_at": runlog.now_iso(),
        "started_at": runlog.now_iso(),
    }])
    svc = _svc(home, make_settings)
    svc.entries["ok"] = _entry(home, "ok")
    svc._reconcile()
    svc._recover_interrupted()
    assert len(svc.state.wq) == 1
    new = svc.state.wq[0]
    assert new["run_id"] != "stale"
    assert new["resume_point_ref"] is True
    assert new["status"] == "waiting"
    # recover_queue 已消费清空
    assert persist.load_recover_queue() == []


def test_crash_recovery_skips_contract_exempt(home, make_settings):
    config.ensure_layout()
    write_task(home, "exempt", contract_exempt=True)
    persist.save_active([{
        "run_id": "stale2", "job_id": "exempt", "task_dir": str(home / "tasks" / "exempt"),
        "pid": 999999, "pgid": 999999, "status": "running", "trigger": "manual",
        "priority": 50, "attempt": 1, "manual": True, "contract_exempt": True,
        "scheduled_at": runlog.now_iso(), "queued_at": runlog.now_iso(),
        "started_at": runlog.now_iso(),
    }])
    svc = _svc(home, make_settings)
    svc.entries["exempt"] = _entry(home, "exempt", contract_exempt=True)
    svc._reconcile()
    svc._recover_interrupted()
    # R3：contract_exempt 不自动恢复
    assert svc.state.wq == []
    decisions = store.read_jsonl(config.decisions_log())
    assert any(d.get("act") == "recover_manual" for d in decisions)


# ================= D3：渐进式提交 =================

def test_staged_admission_one_by_one_until_healthy(home, make_settings):
    svc = _svc(home, make_settings)
    svc.capacity = FakeCapacity(total_memory_mb=1000, total_cpu=4.0)
    for n in ("a", "b", "c"):
        svc.entries[n] = _entry(home, n, real_sleep=0.5, total=2,
                                resources={"cpu": 0.1, "memory_mb": 600, "soft": {}})
    for n in ("a", "b", "c"):
        svc.trigger(n, trigger="manual")

    # 冷启动 + 有固定资源等待者 → 只准出 1 个
    svc._tick_admission()
    assert len(svc.running) == 1
    first_rid = next(iter(svc.running))
    assert svc._staged_run_id == first_rid

    # 观察期内不放开
    svc._tick_admission()
    assert len(svc.running) == 1

    # 等待任务真正启动（写出 state.json 心跳），避免心跳超时干扰门禁
    task_dir = svc.entries[svc.running[first_rid].run["job_id"]].task_dir
    deadline = time.time() + 10
    while not (task_dir / "cache" / "state.json").exists() and time.time() < deadline:
        time.sleep(0.05)

    # 观察期过后放开（_tick_monitor 推进）
    svc.staged_observe_sec = 0.05
    time.sleep(0.1)
    svc._tick_monitor()
    assert svc._staged_released is True

    # 首个任务完成后，后续任务可正常准出
    re = svc.running[first_rid]
    deadline = time.time() + 15
    while first_rid in svc.running and time.time() < deadline:
        time.sleep(0.1)
    svc._tick_admission()
    assert len(svc.running) >= 1
    # 收尾
    deadline = time.time() + 30
    while svc.running and time.time() < deadline:
        svc._tick_admission()
        time.sleep(0.1)


# ================= D4：max_instances / overflow =================

def test_overflow_skip_drops_trigger(home, make_settings):
    _write_task_with_schedule(home, "iv", {"type": "interval", "interval": 60,
                                           "max_instances": 1, "overflow": "skip"})
    svc = _svc(home, make_settings)
    svc.entries["iv"] = JobEntry(job_id="iv", task_dir=home / "tasks" / "iv",
                                 doc=taskdoc.load_task_doc(home / "tasks" / "iv"), enabled=True)
    # 已有一个在途实例
    svc.state.enqueue({"run_id": "w1", "job_id": "iv", "priority": 50})
    svc.engine._next["iv"] = datetime.now().astimezone() - timedelta(seconds=1)
    svc._tick_triggers()
    assert len(svc.state.wq) == 1  # skip：不再入队
    decisions = store.read_jsonl(config.decisions_log())
    assert any(d.get("act") == "skip_trigger" for d in decisions)


def test_overflow_queue_keeps_trigger(home, make_settings):
    _write_task_with_schedule(home, "iv2", {"type": "interval", "interval": 60,
                                            "max_instances": 1, "overflow": "queue"})
    svc = _svc(home, make_settings)
    svc.entries["iv2"] = JobEntry(job_id="iv2", task_dir=home / "tasks" / "iv2",
                                  doc=taskdoc.load_task_doc(home / "tasks" / "iv2"), enabled=True)
    svc.state.enqueue({"run_id": "w1", "job_id": "iv2", "priority": 50})
    svc.engine._next["iv2"] = datetime.now().astimezone() - timedelta(seconds=1)
    svc._tick_triggers()
    assert len(svc.state.wq) == 2  # queue：照常入队


def test_admit_held_at_max_instances(home, make_settings):
    svc = _svc(home, make_settings)
    svc.entries["ok"] = _entry(home, "ok")  # max_instances=1
    rec = svc.trigger("ok", trigger="manual")
    # 模拟同 job 已有一个运行中实例
    svc.running["other"] = RunningEntry(run={"run_id": "other", "job_id": "ok"}, spec=None)
    svc._tick_admission()
    # 达到并发上限 → 留在队列等待，不准出
    assert svc.state.wq and svc.state.wq[0]["run_id"] == rec["run_id"]
    assert len(svc.running) == 1  # 只有伪造的 other


# ================= D5：数据风险（110）二刷 =================

def test_data_risk_reschedules_second_pass(home, make_settings):
    svc = _svc(home, make_settings)
    svc.entries["ok"] = _entry(home, "ok")
    rec = {"job_id": "ok", "run_id": "r1", "status": "failed", "attempt": 1,
           "exit_code": errors.EXIT_DATA_RISK, "trigger": "interval"}
    svc._after_finalize(rec, runlog.STATUS_FAILED)
    assert len(svc.state.wq) == 1
    new = svc.state.wq[0]
    assert new["trigger"] == "second_pass"
    assert new["data_risk_rounds"] == 1


def test_data_risk_gives_up_after_max_rounds(home, make_settings):
    svc = _svc(home, make_settings)
    svc.entries["ok"] = _entry(home, "ok")
    rec = {"job_id": "ok", "run_id": "r1", "status": "failed", "attempt": 1,
           "exit_code": errors.EXIT_DATA_RISK, "trigger": "second_pass",
           "data_risk_rounds": svc.data_risk_max_reschedules}
    svc._after_finalize(rec, runlog.STATUS_FAILED)
    assert svc.state.wq == []  # 达上限转人工
    decisions = store.read_jsonl(config.decisions_log())
    assert any(d.get("act") == "terminal" and d.get("status") == "data_risk" for d in decisions)


# ================= D6：软资源降额 =================

def test_soft_degrade_and_restore(home, make_settings):
    svc = _svc(home, make_settings)
    svc.state.soft_capacities["proxy"] = 8
    svc._restore_soft_capacities()
    assert svc.soft_pool.capacity("proxy") == 8
    for _ in range(svc.soft_degrade_threshold):
        svc.monitor.record_soft_error("proxy")
    svc._apply_soft_degrade()
    assert svc.soft_pool.capacity("proxy") == 4  # 降额一半
    # 冷却期后恢复
    svc._soft_degraded_until["proxy"] = time.monotonic() - 1
    svc._apply_soft_degrade()
    assert svc.soft_pool.capacity("proxy") == 8


def test_finalize_records_soft_error_on_101(home, make_settings):
    svc = _svc(home, make_settings)
    svc.entries["ok"] = _entry(home, "ok", resources={"cpu": 0.1, "memory_mb": 64, "soft": {"proxy": 1}})
    re = RunningEntry(
        run={"run_id": "r1", "job_id": "ok", "dry_run": False, "duration_ms": 1000,
             "queued_at": runlog.now_iso(), "status": "resource", "trigger": "manual",
             "exit_code": errors.EXIT_SOFT_RESOURCE},
        spec=None, est_cpu=0.1, est_mem_mb=64, soft={"proxy": 1}, cgroup_name="ok/r1",
    )
    svc._finalize(re, runlog.STATUS_RESOURCE, mem_peak_mb=50, cpu_sec=0.5)
    assert svc.monitor.soft_error_rate("proxy") == 1


# ================= D7：窗口收口每 tick 检查 =================

def test_close_expired_windows_in_monitor_tick(home, make_settings):
    _write_task_with_schedule(home, "win", {"type": "interval", "interval": 3600,
                                            "start_time": "00:00", "end_time": "00:01",
                                            "allow_overrun": False})
    svc = _svc(home, make_settings)
    svc.entries["win"] = JobEntry(job_id="win", task_dir=home / "tasks" / "win",
                                  doc=taskdoc.load_task_doc(home / "tasks" / "win"), enabled=True)
    # 心跳正常（写新 state.json），排除心跳杀干扰
    cache = home / "tasks" / "win" / "cache"
    cache.mkdir(parents=True, exist_ok=True)
    (cache / "state.json").write_text(json.dumps({"status": "running", "updated_at": runlog.now_iso()}))
    svc.monitor.heartbeat_timeout_sec = 10 ** 9

    today = datetime.now().astimezone().replace(hour=0, minute=0, second=30, microsecond=0)
    spec = _spec(home, "win", "wrun")
    re = RunningEntry(
        run={"run_id": "wrun", "job_id": "win", "priority": 50, "manual": False,
             "task_dir": str(home / "tasks" / "win"),
             "started_at": today.isoformat(timespec="seconds"), "scheduled_at": today.isoformat(timespec="seconds")},
        spec=spec, est_cpu=0.1, est_mem_mb=64, cgroup_name="win/wrun",
    )
    svc.running["wrun"] = re
    svc._tick_monitor()  # 无到期任务也应收口过期窗口
    if datetime.now().astimezone() > today.replace(minute=1, second=0):
        assert spec.cancel.is_set(), "过期窗口内的运行应被终止"
    else:  # 极端情况：测试恰在窗口内运行
        assert not spec.cancel.is_set()


# ================= D8：manual 心跳放宽不豁免（见 test_monitor.py） =================

def test_manual_heartbeat_uses_relaxed_timeout(home, make_settings):
    # service 侧：manual 观察值带 ×2 放宽阈值
    svc = _svc(home, make_settings)
    svc.entries["ok"] = _entry(home, "ok")
    svc.monitor.heartbeat_timeout_sec = 100
    re = RunningEntry(
        run={"run_id": "r1", "job_id": "ok", "priority": 50, "manual": True,
             "task_dir": str(home / "tasks" / "ok"),
             "started_at": runlog.now_iso()},
        spec=_spec(home, "ok", "r1"), est_cpu=0.1, est_mem_mb=64, cgroup_name="ok/r1",
    )
    svc.running["r1"] = re
    # 无 state.json → 心跳年龄无穷大；manual 放宽后仍应触发（不豁免）
    svc._tick_monitor()
    assert re.spec.cancel.is_set()
