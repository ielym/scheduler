import json
import time

from sched_v002 import config, persist, runlog, store
from sched_v002.service import SchedulerService

from conftest import write_task


def _svc(settings):
    return SchedulerService(settings)


def _wait(pred, timeout=30.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.1)
    return False


def test_five_gates_pass(home, make_settings):
    write_task(home, "gated")
    svc = SchedulerService(make_settings(run_user=""))
    r = svc.register_job("gated")
    assert r["job_id"] == "gated"
    # 非 contract_exempt：应跑满五道闸
    assert len(r["gates"]) == 5


def test_register_rejects_missing_entry(home, make_settings):
    # 无 task.json 的目录
    (home / "tasks" / "broken").mkdir(parents=True, exist_ok=True)
    svc = SchedulerService(make_settings(run_user=""))
    from sched_v002 import taskdoc
    try:
        svc.register_job("broken")
        assert False, "应抛 TaskDocError"
    except taskdoc.TaskDocError:
        pass


def test_run_to_success(home, make_settings):
    write_task(home, "ok", real_sleep=0.02)
    svc = SchedulerService(make_settings(run_user=""))
    svc.register_job("ok")
    svc.trigger("ok", trigger="manual")
    svc._tick_admission()
    # 终态记录写盘晚于 running 清空（终态先 pop 后落盘），以 runlog 出现终态为准
    assert _wait(lambda: bool(runlog.query_runs(job_id="ok")))

    rows = runlog.query_runs(job_id="ok")
    assert rows, "应产生运行历史"
    assert rows[0]["status"] == "success"
    # 交付物已生成
    assert (home / "tasks" / "ok" / "results" / "final.json").exists()


def test_reconcile_interrupted(home, make_settings):
    config.ensure_layout()
    # 模拟上次崩溃遗留的在途运行（pgid 已不存在 → 不会真 kill）
    persist.save_active([{
        "run_id": "stale", "job_id": "ok", "task_dir": str(home / "tasks" / "ok"),
        "pid": 999999, "pgid": 999999, "status": "running", "trigger": "manual",
        "scheduled_at": runlog.now_iso(), "queued_at": runlog.now_iso(),
        "started_at": runlog.now_iso(),
    }])
    write_task(home, "ok", real_sleep=0.01)
    svc = SchedulerService(make_settings(run_user=""))
    svc._reconcile()
    rows = runlog.query_runs(job_id="ok", status="interrupted")
    assert len(rows) == 1
    assert rows[0]["run_id"] == "stale"
    # active.json 已清空
    assert persist.load_active() == []


def test_soft_pool_restore(home, make_settings):
    config.ensure_layout()
    st = persist.load_scheduler_state()
    st.soft_capacities["proxy"] = 7
    persist.save_scheduler_state(st)
    svc = SchedulerService(make_settings(run_user=""))
    svc._restore_soft_capacities()
    assert svc.soft_pool.capacity("proxy") == 7