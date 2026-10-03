from sched import taskdoc
from sched.registry import JobEntry
from sched.service import SchedulerService

from conftest import write_task


def _svc_with_entry(home, make_settings, name="ok", **settings_kw):
    write_task(home, name, real_sleep=0.01)
    task_dir = home / "tasks" / name
    doc = taskdoc.load_task_doc(task_dir)
    svc = SchedulerService(make_settings(run_user="", **settings_kw))
    svc.entries[name] = JobEntry(job_id=name, task_dir=task_dir, doc=doc, enabled=True)
    return svc


def test_requeue_resume(home, make_settings):
    svc = _svc_with_entry(home, make_settings)
    rec = {"job_id": "ok", "run_id": "r1", "status": "preempted", "attempt": 1,
           "queued_at": "2026-10-03T00:00:00+08:00"}
    svc._requeue_resume(rec)
    assert len(svc.state.wq) == 1
    new = svc.state.wq[0]
    assert new["run_id"] != "r1"
    assert new["resume_point_ref"] is True
    assert new["status"] == "waiting"


def test_handle_retry_enqueues_with_backoff(home, make_settings):
    svc = _svc_with_entry(home, make_settings)
    rec = {"job_id": "ok", "run_id": "r1", "status": "resource", "attempt": 1}
    svc._handle_retry(rec, "resource")
    assert len(svc.state.wq) == 1
    assert svc.state.wq[0]["attempt"] == 2
    assert "ok" in svc.state.backoffs


def test_handle_retry_never_gives_up_by_default(home, make_settings):
    # 默认无重试上限——第 5 次资源错误仍重入队退避，
    # 保证「资源不足不得导致任务失败」
    svc = _svc_with_entry(home, make_settings)
    rec = {"job_id": "ok", "run_id": "r1", "status": "resource", "attempt": 5}
    svc._handle_retry(rec, "resource")
    assert len(svc.state.wq) == 1
    assert svc.state.wq[0]["attempt"] == 6


def test_handle_retry_gives_up_at_configured_cap(home, make_settings):
    # 运维显式配置 max_resource_attempts>0 时按上限止损转人工（逃生舱）
    svc = _svc_with_entry(home, make_settings, runner={"max_resource_attempts": 2})
    rec = {"job_id": "ok", "run_id": "r1", "status": "resource", "attempt": 5}
    svc._handle_retry(rec, "resource")
    assert len(svc.state.wq) == 0  # 达配置上限，转人工不再自动重试