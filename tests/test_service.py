from sched_v002 import taskdoc
from sched_v002.registry import JobEntry
from sched_v002.service import SchedulerService

from conftest import write_task


def _svc_with_entry(home, make_settings, name="ok"):
    write_task(home, name, real_sleep=0.01)
    task_dir = home / "tasks" / name
    doc = taskdoc.load_task_doc(task_dir)
    svc = SchedulerService(make_settings(run_user=""))
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


def test_handle_retry_gives_up_after_max(home, make_settings):
    svc = _svc_with_entry(home, make_settings)
    rec = {"job_id": "ok", "run_id": "r1", "status": "resource", "attempt": 5}
    svc._handle_retry(rec, "resource")
    assert len(svc.state.wq) == 0  # 达上限，不再自动重试