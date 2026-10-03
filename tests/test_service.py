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


def test_handle_retry_immediate_requeue_no_backoff(home, make_settings):
    # 新语义：100/101 资源错误后立即重排队（无 cooldown/backoff 字典、无指数退避）
    svc = _svc_with_entry(home, make_settings)
    assert not hasattr(svc.state, "backoffs")
    assert not hasattr(svc.state, "cooldowns")
    rec = {"job_id": "ok", "run_id": "r1", "status": "resource", "attempt": 1}
    svc._handle_retry(rec, "resource")
    assert len(svc.state.wq) == 1
    new = svc.state.wq[0]
    assert new["attempt"] == 2
    assert new["not_before"] == ""  # 默认 delay_sec=0，立即可准出
    assert new["resume_point_ref"] is False
    assert new["status"] == "waiting"


def test_handle_retry_respects_delay_sec(home, make_settings):
    # 任务在 task.json retry.delay_sec 声明重试节奏 → 新等待项带 not_before
    write_task(home, "ok", retry={"delay_sec": 30})
    task_dir = home / "tasks" / "ok"
    doc = taskdoc.load_task_doc(task_dir)
    svc = SchedulerService(make_settings(run_user=""))
    svc.entries["ok"] = JobEntry(job_id="ok", task_dir=task_dir, doc=doc, enabled=True)
    rec = {"job_id": "ok", "run_id": "r1", "status": "resource", "attempt": 1}
    svc._handle_retry(rec, "resource")
    assert len(svc.state.wq) == 1
    assert svc.state.wq[0]["not_before"]
    assert svc._not_before_pending(svc.state.wq[0]) is True


def test_handle_retry_never_gives_up_by_default(home, make_settings):
    # 默认 retry.max_attempts=0（不放弃）——第 5 次资源错误仍立即重入队，
    # 保证「有资源必须跑完」
    svc = _svc_with_entry(home, make_settings)
    rec = {"job_id": "ok", "run_id": "r1", "status": "resource", "attempt": 5}
    svc._handle_retry(rec, "resource")
    assert len(svc.state.wq) == 1
    assert svc.state.wq[0]["attempt"] == 6


def test_handle_retry_gives_up_at_task_declared_cap(home, make_settings):
    # 任务在 task.json 声明 retry.max_attempts=2 → 第 2 次尝试失败后转人工
    write_task(home, "ok", retry={"max_attempts": 2})
    task_dir = home / "tasks" / "ok"
    doc = taskdoc.load_task_doc(task_dir)
    svc = SchedulerService(make_settings(run_user=""))
    svc.entries["ok"] = JobEntry(job_id="ok", task_dir=task_dir, doc=doc, enabled=True)
    rec = {"job_id": "ok", "run_id": "r1", "status": "resource", "attempt": 2}
    svc._handle_retry(rec, "resource")
    assert len(svc.state.wq) == 0  # 达任务声明上限，转人工不再自动重试


def test_handle_retry_gives_up_after_not_after(home, make_settings):
    # 任务运行时通过 retry_hint 声明时效截止（已过期）→ 转人工
    write_task(home, "ok", retry={"not_after": "2020-01-01T00:00:00+08:00"})
    task_dir = home / "tasks" / "ok"
    doc = taskdoc.load_task_doc(task_dir)
    svc = SchedulerService(make_settings(run_user=""))
    svc.entries["ok"] = JobEntry(job_id="ok", task_dir=task_dir, doc=doc, enabled=True)
    rec = {"job_id": "ok", "run_id": "r1", "status": "resource", "attempt": 1}
    svc._handle_retry(rec, "resource")
    assert len(svc.state.wq) == 0
