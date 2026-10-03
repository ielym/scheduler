import json
import threading
import time

from sched_v002 import runner, taskdoc

from conftest import write_task


def test_runner_preempt_exits_99_and_saves_checkpoint(home, make_settings):
    # 慢任务（real_sleep=1.0×3），中途 preempt → 应托管断点后退出 99
    write_task(home, "p", real_sleep=1.0, total=3, dry_sleep=0.8)
    task_dir = home / "tasks" / "p"
    doc = taskdoc.load_task_doc(task_dir)

    spec = runner.RunSpec(
        job_id="p", run_id="r_pre", task_dir=task_dir,
        entry_file=doc.entry.file, interpreter=doc.entry.interpreter,
        args=list(doc.entry.args), cwd=doc.entry.cwd, env=dict(doc.env),
        trigger="manual", attempt=1, scheduled_at="2026-10-03T00:00:00+08:00",
        expect_outputs=list(doc.outputs.expect), run_user="",
    )
    outcome_box = {}

    def _run():
        outcome_box["out"] = runner.execute(spec)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    time.sleep(1.5)
    spec.preempt.set()
    t.join(timeout=30)

    out = outcome_box["out"]
    assert out.status == "preempted"
    assert out.exit_code == 99

    # 断点已落盘，且下一步可恢复
    # F4：实例级状态在 cache/runs/<run_id>/state.json；断点在 cache/resume_point.json
    st = json.loads((task_dir / "cache" / "runs" / "r_pre" / "state.json").read_text())
    assert st["status"] == "preempted"
    rp = json.loads((task_dir / "cache" / "resume_point.json").read_text())
    assert rp.get("resume_point")