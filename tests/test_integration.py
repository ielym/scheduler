"""端到端集成测试（真实子进程，覆盖修复的全部闭环）。

场景：
1. happy path：生产者发布 → 接收 → 准入 → RUNNING → SUCCEEDED + 回执 + 归档
2. 不足退避闭环：exit 75 → BACKOFF（指数退避）→ 到期重排 → 重试成功
3. 协作暂停闭环：退让 → PAUSED → 重新排队 → 重入 → 完成（业务幂等，单元恰好一次）
4. 显式取消：cancel_job → 进程树退出 → CANCELLED
5. 重启接管：模拟调度器崩溃（进程不撤）→ 新实例核实身份 → 旧进程协作退出 →
   RECOVERING → QUEUED → 重放完成；生产者任务级单实例锁接管（无重复生产者）
6. CLI IPC：status 读快照显示 serve=alive；job cancel 走命令通道并被 serve 确认
"""

from __future__ import annotations

import json
import os
import time

import pytest

from sched import config, db as dbmod, runner, store
from sched.__main__ import main as cli_main
from sched.config import JobStatus
from sched.scheduler import SchedulerService


def wait_until(pred, timeout=25.0, interval=0.1, desc=""):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = pred()
        except Exception as e:  # noqa: BLE001
            last = f"error: {e}"
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"timeout waiting for {desc!r}; last={last!r}")


def _fast_config(home):
    cfg = config.SchedulerConfig()
    c = cfg.control
    c.loop_tick_sec = 0.2
    c.sample_interval_sec = 0.5
    c.start_interval_sec = 0.2
    c.grow_observe_sec = 2.0
    c.feedback_interval_init_sec = 1.0
    c.heartbeat_interval_sec = 1.0
    c.heartbeat_stale_multiplier = 6.0
    c.shortage_retry_base_sec = 0.5
    c.shortage_retry_cap_sec = 2.0
    c.task_backoff_probe_base_sec = 0.5
    c.task_backoff_probe_cap_sec = 2.0
    c.pause_grace_base_sec = 2.0
    c.pause_grace_max_sec = 4.0
    c.protect_deadline_sec = 6.0
    c.protect_round_backoff_sec = 2.0
    c.recovery_probe_sec = 0.5
    c.recover_grace_sec = 3.0
    c.cmd_ack_wait_sec = 5.0
    c.producer_restart_base_sec = 0.5
    store.save_json(config.config_file(), cfg.model_dump())
    return cfg


_PRODUCER_TMPL = '''
from sched_task_sdk import Producer, run_producer

class P(Producer):
    def discover(self, cursor):
        if cursor is None:
            return {{"done": True}}, [{{
                "event_id": "{event_id}",
                "business_version": "1",
                "idempotency_key": "{event_id}",
                "entry_args": [],
                "env": {env_json},
            }}]
        return cursor, []

run_producer(P(), tick_sec=0.2)
'''


def _make_task(home, task_id, run_body, env=None, priority=50, event_id="e1"):
    td = home / "tasks" / task_id
    td.mkdir(parents=True, exist_ok=True)
    (td / "producer.py").write_text(
        _PRODUCER_TMPL.format(event_id=event_id,
                              env_json=json.dumps(env or {})))
    (td / "run.py").write_text(run_body)
    store.save_json(td / "task.json", {
        "schema_version": 1, "task_id": task_id, "enabled": True,
        "priority": priority,
        "producer_entry": {"file": "producer.py"},
        "executor_entry": {"file": "run.py"},
        "recovery": {"replay": "outbox", "retain": "测试保留", "replay_note": "测试重放"},
    })
    return td


_RUN_OK = '''
import os, sys
from sched_task_sdk import ExecutorTask, run_task

class T(ExecutorTask):
    def __init__(self):
        super().__init__("t")
    def execute(self):
        self.report_progress(0)
        self.complete({"ok": True})

sys.exit(run_task(T()))
'''

_RUN_SHORTAGE = '''
import os, sys
from sched_task_sdk import ExecutorTask, run_task

MARK = os.environ["MARKER"]

class T(ExecutorTask):
    def __init__(self):
        super().__init__("t")
    def execute(self):
        if not os.path.exists(MARK):
            open(MARK, "w").write("1")
            self.report_progress(0)
            self.report_shortage("外部不足演练")
        self.report_progress(0)
        self.complete({"ok": True})

sys.exit(run_task(T()))
'''

_RUN_LONG = '''
import json, os, sys, time
from sched_task_sdk import ExecutorTask, run_task

LOG = os.environ["WORKLOG"]
TOTAL = int(os.environ.get("TOTAL", "10"))

class T(ExecutorTask):
    def __init__(self):
        super().__init__("t")
    def execute(self):
        done = set()
        if os.path.exists(LOG):
            try:
                done = set(json.loads(open(LOG).read()))
            except Exception:
                done = set()
        for i in range(TOTAL):
            self.checkpoint({"next": i})   # 安全边界：保存断点 + 检查暂停
            if i not in done:
                time.sleep(0.3)
                done.add(i)
                with open(LOG, "w") as f:
                    f.write(json.dumps(sorted(done)))
            self.report_progress(len(done))
        self.complete({"units": sorted(done)})

sys.exit(run_task(T()))
'''


# 第一次执行：裸退 0、无 completed 事件（模拟"退出 0 但无完成记录"）；
# 重排重放后：走 SDK 正常完成
_RUN_EXIT0_THEN_OK = '''
import os, sys
MARK = os.environ["MARKER"]

if not os.path.exists(MARK):
    open(MARK, "w").write("1")
    sys.exit(0)

from sched_task_sdk import ExecutorTask, run_task

class T(ExecutorTask):
    def __init__(self):
        super().__init__("t")
    def execute(self):
        self.report_progress(0)
        self.complete({"ok": True})

sys.exit(run_task(T()))
'''


def _wait_running(svc):
    return wait_until(
        lambda: any(m.status == JobStatus.RUNNING for m in svc.meta.values()),
        desc="attempt RUNNING")


def _job_id(svc, task_id):
    d = svc.db
    return wait_until(
        lambda: next((j["job_id"] for j in d.list_jobs(task_id=task_id)), None),
        desc="job row created")


class TestEndToEnd:
    def test_happy_path(self, home):
        _fast_config(home)
        _make_task(home, "t_ok", _RUN_OK, env={"MARKER": "unused"})
        svc = SchedulerService()
        svc.start()
        try:
            jid = _job_id(svc, "t_ok")
            wait_until(lambda: svc.db.get_job(jid)["status"] == JobStatus.SUCCEEDED,
                       desc="job SUCCEEDED")
            job = svc.db.get_job(jid)
            assert job["retry_count"] == 0
            # 回执 + outbox 归档。C1：回执是一次性确认凭证——生产者消费确认
            # （unacked 清空落盘）后回执文件被删除，不无限累积
            td = home / "tasks" / "t_ok"
            wait_until(lambda: jid not in (json.loads(
                (td / "producer_state" / "state.json").read_text()
            ).get("unacked") or {}), desc="producer consumed receipt")
            assert not (td / "receipts" / f"{jid}.json").exists()
            assert not list((td / "outbox").glob("*.json"))
            # attempt 记录
            atts = svc.db.list_attempts(job_id=jid)
            assert len(atts) == 1 and atts[0]["status"] == JobStatus.SUCCEEDED
        finally:
            svc.stop()

    def test_shortage_backoff_retry_loop(self, home):
        _fast_config(home)
        marker = home / "shortage.marker"
        _make_task(home, "t_short", _RUN_SHORTAGE, env={"MARKER": str(marker)})
        svc = SchedulerService()
        svc.start()
        try:
            jid = _job_id(svc, "t_short")
            # 先观察到 BACKOFF（指数退避排程）；BACKOFF→QUEUED 可能瞬移，等持久的 attempt 终态
            wait_until(
                lambda: any(a["status"] == JobStatus.BACKOFF and a["exit_code"] == 75
                            for a in svc.db.list_attempts(job_id=jid)),
                timeout=15, desc="first attempt BACKOFF(75)")
            job = svc.db.get_job(jid)
            assert job["retry_count"] == 1
            assert job["next_retry_at"] != ""
            # 到期重排 → 第二次尝试（marker 已存在）→ 成功
            wait_until(lambda: svc.db.get_job(jid)["status"] == JobStatus.SUCCEEDED,
                       timeout=25, desc="job SUCCEEDED after retry")
            atts = svc.db.list_attempts(job_id=jid)
            assert len(atts) == 2
            # seq DESC：atts[0]=第二次（成功），atts[1]=第一次（退出 75 归因 BACKOFF）
            assert atts[0]["status"] == JobStatus.SUCCEEDED
            assert atts[1]["status"] == JobStatus.BACKOFF
            assert atts[1]["exit_code"] == 75
        finally:
            svc.stop()

    def test_pause_and_resume_loop(self, home):
        _fast_config(home)
        worklog = home / "worklog.json"
        _make_task(home, "t_pause", _RUN_LONG,
                   env={"WORKLOG": str(worklog), "TOTAL": "6"})
        svc = SchedulerService()
        svc.start()
        try:
            jid = _job_id(svc, "t_pause")
            _wait_running(svc)
            att_id = next(iter(svc.attempts.keys()))
            # 高优先级保护退让：协作暂停
            svc._request_pause(att_id, reason="测试退让")
            # job 的 PAUSED 是瞬时状态（恢复后立即 QUEUED），改等持久的 attempt 终态
            wait_until(
                lambda: any(a["status"] == JobStatus.PAUSED
                            for a in svc.db.list_attempts(job_id=jid)),
                timeout=10, desc="first attempt PAUSED")
            # 无保护轮 → PAUSED 重新排队 → 重入 → 完成
            wait_until(lambda: svc.db.get_job(jid)["status"] == JobStatus.SUCCEEDED,
                       timeout=30, desc="job SUCCEEDED after resume")
            # 业务幂等：每个单元恰好一次
            done = json.loads(worklog.read_text())
            assert done == list(range(6))
            # 退让不计为任务错误：两次 attempt，seq DESC：atts[0]=第二次成功，atts[1]=第一次 PAUSED
            atts = svc.db.list_attempts(job_id=jid)
            assert len(atts) == 2
            assert atts[0]["status"] == JobStatus.SUCCEEDED
            assert atts[1]["status"] == JobStatus.PAUSED
        finally:
            svc.stop()

    def test_cancel_job(self, home):
        _fast_config(home)
        worklog = home / "worklog.json"
        _make_task(home, "t_cancel", _RUN_LONG,
                   env={"WORKLOG": str(worklog), "TOTAL": "30"})
        svc = SchedulerService()
        svc.start()
        try:
            jid = _job_id(svc, "t_cancel")
            _wait_running(svc)
            a = next(iter(svc.attempts.values()))
            pid = a.pid
            assert runner.pid_alive_with_start(pid, a.start_time)
            assert svc.cancel_job(jid)
            wait_until(lambda: svc.db.get_job(jid)["status"] == JobStatus.CANCELLED,
                       timeout=15, desc="job CANCELLED")
            # 进程树退出
            wait_until(lambda: not runner.pid_alive_with_start(pid, a.start_time),
                       timeout=10, desc="process tree dead")
            # attempt 行终态与 job 同一事务提交（进程退出早于下一轮 finalize 时
            # 需等主循环提交，不能立即断言）
            wait_until(lambda: (att := svc.db.list_attempts(job_id=jid)[0])
                       ["status"] == JobStatus.CANCELLED,
                       timeout=15, desc="attempt CANCELLED")
            att = svc.db.list_attempts(job_id=jid)[0]
            assert att["status"] == JobStatus.CANCELLED
        finally:
            svc.stop()

    def test_restart_recovery_and_producer_adoption(self, home):
        _fast_config(home)
        worklog = home / "worklog.json"
        _make_task(home, "t_long", _RUN_LONG,
                   env={"WORKLOG": str(worklog), "TOTAL": "6"})
        svc1 = SchedulerService()
        svc1.start()
        jid = _job_id(svc1, "t_long")
        _wait_running(svc1)
        old = next(iter(svc1.attempts.values()))
        old_pid, old_start = old.pid, old.start_time

        # ---- 模拟调度器崩溃：停 loop 线程，不撤任何子进程，只释放锁与 DB 连接 ----
        svc1._stopping.set()
        svc1._loop_thread.join(timeout=3)
        os.close(svc1._lock_fd)
        svc1._lock_fd = -1
        svc1.db.close()
        assert runner.pid_alive_with_start(old_pid, old_start)

        # ---- 重启：新实例接管 ----
        svc2 = SchedulerService()
        svc2.start()
        try:
            # 生产者任务级单实例锁接管：external 且存活，未重复拉起
            producers = [p for p in svc2.guard.list() if p["task_id"] == "t_long"]
            assert producers and producers[0]["external"] is True
            assert producers[0]["alive"] is True
            # 系统中只有一个该任务的生产者进程
            def producer_proc_count():
                n = 0
                for pid_dir in os.listdir("/proc"):
                    if not pid_dir.isdigit():
                        continue
                    try:
                        with open(f"/proc/{pid_dir}/cmdline", "rb") as f:
                            cmd = f.read().decode("utf-8", "replace")
                    except OSError:
                        continue
                    if "t_long/producer.py" in cmd:
                        n += 1
                return n
            wait_until(lambda: producer_proc_count() == 1,
                       desc="single producer process")

            # 审查修复后的接管语义（设计第十三节 L221）：存活的旧 worker 被
            # 新实例接管继续跑完，不中断业务、不重放——同一 attempt 完成
            wait_until(lambda: svc2.db.get_job(jid)["status"] == JobStatus.SUCCEEDED,
                       timeout=40, desc="job SUCCEEDED after restart adoption")
            atts = svc2.db.list_attempts(job_id=jid)
            assert len(atts) == 1, "接管后不得新建 attempt（无重放）"
            assert atts[0]["status"] == JobStatus.SUCCEEDED
            assert atts[0]["pid"] == old_pid, "完成的应是接管的旧 worker"
            # 业务幂等：单元恰好一次
            done = json.loads(worklog.read_text())
            assert done == list(range(6))
        finally:
            svc2.stop()

    def test_cli_ipc_status_and_cancel(self, home, capsys):
        _fast_config(home)
        worklog = home / "worklog.json"
        _make_task(home, "t_cli", _RUN_LONG,
                   env={"WORKLOG": str(worklog), "TOTAL": "40"})
        svc = SchedulerService()
        svc.start()
        try:
            jid = _job_id(svc, "t_cli")
            _wait_running(svc)
            # 快照已由 serve 循环写出
            wait_until(lambda: store.load_json(config.snapshot_file(), default=None)
                       is not None, desc="snapshot written")
            # status：读快照显示 serve=alive（而非空内存态）
            assert cli_main(["status"]) == 0
            out = capsys.readouterr().out
            assert "serve=alive" in out
            assert "t_cli" in out
            # job cancel：命令通道 → serve 消费 → 回执 → CANCELLED
            assert cli_main(["job", "cancel", jid]) == 0
            out = capsys.readouterr().out
            assert "CANCELLED" in out
            wait_until(lambda: svc.db.get_job(jid)["status"] == JobStatus.CANCELLED,
                       timeout=15, desc="job CANCELLED via IPC")
            # 快照可见恢复/队列等状态字段
            snap = store.load_json(config.snapshot_file(), default={})
            for key in ("producers", "scale", "live_attempts", "protected_task",
                        "observability_ok", "exec_io_bytes", "scan_pending"):
                assert key in snap
        finally:
            svc.stop()

    def test_cli_direct_cancel_when_serve_down(self, home, capsys):
        # serve 不在：CLI 直连取消排队中作业
        _fast_config(home)
        _make_task(home, "t_down", _RUN_OK, env={"MARKER": "x"})
        svc = SchedulerService()
        svc.start()
        jid = _job_id(svc, "t_down")
        svc.stop()  # serve 退出
        # 作业可能已在停机前完成/排队；构造一个排队中的作业更稳：
        d = dbmod.DB()
        if d.get_job(jid) is None:
            pytest.skip("job not created before stop")
        status = d.get_job(jid)["status"]
        if status in JobStatus.TERMINAL:
            d.set_job_status(jid, JobStatus.QUEUED, "测试重置", runner.now_iso())
        assert cli_main(["job", "cancel", jid]) == 0
        out = capsys.readouterr().out
        assert "CANCELLED" in out
        assert dbmod.DB().get_job(jid)["status"] == JobStatus.CANCELLED

    def test_requeue_recovery_blocked_via_command_channel(self, home):
        # 退出 0 但无完成记录 → RECOVERY_BLOCKED → 命令通道 requeue → 重放完成
        _fast_config(home)
        marker = home / "requeue.marker"
        _make_task(home, "t_rq", _RUN_EXIT0_THEN_OK, env={"MARKER": str(marker)})
        svc = SchedulerService()
        svc.start()
        try:
            jid = _job_id(svc, "t_rq")
            wait_until(lambda: svc.db.get_job(jid)["status"] == JobStatus.RECOVERY_BLOCKED,
                       timeout=20, desc="job RECOVERY_BLOCKED")
            att = svc.db.list_attempts(job_id=jid)[0]
            assert att["status"] == JobStatus.RECOVERING  # 身份未知的存活 attempt
            # serve 在线：走命令通道（CLI 语义 = _send_command("requeue_job", ...)）
            cid = runner.new_id("cmd")
            store.append_jsonl(config.commands_file(),
                               {"id": cid, "kind": "requeue_job", "target": jid,
                                "reason": "测试重排", "ts": runner.now_iso()})

            def acked():
                return next((a for a in store.read_jsonl(config.commands_ack_file())
                             if a.get("id") == cid and a.get("ok")), None)

            wait_until(acked, desc="requeue ack ok")
            # 旧 attempt 被关闭为 CANCELLED（RECOVERING 遗留行也一并清理）
            wait_until(
                lambda: any(a["status"] == JobStatus.CANCELLED and "重排关闭" in (a["reason"] or "")
                            for a in svc.db.list_attempts(job_id=jid)),
                timeout=10, desc="legacy attempt closed by requeue")
            # 新 attempt 准入并完成（QUEUED 是瞬态，直接等终态）
            wait_until(lambda: svc.db.get_job(jid)["status"] == JobStatus.SUCCEEDED,
                       timeout=25, desc="job SUCCEEDED after requeue replay")
            atts = svc.db.list_attempts(job_id=jid)
            assert len(atts) == 2
            assert atts[0]["status"] == JobStatus.SUCCEEDED
            assert atts[1]["status"] == JobStatus.CANCELLED
            assert "重排关闭" in atts[1]["reason"]
        finally:
            svc.stop()

    def test_requeue_cli_offline_direct(self, home, capsys):
        # serve 不在：CLI 直连重排 RECOVERY_BLOCKED 作业
        _fast_config(home)
        marker = home / "requeue2.marker"
        _make_task(home, "t_rq2", _RUN_EXIT0_THEN_OK, env={"MARKER": str(marker)})
        svc = SchedulerService()
        svc.start()
        jid = _job_id(svc, "t_rq2")
        wait_until(lambda: svc.db.get_job(jid)["status"] == JobStatus.RECOVERY_BLOCKED,
                   timeout=20, desc="job RECOVERY_BLOCKED")
        svc.stop()  # serve 退出（释放单实例锁 → CLI 直连分支）
        assert cli_main(["job", "requeue", jid, "--reason", "offline test"]) == 0
        out = capsys.readouterr().out
        assert "REQUEUED" in out
        d = dbmod.DB()
        assert d.get_job(jid)["status"] == JobStatus.QUEUED
        att = d.list_attempts(job_id=jid)[0]
        assert att["status"] == JobStatus.CANCELLED
        assert "重排关闭" in att["reason"]

    def test_requeue_rejected_while_running(self, home):
        # 运行中的作业拒绝重排（防重复执行）
        _fast_config(home)
        worklog = home / "worklog.json"
        _make_task(home, "t_run", _RUN_LONG,
                   env={"WORKLOG": str(worklog), "TOTAL": "40"})
        svc = SchedulerService()
        svc.start()
        try:
            jid = _job_id(svc, "t_run")
            _wait_running(svc)
            assert svc.requeue_job(jid) is False
            assert svc.db.get_job(jid)["status"] == JobStatus.RUNNING
            svc.cancel_job(jid)
            wait_until(lambda: svc.db.get_job(jid)["status"] == JobStatus.CANCELLED,
                       timeout=15, desc="job CANCELLED")
        finally:
            svc.stop()
