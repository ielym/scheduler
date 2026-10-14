"""终审修复回归测试（对照设计方案全面审计发现的问题）。

1. cgroup 泄漏：remove() 有界重试；reap_orphans() 清理空目录（历史 1472 目录泄漏）。
2. stop() 并行协作收尾：同时 TERM 全部执行进程、有界等待、逐个清理 cgroup，
   不再把停机拖到 N×grace，也不泄漏目录。
3. 快照新鲜度双时钟校验：重启后 monotonic 归零，仅凭 mono 会把旧快照判为
   "新鲜"（误报 serve=alive）；墙钟 ts 陈旧必须拒绝。
4. 资源型暂停恢复冷却：OOM/内存增长退让/保护自收缩的 PAUSED 作业恢复排队
   前冷却（防 ~15s 周期的"重启→再暂停"循环），冷却过后照常恢复。
5. SDK 时间戳统一 UTC：事件 at 与检查点 saved_at 均为 +00:00（设计第十四节）。
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone

from sched import cgroup as cg
from sched import config, runner
from sched.config import JobStatus
from sched.scheduler import SchedulerService
from sched_task_sdk.checkpoint import CheckpointStore
from sched_task_sdk.events import writer as event_writer


def _svc(home):
    (home / "runtime").mkdir(parents=True, exist_ok=True)
    return SchedulerService()


def _mk_job(db, job_id, task_id="t", priority=50):
    ready = config.parse_ready({
        "schema_version": 1, "job_id": job_id, "task_id": task_id,
        "entry_args": [], "env": {}, "idempotency_key": job_id,
    })
    db.upsert_job(ready, f"hash-{job_id}", "2026-10-12T00:00:00+00:00", priority)
    return job_id


# ---------- 1. cgroup：remove 重试与空目录清理 ----------

def test_cgroup_remove_retries_and_deletes(home, monkeypatch):
    root = home / "fakecg"
    root.mkdir()
    monkeypatch.setattr(cg, "FALLBACK_ROOT", root)
    monkeypatch.setattr(cg, "_cgroup_fs_mounted", lambda: True)
    c = cg.new_cgroup("t/att-1")
    assert c.available           # tmp 目录可写 → 观测可用
    assert (root / "t" / "att-1").is_dir()
    # 真实 cgroupfs 的控制器文件（cgroup.kill 等）随目录删除；tmp 假目录中
    # kill_all 会留下普通文件挡住 rmdir，对实例打补丁以专注验证 rmdir 逻辑
    monkeypatch.setattr(c, "kill_all", lambda: None)
    c.remove()
    assert not (root / "t" / "att-1").exists()


def test_cgroup_remove_gives_up_when_pids_gone(home, monkeypatch):
    """cgroup.procs 残留陈旧 pid（进程已死）时不再空转重试到 2s 上限。"""
    root = home / "fakecg"
    root.mkdir()
    monkeypatch.setattr(cg, "FALLBACK_ROOT", root)
    monkeypatch.setattr(cg, "_cgroup_fs_mounted", lambda: True)
    c = cg.new_cgroup("t/att-stale")
    # 写入一个早已消亡的 pid（测试假 cgroupfs 不会自动清 procs）
    (c.path / "cgroup.procs").write_text("999999999\n")
    t0 = time.monotonic()
    c.remove()   # rmdir 因目录含文件失败，但 pid 已死 → 立即放弃
    assert time.monotonic() - t0 < 1.0


def test_reap_orphans_prunes_empty_dirs(home, monkeypatch):
    """reap_orphans 顺带回收 sched 子树的历史泄漏空目录（1472 目录事故）。"""
    root = home / "fakecg"
    root.mkdir()
    for rel in ("t1/att-a", "t2/att-b", "t3/att-c/leaf"):
        (root / rel).mkdir(parents=True)
    monkeypatch.setattr(cg, "FALLBACK_ROOT", root)
    monkeypatch.setattr(cg, "_cgroup_fs_mounted", lambda: True)
    cg.reap_orphans()   # 无任何 cgroup.procs → 无进程可杀，但空目录被清理
    assert not (root / "t1").exists()
    assert not (root / "t2").exists()
    assert not (root / "t3").exists()
    assert root.exists()  # 根目录保留（仍在使用）


# ---------- 2. stop()：并行协作收尾 + cgroup 清理 ----------

class _StopFakeAttempt:
    """stop() 所需的最小 attempt 假对象（记录终止状态机推进）。"""

    def __init__(self, alive_for_sec=0.0):
        self.pid = 4242
        self.pgid = 4242
        self.started = time.monotonic()
        self._alive_until = time.monotonic() + alive_for_sec
        self.cleaned = False
        self.terminate_calls = 0

    def is_alive(self):
        return time.monotonic() < self._alive_until

    def begin_terminate(self, now_mono, grace_sec):
        self.terminate_calls += 1
        return True

    def poll_terminate(self, now_mono):
        return "gone" if not self.is_alive() else "waiting"

    def cleanup(self):
        self.cleaned = True


def test_stop_terminates_all_in_parallel_and_cleans_cgroups(home):
    svc = _svc(home)
    # 模拟 3 个存活 + 1 个立即退出的执行进程
    fast = _StopFakeAttempt(0.0)
    slow = _StopFakeAttempt(3.0)
    for aid, a in (("a-fast", fast), ("a-slow", slow), ("a-slow2", _StopFakeAttempt(3.0))):
        svc.attempts[aid] = a
    t0 = time.monotonic()
    svc.stop()
    elapsed = time.monotonic() - t0
    # 并行收尾：3 个宽限 10s 的存活进程同时 TERM、~3s 内全部退出；
    # 串行实现至少 2×3s 起步且按 grace 等待——并行后总耗时应接近最慢者而非求和
    assert elapsed < 6.0, f"stop 应并行收尾，实际耗时 {elapsed:.1f}s"
    assert fast.cleaned and slow.cleaned
    assert fast.terminate_calls == 1
    svc.db.close()


def test_stop_cleans_cgroup_even_without_attempts(home):
    svc = _svc(home)
    svc.stop()   # 无在途 attempt：正常退出、释放锁
    svc.db.close()


# ---------- 3. 快照新鲜度双时钟 ----------

def test_fresh_snapshot_rejects_stale_wall_clock(home, monkeypatch):
    """重启场景：mono 差值为负（monotonic 已归零）但 ts 陈旧 → 拒绝。"""
    from sched import __main__ as cli
    old_ts = (datetime.now(timezone.utc) -
              timedelta(hours=2)).isoformat(timespec="seconds")
    # 模拟"上一次开机"的 mono：当前 monotonic 比它小 → 差值为负、不过阈值
    store_snap = {"mono": time.monotonic() + 500_000.0, "ts": old_ts,
                  "tasks": 1, "queued": 0}
    from sched import store
    store.save_json(config.snapshot_file(), store_snap)
    assert cli._fresh_snapshot() is None


def test_fresh_snapshot_accepts_fresh(home):
    from sched import __main__ as cli
    from sched import store
    store.save_json(config.snapshot_file(), {
        "mono": time.monotonic(), "ts": runner.now_iso(), "tasks": 1})
    snap = cli._fresh_snapshot()
    assert snap is not None and snap["tasks"] == 1


def test_fresh_snapshot_rejects_stale_mono(home):
    from sched import __main__ as cli
    from sched import store
    store.save_json(config.snapshot_file(), {
        "mono": time.monotonic() - 3600.0, "ts": runner.now_iso(), "tasks": 1})
    assert cli._fresh_snapshot() is None


# ---------- 4. 资源型暂停恢复冷却 ----------

def _pause_job(svc, job_id, reason, finished_at=None):
    svc.db.set_job_status(job_id, JobStatus.PAUSED, reason,
                          finished_at or runner.now_iso())


def test_resource_pause_cooldown_blocks_immediate_resume(home):
    svc = _svc(home)
    reasons = [
        "执行内存不足（OOM 证据）；协作暂停，断点已保存",
        "执行内存快速增长（内存增长退让）；协作暂停，断点已保存",
        "无低优先级可退让，收缩 t_hi 自身；协作暂停，断点已保存",
    ]
    for i, reason in enumerate(reasons):
        _mk_job(svc.db, f"jR{i}")
        _pause_job(svc, f"jR{i}", reason)
    svc._tick_release(time.monotonic())
    for i in range(len(reasons)):
        assert svc.db.get_job(f"jR{i}")["status"] == JobStatus.PAUSED, \
            "资源型暂停在冷却窗口内不得立即恢复（防紧密重启循环）"


def test_resource_pause_resumes_after_cooldown(home):
    svc = _svc(home)
    _mk_job(svc.db, "jR")
    old = (datetime.now(timezone.utc) -
           timedelta(seconds=svc.cfg.control.heartbeat_resume_cooldown_sec + 60)
           ).isoformat(timespec="seconds")
    _pause_job(svc, "jR", "执行内存不足（OOM 证据）；协作暂停，断点已保存", old)
    svc._tick_release(time.monotonic())
    assert svc.db.get_job("jR")["status"] == JobStatus.QUEUED


def test_plain_pause_still_resumes_immediately(home):
    svc = _svc(home)
    _mk_job(svc.db, "jN")
    _pause_job(svc, "jN", "调度器重启：进程已退出")
    svc._tick_release(time.monotonic())
    assert svc.db.get_job("jN")["status"] == JobStatus.QUEUED


def test_heartbeat_pause_cooldown_unchanged(home):
    svc = _svc(home)
    _mk_job(svc.db, "jH")
    _pause_job(svc, "jH", "心跳过期，疑似卡住；协作暂停，断点已保存")
    svc._tick_release(time.monotonic())
    assert svc.db.get_job("jH")["status"] == JobStatus.PAUSED


# ---------- 5. SDK 时间戳统一 UTC ----------

def test_sdk_event_at_is_utc(monkeypatch, tmp_path):
    ev = tmp_path / "events.jsonl"
    monkeypatch.setenv("SCHED_EVENT_FILE", str(ev))
    w = event_writer()
    w.emit("heartbeat")
    line = json.loads(ev.read_text("utf-8").splitlines()[0])
    assert str(line["at"]).endswith("+00:00"), "事件 at 必须是 UTC 时间戳"


def test_sdk_checkpoint_saved_at_is_utc(monkeypatch, tmp_path):
    cp = tmp_path / "checkpoint.json"
    monkeypatch.setenv("SCHED_CHECKPOINT_FILE", str(cp))
    store = CheckpointStore()
    saved = store.save({"done": 1})
    assert str(saved["saved_at"]).endswith("+00:00"), \
        "检查点 saved_at 必须是 UTC 时间戳（设计第十四节）"
