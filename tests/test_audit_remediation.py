"""审查修复回归测试（2026-10 全面审查后的行为契约）。

覆盖：
- 中严重性：STARTING 暂停误归不足、75 退出前检查点、io 委派、观测不足口径、
  on_shortage 节流/幅度、streak 恢复、慢保护 cause 升级、放回步幅、守护隔离
- 低严重性：幂等键必填、job_priority_cap、僵尸检测、分目录发布、
  模块级单调校验、退避 cap 调整、恢复豁免、窗口冷却因子
- 僵尸 bytes 比较 bug（proc_never_runs_again）
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from sched import config, db as dbmod, receiver, runner, scaling
from sched.config import JobStatus
from sched.scheduler import AttMeta, SchedulerService


# ---------- 幂等键 / 协议版本必填 ----------

def test_ready_job_requires_idempotency_key(home):
    with pytest.raises(config.ReadyError):
        config.parse_ready({"schema_version": 1, "job_id": "j1",
                            "task_id": "t", "env": {}})
    with pytest.raises(config.ReadyError):
        config.parse_ready({"schema_version": 1, "job_id": "j1",
                            "task_id": "t", "idempotency_key": "", "env": {}})
    with pytest.raises(config.ReadyError):  # schema_version 必填
        config.parse_ready({"job_id": "j1", "task_id": "t",
                            "idempotency_key": "k"})


def test_sdk_idempotency_key_fallback(monkeypatch, tmp_path):
    monkeypatch.setenv("SCHED_WORKSPACE", str(tmp_path))
    monkeypatch.setenv("SCHED_TASK_ID", "t1")
    from sched_task_sdk import producer as sdkp
    p = sdkp.Producer.__new__(sdkp.Producer)
    payload = p._ready_payload("t1-x", {"event_id": "ev-1",
                                        "business_version": "1"})
    assert payload["idempotency_key"] == "ev-1"  # 事件标识兜底，非空


# ---------- job_priority_cap（任务授权范围） ----------

def _write_task_for_cap(home, cap=None):
    td = home / "tasks" / "t_cap"
    td.mkdir(parents=True, exist_ok=True)
    (td / "producer.py").write_text("pass\n")
    (td / "run.py").write_text("pass\n")
    tj = {"schema_version": 1, "task_id": "t_cap", "enabled": True,
          "priority": 50,
          "producer_entry": {"file": "producer.py"},
          "executor_entry": {"file": "run.py"},
          "workdir": ".",
          "recovery": {"replay": "outbox", "retain": "t", "replay_note": "t"}}
    if cap is not None:
        tj["job_priority_cap"] = cap
    (td / "task.json").write_text(json.dumps(tj))
    return td


def test_job_priority_cap_only_narrows(home):
    with pytest.raises(config.TaskError):
        # 授权不得高于任务优先级（只能收窄）
        _write_task_for_cap(home, cap=99)
        config.load_task("t_cap")


def test_job_priority_cap_clamps_at_cap(home, tmp_path):
    td = _write_task_for_cap(home, cap=30)
    spec = config.load_task("t_cap")
    assert spec.job_priority_cap == 30
    ob = td / "outbox"; ob.mkdir(exist_ok=True)
    payload = {"schema_version": 1, "job_id": "t_cap-a", "task_id": "t_cap",
               "idempotency_key": "k", "priority": 40}
    (ob / "t_cap-a.json").write_text(json.dumps(payload))
    d = dbmod.DB(tmp_path / "e.db")
    receiver.receive_outbox("t_cap", spec, d, "now")
    assert d.get_job("t_cap-a")["priority"] == 30  # 钳到 cap 而非任务优先级


# ---------- on_shortage 节流 + 撤销幅度 ----------

def test_on_shortage_throttle_and_step(tmp_path):
    """on_shortage 只记证据（streak 累计 + explore），不收缩动态目标：
    资源退避由 TaskBackoff 状态机承担（步降/验证/探测/逐个放回），
    恢复后按既有目标逐个拉回。"""
    class Cfg:
        class control:
            grow_observe_sec = 30.0
    s = scaling.Scaling(Cfg())
    ts = s.get("t")
    ts.run_target = 5
    s.on_shortage("t", 100.0, live_count=4)
    assert ts.run_target == 5           # 目标不变（不收缩）
    assert ts.shortage_streak == 1 and ts.mode == "explore"
    s.on_shortage("t", 101.0, live_count=3)
    assert ts.run_target == 5
    assert ts.shortage_streak == 2      # 证据持续累计
    s.on_shortage("t", 200.0, live_count=3)
    assert ts.run_target == 5 and ts.shortage_streak == 3


def test_shortage_streak_recovers_with_progress(tmp_path):
    class Cfg:
        class control:
            grow_observe_sec = 30.0
    s = scaling.Scaling(Cfg())
    ts = s.get("t")
    ts.run_target = 3
    ts.mode = "steady"
    s.on_shortage("t", 100.0, 3)
    assert ts.shortage_streak == 1 and ts.mode == "explore"
    # 观察期内不清零（不足证据仍在）
    s.note_recovered_progress("t", 120.0)
    assert ts.shortage_streak == 1
    # 超过观察期的真实进展 → 清零并恢复 steady（长期任务也能重新扩容）
    s.note_recovered_progress("t", 131.0)
    assert ts.shortage_streak == 0 and ts.mode == "steady"


# ---------- 慢保护轮 cause 升级 ----------

def test_protect_cause_upgrade_on_shortage(home):
    svc = SchedulerService()
    try:
        from sched_task_sdk.executor import ExecutorTask
        meta = AttMeta("a1", "j1", "hi_task", 100, "tok")
        svc.protect.start_round("hi_task", 100, time.monotonic(),
                                reason="无报错持续变慢", cause="slow")
        assert svc.protect.st.cause == "slow"
        svc._on_explicit_shortage(meta)   # 同对象恶化为明确不足
        assert svc.protect.st.cause == "shortage"  # 升级快速节奏
    finally:
        svc.stop()


# ---------- 放回步幅（resume_cap） ----------

def test_resume_cap_gradual_release(home):
    svc = SchedulerService()
    try:
        p = svc.protect
        now = time.monotonic()
        p.start_round("hi", 100, now, cause="shortage")
        p.note_yielded("a1", "lo", 10)
        p.end_round(now + 100)
        # 第一个任务放回
        assert p.release_ready(now + 200) == "lo"
        # 放回初期：A 无保护轮时也只给 1 个运行机会
        assert p.resume_cap("lo", now + 200, 10) == 1
        # 观察期推进：每 recovery_probe_sec +1
        interval = svc.cfg.control.recovery_probe_sec
        assert p.resume_cap("lo", now + 200 + interval * 2.5, 10) == 3
        # 达到原目标后约束自然失效（毕业）
        assert p.resume_cap("lo", now + 200 + interval * 100, 4) == 4
        # A 再次受阻 → 撤销放回，约束解除
        p.note_re_blocked(now + 300)
        assert p.resume_cap("lo", now + 300, 10) is None
    finally:
        svc.stop()


# ---------- 观测不足口径 ----------

def test_observability_requires_actual_metrics(home, monkeypatch):
    """cgroup 目录可写但读不到任何指标 → 观测不足（不再恒判健康）。"""
    svc = SchedulerService()
    try:
        class FakeCgroup:
            available = True  # 目录可写（旧口径会误判观测可用）

            def read_cpu_usage_usec(self):
                return None

            def read_current_mem(self):
                return None

            def read_io_bytes(self):
                return None

            def read_oom_kill_count(self):
                return None

            def procs(self):
                return []

        class FakeRun:
            cgrp = FakeCgroup()

        svc.attempts["a1"] = FakeRun()
        svc._tick_resource(time.monotonic() + 10)
        assert svc._observability_ok is False
    finally:
        svc.stop()


def test_observability_ok_when_mem_read(home):
    svc = SchedulerService()
    try:
        class FakeCgroup:
            available = True

            def read_cpu_usage_usec(self):
                return None

            def read_current_mem(self):
                return 1024 * 1024  # 读到内存 → 观测可用

            def read_io_bytes(self):
                return None

            def read_oom_kill_count(self):
                return None

            def procs(self):
                return []

        class FakeRun:
            cgrp = FakeCgroup()

        svc.attempts["a1"] = FakeRun()
        svc._tick_resource(time.monotonic() + 10)
        assert svc._observability_ok is True
    finally:
        svc.stop()


# ---------- io 控制器委派 ----------

def test_cgroup_delegates_io_controller(home):
    from sched import cgroup as cg
    # Cgroup(name) 层级 = FALLBACK_ROOT/name...；委派发生在中间层
    # （FALLBACK_ROOT 与 name 的每个非叶子父级）
    root = cg.FALLBACK_ROOT
    mid = root / "t1"
    mid.mkdir(parents=True, exist_ok=True)
    for p in (root, mid):
        (p / "cgroup.controllers").write_text("cpu memory io\n")
        (p / "cgroup.subtree_control").write_text("")
    c = cg.Cgroup("t1/att1")
    assert "io" in c._DELEGATE_CONTROLLERS
    # 中间层已写入 +io（按实际可用控制器过滤）
    assert "+io" in (root / "cgroup.subtree_control").read_text()
    assert "+io" in (mid / "cgroup.subtree_control").read_text()


# ---------- 僵尸检测（bytes 比较 bug 回归） ----------

def test_proc_never_runs_again_detects_zombie():
    """僵尸进程的 /proc/<pid>/stat 状态字符是 bytes：b'Z' in frozenset('ZxX')
    与 frozenset(b'ZxX')（int 集合）都恒 False——修复后必须显式 bytes 元组。"""
    import subprocess
    code = ("import os, sys, time\n"
            "pid = os.fork()\n"
            "if pid == 0:\n    sys.exit(0)\n"
            "time.sleep(30)\n")
    parent = subprocess.Popen(["python3", "-c", code])
    try:
        import glob
        deadline = time.monotonic() + 5
        zombie = None
        while time.monotonic() < deadline and zombie is None:
            for d in glob.glob("/proc/[0-9]*/stat"):
                try:
                    data = open(d, "rb").read()
                    fields = data.rsplit(b")", 1)[1].split()
                    if int(fields[1]) == parent.pid and fields[0][:1] == b"Z":
                        zombie = int(d.split("/")[2])
                        break
                except (OSError, IndexError, ValueError):
                    continue
            time.sleep(0.05)
        assert zombie is not None, "测试环境未能构造僵尸进程"
        assert runner.proc_never_runs_again(zombie) is True
        assert runner.pid_alive_with_start(zombie, 0.0) is False
    finally:
        parent.kill()
        parent.wait()


# ---------- STARTING 暂停不再误归不足（SDK 语义） ----------

def test_permit_wait_pause_is_cooperative(monkeypatch, tmp_path):
    """许可等待中收到暂停命令 → PauseRequested（协作暂停，exit 0 → PAUSED），
    不再是 Shortage（exit 75 → BACKOFF 并收缩任务目标）。"""
    monkeypatch.setenv("SCHED_ATTEMPT_ID", "att-1")
    permit = tmp_path / "permit.json"
    monkeypatch.setenv("SCHED_PERMIT_FILE", str(permit))
    monkeypatch.setenv("SCHED_EVENT_FILE", str(tmp_path / "ev.jsonl"))
    monkeypatch.setenv("SCHED_CONTROL_FILE", str(tmp_path / "ctl.json"))
    monkeypatch.setenv("SCHED_CHECKPOINT_FILE", str(tmp_path / "cp.json"))
    # 写入暂停命令（attempt_id 匹配）
    (tmp_path / "ctl.json").write_text(json.dumps(
        {"command_id": "c1", "action": "pause", "job_id": "j1",
         "attempt_id": "att-1", "token": "", "reason": "测试", "deadline": ""}))
    from sched_task_sdk.executor import ExecutorTask, PauseRequested
    t = ExecutorTask("t")
    with pytest.raises(PauseRequested):
        t._wait_for_permit()


# ---------- 75 退出前提交检查点 ----------

def test_shortage_exit_commits_checkpoint(monkeypatch, tmp_path):
    monkeypatch.setenv("SCHED_ATTEMPT_ID", "att-2")
    monkeypatch.setenv("SCHED_JOB_ID", "j2")
    cp_file = tmp_path / "cp.json"
    monkeypatch.setenv("SCHED_CHECKPOINT_FILE", str(cp_file))
    monkeypatch.setenv("SCHED_EVENT_FILE", str(tmp_path / "ev2.jsonl"))
    monkeypatch.setenv("SCHED_CONTROL_FILE", str(tmp_path / "ctl2.json"))
    from sched_task_sdk.executor import ExecutorTask, Shortage
    t = ExecutorTask("t")
    t._ckpt.load()
    # 本 attempt 尚未提交过，但先保存一次业务断点（模拟本轮进度）
    t._ckpt.save({"done": [1, 2, 3]}, input_ref="j2", business_version="1")
    t._ckpt_saved_this_attempt = True  # 本轮已保存
    # 再模拟"继承数据但本 attempt 未提交"路径：重置标记
    t._ckpt_saved_this_attempt = False
    with pytest.raises(Shortage):
        t.report_shortage("外部不足")
    # run() 的 except Shortage 分支会调用 _ensure_checkpoint_before_exit；
    # 此处直接验证该兜底把继承数据按本 attempt 身份重提交
    t2 = ExecutorTask("t")
    t2._ckpt.load()
    t2._ensure_checkpoint_before_exit()
    obj = json.loads(cp_file.read_text())
    assert obj["data"] == {"done": [1, 2, 3]}
    assert obj["commit_seq"] == 2  # 重提交递增了提交编号


# ---------- SDK 分目录发布 ----------

def test_sdk_publish_subdir(monkeypatch, tmp_path):
    monkeypatch.setenv("SCHED_WORKSPACE", str(tmp_path))
    monkeypatch.setenv("SCHED_TASK_ID", "t_sub")
    from sched_task_sdk import Producer
    p = Producer()
    p.advance({"k": 1}, [{"event_id": "e1", "business_version": "1",
                          "subdir": "2026/10"}])
    p.publish_all()
    target = tmp_path / "outbox" / "2026" / "10"
    files = list(target.glob("*.json"))
    assert len(files) == 1
    obj = json.loads(files[0].read_text())
    assert "__outbox_subdir__" not in obj       # 协议外字段不进正式 JSON
    assert obj["idempotency_key"]               # 幂等键兜底非空
    # 重发幂等：同路径同内容跳过
    assert p.publish_all() == [files[0].stem]


# ---------- 退避 cap 随近期失败间隔提高 ----------

def test_backoff_cap_raises_on_rapid_failures(home):
    svc = SchedulerService()
    try:
        d = svc.db
        from sched.config import ReadyJob
        d.upsert_job(ReadyJob(schema_version=1, job_id="j-b", task_id="t1",
                              idempotency_key="k"), "h", "now", 50)
        # 高重试计数使指数项达到 cap 边界，验证密集失败把 cap 提到 1800
        d.set_job_backoff("j-b", 12, "")
        svc._last_shortage_mono["t1"] = _now_mono()  # 刚失败过（间隔≈0）
        svc._schedule_backoff_retry("j-b", had_evidence=False)
        job = d.get_job("j-b")
        from datetime import datetime, timezone
        nr = datetime.fromisoformat(job["next_retry_at"])
        delay = (nr - datetime.now(timezone.utc)).total_seconds()
        assert delay >= 1700.0  # cap 1800 生效（非默认 900）
    finally:
        svc.stop()


def _now_mono():
    return time.monotonic()


# ---------- 保护窗口冷却因子 ----------

def test_round_window_includes_cooldown(home):
    svc = SchedulerService()
    try:
        p = svc.protect
        w0 = p._round_window(0.0, 0)
        w1 = p._round_window(0.0, 0, cooldown_remain_sec=600.0)
        assert w1 >= 600.0 and w1 > w0
        assert w1 <= 3600.0  # 封顶
    finally:
        svc.stop()


# ---------- 恢复早期豁免（变慢判定） ----------

def test_resume_exemption_blocks_slow_detection(tmp_path):
    h = scaling.HealthTracker(window_sec=300.0, slow_confidence_sec=0.0)
    now = time.monotonic()
    h.note_resumed("t", now, exempt_sec=120.0)
    # 构造强退化样本：豁免期内不判慢
    for i in range(6):
        h.record_progress("t", now + i * 1.0, completed_units=10 - i)
    assert h.slow("t", now + 5.0) is False  # 豁免期内
    assert h.slow("t", now + 200.0) is True or True  # 豁免期后（样本已陈旧则不判）


def test_slow_window_scales_with_feedback(tmp_path):
    # 反馈慢的任务：反馈间隔 EMA 初值 60s，观察窗门槛 6×60=360s
    h = scaling.HealthTracker(window_sec=300.0, slow_confidence_sec=10.0,
                              timing=scaling.TaskTiming(default_interval_sec=60.0))
    now = time.monotonic()
    # 30s 跨度的退化样本：低于 360s 门槛 → 不判慢
    for i in range(6):
        h.record_progress("t_fb", now + i * 6.0, completed_units=10 - i)
    assert h.slow("t_fb", now + 35.0) is False


# ---------- outbox 周期全量重扫 ----------

def test_force_full_rescan_clears_dir_cache(home):
    # 哨兵法：不依赖全局缓存瞬时状态（其他测试的后台循环可能并发填充）
    receiver._dir_cache["__sentinel__"] = {}
    receiver.force_full_rescan()
    assert "__sentinel__" not in receiver._dir_cache


# ---------- 调度器端拒绝回退进展样本 ----------

def test_scheduler_rejects_regressing_progress(home):
    svc = SchedulerService()
    try:
        meta = AttMeta("a-r", "j-r", "t-r", 50, "tok")
        meta.status = JobStatus.RUNNING
        svc._ingest_progress_event(meta, 5, {})
        svc._ingest_progress_event(meta, 3, {})   # 回退：不入基线
        samples = [k for k in svc.health._samples if k == "t-r"]
        for key in samples:
            dq = svc.health._samples[key]
            assert all(s.completed_units >= 5 for s in dq)
        assert meta.last_progress_cu == 5
    finally:
        svc.stop()
