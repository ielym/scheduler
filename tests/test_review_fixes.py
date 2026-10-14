"""审查修复回归测试（对应 2026-10-12/13 符合性审查报告的问题清单）。

#1a 准入候选窗口饥饿（严重）：全局 receive_seq 前 500 窗口使早期任务
    独占准入、同优先级其它任务永久饥饿 → 改为任务级轮转取数（设计第十节
    L163 同级任务轮转 + 第九节 L145 恢复探测最小探索机会）；
#1b 高优先级新需求探测同样脱离全局窗口（设计第十节）；
#1c run_target=0 的任务其作业可进候选并获冷启动兜底（0→1）；
#1d 作业级优先级（仅允许低于任务优先级）合并后仍保持全局降序；
#2  接收器增量目录扫描（设计第五节"流式分页"）：目录 mtime 缓存、
    新增/删除/新子目录均正确反映；
#3  数据库时区归一迁移（设计第十四节"持久化时间记录 UTC"）：历史
    +08:00 行一次性转为 +00:00，user_version 防重放；
#4  保护窗口计入低优先级任务数（设计第十节 L181）。
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = str(Path(__file__).resolve().parent.parent)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from sched import config, db as dbmod, receiver  # noqa: E402
from sched.scheduler import SchedulerService  # noqa: E402


def _write_task(home, task_id, priority=50, enabled=True):
    td = home / "tasks" / task_id
    td.mkdir(parents=True, exist_ok=True)
    (td / "producer.py").write_text("")
    (td / "run.py").write_text("")
    from sched import store
    store.save_json(td / "task.json", {
        "schema_version": 1, "task_id": task_id, "enabled": enabled,
        "priority": priority,
        "producer_entry": {"file": "producer.py"},
        "executor_entry": {"file": "run.py"},
        "recovery": {"replay": "outbox", "retain": "测试保留", "replay_note": "测试重放"},
    })
    return td


def _ready(job_id, task_id="t", priority=None):
    doc = {
        "schema_version": 1, "job_id": job_id, "task_id": task_id,
        "entry_args": [], "env": {}, "idempotency_key": job_id,
    }
    if priority is not None:
        doc["priority"] = priority
    return config.parse_ready(doc)


def _mk_jobs(svc, task_id, n, priority):
    for i in range(n):
        svc.db.upsert_job(_ready(f"{task_id}-j{i}", task_id), f"h-{task_id}-{i}",
                          "2026-10-11T00:00:00+00:00", priority)


def _svc(home):
    (home / "runtime").mkdir(parents=True, exist_ok=True)
    return SchedulerService()


# ---------- #1a 准入公平性：积压规模下同级任务轮转 ----------

def test_admission_candidates_include_all_same_priority_tasks(home):
    """600 作业的同优先级任务不再独占候选窗口：小任务同样出现在候选中。"""
    svc = _svc(home)
    for tid, n in (("task_a", 600), ("task_b", 1), ("task_c", 1)):
        _write_task(home, tid, priority=30)
    svc.refresh_specs()
    _mk_jobs(svc, "task_a", 600, 30)
    _mk_jobs(svc, "task_b", 1, 30)
    _mk_jobs(svc, "task_c", 1, 30)

    seen_tasks = set()
    for _ in range(3):  # 多轮评估，轮转应覆盖全部任务
        cands = svc._candidates()
        seen_tasks |= {c["task_id"] for c in cands}
    assert seen_tasks == {"task_a", "task_b", "task_c"}
    # 单轮内：窗口切片限制大任务贡献条数，小任务与它并列可见
    cands = svc._candidates()
    task_counts = {}
    for c in cands:
        task_counts[c["task_id"]] = task_counts.get(c["task_id"], 0) + 1
    assert task_counts["task_a"] == SchedulerService.ADMIT_HEAD_PER_TASK
    assert task_counts["task_b"] == 1
    assert task_counts["task_c"] == 1
    # 任务内 FIFO：task_a 的头部候选是接收序最早的作业
    a_jobs = [c["job_id"] for c in cands if c["task_id"] == "task_a"]
    assert a_jobs[0] == "task_a-j0"


def test_admission_rotation_order_changes(home):
    """同级轮转：多次评估的起始任务轮换（防单任务长期霸占准入窗口）。"""
    svc = _svc(home)
    for tid in ("ta", "tb", "tc"):
        _write_task(home, tid, priority=30)
    svc.refresh_specs()
    for tid in ("ta", "tb", "tc"):
        _mk_jobs(svc, tid, 2, 30)

    seqs = []
    for _ in range(3):
        cands = svc._candidates()
        seqs.append([c["task_id"] for c in cands])
    # 轮转使三次评估的起始任务互不相同
    assert seqs[0][0] != seqs[1][0] or seqs[1][0] != seqs[2][0]
    # 同优先级候选间保持优先级降序、轮转次序为决胜
    assert all(int(c["priority"]) == 30 for c in cands)


def test_admission_task_priority_groups_desc(home):
    """任务优先级降序：高优先级任务的候选先于低优先级任务。"""
    svc = _svc(home)
    _write_task(home, "t_high", priority=90)
    _write_task(home, "t_low", priority=10)
    svc.refresh_specs()
    _mk_jobs(svc, "t_high", 2, 90)
    _mk_jobs(svc, "t_low", 2, 10)
    cands = svc._candidates()
    assert [c["task_id"] for c in cands[:2]] == ["t_high", "t_high"]
    assert {c["task_id"] for c in cands[2:]} == {"t_low"}


# ---------- #1d 作业级优先级（授权范围内降低）保持全局降序 ----------

def test_admission_lowered_job_priority_keeps_global_order(home):
    """高优先级任务中被降低的作业不得插队到其它任务更高优先级作业之前。"""
    svc = _svc(home)
    _write_task(home, "t_hi", priority=50)    # 任务优先级高
    _write_task(home, "t_lo", priority=30)
    svc.refresh_specs()
    # t_hi 的作业级优先级降到 10（任务授权范围内）；t_lo 的作业保持 30
    svc.db.upsert_job(_ready("hi-low", "t_hi", priority=10), "h1",
                      "2026-10-11T00:00:00+00:00", 10)
    svc.db.upsert_job(_ready("lo-30", "t_lo", priority=30), "h2",
                      "2026-10-11T00:00:00+00:00", 30)
    cands = svc._candidates()
    # 合并后按作业优先级全局降序：lo-30(30) 在 hi-low(10) 之前
    assert [c["job_id"] for c in cands] == ["lo-30", "hi-low"]


# ---------- #1c run_target=0 冷启动兜底可达 ----------

def test_admit_job_cold_start_bump_for_starved_task(home, monkeypatch):
    """run_target=0 且无存活执行的任务：其作业进候选后准入即获 0→1 探测机会。"""
    svc = _svc(home)
    _write_task(home, "t_starved", priority=30)
    svc.refresh_specs()
    _mk_jobs(svc, "t_starved", 5, 30)
    assert svc.scaling.get("t_starved").run_target == 0

    launched = []
    monkeypatch.setattr(svc, "_launch",
                        lambda job, spec, why="": launched.append(job["job_id"]) or True)
    cands = svc._candidates()
    assert cands, "饥饿任务作业必须出现在候选中"
    assert svc._admit_job(cands[0]) is True
    assert launched == [cands[0]["job_id"]]
    # 动态目标持久化为 1（重启不回退），模式进入 steady
    ts = svc.scaling.get("t_starved")
    assert ts.run_target == 1 and ts.mode == "steady"
    assert next(s for s in svc.db.all_task_states()
                if s["task_id"] == "t_starved")["run_target"] == 1


# ---------- #1b 高优先级新需求探测脱离全局窗口 ----------

def test_highest_queued_demand_beyond_window(home):
    """需求探测按任务聚合：600 个低优作业之后的高优需求仍可见。"""
    svc = _svc(home)
    _write_task(home, "t_bulk", priority=30)
    _write_task(home, "t_hi", priority=90)
    _write_task(home, "t_off", priority=95, enabled=False)
    svc.refresh_specs()
    _mk_jobs(svc, "t_bulk", 600, 30)
    _mk_jobs(svc, "t_hi", 1, 90)
    _mk_jobs(svc, "t_off", 1, 95)
    # 最高优先级需求是 t_hi（t_off 已禁用，跳过）
    assert svc._highest_queued_demand() == ("t_hi", 90)


# ---------- #2 接收器增量目录扫描 ----------

def test_receiver_incremental_scan_finds_new_files_and_subdirs(home):
    _write_task(home, "t", priority=50)
    spec = config.load_task("t")
    out = config.outbox_dir("t")
    (out / "batch1").mkdir(parents=True, exist_ok=True)

    def _publish(rel: str, job_id: str):
        from sched_task_sdk.producer import atomic_write_json
        p = out / rel
        atomic_write_json(p, {
            "schema_version": 1, "job_id": job_id, "task_id": "t",
            "entry_args": [], "env": {}, "idempotency_key": job_id,
        })

    db = dbmod.DB()
    now = "2026-10-12T00:00:00+00:00"
    # 首轮：根 + 分目录
    _publish("j1.json", "j1")
    _publish("batch1/j2.json", "j2")
    s1 = receiver.receive_outbox("t", spec, db, now, batch_size=None)
    assert (s1["created"], s1["deferred"]) == (2, 0)
    assert set(receiver._dir_cache["t"]) == {".", "batch1"}

    # 次轮：目录未变化 → 缓存命中路径下结果一致
    s2 = receiver.receive_outbox("t", spec, db, now, batch_size=None)
    assert (s2["created"], s2["existing"]) == (0, 0)

    # 新文件落根目录 / 新子目录 → 全部被发现（增量列举不漏）
    _publish("j3.json", "j3")
    _publish("batch2/b1/j4.json", "j4")
    s3 = receiver.receive_outbox("t", spec, db, now, batch_size=None)
    assert s3["created"] == 2
    assert set(receiver._dir_cache["t"]) == {".", "batch1", "batch2", "batch2/b1"}
    assert len(db.list_jobs("t", limit=1000)) == 4

    # 删除文件（模拟归档后 outbox 缩空）：列举集合收缩、缓存不再返回旧名
    for p in out.rglob("*.json"):
        p.unlink()
    for j in db.list_jobs(task_id="t", limit=10):
        if j["status"] == config.JobStatus.QUEUED:
            db.set_job_status(j["job_id"], config.JobStatus.SUCCEEDED, "t", now)
    s4 = receiver.receive_outbox("t", spec, db, now, batch_size=None)
    assert s4["seen"] == 0
    db.close()


def test_receiver_incremental_scan_batch_defer_still_counts(home):
    """批量时间片语义不变：超出 batch 的文件计入 deferred、下轮继续。"""
    _write_task(home, "t", priority=50)
    spec = config.load_task("t")
    out = config.outbox_dir("t")
    from sched_task_sdk.producer import atomic_write_json
    for i in range(10):
        atomic_write_json(out / f"j{i}.json", {
            "schema_version": 1, "job_id": f"j{i}", "task_id": "t",
            "entry_args": [], "env": {}, "idempotency_key": f"j{i}",
        })
    db = dbmod.DB()
    s1 = receiver.receive_outbox("t", spec, db, "2026-10-12T00:00:00+00:00",
                                 batch_size=4)
    # B3 语义：新文件半数配额（batch//2=2），其余计入 deferred 下轮继续
    assert s1["seen"] == 2 and s1["deferred"] == 8
    s2 = receiver.receive_outbox("t", spec, db, "2026-10-12T00:00:01+00:00",
                                 batch_size=200)
    assert s2["seen"] == 8 and s2["deferred"] == 0
    db.close()


# ---------- #3 数据库时区归一迁移 ----------

def test_tz_migration_normalizes_legacy_local_timestamps(home):
    """历史 +08:00 行一次性转为 UTC；再开库不重复处理（user_version）。"""
    db_path = home / "runtime" / "exec.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = dbmod.DB()
    now = "2026-10-11T13:41:11+00:00"
    db.upsert_job(_ready("j-utc", "t"), "h1", now, 50)
    db.upsert_job(_ready("j-local", "t"), "h2", now, 50)
    db.close()

    # 模拟旧版写入的本地时间行（queue 后被置为本地格式 + 落终态/退避时间）
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA user_version=0")  # 回滚迁移标记
    conn.execute(
        "UPDATE jobs SET queued_at='2026-10-11T17:44:46+08:00', "
        "next_retry_at='2026-10-11T19:00:00+08:00' WHERE job_id='j-local'")
    conn.execute("UPDATE jobs SET finished_at='2026-10-11T18:00:00+08:00' "
                 "WHERE job_id='j-utc'")
    conn.commit()
    conn.close()

    db = dbmod.DB()  # 迁移触发
    row = db.get_job("j-local")
    # 17:44:46+08:00 == 09:44:46+00:00
    assert row["queued_at"] == "2026-10-11T09:44:46+00:00"
    assert row["next_retry_at"] == "2026-10-11T11:00:00+00:00"
    row = db.get_job("j-utc")
    assert row["finished_at"] == "2026-10-11T10:00:00+00:00"
    assert int(db._conn.execute("PRAGMA user_version").fetchone()[0]) >= 1

    # 字符串比较恢复正确：j-local(09:44) 早于 j-utc(13:41)
    rows = db._conn.execute(
        "SELECT job_id FROM jobs ORDER BY queued_at").fetchall()
    assert [r[0] for r in rows] == ["j-local", "j-utc"]
    db.close()

    # 再开库幂等：不改动、不报错
    db = dbmod.DB()
    assert db.get_job("j-local")["queued_at"] == "2026-10-11T09:44:46+00:00"
    db.close()


# ---------- #4 保护窗口计入低优先级任务数 ----------

def test_protect_window_scales_with_lower_task_count(home):
    from sched.scaling import Protect
    cfg = config.SchedulerConfig()
    p = Protect(cfg)
    base = cfg.control.protect_deadline_sec
    fi = 50.0
    w_plain = p._round_window(fi, lower_tasks=0)
    w_many = p._round_window(fi, lower_tasks=5)
    assert w_plain == max(base, fi * 4)
    assert w_many == w_plain + 5 * cfg.control.yield_observe_sec
    # 上限封顶：超过 20 个不再线性加长
    w_huge = p._round_window(fi, lower_tasks=50)
    assert w_huge == w_plain + 20 * cfg.control.yield_observe_sec

    # start_round 透传：窗口随 lower_tasks 增大
    t0 = 1000.0
    assert p.start_round("A", 90, t0, cause="shortage",
                         feedback_interval_sec=fi, lower_tasks=3)
    assert p.st.deadline_mono - t0 == w_plain + 3 * cfg.control.yield_observe_sec
