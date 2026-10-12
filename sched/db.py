"""SQLite 执行库：jobs / attempts / events / checkpoints / task_state。

单机单实例，WAL 模式 + busy_timeout。数据库与锁放本机文件系统（config.runtime_dir()）。
所有写走统一连接 + RLock；事件按 (attempt_id, seq) 去重。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

from . import config
from .config import JobStatus

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    content_hash TEXT NOT NULL,
    env_json TEXT NOT NULL DEFAULT '{}',
    entry_args_json TEXT NOT NULL DEFAULT '[]',
    workdir TEXT NOT NULL DEFAULT '.',
    input_refs_json TEXT NOT NULL DEFAULT '[]',
    idempotency_key TEXT NOT NULL DEFAULT '',
    progress_semver TEXT NOT NULL DEFAULT '1.0.0',
    status TEXT NOT NULL DEFAULT 'QUEUED',
    reason TEXT NOT NULL DEFAULT '',
    queued_at TEXT NOT NULL,
    started_at TEXT NOT NULL DEFAULT '',
    finished_at TEXT NOT NULL DEFAULT '',
    receive_seq INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT NOT NULL DEFAULT '',
    status_seq INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, priority DESC, receive_seq ASC);
CREATE INDEX IF NOT EXISTS idx_jobs_task_status_pri_seq ON jobs(task_id, status, priority DESC, receive_seq ASC);
DROP INDEX IF EXISTS idx_jobs_task_pri_status;

CREATE TABLE IF NOT EXISTS attempts (
    attempt_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    seq INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    token TEXT NOT NULL,
    pid INTEGER,
    pgid INTEGER,
    start_monotonic REAL,
    cgroup TEXT NOT NULL DEFAULT '',
    started_at TEXT NOT NULL DEFAULT '',
    finished_at TEXT NOT NULL DEFAULT '',
    exit_code INTEGER,
    reason TEXT NOT NULL DEFAULT '',
    pid_start_time REAL
);
CREATE INDEX IF NOT EXISTS idx_attempts_job ON attempts(job_id, seq DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_attempts_one_live ON attempts(job_id)
    WHERE status IN ('STARTING','RUNNING','PAUSING');

CREATE TABLE IF NOT EXISTS events (
    attempt_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    at TEXT NOT NULL,
    PRIMARY KEY (attempt_id, seq)
);

CREATE TABLE IF NOT EXISTS checkpoints (
    attempt_id TEXT NOT NULL,
    cp_seq INTEGER NOT NULL,
    ref TEXT NOT NULL DEFAULT '',
    at TEXT NOT NULL,
    PRIMARY KEY (attempt_id, cp_seq)
);

CREATE TABLE IF NOT EXISTS task_state (
    task_id TEXT PRIMARY KEY,
    run_target INTEGER NOT NULL DEFAULT 0,
    mode TEXT NOT NULL DEFAULT 'explore'
);
"""


class DB:
    def __init__(self, path: Path | None = None):
        self.path = path or config.db_file()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(self.path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._migrate()

    def _migrate(self) -> None:
        """旧库平滑加列（幂等）。"""
        job_cols = {r[1] for r in self._conn.execute("PRAGMA table_info(jobs)")}
        if "retry_count" not in job_cols:
            self._conn.execute(
                "ALTER TABLE jobs ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0")
        if "next_retry_at" not in job_cols:
            self._conn.execute(
                "ALTER TABLE jobs ADD COLUMN next_retry_at TEXT NOT NULL DEFAULT ''")
        if "status_seq" not in job_cols:
            # 状态变更事件序号（设计第十二节：每次状态变更带事件序号）
            self._conn.execute(
                "ALTER TABLE jobs ADD COLUMN status_seq INTEGER NOT NULL DEFAULT 0")
        att_cols = {r[1] for r in self._conn.execute("PRAGMA table_info(attempts)")}
        if "pid_start_time" not in att_cols:
            self._conn.execute(
                "ALTER TABLE attempts ADD COLUMN pid_start_time REAL")
        # 一次性时区归一（设计第十四节：持久化时间记录 UTC）。
        # 历史行存在 +08:00 本地格式（旧版写入），归档对账/完结清理的字符串
        # 比较仅在统一 UTC 格式下成立。user_version=1 保证只执行一次。
        ver = int(self._conn.execute("PRAGMA user_version").fetchone()[0])
        if ver < 1:
            self._normalize_timestamps()
            self._conn.execute("PRAGMA user_version=1")
        # 内存计数器从库内最大值续接（重启不回退、不重扫全表）
        self._status_seq = int(self._conn.execute(
            "SELECT COALESCE(MAX(status_seq),0) FROM jobs").fetchone()[0])
        self._receive_seq = int(self._conn.execute(
            "SELECT COALESCE(MAX(receive_seq),0) FROM jobs").fetchone()[0])

    def _normalize_timestamps(self) -> None:
        """把历史非 UTC 后缀的时间戳统一转为 +00:00 UTC（一次性）。"""
        from datetime import datetime, timezone

        def _fix(v: str) -> str | None:
            try:
                dt = datetime.fromisoformat(v)
            except (ValueError, TypeError):
                return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)  # 无 tz 的按 UTC 处理
            return dt.astimezone(timezone.utc).isoformat(timespec="seconds")

        for table, key in (("jobs", "job_id"), ("attempts", "attempt_id")):
            cols = ("queued_at", "started_at", "finished_at", "next_retry_at") \
                if table == "jobs" else ("started_at", "finished_at")
            for col in cols:
                rows = self._conn.execute(
                    f"SELECT {key}, {col} FROM {table} "
                    f"WHERE {col} != '' AND {col} NOT LIKE '%+00:00'"
                ).fetchall()
                for r in rows:
                    fixed = _fix(r[1])
                    if fixed is not None and fixed != r[1]:
                        self._conn.execute(
                            f"UPDATE {table} SET {col}=? WHERE {key}=?",
                            (fixed, r[0]))

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---------- jobs ----------

    def upsert_job(self, ready: "config.ReadyJob", content_hash: str,
                   now: str, priority: int) -> tuple[str, bool]:
        """接收作业：相同 job_id+相同内容视为已存在（返回 existing）；内容不同由调用方隔离。

        返回 (status, created)。status ∈ {existing, created, conflict}。
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT content_hash FROM jobs WHERE job_id=?", (ready.job_id,)
            ).fetchone()
            if row is not None:
                if row["content_hash"] == content_hash:
                    return "existing", False
                return "conflict", False
            self._conn.execute(
                """INSERT INTO jobs
                   (job_id, task_id, priority, content_hash, env_json, entry_args_json,
                    workdir, input_refs_json, idempotency_key, progress_semver,
                    status, queued_at, receive_seq)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (ready.job_id, ready.task_id, priority, content_hash,
                 json.dumps(ready.env, ensure_ascii=False),
                 json.dumps(ready.entry_args, ensure_ascii=False),
                 ready.workdir,
                 json.dumps(ready.input_refs, ensure_ascii=False),
                 ready.idempotency_key, ready.progress_semver,
                 JobStatus.QUEUED, now,
                 self._next_receive_seq()),
            )
        return "created", True

    def _next_receive_seq(self) -> int:
        """内存计数器递增（启动时从库内最大值续接，不再每条全表 MAX 扫描）。"""
        self._receive_seq += 1
        return self._receive_seq

    def get_job(self, job_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        return dict(row) if row else None

    def _next_status_seq(self) -> int:
        """作业状态变更事件序号：单调递增（设计第十二节：状态变更带事件序号）。"""
        self._status_seq += 1
        return self._status_seq

    def set_job_status(self, job_id: str, status: str, reason: str = "",
                       ts: str = "") -> None:
        seq = self._next_status_seq()
        with self._lock:
            if status in (JobStatus.STARTING, JobStatus.RUNNING) and ts:
                self._conn.execute(
                    "UPDATE jobs SET status=?, reason=?, started_at=?, status_seq=? "
                    "WHERE job_id=?",
                    (status, reason, ts, seq, job_id))
            elif status in JobStatus.TERMINAL | JobStatus.RESUMPTABLE and ts:
                self._conn.execute(
                    "UPDATE jobs SET status=?, reason=?, finished_at=?, status_seq=? "
                    "WHERE job_id=?",
                    (status, reason, ts, seq, job_id))
            else:
                self._conn.execute(
                    "UPDATE jobs SET status=?, reason=?, status_seq=? WHERE job_id=?",
                    (status, reason, seq, job_id))

    def queued_jobs(self, limit: int | None = None) -> list[dict]:
        with self._lock:
            sql = ("SELECT * FROM jobs WHERE status=? "
                   "ORDER BY priority DESC, receive_seq ASC")
            if limit is not None:
                sql += f" LIMIT {int(limit)}"
            rows = self._conn.execute(sql, (JobStatus.QUEUED,)).fetchall()
        return [dict(r) for r in rows]

    def queued_head_jobs(self, task_id: str, limit: int = 8) -> list[dict]:
        """任务内 FIFO 头部候选（作业优先级降序、接收顺序升序，索引直取）。

        审查问题一修复：准入改为按任务取数——每任务最多取 limit 条头部，
        任何同优先级任务每轮都可见，不再被全局 receive_seq 窗口遮蔽
        （设计第十节：同级任务轮转；设计第五节：海量旧文件不遮蔽新任务）。
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs WHERE task_id=? AND status=? "
                "ORDER BY priority DESC, receive_seq ASC LIMIT ?",
                (task_id, JobStatus.QUEUED, int(limit))).fetchall()
        return [dict(r) for r in rows]

    def queued_task_priorities(self, limit: int = 64) -> list[tuple[str, int]]:
        """按任务聚合的排队需求：(task_id, 该任务最高作业优先级)，按优先级降序。

        审查问题一连带修复：高优先级新需求探测不再受全局窗口限制
        （设计第十节：高优先级新的执行请求到达……优先给高优先级）。
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT task_id, MAX(priority) p FROM jobs WHERE status=? "
                "GROUP BY task_id ORDER BY p DESC LIMIT ?",
                (JobStatus.QUEUED, int(limit))).fetchall()
        return [(r["task_id"], int(r["p"])) for r in rows]

    def jobs_in_status(self, status: str, task_id: str | None = None,
                       limit: int = 1000, offset: int = 0) -> list[dict]:
        """按状态取作业（优先级降序、接收顺序升序，带 LIMIT 防大库全表扫描），
        供恢复闭环使用；task_id 非空时仅取该任务的作业。

        offset：滚动游标起点——调用方轮转 offset 可让海量同状态作业都被
        覆盖到，避免固定窗口把排在窗口外的作业永久饿死（设计第五节）。
        """
        with self._lock:
            sql = "SELECT * FROM jobs WHERE status=?"
            params: list = [status]
            if task_id:
                sql += " AND task_id=?"
                params.append(task_id)
            sql += " ORDER BY priority DESC, receive_seq ASC LIMIT ? OFFSET ?"
            params.extend([int(limit), max(0, int(offset))])
            rows = self._conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def set_job_backoff(self, job_id: str, retry_count: int, next_retry_at: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET retry_count=?, next_retry_at=? WHERE job_id=?",
                (retry_count, next_retry_at, job_id))

    def reset_job_retry(self, job_id: str) -> None:
        """复位不足重试计数（设计第十四节：恢复后逐步减少等待）——
        作业成功或本轮已有真实业务进展时调用，使下次退避从起点重新开始。"""
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET retry_count=0, next_retry_at='' WHERE job_id=?",
                (job_id,))

    def has_event_kind(self, attempt_id: str, kind: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM events WHERE attempt_id=? AND kind=? LIMIT 1",
                (attempt_id, kind)).fetchone()
        return row is not None

    def status_counts(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) n FROM jobs GROUP BY status").fetchall()
        return {r["status"]: int(r["n"]) for r in rows}

    def all_task_states(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM task_state").fetchall()
        return [dict(r) for r in rows]

    def commit_transition(self, attempt_id: str, attempt_fields: dict,
                          job_id: str, job_status: str, reason: str = "",
                          ts: str = "") -> None:
        """作业状态 + attempt 状态在同一事务中提交（设计文档第十二节）。"""
        with self._lock:
            seq = self._next_status_seq()
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if attempt_fields:
                    cols = ", ".join(f"{k}=?" for k in attempt_fields)
                    vals = list(attempt_fields.values()) + [attempt_id]
                    self._conn.execute(
                        f"UPDATE attempts SET {cols} WHERE attempt_id=?", vals)
                if job_status in (JobStatus.STARTING, JobStatus.RUNNING) and ts:
                    self._conn.execute(
                        "UPDATE jobs SET status=?, reason=?, started_at=?, status_seq=? "
                        "WHERE job_id=?",
                        (job_status, reason, ts, seq, job_id))
                elif job_status in JobStatus.TERMINAL | JobStatus.RESUMPTABLE and ts:
                    self._conn.execute(
                        "UPDATE jobs SET status=?, reason=?, finished_at=?, status_seq=? "
                        "WHERE job_id=?",
                        (job_status, reason, ts, seq, job_id))
                else:
                    self._conn.execute(
                        "UPDATE jobs SET status=?, reason=?, status_seq=? WHERE job_id=?",
                        (job_status, reason, seq, job_id))
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def min_queued_priority(self) -> int | None:
        """排队作业中的最低优先级（A2 保护门槛：报不足对象须严格高于它，
        否则无退让空间，同优先级互相报不足会互冻结）。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT MIN(priority) p FROM jobs WHERE status=?",
                (JobStatus.QUEUED,)).fetchone()
        v = row["p"] if row else None
        return None if v is None else int(v)

    def jobs_received_since(self, cutoff_iso: str) -> list[tuple[str, str]]:
        """cutoff 之后接收（queued_at >= cutoff）的作业 (task_id, job_id)。

        C1 归档窗口对账用：窗口内作业逐一核对归档文件存在；旧归档已被 GC
        清理属正常，不在对账范围。
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT task_id, job_id FROM jobs WHERE queued_at >= ?",
                (cutoff_iso,)).fetchall()
        return [(r["task_id"], r["job_id"]) for r in rows]

    def count_queued(self, task_id: str | None = None) -> int:
        with self._lock:
            if task_id:
                row = self._conn.execute(
                    "SELECT COUNT(*) n FROM jobs WHERE status=? AND task_id=?",
                    (JobStatus.QUEUED, task_id)).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT COUNT(*) n FROM jobs WHERE status=?",
                    (JobStatus.QUEUED,)).fetchone()
        return int(row["n"])

    def count_jobs(self, task_id: str) -> int:
        """该任务全部状态的作业总数（接收对账用：应与 archive 归档文件数一致）。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) n FROM jobs WHERE task_id=?", (task_id,)).fetchone()
        return int(row["n"])

    def live_jobs_for_job(self, job_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM attempts WHERE job_id=? AND status IN ('STARTING','RUNNING','PAUSING')",
                (job_id,)).fetchone()
        return row is not None

    # ---------- attempts ----------

    def next_attempt_seq(self, job_id: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(MAX(seq),0)+1 AS n FROM attempts WHERE job_id=?",
                (job_id,)).fetchone()
        return int(row["n"])

    def create_attempt(self, attempt_id: str, job_id: str, seq: int,
                       token: str, now: str, cgroup: str = "") -> None:
        with self._lock:
            self._conn.execute(
                """INSERT INTO attempts
                   (attempt_id, job_id, seq, status, token, cgroup, started_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (attempt_id, job_id, seq, JobStatus.STARTING, token, cgroup, now))

    def update_attempt(self, attempt_id: str, **fields) -> None:
        if not fields:
            return
        cols = ", ".join(f"{k}=?" for k in fields)
        vals = list(fields.values()) + [attempt_id]
        with self._lock:
            self._conn.execute(
                f"UPDATE attempts SET {cols} WHERE attempt_id=?", vals)

    def get_attempt(self, attempt_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
        return dict(row) if row else None

    def live_attempts(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM attempts WHERE status IN ('STARTING','RUNNING','PAUSING')").fetchall()
        return [dict(r) for r in rows]

    def open_attempts_for_job(self, job_id: str) -> list[dict]:
        """作业的所有非终态 attempt（含 RECOVERING/RECOVERY_BLOCKED/PAUSED/BACKOFF）。

        live_attempts 只覆盖内存句柄对应的存活状态；重排（requeue）还需关闭
        身份未知/待核实的遗留 attempt（设计第十二节）。
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM attempts WHERE job_id=? AND status NOT IN "
                "('SUCCEEDED','FAILED','CANCELLED')",
                (job_id,)).fetchall()
        return [dict(r) for r in rows]

    # ---------- events / checkpoints ----------

    def append_event(self, attempt_id: str, seq: int, kind: str,
                     payload: dict, at: str) -> bool:
        """按 (attempt_id, seq) 去重；已存在返回 False。"""
        with self._lock:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO events (attempt_id, seq, kind, payload, at) VALUES (?,?,?,?,?)",
                (attempt_id, seq, kind, json.dumps(payload, ensure_ascii=False), at))
        return cur.rowcount > 0

    def record_checkpoint(self, attempt_id: str, cp_seq: int, ref: str, at: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO checkpoints (attempt_id, cp_seq, ref, at) VALUES (?,?,?,?)",
                (attempt_id, cp_seq, ref, at))

    def list_jobs(self, task_id: str | None = None, status: str | None = None,
                  limit: int = 100) -> list[dict]:
        with self._lock:
            if task_id and status:
                rows = self._conn.execute(
                    "SELECT * FROM jobs WHERE task_id=? AND status=? ORDER BY receive_seq DESC LIMIT ?",
                    (task_id, status, limit)).fetchall()
            elif task_id:
                rows = self._conn.execute(
                    "SELECT * FROM jobs WHERE task_id=? ORDER BY receive_seq DESC LIMIT ?",
                    (task_id, limit)).fetchall()
            elif status:
                rows = self._conn.execute(
                    "SELECT * FROM jobs WHERE status=? ORDER BY receive_seq DESC LIMIT ?",
                    (status, limit)).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM jobs ORDER BY receive_seq DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def list_attempts(self, job_id: str | None = None, limit: int = 100) -> list[dict]:
        with self._lock:
            if job_id:
                rows = self._conn.execute(
                    "SELECT * FROM attempts WHERE job_id=? ORDER BY seq DESC LIMIT ?",
                    (job_id, limit)).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM attempts ORDER BY started_at DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    # ---------- 周期清理（B13：已完成作业的 attempt 工件随时间清理） ----------

    def stale_terminal_attempts(self, before_iso: str, limit: int = 100) -> list[str]:
        """取「已完成作业（SUCCEEDED/CANCELLED）且作业结束时间早于阈值」的 attempt_id。

        FAILED 作业的 attempt 工件保留——执行诊断需要 events/checkpoints 证据；
        仅清理已确认完结的作业（设计第十三节：清理已确认完成的执行记录）。
        """
        with self._lock:
            rows = self._conn.execute(
                """SELECT a.attempt_id FROM attempts a
                   JOIN jobs j ON a.job_id = j.job_id
                   WHERE j.status IN (?,?) AND j.finished_at != '' AND j.finished_at < ?
                   ORDER BY a.started_at LIMIT ?""",
                (JobStatus.SUCCEEDED, JobStatus.CANCELLED,
                 before_iso, int(limit))).fetchall()
        return [r["attempt_id"] for r in rows]

    def delete_attempt_artifacts(self, attempt_id: str) -> None:
        """同一事务内删除指定 attempt 的 events / checkpoints / attempts 行。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute("DELETE FROM events WHERE attempt_id=?",
                                   (attempt_id,))
                self._conn.execute("DELETE FROM checkpoints WHERE attempt_id=?",
                                   (attempt_id,))
                self._conn.execute("DELETE FROM attempts WHERE attempt_id=?",
                                   (attempt_id,))
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    # ---------- task_state ----------

    def get_task_state(self, task_id: str) -> dict:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM task_state WHERE task_id=?", (task_id,)).fetchone()
        if row:
            return dict(row)
        return {"task_id": task_id, "run_target": 0, "mode": "explore"}

    def set_task_state(self, task_id: str, **fields) -> None:
        with self._lock:
            existing = self._conn.execute(
                "SELECT 1 FROM task_state WHERE task_id=?", (task_id,)).fetchone()
        if existing:
            cols = ", ".join(f"{k}=?" for k in fields)
            vals = list(fields.values()) + [task_id]
            with self._lock:
                self._conn.execute(
                    f"UPDATE task_state SET {cols} WHERE task_id=?", vals)
        else:
            base = {"task_id": task_id}
            base.update(fields)
            cols = ", ".join(base.keys())
            marks = ", ".join("?" for _ in base)
            with self._lock:
                self._conn.execute(
                    f"INSERT INTO task_state ({cols}) VALUES ({marks})",
                    list(base.values()))