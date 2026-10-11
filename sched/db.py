"""SQLite 执行库：jobs / attempts / events / checkpoints / receipts / task_state。

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
    receive_seq INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_jobs_task_pri_status ON jobs(task_id, priority, status);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, priority DESC, receive_seq ASC);

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
    reason TEXT NOT NULL DEFAULT ''
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

CREATE TABLE IF NOT EXISTS receipts (
    job_id TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    received_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS task_state (
    task_id TEXT PRIMARY KEY,
    priority INTEGER NOT NULL DEFAULT 0,
    enabled INTEGER NOT NULL DEFAULT 1,
    run_target INTEGER NOT NULL DEFAULT 0,
    mode TEXT NOT NULL DEFAULT 'explore',
    last_grow_at TEXT NOT NULL DEFAULT '',
    last_shrink_at TEXT NOT NULL DEFAULT '',
    backoff_until TEXT NOT NULL DEFAULT '',
    protect_role TEXT NOT NULL DEFAULT '',
    yield_until TEXT NOT NULL DEFAULT '',
    recover_at TEXT NOT NULL DEFAULT ''
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
        row = self._conn.execute(
            "SELECT COALESCE(MAX(receive_seq),0)+1 AS n FROM jobs").fetchone()
        return int(row["n"])

    def get_job(self, job_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        return dict(row) if row else None

    def set_job_status(self, job_id: str, status: str, reason: str = "",
                       ts: str = "") -> None:
        with self._lock:
            if status in (JobStatus.STARTING, JobStatus.RUNNING) and ts:
                self._conn.execute(
                    "UPDATE jobs SET status=?, reason=?, started_at=? WHERE job_id=?",
                    (status, reason, ts, job_id))
            elif status in JobStatus.TERMINAL | JobStatus.RESUMPTABLE and ts:
                self._conn.execute(
                    "UPDATE jobs SET status=?, reason=?, finished_at=? WHERE job_id=?",
                    (status, reason, ts, job_id))
            else:
                self._conn.execute(
                    "UPDATE jobs SET status=?, reason=? WHERE job_id=?",
                    (status, reason, job_id))

    def queued_jobs(self, exclude_tasks: set[str] | None = None) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs WHERE status=? ORDER BY priority DESC, receive_seq ASC",
                (JobStatus.QUEUED,)).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            if exclude_tasks and d["task_id"] in exclude_tasks:
                continue
            out.append(d)
        return out

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

    # ---------- events / checkpoints / receipts ----------

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

    def write_receipt(self, job_id: str, content_hash: str, now: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO receipts (job_id, content_hash, received_at) VALUES (?,?,?)",
                (job_id, content_hash, now))

    # ---------- task_state ----------

    def get_task_state(self, task_id: str) -> dict:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM task_state WHERE task_id=?", (task_id,)).fetchone()
        if row:
            return dict(row)
        return {"task_id": task_id, "priority": 0, "enabled": 1, "run_target": 0,
                "mode": "explore", "last_grow_at": "", "last_shrink_at": "",
                "backoff_until": "", "protect_role": "", "yield_until": "", "recover_at": ""}

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

    # ---------- 恢复到非存活状态（调度器重启） ----------

    def recover_inflight(self, now: str) -> list[dict]:
        """重启恢复：把 STARTING/RUNNING/PAUSING 的 attempt 对应 job 置为 RECOVERING，
        返回受影响 job。存活 worker 可经事件协议接管，已退出者从完成记录/检查点恢复。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT job_id FROM attempts WHERE status IN ('STARTING','RUNNING','PAUSING')"
            ).fetchall()
            out = []
            for r in rows:
                self._conn.execute(
                    "UPDATE jobs SET status=?, reason=? WHERE job_id=?",
                    (JobStatus.RECOVERING, "调度器重启，恢复核实中", r["job_id"]))
                out.append(r["job_id"])
        return out