"""运行历史（data/runs/*.jsonl）与运行日志文件读写（append-only）。

状态机含抢占/资源两类非终态；仅终态落盘 data/runs/*.jsonl。
"""

from __future__ import annotations

import json
import secrets
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

from . import config, store

STATUS_WAITING = "waiting"
STATUS_RUNNING = "running"
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"
STATUS_RESOURCE = "resource"
STATUS_PREEMPTED = "preempted"
STATUS_TIMEOUT = "timeout"
STATUS_KILLED = "killed"
STATUS_INTERRUPTED = "interrupted"

ACTIVE_STATUSES = (STATUS_WAITING, STATUS_RUNNING)
TERMINAL_STATUSES = (
    STATUS_SUCCESS,
    STATUS_FAILED,
    STATUS_RESOURCE,
    STATUS_PREEMPTED,
    STATUS_TIMEOUT,
    STATUS_KILLED,
    STATUS_INTERRUPTED,
)
RESUMABLE_STATUSES = (STATUS_PREEMPTED, STATUS_RESOURCE, STATUS_TIMEOUT, STATUS_INTERRUPTED)


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _iso_seconds_between(a: str, b: str) -> float:
    try:
        da = datetime.fromisoformat(str(a))
        db = datetime.fromisoformat(str(b))
    except (ValueError, TypeError):
        return 0.0
    return max(0.0, (db - da).total_seconds())


def new_run_id() -> str:
    return f"{datetime.now().strftime('%Y%m%dT%H%M%S')}-{secrets.token_hex(4)}"


def run_log_path(subtask_id: str) -> Path:
    return config.runs_dir() / f"{subtask_id}.jsonl"


def task_log_dir(subtask_id: str) -> Path:
    return config.runs_dir() / subtask_id


def stdout_path(subtask_id: str, run_id: str) -> Path:
    return task_log_dir(subtask_id) / f"{run_id}.stdout.log"


def stderr_path(subtask_id: str, run_id: str) -> Path:
    return task_log_dir(subtask_id) / f"{run_id}.stderr.log"


def rel_to_runs(path: Path) -> str:
    try:
        return str(path.relative_to(config.data_dir()))
    except ValueError:
        return str(path)


def append_run(record: dict[str, Any]) -> dict[str, Any]:
    subtask_id = str(record.get("subtask_id") or "unknown")
    store.append_jsonl(run_log_path(subtask_id), record)
    return record


def remove_run(subtask_id: str, run_id: str) -> int:
    """从 data/runs/<subtask>.jsonl 中移除指定 run 记录（原子重写，与 append 串行）。

    返回移除条数。用于「删除进程」时让该历史 run 真正从列表消失。
    """
    return store.remove_jsonl(run_log_path(subtask_id), lambda rec: rec.get("run_id") == run_id)


def iter_all_runs() -> Iterator[dict]:
    d = config.runs_dir()
    if not d.exists():
        return
    files = sorted(
        (p for p in d.glob("*.jsonl") if p.is_file()),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for f in files:
        for rec in store.iter_jsonl(f):
            yield rec


def query_runs(
    subtask_id: str | None = None,
    status: str | None = None,
    statuses: tuple[str, ...] | None = None,
    since: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    records: list[dict] = []
    source = store.iter_jsonl(run_log_path(subtask_id)) if subtask_id else iter_all_runs()
    for rec in source:
        if status and rec.get("status") != status:
            continue
        if statuses and rec.get("status") not in statuses:
            continue
        if since:
            ts = rec.get("scheduled_at") or rec.get("queued_at") or ""
            if ts and ts < since:
                continue
        records.append(rec)

    records.sort(key=lambda r: r.get("scheduled_at") or r.get("queued_at") or "", reverse=True)
    return records[offset : offset + limit] if limit else records[offset:]


def find_run(run_id: str) -> dict | None:
    for rec in iter_all_runs():
        if rec.get("run_id") == run_id:
            return rec
    return None


def read_log(path: Path, tail: int = 0, offset: int = 0, max_bytes: int = 512 * 1024) -> dict:
    if not path.exists():
        return {"content": "", "size": 0, "offset": 0, "eof": True}
    size = path.stat().st_size
    with open(path, "rb") as f:
        if tail:
            start = max(0, size - max_bytes)
            f.seek(start)
            data = f.read()
            text = data.decode("utf-8", errors="replace")
            lines = text.splitlines()
            if start > 0:
                lines = lines[1:]
            out = "\n".join(lines[-tail:])
            return {"content": out, "size": size, "offset": size, "eof": True}
        start = max(0, offset)
        if start > size:
            start = size
        f.seek(start)
        data = f.read(max_bytes)
    return {
        "content": data.decode("utf-8", errors="replace"),
        "size": size,
        "offset": start + len(data),
        "eof": start + len(data) >= size,
    }
