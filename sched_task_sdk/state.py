"""状态持久化：任务侧 cache/state.json 的读写（自我原子写，不依赖调度器）。"""

from __future__ import annotations

import fcntl
import json
import os
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


class StateStore:
    """任务工作目录下 cache/state.json 的原子读写。工作目录来自 SCHED_WORKSPACE。"""

    def __init__(self, workspace: Path | None = None):
        ws = workspace or Path(os.environ.get("SCHED_WORKSPACE") or os.getcwd())
        self.path = Path(ws) / "cache" / "state.json"

    def load(self) -> dict:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text("utf-8"))
        except (json.JSONDecodeError, OSError, UnicodeDecodeError):
            return {}

    def save(self, obj: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lock = self.path.with_name(self.path.name + ".lock")
        fd = os.open(lock, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            f, tmp = tempfile.mkstemp(dir=str(self.path.parent), prefix="state.", suffix=".tmp")
            with os.fdopen(f, "w", encoding="utf-8") as fh:
                json.dump(obj, fh, ensure_ascii=False, indent=2)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)


class TaskState:
    """跑在任务进程内的状态快照。mtime 即心跳（updated_at 每次 flush 刷新）。"""

    def __init__(self, workspace: Path | None = None):
        self.store = StateStore(workspace)
        raw = self.store.load()
        self.schema_version = 2
        self.status: str = "running"
        self.current_step: str = raw.get("current_step", "")
        self.next_action: str = raw.get("next_action", "")
        self.progress: int = int(raw.get("progress") or 0)
        self.loop_count: int = int(raw.get("loop_count") or 0)
        self.resume_point: dict[str, Any] = raw.get("resume_point") or {}
        self.metrics: dict[str, Any] = raw.get("metrics") or {}
        self.last_error: dict[str, Any] = raw.get("last_error") or {}
        self.started_at: str = raw.get("started_at") or now_iso()
        self.updated_at: str = now_iso()
        self._lock = threading.Lock()

    def touch(self) -> None:
        self.updated_at = now_iso()

    def flush(self) -> None:
        # 构建快照与落盘整体加锁，避免心跳线程与主线程交错导致旧值覆盖新断点。
        with self._lock:
            self.touch()
            self.store.save(self.as_dict())

    def as_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "current_step": self.current_step,
            "next_action": self.next_action,
            "progress": self.progress,
            "loop_count": self.loop_count,
            "resume_point": self.resume_point,
            "metrics": self.metrics,
            "last_error": self.last_error,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
        }

    def mark_success(self) -> None:
        self.status = "success"
        self.progress = 100
        self.resume_point = {}

    def mark_preempted(self) -> None:
        self.status = "preempted"

    def mark_failed(self, category: str, message: str) -> None:
        self.status = "failed"
        self.last_error = {"category": category, "message": message}