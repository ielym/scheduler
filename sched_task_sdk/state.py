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
    """任务工作目录下的状态原子读写。工作目录来自 SCHED_WORKSPACE。

    布局（F4 多实例隔离）：
    - 实例级心跳/错误状态：cache/runs/<SCHED_RUN_ID>/state.json（并发实例互不干扰）
    - 任务级断点 resume_point：cache/resume_point.json（跨运行共享，供恢复续跑）
    无 SCHED_RUN_ID（本地调试）时实例级状态落在 cache/runs/local/state.json。
    """

    def __init__(self, workspace: Path | None = None):
        ws = Path(workspace or Path(os.environ.get("SCHED_WORKSPACE") or os.getcwd()))
        run_id = os.environ.get("SCHED_RUN_ID") or "local"
        self.path = ws / "cache" / "runs" / run_id / "state.json"
        self.resume_path = ws / "cache" / "resume_point.json"
        self.legacy_path = ws / "cache" / "state.json"

    def load(self) -> dict:
        for p in (self.path, self.legacy_path):
            if p.exists():
                try:
                    return json.loads(p.read_text("utf-8"))
                except (json.JSONDecodeError, OSError, UnicodeDecodeError):
                    continue
        return {}

    def load_resume_point(self) -> dict:
        """任务级断点：优先 cache/resume_point.json，回落 legacy state.json 的 resume_point。"""
        if self.resume_path.exists():
            try:
                obj = json.loads(self.resume_path.read_text("utf-8"))
                if isinstance(obj, dict):
                    return obj.get("resume_point") or {}
            except (json.JSONDecodeError, OSError, UnicodeDecodeError):
                pass
        legacy = self.load()
        rp = legacy.get("resume_point")
        return rp if isinstance(rp, dict) else {}

    def save(self, obj: dict) -> None:
        self._atomic_write(self.path, obj)

    def save_resume_point(self, resume_point: dict) -> None:
        self._atomic_write(self.resume_path, {"resume_point": resume_point})

    @staticmethod
    def _atomic_write(path: Path, obj: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        lock = path.with_name(path.name + ".lock")
        fd = os.open(lock, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            f, tmp = tempfile.mkstemp(dir=str(path.parent), prefix="state.", suffix=".tmp")
            with os.fdopen(f, "w", encoding="utf-8") as fh:
                json.dump(obj, fh, ensure_ascii=False, indent=2)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
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
        # resume_point 是任务级断点（跨运行共享），从独立文件读取（F4）
        self.resume_point: dict[str, Any] = self.store.load_resume_point()
        self.metrics: dict[str, Any] = raw.get("metrics") or {}
        self.last_error: dict[str, Any] = raw.get("last_error") or {}
        # 重试策略运行时覆盖（调度器在失败终态读取，合并 task.json 的 retry 静态声明）：
        # {"delay_sec": 60, "max_attempts": 0, "not_after": "2026-10-04T15:00:00+08:00"}
        self.retry_hint: dict[str, Any] = raw.get("retry_hint") or {}
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
            self.store.save_resume_point(self.resume_point)

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
            "retry_hint": self.retry_hint,
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