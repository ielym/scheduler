"""执行事件通道写侧（worker 用）。

事件以 JSON 行追加到 SCHED_EVENT_FILE，字段：
  {"seq": N, "kind": "...", "at": ISO, "...payload 平铺..."}

seq 单调递增，由本进程维护；调度器按 (attempt_id, seq) 去重。
kind ∈ {started, heartbeat, progress, external_shortage, control_ack,
        checkpoint, completed}（见 errors.EventKind）。
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from .errors import EventKind


def event_file() -> Path:
    v = os.environ.get("SCHED_EVENT_FILE")
    if not v:
        raise RuntimeError("SCHED_EVENT_FILE 未设置（非调度器启动？）")
    return Path(v)


class EventWriter:
    """线程安全的事件追加器。"""

    def __init__(self):
        self._lock = threading.Lock()
        self._seq = 0

    def emit(self, kind: str, **payload: Any) -> int:
        with self._lock:
            self._seq += 1
            seq = self._seq
            line = {"seq": seq, "kind": kind, "at": _now_iso(), **payload}
            try:
                with open(event_file(), "a", encoding="utf-8") as f:
                    f.write(json.dumps(line, ensure_ascii=False, separators=(",", ":")) + "\n")
                    f.flush()
                    os.fsync(f.fileno())
            except OSError:
                pass
            return seq

    @property
    def seq(self) -> int:
        with self._lock:
            return self._seq


_global_writer: EventWriter | None = None


def writer() -> EventWriter:
    global _global_writer
    if _global_writer is None:
        _global_writer = EventWriter()
    return _global_writer


def emit(kind: str, **payload: Any) -> int:
    return writer().emit(kind, **payload)


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


__all__ = ["EventWriter", "writer", "emit", "event_file", "EventKind"]