"""执行事件通道写侧（worker 用）。

事件以 JSON 行追加到 SCHED_EVENT_FILE，字段：
  {"seq": N, "kind": "...", "at": ISO(UTC), "job_id": "...", "attempt_id": "...", "...payload 平铺..."}

seq 单调递增，由本进程维护；调度器按 (attempt_id, seq) 去重。
kind ∈ {started, progress, error, done, ack}（见 errors.EventKind）。
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from datetime import datetime, timezone
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
        # 事件归属（构造时从环境缓存）：每行自带 job_id/attempt_id，脱离 run_dir 也可定位
        self._ids = {"job_id": os.environ.get("SCHED_JOB_ID") or "",
                     "attempt_id": os.environ.get("SCHED_ATTEMPT_ID") or ""}

    _ROTATE_BYTES = 16 * 1024 * 1024
    _RETRY_DELAYS = (0.05, 0.2, 0.5)

    def emit(self, kind: str, **payload: Any) -> int:
        with self._lock:
            self._seq += 1
            seq = self._seq
            line = {"seq": seq, "kind": kind, "at": _now_iso(),
                    **self._ids, **payload}
            last_err: OSError | None = None
            for delay in (0.0, *self._RETRY_DELAYS):
                if delay:
                    time.sleep(delay)
                try:
                    p = event_file()
                    # 16MB 轮转：写满后原子改名保留 .1，再从空文件续写
                    if p.exists() and p.stat().st_size >= self._ROTATE_BYTES:
                        os.replace(p, p.with_name(p.name + ".1"))
                    with open(p, "a", encoding="utf-8") as f:
                        f.write(json.dumps(line, ensure_ascii=False,
                                           separators=(",", ":")) + "\n")
                        f.flush()
                        os.fsync(f.fileno())
                    last_err = None
                    break
                except OSError as e:
                    last_err = e
            if last_err is not None:
                # C2：事件写失败必须大声告警（丢 completed 事件会让本次执行被判
                # "退出 0 无完成记录"而回队列重跑），但不向上抛——抛异常会把退出 0
                # 变非 0，被误归因为业务 FAILED。
                print(f"[sched-sdk] 事件写入失败（seq={seq} kind={kind}）：{last_err}",
                      file=sys.stderr, flush=True)
            return seq


_global_writer: EventWriter | None = None


def writer() -> EventWriter:
    global _global_writer
    if _global_writer is None:
        _global_writer = EventWriter()
    return _global_writer


def emit(kind: str, **payload: Any) -> int:
    return writer().emit(kind, **payload)


def _now_iso() -> str:
    """统一 UTC 时间戳（设计第十四节：持久化恢复时间记录 UTC）。"""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


__all__ = ["EventWriter", "writer", "emit", "event_file", "EventKind"]