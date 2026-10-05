"""SDK 写 state.json 的实现：心跳、断点、错误分类、控制指令。

state.json 结构：
{
  "schema_version": 1,
  "current_step": "...",
  "total_steps": 0,
  "progress": 0.0,
  "heartbeat_at": "ISO",
  "updated_at": "ISO",
  "resume_point": { "at_step": "...", "done_items": [...] },
  "last_error": { "category": "resource|logic", "message": "..." },
  "control": { "action": "none|stop|no_retry|reschedule", "delay_sec": 0, "reason": "..." }
}
"""

from __future__ import annotations

import json
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from .errors import (EXIT_DATA_RISK, EXIT_NO_RETRY, EXIT_OK, EXIT_PREEMPTED,
                     EXIT_RESOURCE, EXIT_STOP_REQUESTED, EXIT_EXTERNAL_RESOURCE)


DEFAULT_HEARTBEAT_INTERVAL_SEC = 10
DEFAULT_STEP_TIMEOUT_SEC = 120


class SDKError(Exception):
    pass


def _workspace() -> Path:
    v = os.environ.get("SCHED_WORKSPACE")
    if not v:
        raise SDKError("SCHED_WORKSPACE 未设置（非调度器启动？）")
    return Path(v)


def _run_id() -> str:
    return os.environ.get("SCHED_RUN_ID") or ""


def _state_paths() -> list[Path]:
    """返回可能的 state.json 路径列表（调度器用 run_id 子目录；fallback 到 cache 根）。"""
    ws = _workspace()
    rid = _run_id()
    out = []
    if rid:
        out.append(ws / "cache" / "runs" / rid / "state.json")
    out.append(ws / "cache" / "state.json")
    return out


def _resume_point_path() -> Path:
    return _workspace() / "cache" / "resume_point.json"


def _atomic_write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


@dataclass
class TaskState:
    step_name: str = ""
    total_steps: int = 0
    progress: float = 0.0
    heartbeat_interval_sec: int = DEFAULT_HEARTBEAT_INTERVAL_SEC
    step_timeout_sec: int = DEFAULT_STEP_TIMEOUT_SEC
    _thread: threading.Thread | None = None
    _stop_evt: threading.Event = field(default_factory=threading.Event)
    _resume_from: dict | None = None
    _preempted: bool = False
    _stop_requested: bool = False
    _no_retry: bool = False
    _active_paths: list[Path] = field(default_factory=list)
    on_sigterm: Callable[[], None] | None = None

    # -------- 初始化 --------
    def start(self) -> "TaskState":
        self._active_paths = _state_paths()
        rp = _resume_point_path()
        if rp.exists():
            try:
                self._resume_from = json.loads(rp.read_text("utf-8"))
            except (json.JSONDecodeError, OSError):
                self._resume_from = None
        self._write_initial()
        self._install_signal()
        self._start_heartbeat()
        return self

    def resume_point(self) -> dict | None:
        return self._resume_from

    def heartbeat(self) -> None:
        """显式写一次心跳（正常由后台心跳线程自动写，供需要立即刷新的调用方）。"""
        self._tick_heartbeat()

    # -------- 心跳线程 --------
    def _write_initial(self) -> None:
        st = {
            "schema_version": 1,
            "current_step": self.step_name,
            "total_steps": self.total_steps,
            "progress": self.progress,
            "heartbeat_at": _now_iso(),
            "updated_at": _now_iso(),
        }
        for p in self._active_paths:
            try:
                _atomic_write(p, st)
            except OSError:
                pass

    def _start_heartbeat(self) -> None:
        self._stop_evt.clear()
        self._thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._thread.start()

    def _heartbeat_loop(self) -> None:
        while not self._stop_evt.wait(self.heartbeat_interval_sec):
            self._tick_heartbeat()
        self._tick_heartbeat()

    def _tick_heartbeat(self) -> None:
        st = self._read()
        st["heartbeat_at"] = _now_iso()
        st["updated_at"] = _now_iso()
        for p in self._active_paths:
            try:
                _atomic_write(p, st)
            except OSError:
                pass

    def _read(self) -> dict:
        for p in reversed(self._active_paths):  # 优先读最新
            try:
                txt = p.read_text("utf-8")
                obj = json.loads(txt)
                if isinstance(obj, dict):
                    return obj
            except (json.JSONDecodeError, OSError):
                continue
        return {"schema_version": 1, "current_step": self.step_name, "updated_at": _now_iso()}

    # -------- 步骤 / 断点 --------
    def enter_step(self, name: str, progress: float | None = None) -> None:
        self.step_name = name
        if progress is not None:
            self.progress = progress
        st = self._read()
        st["current_step"] = name
        st["progress"] = self.progress
        st["updated_at"] = _now_iso()
        for p in self._active_paths:
            try:
                _atomic_write(p, st)
            except OSError:
                pass

    def save_checkpoint(self, marker: dict | Any) -> None:
        """落断点（当前已完成位点）。返回 marker（供 resume 后比对）。"""
        rp = {"resume_point": marker, "saved_at": _now_iso()}
        try:
            _atomic_write(_resume_point_path(), rp)
        except OSError:
            pass

    def reset_checkpoint(self) -> None:
        rp = _resume_point_path()
        try:
            if rp.exists():
                rp.unlink()
        except OSError:
            pass

    # -------- 错误分类 --------
    def error(self, category: str, message: str) -> None:
        st = self._read()
        st["last_error"] = {"category": category, "message": message, "at": _now_iso()}
        for p in self._active_paths:
            try:
                _atomic_write(p, st)
            except OSError:
                pass

    # -------- 控制指令 --------
    def _set_control(self, action: str, delay_sec: float = 0, reason: str = "") -> None:
        st = self._read()
        st["control"] = {"action": action, "delay_sec": delay_sec, "reason": reason}
        st["updated_at"] = _now_iso()
        for p in self._active_paths:
            try:
                _atomic_write(p, st)
            except OSError:
                pass

    def request_stop(self, reason: str = "") -> None:
        self._set_control("stop", 0, reason)
        self._stop_requested = True

    def no_retry(self, reason: str = "") -> None:
        self._set_control("no_retry", 0, reason)
        self._no_retry = True

    def reschedule(self, delay_sec: float, reason: str = "") -> None:
        self._set_control("reschedule", max(0.0, float(delay_sec)), reason)

    # -------- 信号 --------
    def _install_signal(self) -> None:
        try:
            signal.signal(signal.SIGTERM, self._on_sigterm)
        except (ValueError, OSError):
            pass  # 非主线程或已被 hook

    def _on_sigterm(self, signum, frame):
        self._preempted = True
        self._stop_evt.set()
        if self.on_sigterm is not None:
            try:
                self.on_sigterm()
            except Exception:
                pass

    # -------- 退出 --------
    def stop(self, checkpoint: dict | None = None) -> None:
        if checkpoint is not None:
            self.save_checkpoint(checkpoint)
        self._stop_evt.set()

    def finalize(self) -> None:
        self._stop_evt.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def exit_code(self) -> int:
        """进程应该用这个退出码退出（根据运行期状态）。"""
        if self._preempted:
            return EXIT_PREEMPTED
        if self._stop_requested:
            return EXIT_STOP_REQUESTED
        if self._no_retry:
            return EXIT_NO_RETRY
        return EXIT_OK


# -------- 顶层便捷函数（模块级便捷 API） --------

_global_state: TaskState | None = None


def _global() -> TaskState:
    global _global_state
    if _global_state is None:
        _global_state = TaskState().start()
    return _global_state


def request_stop(reason: str = "") -> None:
    _global().request_stop(reason)


def no_retry(reason: str = "") -> None:
    _global().no_retry(reason)


def reschedule(delay_sec: float, reason: str = "") -> None:
    _global().reschedule(delay_sec, reason)


def heartbeat() -> None:
    _global().heartbeat()


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")