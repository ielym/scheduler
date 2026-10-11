"""执行者 SDK 基类：心跳 / 进展 / 断点 / 幂等提交 / 协作暂停 / 统一不足。

契约（设计文档第六、七节）：
- 事件种类：started / heartbeat / progress / external_shortage /
  control_ack / checkpoint / completed
- 进展必须给出单调递增已完成单元数、当前未完成工作是否存在、业务处理阶段、
  指标语义版本，以及可得的有效处理耗时 / 在途时长 / 工作量计数。
- 断点恢复、协作暂停、可重放执行、幂等提交是必需能力。
- 统一外部不足用 exit 75 表达；协作暂停由调度器发起的 PAUSING 意图识别。

子类只需实现 execute()；在安全边界调用 self.checkpoint()（保存断点，并顺带检查暂停）。
"""

from __future__ import annotations

import os
import signal
import sys
import threading
import time
from datetime import datetime
from typing import Any

from . import control as _control
from . import events as _events
from .checkpoint import CheckpointStore
from .errors import EXIT_OK, EXIT_SHORTAGE, EventKind


class Shortage(Exception):
    """统一外部不足：业务主动抛出，调度器据此退让（exit 75）。"""

    def __init__(self, reason: str = ""):
        super().__init__(reason)
        self.reason = reason


class PauseRequested(Exception):
    """内部：收到协作暂停，应在安全边界抛出以结束当前 attempt。"""


class ExecutorTask:
    heartbeat_interval_sec: float = 5.0

    def __init__(self, name: str = "task", progress_semver: str | None = None,
                 heartbeat_interval_sec: float | None = None):
        self.name = name
        self.progress_semver = progress_semver or _env("SCHED_PROGRESS_SEMVER", "1.0.0")
        if heartbeat_interval_sec is not None:
            self.heartbeat_interval_sec = heartbeat_interval_sec
        self._ckpt = CheckpointStore()
        self._events = _events.writer()
        self._stop_evt = threading.Event()
        self._pause: _control.PauseCommand | None = None
        self._completed = False
        self._heartbeat_thread: threading.Thread | None = None

    # ---------- 子类实现 ----------
    def execute(self) -> None:
        raise NotImplementedError

    # ---------- 生命周期 ----------
    def run(self) -> int:
        self._ckpt.load()
        self._install_signal()
        self._events.emit(EventKind.STARTED, name=self.name,
                          progress_semver=self.progress_semver)
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()
        exit_code = EXIT_OK
        try:
            self.execute()
            if not self._completed:
                self._events.emit(EventKind.COMPLETED)
                self._completed = True
        except PauseRequested:
            self._on_pause()
        except Shortage as e:
            self._events.emit(EventKind.SHORTAGE, reason=e.reason)
            exit_code = EXIT_SHORTAGE
        except Exception:
            # 业务/未知失败：不冒充外部不足，直接非 0 退出
            import traceback
            traceback.print_exc(file=sys.stderr)
            exit_code = 1
        finally:
            self._stop_evt.set()
            if self._heartbeat_thread is not None:
                self._heartbeat_thread.join(timeout=2)
        return exit_code

    # ---------- 协作暂停 ----------
    def _install_signal(self) -> None:
        try:
            signal.signal(signal.SIGTERM, self._on_signal)
        except (ValueError, OSError):
            pass

    def _on_signal(self, signum, frame):
        self._stop_evt.set()

    def checkpoint(self, data: Any = None, *, input_ref: str = "",
                   business_version: str = "", progress: Any = None,
                   noop: bool = False) -> dict | None:
        """保存断点（原子提交、提交编号递增），并顺带检查暂停。

        在安全边界调用：若已收到暂停，则保存断点后抛 PauseRequested。
        这是"周期性提交检查点 + 检查暂停"的统一入口。
        """
        cp = None
        if not noop:
            cp = self._ckpt.save(data, input_ref=input_ref,
                                 business_version=business_version,
                                 progress=progress)
            self._events.emit(EventKind.CHECKPOINT, commit_seq=cp["commit_seq"],
                              input_ref=cp["input_ref"])
        self.checkpoint_now_ack()
        return cp

    def checkpoint_now_ack(self) -> None:
        """只检查暂停（不保存断点）。"""
        if self._pause is None:
            self._pause = _control.current_pause()
        if self._pause is not None or self._stop_evt.is_set():
            raise PauseRequested()

    def should_stop(self) -> bool:
        return self._pause is not None or self._stop_evt.is_set()

    def resume_data(self) -> Any:
        """读取最近检查点的业务数据（用于断点恢复）。"""
        ck = self._ckpt.load()
        return (ck or {}).get("data") if isinstance(ck, dict) else None

    def _on_pause(self) -> None:
        pause = self._pause or _control.PauseCommand(command_id="", action="pause")
        self._events.emit(EventKind.CONTROL_ACK, command_id=pause.command_id)
        # 断点已在 checkpoint() 或业务周期提交中持久化
        self._completed = False

    # ---------- 进度 / 不足 ----------
    def report_progress(self, completed_units: int, has_unfinished_work: bool,
                        stage: str, *, active_duration_sec: float | None = None,
                        in_flight_duration_sec: float | None = None,
                        work_units: float | None = None) -> None:
        self._events.emit(
            EventKind.PROGRESS,
            completed_units=int(completed_units),
            has_unfinished=bool(has_unfinished_work),
            stage=str(stage),
            metric_semver=self.progress_semver,
            active_duration_sec=active_duration_sec,
            in_flight_duration_sec=in_flight_duration_sec,
            work_units=work_units,
        )

    def report_shortage(self, reason: str = "") -> None:
        raise Shortage(reason)

    def complete(self, record: dict | None = None) -> None:
        """标记完成并提交持久化完成记录（幂等）。"""
        self._events.emit(EventKind.COMPLETED, record=record or {})
        self._completed = True

    # ---------- 心跳 ----------
    def _heartbeat_loop(self) -> None:
        while not self._stop_evt.wait(self.heartbeat_interval_sec):
            self._events.emit(EventKind.HEARTBEAT)

    # ---------- 工具 ----------
    def log(self, msg: str) -> None:
        print(msg, flush=True)


def _env(k: str, default: str) -> str:
    return os.environ.get(k) or default


def run_task(task: ExecutorTask) -> int:
    if not os.environ.get("SCHED_ATTEMPT_ID") and not os.environ.get("SCHED_DEV"):
        print("缺少 SCHED_ATTEMPT_ID（仅允许调度器触发正式执行）", file=sys.stderr, flush=True)
        return 1
    return task.run()


__all__ = [
    "ExecutorTask", "Shortage", "PauseRequested", "run_task",
    "EXIT_OK", "EXIT_SHORTAGE", "EventKind",
    # 便捷重导出
    "emit_event", "report_progress", "report_shortage", "complete",
]


# ---- 模块级便捷 API（面向"裸脚本"但走事件通道） ----
def emit_event(kind: str, **payload) -> int:
    return _events.emit(kind, **payload)


def report_progress(completed_units: int, has_unfinished_work: bool, stage: str,
                    **kw) -> None:
    emit_event(EventKind.PROGRESS, completed_units=int(completed_units),
               has_unfinished=bool(has_unfinished_work), stage=str(stage),
               metric_semver=_env("SCHED_PROGRESS_SEMVER", "1.0.0"), **kw)


def report_shortage(reason: str = "") -> None:
    raise Shortage(reason)


def complete(record: dict | None = None) -> None:
    emit_event(EventKind.COMPLETED, record=record or {})


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _sleep(s: float) -> None:
    time.sleep(s)