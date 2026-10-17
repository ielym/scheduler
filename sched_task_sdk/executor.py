"""执行者 SDK 基类：进展 / 断点 / 幂等提交 / 协作暂停。

契约：
- 事件种类统一 5 种：started / progress / error / done / ack（见 errors.py）。
  通信格式统一（同一信封），type 只区分语义。
- 进展只报单调递增已完成单元数；耗时、资源消耗、存活节奏一律由调度器
  自行观测统计（cgroup / 墙钟 / 事件间隔 EMA），任务不预估。
- 协作暂停、可重放执行、幂等提交是必需能力；断点是任务可选实践。
- 终止结果由退出码表达：0=成功（须发 done）、75=资源不足、其他=业务失败；
  错误归因统一发 error 事件（reason 携带错误类型+资源标识）。

子类只需实现 execute()；在安全边界调用 self.checkpoint()（保存断点，顺带检查暂停）。
"""

from __future__ import annotations

import json
import os
import re
import signal
import threading
import time
from pathlib import Path
from typing import Any

from . import control as _control
from . import events as _events
from .checkpoint import CheckpointStore
from .errors import EXIT_OK, EXIT_SHORTAGE, EventKind


# reason 整理：保留错误类型与资源标识（归因依据），仅做不可见字符清理、
# 空白压缩与长度截断。凭证/账号类敏感信息由任务侧保证不写入。
_MAX_REASON_LEN = 300


def _clean_reason(reason: str) -> str:
    s = str(reason or "")
    s = "".join(ch for ch in s if ch.isprintable())
    s = re.sub(r"\s+", " ", s).strip()
    return s[:_MAX_REASON_LEN]


class Shortage(Exception):
    """资源不足：业务主动抛出，调度器据此退让（exit 75，资源类错误）。

    它是退出码 75 的任务侧表达，不是独立事件类型——终止前统一发 error 事件。
    """

    def __init__(self, reason: str = ""):
        reason = _clean_reason(reason)
        super().__init__(reason)
        self.reason = reason


class PauseRequested(Exception):
    """内部：收到协作暂停，应在安全边界抛出以结束当前 attempt。"""


class ExecutorTask:
    def __init__(self, name: str = "task", permit_timeout_sec: float | None = None):
        self.name = name
        # 启动许可等待上限（超时按资源不足退出，不误判为业务失败）
        self.permit_timeout_sec = 60.0 if permit_timeout_sec is None \
            else float(permit_timeout_sec)
        self._ckpt = CheckpointStore()
        self._events = _events.writer()
        self._stop_evt = threading.Event()
        self._pause: _control.PauseCommand | None = None
        self._ckpt_saved_this_attempt = False   # 本 attempt 是否已提交过检查点
        self._last_completed_units: int | None = None  # 进展单调校验基准

    # ---------- 子类实现 ----------
    def execute(self) -> None:
        raise NotImplementedError

    # ---------- 生命周期 ----------
    def run(self) -> int:
        self._ckpt.load()
        self._install_signal()
        self._events.emit(EventKind.STARTED, name=self.name)
        exit_code = EXIT_OK
        try:
            self._wait_for_permit()
            # 完成必须由业务显式 complete() 幂等提交；退出 0 且无 done 由调度器
            # 按"待核实"处理，不得让"忘提交"伪装成成功。
            self.execute()
        except PauseRequested:
            self._on_pause()
        except Shortage as e:
            # 资源不足退出前提交检查点，避免 75 退出回退到旧检查点丢本轮进度。
            try:
                self._ensure_checkpoint_before_exit()
            except Exception:
                pass
            self._events.emit(EventKind.ERROR, reason=e.reason)
            exit_code = EXIT_SHORTAGE
        except Exception as e:
            # 业务/未知失败：统一发 error 事件归因，退出码非 0/75 表达"业务失败"
            import traceback
            traceback.print_exc(file=sys.stderr)
            self._events.emit(
                EventKind.ERROR, reason=_clean_reason(f"{type(e).__name__}: {e}"))
            exit_code = 1
        finally:
            self._stop_evt.set()
        return exit_code

    def _wait_for_permit(self) -> None:
        """启动许可门：调度器确认 STARTED/RUNNING 后写 permit.json，
        worker 在此之前不得产生业务副作用。

        未设置 SCHED_PERMIT_FILE（旧调度器/本地调试）时直接放行。
        等待期间收到暂停命令/TERM 属"调度器主动暂停"，抛 PauseRequested 走
        协作暂停，不得伪装成资源不足。仅"超时仍无许可"才是资源不足语义。
        """
        path = os.environ.get("SCHED_PERMIT_FILE")
        if not path:
            return
        att = os.environ.get("SCHED_ATTEMPT_ID") or ""
        deadline = time.monotonic() + self.permit_timeout_sec
        while True:
            obj = None
            try:
                obj = json.loads(Path(path).read_text("utf-8"))
            except (OSError, ValueError):
                obj = None
            if isinstance(obj, dict) and obj.get("action") == "permit" \
                    and str(obj.get("attempt_id") or "") == att:
                return
            if self._stop_evt.is_set() or _control.current_pause() is not None:
                raise PauseRequested()
            if time.monotonic() >= deadline:
                raise Shortage("等待启动许可超时")
            time.sleep(0.2)

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

        断点只写文件，不发事件——调度器需要"断点推进"信号时直接读文件。
        在安全边界调用：若已收到暂停，保存断点后抛 PauseRequested。
        """
        cp = None
        if not noop:
            cp = self._ckpt.save(data, input_ref=input_ref,
                                 business_version=business_version,
                                 progress=progress)
            self._ckpt_saved_this_attempt = True
        self.checkpoint_now_ack()
        return cp

    def checkpoint_now_ack(self) -> None:
        """只检查暂停（不保存断点）。

        确认暂停时若本 attempt 尚未提交过任何检查点、但继承了上一 attempt
        的检查点数据，则先把继承数据按本 attempt 身份重提交一次。
        """
        if self._pause is None:
            self._pause = _control.current_pause()
        if self._pause is not None or self._stop_evt.is_set():
            self._ensure_checkpoint_before_exit()
            raise PauseRequested()

    def _ensure_checkpoint_before_exit(self) -> None:
        """退出前（暂停确认 / 资源不足退出）的检查点兜底：本 attempt 尚未
        提交过但持有断点数据时，按本 attempt 身份重提交一次。"""
        if self._ckpt_saved_this_attempt:
            return
        prev = self._ckpt.last
        if not isinstance(prev, dict) or prev.get("data") is None:
            return
        self._ckpt.save(prev["data"],
                        input_ref=str(prev.get("input_ref") or ""),
                        business_version=str(prev.get("business_version") or ""),
                        progress=prev.get("progress"))
        self._ckpt_saved_this_attempt = True

    def wait_interruptible(self, sec: float) -> None:
        """可中断等待：小步睡眠并检查暂停/停止，长等待必须用它替代 time.sleep。"""
        deadline = time.monotonic() + max(0.0, float(sec))
        while time.monotonic() < deadline:
            self.checkpoint_now_ack()   # 暂停/停止到达即抛 PauseRequested
            time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))

    def should_stop(self) -> bool:
        return self._pause is not None or self._stop_evt.is_set()

    def resume_data(self) -> Any:
        """读取最近检查点的业务数据（用于断点恢复）。"""
        ck = self._ckpt.load()
        return (ck or {}).get("data") if isinstance(ck, dict) else None

    def _on_pause(self) -> None:
        # SIGTERM 可能先于控制文件读取到达——退出路径再读一次控制文件，
        # 能拿到 command_id 就关联具体命令（纯 TERM 无命令时才置空）
        pause = self._pause or _control.current_pause() or \
            _control.PauseCommand(command_id="", action="pause")
        self._events.emit(EventKind.ACK, command_id=pause.command_id)
        # 断点已在 checkpoint() 或业务周期提交中持久化

    # ---------- 进度 / 错误 / 完成 ----------
    def report_progress(self, completed_units: int) -> None:
        # completed_units 必须单调递增——回退是指标污染，立即抛错落 FAILED。
        # 新 attempt 从 0 重新计数合法（重放场景：本对象为全新实例）。
        cu = int(completed_units)
        last = self._last_completed_units
        if last is not None and cu < last:
            raise ValueError(
                f"completed_units 回退：{cu} < 已报告的 {last}"
                "（进展必须单调递增）")
        self._last_completed_units = cu
        self._events.emit(EventKind.PROGRESS, completed_units=cu)

    def report_shortage(self, reason: str = "") -> None:
        """报告资源不足并退出（raise Shortage → exit 75）。"""
        raise Shortage(reason)

    def complete(self, record: dict | None = None) -> None:
        """标记完成并提交持久化完成记录（幂等）。"""
        self._events.emit(EventKind.DONE, record=record or {})

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
]