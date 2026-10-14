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

import json
import os
import re
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any

from . import control as _control
from . import events as _events
from .checkpoint import CheckpointStore
from .errors import EXIT_OK, EXIT_SHORTAGE, EventKind


# A5：reason 纯净化——外部对象（URL/HTTP 状态码/异常类名）不得泄漏进调度事件
_URL_RE = re.compile(r"https?://\S+|www\.\S+")
_STATUS_RE = re.compile(
    r"\b(?:HTTP[\s/_-]*\d{3}|status[\s_-]*(?:code)?[\s:=]*\d{3})\b", re.IGNORECASE)
# 独立的 4xx/5xx 三位数（含代理 517 等）大概率是 HTTP 状态码残留
_BARE_CODE_RE = re.compile(r"\b[45]\d{2}\b")
_EXC_RE = re.compile(
    r"\b(?:Connection\w*Error|Timeout\w*|SSL\w*Error|HTTP\w*Error|URL\w*Error|"
    r"Request\w*|JSONDecode\w*|OS\w*|IO\w*|Socket\w*|\w*Interrupted\w*)\b")


def _clean_reason(reason: str) -> str:
    """净化退出/不足原因：剥离 URL、HTTP 状态码、异常类型名，压缩空白并截断。

    只留人类可读的状态描述，防止外部资源指纹进入调度事件/日志（设计第六节 + H9）。
    """
    s = str(reason or "")
    s = _URL_RE.sub(" ", s)
    s = _STATUS_RE.sub(" ", s)
    s = _EXC_RE.sub(" ", s)
    s = _BARE_CODE_RE.sub(" ", s)
    s = "".join(ch for ch in s if ch.isprintable())
    s = re.sub(r"\s+", " ", s).strip()
    return s[:200]


class Shortage(Exception):
    """统一外部不足：业务主动抛出，调度器据此退让（exit 75）。

    reason 只描述状态，不记录资源指纹/URL/原始异常文本（设计第六节 + H9）。
    """

    def __init__(self, reason: str = ""):
        reason = _clean_reason(reason)
        super().__init__(reason)
        self.reason = reason


class PauseRequested(Exception):
    """内部：收到协作暂停，应在安全边界抛出以结束当前 attempt。"""


class ExecutorTask:
    heartbeat_interval_sec: float = 5.0

    def __init__(self, name: str = "task", progress_semver: str | None = None,
                 heartbeat_interval_sec: float | None = None,
                 max_step_sec: float | None = None,
                 permit_timeout_sec: float | None = None):
        self.name = name
        self.progress_semver = progress_semver or _env("SCHED_PROGRESS_SEMVER", "1.0.0")
        if heartbeat_interval_sec is not None:
            self.heartbeat_interval_sec = heartbeat_interval_sec
        # B14：申报单步最长耗时——STARTED 事件携带，调度器据此推导暂停宽限
        self.max_step_sec = float(max_step_sec) if max_step_sec else None
        # A7：启动许可等待上限（超时按统一外部不足退出，不误判为业务失败）
        self.permit_timeout_sec = 60.0 if permit_timeout_sec is None \
            else float(permit_timeout_sec)
        self._ckpt = CheckpointStore()
        self._events = _events.writer()
        self._stop_evt = threading.Event()
        self._pause: _control.PauseCommand | None = None
        self._ckpt_saved_this_attempt = False   # B3：本 attempt 是否已提交过检查点
        self._last_completed_units: int | None = None  # B5：进展单调校验基准
        self._heartbeat_thread: threading.Thread | None = None

    # ---------- 子类实现 ----------
    def execute(self) -> None:
        raise NotImplementedError

    # ---------- 生命周期 ----------
    def run(self) -> int:
        self._ckpt.load()
        self._install_signal()
        started = {"name": self.name,
                   "progress_semver": self.progress_semver,
                   "feedback_interval_sec": self.heartbeat_interval_sec}
        if self.max_step_sec:
            started["max_step_sec"] = self.max_step_sec
        self._events.emit(EventKind.STARTED, **started)
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()
        exit_code = EXIT_OK
        try:
            self._wait_for_permit()
            # B2：execute 正常返回不再自动补发 COMPLETED——完成必须由业务显式
            # complete() 幂等提交；退出 0 且无完成记录由调度器按"待核实"处理，
            # 不得让"忘提交"伪装成成功（设计第六、十二节）。
            self.execute()
        except PauseRequested:
            self._on_pause()
        except Shortage as e:
            # 持续不足退出前提交检查点（审查修复，设计第六节 L101"持续不足时
            # 提交检查点并以约定退出码退出"）：本 attempt 尚未提交过但存在业务
            # 断点数据（本轮已保存或继承自上一 attempt）时，按本 attempt 身份
            # 重提交一次，避免 75 退出回退到旧检查点丢失本轮进度。
            try:
                self._ensure_checkpoint_before_exit()
            except Exception:
                pass
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

    def _wait_for_permit(self) -> None:
        """A7 启动许可门：调度器确认 STARTED/RUNNING 后写 permit.json，
        worker 在此之前不得产生业务副作用。

        未设置 SCHED_PERMIT_FILE（旧调度器/本地调试）时直接放行。
        审查修复（语义纠正）：等待期间收到暂停命令/TERM 属"调度器主动暂停"，
        抛 PauseRequested 走协作暂停（退出 0 → PAUSED），不得伪装成统一外部
        不足（exit 75 → BACKOFF 并收缩任务目标）——调度器主动暂停不计为
        任务错误或不足证据（设计第六节 L103、第十二节 L211）。
        仅"超时仍无许可"才是外部不足语义（调度器长时间不授予许可）。
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
            self._ckpt_saved_this_attempt = True
        self.checkpoint_now_ack()
        return cp

    def checkpoint_now_ack(self) -> None:
        """只检查暂停（不保存断点）。

        B3：确认暂停时若本 attempt 尚未提交过任何检查点、但继承了上一 attempt
        的检查点数据，则先把继承数据按本 attempt 身份重提交一次——避免
        "暂停已被确认、检查点却仍挂在旧 attempt 名下"导致恢复后从零重放
        （设计第七节：暂停命令先存检查点再退出）。
        """
        if self._pause is None:
            self._pause = _control.current_pause()
        if self._pause is not None or self._stop_evt.is_set():
            self._ensure_checkpoint_before_exit()
            raise PauseRequested()

    def _ensure_checkpoint_before_exit(self) -> None:
        """退出前（暂停确认 / 持续不足退出）的检查点兜底：本 attempt 尚未
        提交过但持有断点数据（本轮已保存或继承自上一 attempt）时，按本
        attempt 身份重提交一次（B3 机制，设计第六、七节）。"""
        if self._ckpt_saved_this_attempt:
            return
        prev = self._ckpt.last
        if not isinstance(prev, dict) or prev.get("data") is None:
            return
        cp = self._ckpt.save(prev["data"],
                             input_ref=str(prev.get("input_ref") or ""),
                             business_version=str(prev.get("business_version") or ""),
                             progress=prev.get("progress"))
        self._events.emit(EventKind.CHECKPOINT, commit_seq=cp["commit_seq"],
                          input_ref=cp["input_ref"])
        self._ckpt_saved_this_attempt = True

    def wait_interruptible(self, sec: float) -> None:
        """可中断等待：小步睡眠并检查暂停/停止，长等待必须用它替代 time.sleep
        （收到暂停立即在安全边界抛 PauseRequested，不再睡满全程）。"""
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
        # 审查修复：SIGTERM 可能先于控制文件读取到达——退出路径再读一次控制
        # 文件，能拿到 command_id 就关联具体命令（纯 TERM 无命令时才置空）
        pause = self._pause or _control.current_pause() or \
            _control.PauseCommand(command_id="", action="pause")
        self._events.emit(EventKind.CONTROL_ACK, command_id=pause.command_id)
        # 断点已在 checkpoint() 或业务周期提交中持久化

    # ---------- 进度 / 不足 ----------
    def report_progress(self, completed_units: int, has_unfinished_work: bool,
                        stage: str, *, active_duration_sec: float | None = None,
                        in_flight_duration_sec: float | None = None,
                        work_units: float | None = None,
                        waiting: float | None = None) -> None:
        # B5（设计第六节）：completed_units 必须单调递增——回退是指标污染，
        # 立即抛错让 attempt 落 FAILED（可见可修），不假装满足继续跑。
        # 新 attempt 从 0 重新计数合法（重放场景：本对象为全新实例）。
        cu = int(completed_units)
        last = self._last_completed_units
        if last is not None and cu < last:
            raise ValueError(
                f"completed_units 回退：{cu} < 已报告的 {last}"
                "（进展必须单调递增，设计第六节）")
        self._last_completed_units = cu
        self._events.emit(
            EventKind.PROGRESS,
            completed_units=cu,
            has_unfinished=bool(has_unfinished_work),
            stage=str(stage),
            metric_semver=self.progress_semver,
            active_duration_sec=active_duration_sec,
            in_flight_duration_sec=in_flight_duration_sec,
            work_units=work_units,
            waiting=waiting,
        )

    def report_shortage(self, reason: str = "") -> None:
        raise Shortage(reason)

    def report_shortage_event(self, reason: str = "") -> None:
        """报告统一外部不足事件但继续运行：短暂失败可自行恢复（设计第六节）。

        持续不足时应调用 report_shortage()（抛 Shortage → exit 75）退让退出。
        """
        self._events.emit(EventKind.SHORTAGE, reason=_clean_reason(reason))

    def complete(self, record: dict | None = None) -> None:
        """标记完成并提交持久化完成记录（幂等）。"""
        self._events.emit(EventKind.COMPLETED, record=record or {})

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
    "emit_event", "report_progress", "report_shortage", "report_shortage_event",
    "complete",
]


# ---- 模块级便捷 API（面向"裸脚本"但走事件通道） ----
# 单调校验基准（模块级 API 与类版本同契约：completed_units 必须单调递增，
# 设计第六节 L91；新进程从头计数合法）
_module_last_cu: int | None = None


def emit_event(kind: str, **payload) -> int:
    return _events.emit(kind, **payload)


def report_progress(completed_units: int, has_unfinished_work: bool, stage: str,
                    **kw) -> None:
    global _module_last_cu
    cu = int(completed_units)
    if _module_last_cu is not None and cu < _module_last_cu:
        raise ValueError(
            f"completed_units 回退：{cu} < 已报告的 {_module_last_cu}"
            "（进展必须单调递增，设计第六节）")
    _module_last_cu = cu
    emit_event(EventKind.PROGRESS, completed_units=cu,
               has_unfinished=bool(has_unfinished_work), stage=str(stage),
               metric_semver=_env("SCHED_PROGRESS_SEMVER", "1.0.0"), **kw)


def report_shortage(reason: str = "") -> None:
    raise Shortage(reason)


def report_shortage_event(reason: str = "") -> None:
    emit_event(EventKind.SHORTAGE, reason=_clean_reason(reason))


def complete(record: dict | None = None) -> None:
    emit_event(EventKind.COMPLETED, record=record or {})