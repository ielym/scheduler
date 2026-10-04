"""【协议 C v1】进程 ↔ 调度器 控制协议（§5.2）。

调度器通过两条固定通道读取进程的生命周期意图：

1. **退出码**（进程退出时的一次性决策）：
   0 成功终结 → 意图 done
   99 抢占/停止，断点已保存 → 续跑同意图
   100/101 资源错误 → 重试同意图（受 lifetime.retry 约束 + 控制收紧）
   110 数据风险 → 可选二刷
   其他非 0 → 默认按 100 处理
   137 SIGKILL → 归因后处理

2. **状态文件 control 段**（进程运行中异步声明，SDK 写、调度器每 tick 轮询）：
   {"action": "stop | no_retry | reschedule", "delay_sec": N, "reason": "..."}

本模块只做"解析 + 决策"，不做实际的进程杀/入队——实际动作由 service.py 主循环执行。
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from . import errors as err


class ControlError(Exception):
    pass


@dataclass
class ControlDirective:
    """调度器从进程读取到的运行时指令。"""
    action: str             # none | stop | no_retry | reschedule
    delay_sec: float = 0    # reschedule 用；stop/no_retry 忽略
    reason: str = ""


@dataclass
class ExitDecision:
    """进程退出后调度器对意图的终结决策。"""
    terminal: bool              # True=意图 done/archived；False=继续续跑/重试同意图
    directive: ControlDirective  # 合并运行时 control 段
    action: str                 # done | resume | retry | skip | second_pass | kill_attribution
    attempt_increment: bool     # 本次是否计入 attempt
    not_before: str = ""        # ISO8601，到点前不参与准出


# ---------- 运行时 control 段读取 ----------

def read_state_file(task_dir: Path, run_id: str = "") -> dict:
    """读 cache/runs/<run_id>/state.json；run_id 为空时不读。"""
    if not run_id:
        return {}
    p = task_dir / "cache" / "runs" / run_id / "state.json"
    if not p.exists():
        return {}
    try:
        obj = json.loads(p.read_text("utf-8"))
        if isinstance(obj, dict):
            return obj
    except (json.JSONDecodeError, OSError):
        return {}
    return {}


def read_control_directive(state: dict) -> ControlDirective:
    """从 state.json 解析 control 段（运行时指令）。"""
    if not isinstance(state, dict):
        return ControlDirective(action="none")
    ctrl = state.get("control")
    if not isinstance(ctrl, dict):
        return ControlDirective(action="none")
    act = str(ctrl.get("action") or "none")
    if act not in ("none", "stop", "no_retry", "reschedule"):
        act = "none"
    delay = ctrl.get("delay_sec", 0)
    try:
        delay = float(delay) if delay is not None else 0
    except (TypeError, ValueError):
        delay = 0
    delay = max(0.0, delay)
    reason = str(ctrl.get("reason") or "")
    return ControlDirective(action=act, delay_sec=delay, reason=reason)


# ---------- 退出码 → ExitDecision ----------

def _not_before(now: datetime, delay_sec: float) -> str:
    if delay_sec <= 0:
        return ""
    return (now + timedelta(seconds=delay_sec)).isoformat(timespec="seconds")


def decide_from_exit(exit_code: int | None, directive: ControlDirective,
                     lifetime_retry: dict[str, Any] | None,
                     attempt: int, now: datetime | None = None) -> ExitDecision:
    """进程退出后，合并「退出码 + 运行时 control + 意图生命周期」给出终结决策。

    优先级：运行时 control（stop/no_retry）> 退出码。
    stop → 无论什么退出码都终结（进程主动请求停止）
    no_retry → 非 0 时终结（进程声明不再重试）
    """
    if now is None:
        now = datetime.now().astimezone()
    retry_cfg = lifetime_retry or {}
    base = float(retry_cfg.get("backoff_base_sec") or 5)

    # --- 运行时 control 优先 ---
    if directive.action == "stop":
        # 进程请求结束：终结意图，不再续跑
        return ExitDecision(terminal=True, directive=directive, action="done", attempt_increment=False)

    if directive.action == "no_retry":
        # 进程声明不再重试：非 0 退出时终结；0 退出本身也终结
        return ExitDecision(terminal=True, directive=directive, action="done", attempt_increment=False)

    if directive.action == "reschedule":
        # 进程声明稍后再跑：回 pending + not_before，不算 attempt
        nb = _not_before(now, directive.delay_sec)
        return ExitDecision(terminal=False, directive=directive, action="skip",
                            attempt_increment=False, not_before=nb)

    # --- 纯退出码判定 ---
    if exit_code is None:
        return ExitDecision(terminal=True, directive=directive, action="done", attempt_increment=False)

    if exit_code == err.EXIT_OK:
        return ExitDecision(terminal=True, directive=directive, action="done", attempt_increment=False)

    if exit_code == err.EXIT_PREEMPTED:
        # 抢占/停止：断点已保存 → 续跑同意图，attempt 不递增
        return ExitDecision(terminal=False, directive=directive, action="resume", attempt_increment=False)

    if exit_code == err.EXIT_DATA_RISK:
        # 数据风险：调度器可选二刷（由意图 lifetime 或后续策略决定）
        return ExitDecision(terminal=False, directive=directive, action="second_pass",
                            attempt_increment=False)

    if exit_code == err.EXIT_SKIP_SCHEDULE:
        # 任务判定当前不该跑（时间窗/节假日等）：跳过本轮，稍后重排再给机会，不计 attempt。
        # 精确的"下次时机"应由任务用 reschedule(delay) 表达；此处给一个保守兜底延迟。
        nb = _not_before(now, base)
        return ExitDecision(terminal=False, directive=directive, action="skip",
                            attempt_increment=False, not_before=nb)

    if exit_code in (err.EXIT_RESOURCE, err.EXIT_EXTERNAL_RESOURCE):
        # 资源不足/冲突：回退 pending 等待合适时机，不计 attempt、不做失败记录。
        # 资源不足是调度器"等资源"的责任，不是任务失败；退避用固定 base 防热循环。
        nb = _not_before(now, base)
        return ExitDecision(terminal=False, directive=directive, action="retry",
                            attempt_increment=False, not_before=nb)

    if exit_code == err.EXIT_SIGKILL:
        # SIGKILL 归因：默认按资源处理（回退 pending，不计 attempt）
        nb = _not_before(now, base)
        return ExitDecision(terminal=False, directive=directive, action="retry",
                            attempt_increment=False, not_before=nb)

    # 其他非 0：兜底按资源错误处理（倒逼正确分类），回退 pending 不计 attempt
    nb = _not_before(now, base)
    return ExitDecision(terminal=False, directive=directive, action="retry",
                        attempt_increment=False, not_before=nb)