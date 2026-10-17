"""协作暂停控制通道读取（worker 侧）。

调度器把暂停命令写入 SCHED_CONTROL_FILE：
  {"command_id", "job_id", "attempt_id", "token", "action": "pause",
   "reason", "deadline"}

worker 每次进入安全边界时调用 current_pause() 检查；确认后保存断点（若有）、
发送 ack 事件，然后正常退出。
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


def control_file() -> Path:
    v = os.environ.get("SCHED_CONTROL_FILE")
    if not v:
        raise RuntimeError("SCHED_CONTROL_FILE 未设置（非调度器启动？）")
    return Path(v)


@dataclass
class PauseCommand:
    command_id: str
    action: str
    reason: str = ""
    deadline: str = ""
    attempt_id: str = ""
    token: str = ""


def current_pause() -> PauseCommand | None:
    """读取当前暂停命令；无或解析失败返回 None。

    B3（设计第七/十三节）：暂停命令必须携带目标 attempt 身份（attempt_id/token），
    与本进程环境不一致（调度器已换发新 attempt、控制文件是残留旧命令）时返回 None，
    防止旧暂停命令误杀新 attempt。
    """
    p = control_file()
    try:
        obj = json.loads(p.read_text("utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if not isinstance(obj, dict):
        return None
    if obj.get("action") != "pause":
        return None
    att = str(obj.get("attempt_id") or "")
    tok = str(obj.get("token") or "")
    env_att = os.environ.get("SCHED_ATTEMPT_ID") or ""
    env_tok = os.environ.get("SCHED_TOKEN") or ""
    if (att and env_att and att != env_att) or (tok and env_tok and tok != env_tok):
        return None
    return PauseCommand(
        command_id=str(obj.get("command_id") or ""),
        action="pause",
        reason=str(obj.get("reason") or ""),
        deadline=str(obj.get("deadline") or ""),
        attempt_id=att,
        token=tok,
    )


__all__ = ["PauseCommand", "current_pause", "control_file"]