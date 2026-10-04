"""任务侧退出码协议（复用 sched.errors 的常量，SDK 独立可用）。

与 sched.errors 完全对齐，避免任务侧额外依赖 sched 包。
"""

from __future__ import annotations

from enum import Enum


class Category(str, Enum):
    RESOURCE = "resource"
    LOGIC = "logic"
    UNCLASSIFIED = "unclassified"


EXIT_OK = 0
EXIT_PREEMPTED = 99
EXIT_RESOURCE = 100
EXIT_EXTERNAL_RESOURCE = 101
EXIT_DATA_RISK = 110
EXIT_LOGIC = 1
EXIT_SIGKILL = 137
EXIT_STOP_REQUESTED = 98  # 进程主动 stop（SDK 的 request_stop 路径）
EXIT_NO_RETRY = 97        # 进程 no_retry 后正常终结
EXIT_SKIP_SCHEDULE = 102  # 任务判定当前不该跑（时间窗/节假日等），跳过本轮

RESERVED_EXIT_CODES = {EXIT_PREEMPTED, EXIT_RESOURCE, EXIT_EXTERNAL_RESOURCE,
                       EXIT_DATA_RISK, EXIT_STOP_REQUESTED, EXIT_NO_RETRY,
                       EXIT_SKIP_SCHEDULE, EXIT_SIGKILL}