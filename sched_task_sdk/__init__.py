"""
1. 统一 SchedTask（daemon 与 oneshot 共用，差别仅在 steps() 是否终止）
2. 断点契约全进程强制：daemon 同样 SIGTERM → 落断点 → exit 99
3. 新增控制 API：request_stop / no_retry / reschedule（写 state.json control 段）
4. 新增意图 API：emit_intent / list_intents（写/读 requests/ 目录）
5. sched.errors 中的退出码常量（EXIT_OK/99/100/101/110）
"""

from .task import SchedTask, DaemonTask, OneshotTask
from .state import TaskState, SDKError, request_stop, no_retry, reschedule, heartbeat
from .intent import emit_intent, list_intents
from .errors import *  # noqa: F401,F403

__all__ = [
    "SchedTask", "DaemonTask", "OneshotTask",
    "TaskState", "SDKError",
    "request_stop", "no_retry", "reschedule", "heartbeat",
    "emit_intent", "list_intents",
]