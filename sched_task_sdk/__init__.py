"""sched_task_sdk：任务侧 SDK。任务继承 SchedTask 即可获得托管断点、心跳、抢占退出(99)、
错误分类、跳过项等能力，无需与调度器源码耦合。"""

from .task import SchedTask, Step, PREEMPT_EXIT_CODE, run_task
from .state import TaskState
from .errors import Category, classify_exception, resource, logic

__all__ = [
    "SchedTask",
    "Step",
    "TaskState",
    "Category",
    "classify_exception",
    "resource",
    "logic",
    "PREEMPT_EXIT_CODE",
    "run_task",
]