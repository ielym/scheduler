"""sched_task_sdk —— 生产/执行子任务 SDK（新协议 v3）。

- 生产者：Producer（发现游标 + 待发布记录 + 无丢失发布 + 稳定作业标识 + 重放）
- 执行者：ExecutorTask（进展/断点/幂等提交/协作暂停/统一终止结果 exit code）
"""

from .errors import EXIT_OK, EXIT_SHORTAGE, EventKind
from .executor import (
    ExecutorTask,
    PauseRequested,
    Shortage,
    complete,
    emit_event,
    report_error_event,
    report_progress,
    report_shortage,
    run_task,
)
from .producer import Producer, ProducerError, atomic_write_json, job_id_of, run_producer
from .checkpoint import CheckpointStore

__all__ = [
    "ExecutorTask", "Shortage", "PauseRequested", "run_task",
    "report_progress", "report_shortage", "report_error_event",
    "complete", "emit_event",
    "Producer", "ProducerError", "job_id_of", "run_producer",
    "atomic_write_json",
    "CheckpointStore",
    "EXIT_OK", "EXIT_SHORTAGE", "EventKind",
]