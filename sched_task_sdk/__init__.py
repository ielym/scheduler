"""sched_task_sdk —— 生产/执行子任务 SDK（新协议 v3）。

- 生产者：Producer（发现游标 + 待发布记录 + 无丢失发布 + 稳定作业标识 + 重放）
- 执行者：ExecutorTask（心跳/进展/断点/幂等提交/协作暂停/统一不足 exit 75）
"""

from .errors import EXIT_OK, EXIT_SHORTAGE, EventKind
from .executor import (
    ExecutorTask,
    PauseRequested,
    Shortage,
    complete,
    emit_event,
    report_progress,
    report_shortage,
    run_task,
)
from .producer import Producer, ProducerError, job_id_of, run_producer
from .checkpoint import CheckpointStore

__all__ = [
    "ExecutorTask", "Shortage", "PauseRequested", "run_task",
    "report_progress", "report_shortage", "complete", "emit_event",
    "Producer", "ProducerError", "job_id_of", "run_producer",
    "CheckpointStore",
    "EXIT_OK", "EXIT_SHORTAGE", "EventKind",
]