"""sched_task_sdk —— 生产/执行子任务 SDK（新协议 v3）。

- 生产者：Producer（发现游标 + 待发布记录 + 无丢失发布 + 稳定作业标识 + 重放）
- 执行者：ExecutorTask（进展/幂等提交/协作暂停/统一终止结果 exit code）
- 恢复/幂等：由任务以外部真源自证（无 SDK 断点设施）
"""

from .errors import EXIT_OK, EXIT_SHORTAGE, EventKind
from .executor import ExecutorTask, PauseRequested, Shortage, run_task
from .producer import Producer, ProducerError, atomic_write_json, job_id_of, run_producer

__all__ = [
    "ExecutorTask", "Shortage", "PauseRequested", "run_task",
    "Producer", "ProducerError", "job_id_of", "run_producer",
    "atomic_write_json",
    "EXIT_OK", "EXIT_SHORTAGE", "EventKind",
]