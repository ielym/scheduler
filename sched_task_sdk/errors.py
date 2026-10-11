"""退出码与事件种类约定（与调度器 sched/config.py 镜像）。

协议约定（设计文档第六、十二节）：
- 0  成功完成（必须已写入持久化完成记录）
- 75 统一外部不足（可恢复退让）
- 其他非 0 业务/未知失败

协作暂停：worker 在收到暂停命令后，提交检查点、发送 control_ack + checkpoint 事件，
随后正常退出；调度器由自身发起的 PAUSING 意图识别为 PAUSED，而不依赖特殊退出码。
"""

EXIT_OK = 0
EXIT_SHORTAGE = 75


class EventKind:
    STARTED = "started"
    HEARTBEAT = "heartbeat"
    PROGRESS = "progress"
    SHORTAGE = "external_shortage"
    CONTROL_ACK = "control_ack"
    CHECKPOINT = "checkpoint"
    COMPLETED = "completed"