"""退出码与事件种类约定（与调度器 sched/config.py 镜像）。

协议约定：
- 0  成功完成（必须已写入持久化完成记录 done）
- 75 统一外部不足（资源类错误，可恢复退让）
- 其他非 0 业务/未知失败

事件种类统一为 5 种（通信格式统一，type 只区分语义）：
- started  ：进程启动（可选 name，纯可观测）
- progress ：业务推进（completed_units 单调递增，可选 stage）
- error    ：错误终止前的归因（reason 携带错误类型+资源标识）；退出码决定
            性质——75=资源不足→退避，其他=业务失败
- done     ：成功完成（可选 record）
- ack      ：确认收到某条调度命令（command_id）

协作暂停：worker 在收到暂停命令后，提交检查点、发送 ack，随后正常退出；
调度器由自身发起的 PAUSING 意图识别为 PAUSED，而不依赖特殊退出码。
"""

EXIT_OK = 0
EXIT_SHORTAGE = 75


class EventKind:
    STARTED = "started"
    PROGRESS = "progress"
    ERROR = "error"
    DONE = "done"
    ACK = "ack"