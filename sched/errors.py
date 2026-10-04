"""错误分类协议：任务侧与调度器侧共享的退出码/类别约定。

调度器不解析业务输出，但需要把「这次 run 为什么失败」归到可后续调度的类别：
- resource：资源不足/冲突（重试、退避、画像上调）
- logic：业务逻辑错误（不该重试，转人工）
- unclassified：任务未分类 —— 按需求 G5 一律当作 resource 处理（倒逼正确分类）

归类的主要信息来源是任务 SDK 写入 state.json 的 last_error.category，
退出码只作为兜底。退出码 99 被保留给「干净抢占（断点已保存）」。
"""

from __future__ import annotations


# 退出码保留段（协议 A2：与任务 SDK 约定）
EXIT_OK = 0
EXIT_PREEMPTED = 99         # 收到 SIGTERM，完成断点保存后主动退出（可恢复）
EXIT_RESOURCE = 100         # 本机资源错误（内存/CPU 不足等，可重试，触发画像上调）
EXIT_EXTERNAL_RESOURCE = 101  # 外部资源错误（代理/隧道、限流 429、配额、连接超时等）：指数退避重试
EXIT_DATA_RISK = 110        # 数据风险：跳过项已写入 pending_skipped，可继续二刷
EXIT_SKIP_SCHEDULE = 102    # 【讨论二 新增】调度器给机会但任务判断"当前不该跑"：
                            # 比如法定节假日、每月特定日期、收盘后无意义等。
                            # 任务通常会先调 SDK state.reschedule(delay_sec=N) 指定下次机会，
                            # 调度器读取 retry_hint.delay_sec 转为 not_before 重排队。
                            # 与 110 data_risk 的区别：没有二刷闭环，只是"跳过本轮 N 秒后再给机会"，
                            # 不受 data_risk_max_reschedules=3 限制。
EXIT_LOGIC = 1              # 逻辑错误的代表码（任务自报 logic 时使用；无自报时按 G5 兜底）
EXIT_SIGKILL = 137          # 128+9 —— 需要归因：cgroup OOM / 调度器 SIGKILL / 外部 kill

RESERVED_EXIT_CODES = {EXIT_PREEMPTED, EXIT_RESOURCE, EXIT_EXTERNAL_RESOURCE,
                       EXIT_DATA_RISK, EXIT_SKIP_SCHEDULE}