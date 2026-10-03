"""错误分类协议：任务侧与调度器侧共享的退出码/类别约定。

调度器不解析业务输出，但需要把「这次 run 为什么失败」归到可后续调度的类别：
- resource：资源不足/冲突（重试、退避、画像上调）
- logic：业务逻辑错误（不该重试，转人工）
- unclassified：任务未分类 —— 按需求 G5 一律当作 resource 处理（倒逼正确分类）

归类的主要信息来源是任务 SDK 写入 state.json 的 last_error.category，
退出码只作为兜底。退出码 99 被保留给「干净抢占（断点已保存）」。
"""

from __future__ import annotations

from enum import Enum


class Category(str, Enum):
    RESOURCE = "resource"
    LOGIC = "logic"
    UNCLASSIFIED = "unclassified"


# 退出码保留段（协议 A2：与任务 SDK 约定）
EXIT_OK = 0
EXIT_PREEMPTED = 99         # 收到 SIGTERM，完成断点保存后主动退出（可恢复）
EXIT_RESOURCE = 100         # 任务明确分类的资源错误（可重试，触发画像上调）
EXIT_SOFT_RESOURCE = 101    # 软资源错误（代理并发/429/配额），走软资源令牌与退避
EXIT_DATA_RISK = 110        # 数据风险：跳过项已写入 pending_skipped，可继续二刷
EXIT_LOGIC = 1              # 逻辑错误的代表码（任务自报 logic 时使用；无自报时按 G5 兜底）
EXIT_SIGKILL = 137          # 128+9 —— 需要归因：cgroup OOM / 调度器 SIGKILL / 外部 kill

RESERVED_EXIT_CODES = {EXIT_PREEMPTED, EXIT_RESOURCE, EXIT_SOFT_RESOURCE, EXIT_DATA_RISK}


def classify_exit_code(code: int | None) -> tuple[str, str]:
    """按退出码做兜底归类，返回 (category, reason)。

    99=干净抢占、100/101=资源、110=数据风险、137=待归因；
    其余非 0（含通用 1）一律按 G5 归为 unclassified → 调度器按资源错误重试，
    倒逼任务通过 last_error.category 显式上报逻辑错误（R19：宁重试不错杀）。
    """
    if code == EXIT_OK:
        return Category.RESOURCE.value, "success"  # 成功不算失败，不该走到这里
    if code == EXIT_PREEMPTED:
        return Category.UNCLASSIFIED.value, "preempted"
    if code in (EXIT_RESOURCE, EXIT_SOFT_RESOURCE):
        return Category.RESOURCE.value, f"exit_code_{code}"
    if code == EXIT_DATA_RISK:
        return Category.LOGIC.value, "data_risk"
    if code == EXIT_SIGKILL:
        return Category.UNCLASSIFIED.value, "sigkill_pending_attribution"
    # G5：无任务自报分类时，未知非 0 退出码一律按资源错误处理（可重试）
    return Category.UNCLASSIFIED.value, f"exit_code_{code}_unclassified"


def is_retryable_category(category: str) -> bool:
    """resource 与 unclassified（G5 兜底按资源处理）都可重试；logic 不可重试。"""
    return category in (Category.RESOURCE.value, Category.UNCLASSIFIED.value)