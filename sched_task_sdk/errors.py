"""任务侧错误分类。任务通过 resource()/logic() 显式归类，或用 classify_exception() 兜底。

默认判定（可按任务 error_rules.resource_regex / logic_regex 微调）：
命中网络/超时/429/连接/资源不足 → resource；其余 → logic。
"""

from __future__ import annotations

import re

RESOURCE_PATTERNS = [
    r"timeout",
    r"timed?\s*out",
    r"429",
    r"rate\s*limit",
    r"too\s*many\s*requests",
    r"connection",
    r"connect\s*error",
    r"proxy",
    r"socket",
    r"refused",
    r"unreachable",
    r"memory\s*error",
    r"out\s*of\s*memory",
    r"oom",
    r"resource\s*busy",
    r"quota",
    r"temporary",
    r"temporarily",
]


class Category:
    RESOURCE = "resource"
    LOGIC = "logic"
    UNCLASSIFIED = "unclassified"


def resource() -> str:
    return Category.RESOURCE


def logic() -> str:
    return Category.LOGIC


def classify_exception(
    exc: BaseException | None,
    resource_regex: list[str] | None = None,
    logic_regex: list[str] | None = None,
) -> dict:
    msg = f"{type(exc).__name__}: {exc}" if exc else ""
    rrules = list(resource_regex or []) + RESOURCE_PATTERNS
    lrules = list(logic_regex or [])
    for pat in lrules:
        if pat and re.search(pat, msg, re.IGNORECASE):
            return {"category": Category.LOGIC, "message": msg}
    for pat in rrules:
        if pat and re.search(pat, msg, re.IGNORECASE):
            return {"category": Category.RESOURCE, "message": msg}
    # 未命中任何规则：按调度体系 G5，未分类一律按资源处理（由调度器兜底），
    # 但 SDK 这里标记 unclassified，让调度器显式走 G5 而非静默。
    return {"category": Category.UNCLASSIFIED, "message": msg}