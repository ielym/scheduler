"""Bootstrap 脚本：为 8 个 topic 一次性创建 controller intent。

用法：python3 /mnt/data01/projects/scheduler/tasks/shared/bootstrap.py

执行后 scheduler 会在下一个 tick 扫到 requests/ 里的 controller intent 并拉起。
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

_SCHED_SRC = os.environ.get("SCHED_SRC", "/mnt/data01/projects/scheduler")
if _SCHED_SRC and os.path.isdir(_SCHED_SRC) and _SCHED_SRC not in sys.path:
    sys.path.insert(0, _SCHED_SRC)

from sched_task_sdk import emit_intent  # noqa: E402

TASKS_DIR = Path("/mnt/data01/projects/scheduler/tasks")

TOPICS = [
    "arxiv_topic_videogeneration",
    "arxiv_topic_imagegeneration",
    "arxiv_topic_rlhf",
    "arxiv_topic_agentic",
    "arxiv_topic_llm",
    "arxiv_topic_multimodal",
    "arxiv_topic_worldmodel",
    "arxiv_topic_embodied",
]


def bootstrap_one(task_id: str) -> bool:
    task_dir = TASKS_DIR / task_id
    if not task_dir.is_dir():
        print(f"[{task_id}] 目录不存在，跳过")
        return False
    manifest = task_dir / "manifest.json"
    if not manifest.is_file():
        print(f"[{task_id}] 缺 manifest.json，跳过")
        return False

    os.environ["SCHED_WORKSPACE"] = str(task_dir)

    controller_rid = f"ctrl-{task_id}-2026"
    existing_files = list((task_dir / "requests").glob(f"{controller_rid}.json")) if (task_dir / "requests").is_dir() else []
    if existing_files:
        print(f"[{task_id}] controller intent 已存在（{existing_files[0].name}），跳过")
        return True

    emit_intent(
        request_id=controller_rid,
        kind="oneshot",
        entry={"file": "controller.py", "interpreter": "python3", "args": [], "cwd": "."},
        config="",
        priority=60,  # 高优先级：先让 controller 跑完 emit 所有日期
        resources={"cpu": 0.1, "memory_mb": 64},
        lifetime={
            "completion": "terminal",
            "retry": {
                "backoff_base_sec": 5,
                "backoff_cap_sec": 60,
                "not_after": "",
            },
        },
    )
    print(f"[{task_id}] 已创建 controller intent → requests/{controller_rid}.json")
    return True


if __name__ == "__main__":
    successes = 0
    for t in TOPICS:
        if bootstrap_one(t):
            successes += 1
        time.sleep(0.3)  # 避免瞬时 8 个并发写

    print(f"\nBootstrap 完成：{successes}/{len(TOPICS)} 个 topic 已创建 controller intent")
    print("等待 scheduler 下一个 tick（约 5-10 秒）自动拉起 controller...")
