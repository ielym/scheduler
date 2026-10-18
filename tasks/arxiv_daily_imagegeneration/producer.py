"""arXiv 每日生产者（常驻）：每天北京 15:00 发现 T-5 目标日期并发布一个就绪作业（arXiv 公布滞后 2-4 天、周末更长，T-2 会赶在公布前查到 0 篇）。

- 生产者在调度器之外独立运行，发现与发布解耦于执行压力。
- 稳定事件标识 = 目标日期；业务版本固定为 1 → 相同日期重放得到相同 job_id（幂等去重）。
- 发现游标（最后已发布日期）与待发布记录同一次原子持久化。
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

_SCHED_SRC = os.environ.get("SCHED_SRC", "/mnt/data01/projects/scheduler")
if _SCHED_SRC and os.path.isdir(_SCHED_SRC) and _SCHED_SRC not in sys.path:
    sys.path.insert(0, _SCHED_SRC)

from sched_task_sdk import Producer, atomic_write_json, run_producer  # noqa: E402

BEIJING = ZoneInfo("Asia/Shanghai")
RUN_AT_HOUR = 15
TICK_SEC = 300


def _ws() -> Path:
    return Path(os.environ.get("SCHED_WORKSPACE") or Path(__file__).resolve().parent)


def _load_cfg() -> dict:
    """任务静态参数统一在 task.json 的 params（tasks/ 下唯一静态定义）。"""
    td = os.environ.get("SCHED_TASK_DIR")
    base = Path(td) if td else Path(__file__).resolve().parent
    p = base / "task.json"
    try:
        if p.is_file():
            return json.loads(p.read_text("utf-8")).get("params") or {}
    except (OSError, ValueError):
        pass
    return {}


def _log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | {msg}", flush=True)


class DailyProducer(Producer):
    def discover(self, cursor):
        cfg = _load_cfg()
        topic = cfg.get("topic")
        if not topic:
            return cursor, []
        now = datetime.now(BEIJING)
        if now.hour < RUN_AT_HOUR:
            return cursor, []
        target = (now.date() - timedelta(days=7))  # T-7：arXiv 公布批次（周日~周四 20:00 ET）最大滞后 4 天 + 索引尾巴 1 天，7 天余量确保创建时数据必已完整
        date_str = target.isoformat()
        last = (cursor or {}).get("last_date")
        if last == date_str:
            return cursor, []      # 已发布，不重复发布

        # 参数写入配置文件（不靠 filename 反推）
        cfg_dir = _ws() / "config" / "daily"
        cfg_dir.mkdir(parents=True, exist_ok=True)
        rel = f"config/daily/{date_str}.json"
        # C4：per-date 配置原子写入——非原子 write_text 半写时 worker 读到
        # 截断 JSON 会误判为业务失败
        atomic_write_json(cfg_dir / f"{date_str}.json", {
            "topic": topic, "date": date_str, "query": cfg.get("query"),
        })

        jobs = [{
            "event_id": date_str,
            "business_version": "1",
            "entry_args": [],
            "input_refs": [rel],
            "idempotency_key": f"{topic}:{date_str}",
            "env": {"SCHED_CONFIG": rel},
        }]
        _log(f"[{topic}] 发布 T-2 目标 {date_str}")
        return {"last_date": date_str}, jobs


def main() -> int:
    cfg = _load_cfg()
    if not cfg.get("topic"):
        print("缺 topic 配置", file=sys.stderr, flush=True)
        return 1
    run_producer(DailyProducer(), tick_sec=TICK_SEC)
    return 0


if __name__ == "__main__":
    sys.exit(main())