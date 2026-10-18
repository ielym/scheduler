"""arXiv 批量回溯生产者（常驻）：发现日期范围内每个未完成日期并发布就绪作业。

- 稳定事件标识 = 目标日期（同一日期重放得到相同 job_id）。
- 游标持久化「已发布或已判定完成」的日期集合，重启后不重复检查/发布。
- 已完成日期（OSS _done.json 存在）跳过；失败/未完成由调度器对已入库作业退让重试。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

_SCHED_SRC = os.environ.get("SCHED_SRC", "/mnt/data01/projects/scheduler")
if _SCHED_SRC and os.path.isdir(_SCHED_SRC) and _SCHED_SRC not in sys.path:
    sys.path.insert(0, _SCHED_SRC)

from sched_task_sdk import Producer, atomic_write_json, run_producer  # noqa: E402

OSS_PAPERS_PREFIX = "ielym-data/academic/papers"
TICK_SEC = 60


def _ws() -> Path:
    return Path(os.environ.get("SCHED_WORKSPACE") or Path(__file__).resolve().parent)


def _log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | {msg}", flush=True)


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


def _oss_done(topic: str, date_str: str) -> bool:
    obj = f"oss://{OSS_PAPERS_PREFIX}/{topic}/{date_str}/_done.json"
    for attempt in range(3):
        try:
            r = subprocess.run(["aliyun-oss", "exists", obj],
                               capture_output=True, timeout=60)
            if r.returncode == 0:
                return True
            if r.returncode == 1:
                return False
        except Exception as e:
            _log(f"  OSS exists 异常 {e}")
        time.sleep(2 * (2 ** attempt))
    return False


def _date_range(start: date, end: date):
    out, cur = [], start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


class TopicProducer(Producer):
    def discover(self, cursor):
        cfg = _load_cfg()
        topic = cfg.get("topic")
        if not topic:
            return cursor, []
        start_d = date.fromisoformat(cfg.get("start_date") or "2026-01-01")
        end_d = date.fromisoformat(cfg.get("end_date") or "2026-09-30")
        all_dates = [d.isoformat() for d in _date_range(start_d, end_d)]
        seen = set((cursor or {}).get("seen_dates") or [])

        cfg_dir = _ws() / "config" / "dates"
        cfg_dir.mkdir(parents=True, exist_ok=True)
        jobs = []
        for date_str in all_dates:
            if date_str in seen:
                continue
            seen.add(date_str)
            if _oss_done(topic, date_str):
                continue
            rel = f"config/dates/{date_str}.json"
            # C4：per-date 配置原子写入——非原子 write_text 半写时 worker 读到
            # 截断 JSON 会误判为业务失败
            atomic_write_json(cfg_dir / f"{date_str}.json", {
                "topic": topic, "date": date_str, "query": cfg.get("query"),
            })
            jobs.append({
                "event_id": date_str,
                "business_version": "1",
                "entry_args": [],
                "input_refs": [rel],
                "idempotency_key": f"{topic}:{date_str}",
                "env": {"SCHED_CONFIG": rel},
            })
        if jobs:
            _log(f"[{topic}] 发布 {len(jobs)} 个未完成日期")
        return {"seen_dates": sorted(seen)}, jobs


def main() -> int:
    cfg = _load_cfg()
    if not cfg.get("topic"):
        print("缺 topic 配置", file=sys.stderr, flush=True)
        return 1
    run_producer(TopicProducer(), tick_sec=TICK_SEC)
    return 0


if __name__ == "__main__":
    sys.exit(main())