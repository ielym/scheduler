"""arXiv 批量回溯生产者（常驻）：**分批**扫描日期范围，扫到未完成日期即发布就绪作业。

- 稳定事件标识 = 目标日期（同一日期重放得到相同 job_id）。
- 游标 = `{scanned_to, seen_dates}`：`scanned_to` 是「已扫到哪个日期」，
  `seen_dates` 是「已检查过（含已判定完成）的日期集合」。重启后从 `scanned_to`
  继续，不从头重扫。
- **每批扫完立即发布**（不再全量扫完才一次性发布）：首批作业在数秒内面世。
- 批内 `_oss_done` 并发检查（降墙钟，不改变逐日语义）。
- 已完成日期（OSS `_done.json` 存在）跳过；失败/未完成由调度器对已入库作业退让重试。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

_SCHED_SRC = os.environ.get("SCHED_SRC", "/mnt/data01/projects/scheduler")
if _SCHED_SRC and os.path.isdir(_SCHED_SRC) and _SCHED_SRC not in sys.path:
    sys.path.insert(0, _SCHED_SRC)

from sched_task_sdk import Producer, atomic_write_json, run_producer  # noqa: E402

OSS_PAPERS_PREFIX = "ielym-data/academic/papers"
TICK_SEC = 1              # 分批循环节拍（实际节拍由批次 OSS 检查耗时决定）
SCAN_BATCH = 20           # 每批检查的日期数：扫完即发布，不累计到全量
SCAN_WORKERS = 8          # 批内 OSS exists 并发度
DAY = timedelta(days=1)


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


class TopicProducer(Producer):
    def discover(self, cursor):
        cfg = _load_cfg()
        topic = cfg.get("topic")
        if not topic:
            return cursor, []
        start_d = date.fromisoformat(cfg.get("start_date") or "2026-01-01")
        end_d = date.fromisoformat(cfg.get("end_date") or "2026-09-30")
        cur = (cursor or {}).get("scanned_to")
        seen = set((cursor or {}).get("seen_dates") or [])

        # 从游标续扫；已扫到末尾则绕回起点（此时 seen 已基本齐全，绕回几乎零 OSS 调用）
        pos = (date.fromisoformat(cur) + DAY) if cur else start_d
        if pos > end_d:
            pos = start_d

        # 组批：推进扫描位置，只把「未见过」的日期计入本批（已见日期零 OSS 调用）
        batch: list[str] = []
        last = cur
        while pos <= end_d and len(batch) < SCAN_BATCH:
            ds = pos.isoformat()
            last = ds
            if ds not in seen:
                batch.append(ds)
            pos += DAY

        # 批内并发检查 OSS 完成标记（逐日语义不变，仅降墙钟）
        done: dict[str, bool] = {}
        if batch:
            with ThreadPoolExecutor(max_workers=min(SCAN_WORKERS, len(batch))) as ex:
                for ds, d in zip(batch, ex.map(lambda s: _oss_done(topic, s), batch)):
                    done[ds] = d

        cfg_dir = _ws() / "config" / "dates"
        jobs = []
        for ds in batch:
            seen.add(ds)
            if done.get(ds):
                continue
            cfg_dir.mkdir(parents=True, exist_ok=True)
            rel = f"config/dates/{ds}.json"
            # C4：per-date 配置原子写入——非原子 write_text 半写时 worker 读到
            # 截断 JSON 会误判为业务失败
            atomic_write_json(cfg_dir / f"{ds}.json", {
                "topic": topic, "date": ds, "query": cfg.get("query"),
            })
            jobs.append({
                "event_id": ds,
                "business_version": "1",
                "entry_args": [],
                "input_refs": [rel],
                "idempotency_key": f"{topic}:{ds}",
                "env": {"SCHED_CONFIG": rel},
            })
        if batch:
            _log(f"[{topic}] 扫描至 {last}（本批 {len(batch)} 天，发布 {len(jobs)} 个未完成日期）")
        return {"scanned_to": last, "seen_dates": sorted(seen)}, jobs


def main() -> int:
    cfg = _load_cfg()
    if not cfg.get("topic"):
        print("缺 topic 配置", file=sys.stderr, flush=True)
        return 1
    run_producer(TopicProducer(), tick_sec=TICK_SEC)
    return 0


if __name__ == "__main__":
    sys.exit(main())
