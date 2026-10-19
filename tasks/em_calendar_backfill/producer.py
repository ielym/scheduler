"""em_calendar_backfill 生产者（有限任务）：动态发现市场 → 每市场一个作业。

- 市场/资产集合**每轮实时发现**（discovery），不写死清单；发现一次后记入游标复用。
- 每个市场/资产键发布一个独立作业（event_id = key）——「每市场一个作业」。
- 幂等跳过：OSS 存在 `_meta/<key>.meta.json` ⇒ 已完成，不再发布。
- 已发布以 **job_id** 记账：`business_version` 变更 → 新 job_id → 自动重发（用于布局/口径升级）。
- 静态参数（start_date）读自 task.json.params。
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

_SCHED_SRC = os.environ.get("SCHED_SRC", "/mnt/data01/projects/scheduler")
if _SCHED_SRC and os.path.isdir(_SCHED_SRC) and _SCHED_SRC not in sys.path:
    sys.path.insert(0, _SCHED_SRC)

from sched_task_sdk import Producer, atomic_write_json, job_id_of, run_producer  # noqa: E402

from emlib import discovery, oss  # noqa: E402
from emlib.version import VERSION  # noqa: E402

TICK_SEC = 60
# 业务版本 = emlib.version.VERSION（schema v3：+ tz_local/sessions）；变更即重发


def _task_dir() -> Path:
    return Path(os.environ.get("SCHED_TASK_DIR") or Path(__file__).resolve().parent)


def _ws() -> Path:
    return Path(os.environ.get("SCHED_WORKSPACE") or Path.cwd())


def _task_id() -> str:
    return os.environ.get("SCHED_TASK_ID") or "em_calendar_backfill"


def _params() -> dict:
    p = _task_dir() / "task.json"
    try:
        if p.is_file():
            return json.loads(p.read_text("utf-8")).get("params") or {}
    except (OSError, ValueError):
        pass
    return {}


def _done(key: str) -> bool:
    return oss.exists(oss.obj(f"_meta/{key}.v{VERSION}.meta.json"))


class CalendarBackfillProducer(Producer):
    def discover(self, cursor):
        cursor = cursor or {}
        plan = cursor.get("plan")
        params = _params()

        # 首轮：实时发现市场/资产集合（动态，不写死）
        if not plan:
            cands = discovery.discover_candidates()
            plan = {}
            for c in cands:
                plan.setdefault(c["key"], []).append(c)
            print(f"[producer] 动态发现 {len(plan)} 个市场/资产键：{sorted(plan)}", flush=True)

        published_ids = set(cursor.get("published_ids") or [])
        jobs = []
        for key, targets in plan.items():
            if _done(key):
                continue
            jid = job_id_of(_task_id(), key, VERSION)
            if jid in published_ids:
                continue
            rel = f"config/units/{key}.json"
            cfg_dir = _ws() / "config" / "units"
            cfg_dir.mkdir(parents=True, exist_ok=True)
            atomic_write_json(cfg_dir / f"{key}.json", {
                "key": key, "targets": targets, "start_date": params.get("start_date"),
            })
            jobs.append({
                "event_id": key, "business_version": VERSION,
                "input_refs": [rel], "idempotency_key": f"calendar_backfill:{key}",
                "env": {"SCHED_CONFIG": rel}, "entry_args": [],
            })
            published_ids.add(jid)

        return {"plan": plan, "published_ids": sorted(published_ids)}, jobs


def main() -> int:
    run_producer(CalendarBackfillProducer(), tick_sec=TICK_SEC)
    return 0


if __name__ == "__main__":
    sys.exit(main())
