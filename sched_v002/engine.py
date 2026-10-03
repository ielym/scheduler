"""触发引擎（自实现，不依赖 APScheduler）：interval/once/manual 与时间窗/星期的下次触发计算。

纯函数计算下次触发时刻；带时间窗的 interval 用「窗口起点对齐的 N 秒网格」，支持跨天窗口。
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

log = logging.getLogger("sched.engine")


def _parse_hms(value: Any) -> int:
    parts = [int(x) for x in str(value).split(":")]
    while len(parts) < 3:
        parts.append(0)
    return parts[0] * 3600 + parts[1] * 60 + parts[2]


def next_interval_window_fire(schedule: dict[str, Any], now: datetime) -> datetime | None:
    """从 now 向后（最多 8 天）找下一个「星期命中 + 窗口内 + 起点对齐」的时刻。"""
    try:
        start_sec = _parse_hms(schedule.get("start_time"))
        end_sec = _parse_hms(schedule.get("end_time"))
    except (TypeError, ValueError):
        return None
    cross_day = end_sec < start_sec
    raw_interval = schedule.get("interval")
    interval = int(raw_interval) if raw_interval else None
    if interval is not None and interval <= 0:
        return None
    raw_days = schedule.get("weekdays") or []
    try:
        days = {int(x) for x in raw_days} or None
    except (TypeError, ValueError):
        return None

    tz = now.tzinfo
    for delta in range(-1, 9):
        day = (now + timedelta(days=delta)).date()
        if days is not None and day.isoweekday() not in days:
            continue
        day_start = datetime(day.year, day.month, day.day, tzinfo=tz) + timedelta(seconds=start_sec)
        day_end = datetime(day.year, day.month, day.day, tzinfo=tz) + timedelta(seconds=end_sec)
        if cross_day:
            day_end += timedelta(days=1)
        if day_end < now:
            continue
        if interval is None:
            if day_start >= now:
                return day_start
            continue
        if now <= day_start:
            cand = day_start
        else:
            steps = math.ceil((now - day_start).total_seconds() / interval)
            cand = day_start + timedelta(seconds=steps * interval)
        if cand <= day_end:
            return cand
    return None


def window_interval_containing(schedule: dict[str, Any], ts: datetime) -> tuple[datetime, datetime] | None:
    try:
        start_sec = _parse_hms(schedule.get("start_time"))
        end_sec = _parse_hms(schedule.get("end_time"))
    except (TypeError, ValueError):
        return None
    cross_day = end_sec < start_sec
    raw_days = schedule.get("weekdays") or []
    try:
        days = {int(x) for x in raw_days} or None
    except (TypeError, ValueError):
        return None
    tz = ts.tzinfo
    for delta in (0, -1):
        day = (ts + timedelta(days=delta)).date()
        if days is not None and day.isoweekday() not in days:
            continue
        day_start = datetime(day.year, day.month, day.day, tzinfo=tz) + timedelta(seconds=start_sec)
        day_end = datetime(day.year, day.month, day.day, tzinfo=tz) + timedelta(seconds=end_sec)
        if cross_day:
            day_end += timedelta(days=1)
        if day_start <= ts <= day_end:
            return day_start, day_end
    return None


def next_fire(schedule: dict[str, Any], now: datetime) -> datetime | None:
    stype = (schedule or {}).get("type") or "manual"
    if stype == "interval":
        if schedule.get("start_time") and schedule.get("end_time"):
            return next_interval_window_fire(schedule, now)
        interval = schedule.get("interval")
        if not interval:
            return None
        # 纯固定间隔：从 now 起一个 interval 后
        return now + timedelta(seconds=int(interval))
    if stype == "once" and schedule.get("once_at"):
        try:
            return datetime.fromisoformat(str(schedule["once_at"]))
        except ValueError:
            return None
    return None


class TriggerEngine:
    """按任务维护下次触发时刻，tick 时把到期任务交给回调。"""

    def __init__(self, timezone: str):
        self._tz = ZoneInfo(timezone)
        self._next: dict[str, datetime | None] = {}

    def upsert(self, job_id: str, schedule: dict[str, Any]) -> None:
        self._next[job_id] = next_fire(schedule, datetime.now(self._tz))

    def remove(self, job_id: str) -> None:
        self._next.pop(job_id, None)

    def next_run_time(self, job_id: str) -> str | None:
        dt = self._next.get(job_id)
        return dt.isoformat(timespec="seconds") if dt else None

    def due(self, now: datetime) -> list[str]:
        out = []
        for jid, nxt in list(self._next.items()):
            if nxt is not None and now >= nxt:
                out.append(jid)
        return out

    def advance(self, job_id: str, schedule: dict[str, Any], now: datetime) -> None:
        self._next[job_id] = next_fire(schedule, now)