"""触发引擎（讨论二简化版）：只做 coarse 节流，精确时机判断交给任务 SDK。

- type=manual：从不自动给机会（调度器只做"盲给机会"的节流器）
- type=auto  + interval=N：每 N 秒给一次机会（now + interval）

不再做时间窗/星期/单次时刻的精确判定——任务进程内用任意规则（法定节假日、
每月特定日期、自定义日历）调 SDK reschedule(delay_sec) + exit_skip() 自行判断。

保留：TriggerEngine 容器、upsert 保序、advance、due、next_run_time。
删除：next_interval_window_fire、window_interval_containing、once_at 分支。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

log = logging.getLogger("sched.engine")


def next_fire(schedule: dict[str, Any], now: datetime) -> datetime | None:
    """按简化后的 schedule 算下一次"给机会"时刻。

    auto 且 interval>0 → now + interval；manual → None（永不自动给）；
    其他非法组合也返回 None。不做任何精确窗口/星期/单次时刻判定。
    """
    stype = (schedule or {}).get("type") or "manual"
    interval = schedule.get("interval")
    if stype == "auto" and interval:
        try:
            iv = int(interval)
        except (TypeError, ValueError):
            return None
        if iv > 0:
            return now + timedelta(seconds=iv)
    return None


class TriggerEngine:
    """按任务维护下次触发时刻，tick 时把到期任务交给回调。"""

    def __init__(self, timezone: str):
        self._tz = ZoneInfo(timezone)
        self._next: dict[str, datetime | None] = {}

    def upsert(self, job_id: str, schedule: dict[str, Any]) -> None:
        """注册/刷新触发配置。

        保序规则：若该任务已有"未到期"的下次触发，且不晚于按新配置算出的
        下次触发，则保留原时刻——避免热加载（refresh）把 auto 任务的
        触发时间整体重置为 now+interval，导致注册频繁时任务永不触发。
        """
        now = datetime.now(self._tz)
        nxt = next_fire(schedule, now)
        cur = self._next.get(job_id)
        if cur is not None and nxt is not None and cur > now and cur <= nxt:
            return
        if cur is None and nxt is None:
            self._next.pop(job_id, None)
            return
        self._next[job_id] = nxt

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
