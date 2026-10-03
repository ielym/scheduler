from datetime import datetime

from sched.engine import next_fire, next_interval_window_fire, window_interval_containing
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Shanghai")


def _dt(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=TZ)


def test_fixed_interval():
    sched = {"type": "interval", "interval": 60}
    now = _dt("2026-10-03T10:00:00")
    nxt = next_fire(sched, now)
    assert nxt == _dt("2026-10-03T10:01:00")


def test_manual_no_fire():
    assert next_fire({"type": "manual"}, _dt("2026-10-03T10:00:00")) is None


def test_once():
    sched = {"type": "once", "once_at": "2026-10-03T12:00:00+08:00"}
    assert next_fire(sched, _dt("2026-10-03T10:00:00")) is not None


def test_window_interval_fire_within_window():
    sched = {"type": "interval", "interval": 3600, "start_time": "01:00", "end_time": "03:00"}
    now = _dt("2026-10-03T00:30:00")
    nxt = next_interval_window_fire(sched, now)
    assert nxt == _dt("2026-10-03T01:00:00")


def test_window_interval_skip_next_day():
    sched = {"type": "interval", "interval": 3600, "start_time": "01:00", "end_time": "03:00"}
    now = _dt("2026-10-03T02:30:00")
    nxt = next_interval_window_fire(sched, now)
    assert nxt == _dt("2026-10-03T03:00:00")


def test_window_containing():
    sched = {"type": "interval", "interval": 3600, "start_time": "01:00", "end_time": "03:00"}
    assert window_interval_containing(sched, _dt("2026-10-03T02:00:00")) is not None
    assert window_interval_containing(sched, _dt("2026-10-03T03:00:01")) is None


def test_cross_day_containing():
    sched = {"type": "interval", "interval": 3600, "start_time": "23:00", "end_time": "06:00"}
    win = window_interval_containing(sched, _dt("2026-10-04T01:00:00"))
    assert win is not None
    assert win[0] == _dt("2026-10-03T23:00:00")
    assert win[1] == _dt("2026-10-04T06:00:00")