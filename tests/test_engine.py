"""讨论二简化版引擎测试：只测 coarse 节流——manual→None、auto+interval→now+interval。

已移除：next_interval_window_fire / window_interval_containing（窗口/星期/单次时刻）。
精确时机判断交给任务进程内 SDK reschedule() / exit_skip() 自行处理。
"""

from datetime import datetime, timedelta

from sched.engine import next_fire, TriggerEngine
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Shanghai")


def _dt(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=TZ)


def test_auto_interval_plus_now():
    sched = {"type": "auto", "interval": 60}
    now = _dt("2026-10-03T10:00:00")
    assert next_fire(sched, now) == _dt("2026-10-03T10:01:00")


def test_auto_interval_zero_or_missing_returns_none():
    # 非法 auto：interval 缺或非正，不算"自动给机会"
    assert next_fire({"type": "auto"}, _dt("2026-10-03T10:00:00")) is None
    assert next_fire({"type": "auto", "interval": 0}, _dt("2026-10-03T10:00:00")) is None
    assert next_fire({"type": "auto", "interval": -5}, _dt("2026-10-03T10:00:00")) is None


def test_manual_never_fires():
    assert next_fire({"type": "manual"}, _dt("2026-10-03T10:00:00")) is None
    # 旧 task.json 带 interval 时也不自动给（extra=ignore + schedule._check 会把 interval 置 None）
    assert next_fire({"type": "manual", "interval": 3600}, _dt("2026-10-03T10:00:00")) is None


def test_trigger_engine_upsert_basic():
    class _FakeDT:
        offset = 0
        @staticmethod
        def now(tz):
            return datetime.now(tz) + timedelta(seconds=_FakeDT.offset)
    import sched.engine as _m
    old = _m.next_fire
    try:
        # 直接测：TriggerEngine 容器的 upsert/advance/due/remove 都依赖 next_fire
        # 简化测法——用真实 next_fire 就行（interval 秒级足够）
        eng = TriggerEngine("Asia/Shanghai")
        eng.upsert("a", {"type": "auto", "interval": 3600})
        assert eng.next_run_time("a") is not None
        assert eng.due(datetime.now(TZ) + timedelta(hours=2)) == ["a"]
        eng.advance("a", {"type": "auto", "interval": 3600}, datetime.now(TZ) + timedelta(hours=2))
        assert eng.next_run_time("a") is not None
        eng.remove("a")
        assert eng.next_run_time("a") is None

        # manual 任务永不自动给机会
        eng.upsert("m", {"type": "manual"})
        assert eng.next_run_time("m") is None
    finally:
        pass


def test_trigger_engine_upsert_preserves_earlier_future():
    """重复 upsert 不应把已排好的下次触发整体往后推（保序规则）。"""
    eng = TriggerEngine("Asia/Shanghai")
    now = datetime.now(TZ)
    eng.upsert("j", {"type": "auto", "interval": 3600})
    first = eng.next_run_time("j")
    # 极短时间后再 upsert——next_fire 算出 now+3600 比 first 晚
    import time; time.sleep(0.01)
    eng.upsert("j", {"type": "auto", "interval": 3600})
    assert eng.next_run_time("j") == first  # 保留原时刻
