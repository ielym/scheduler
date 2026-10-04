"""admission 排序：饿死提权、同优先级 cold-start round-robin → SJF（有数据后）。"""

from sched import admission

# 对不关心饿死提权的用例，用超长阈值彻底屏蔽提权，只测本层级逻辑
NO_BOOST = 1 << 30


def _item(run_id, sid, pri=50, enq="2026-10-04T10:00:00+08:00"):
    return {"run_id": run_id, "subtask_id": sid, "priority": pri,
            "enqueued_at": enq, "resources": {"cpu": 0.1, "memory_mb": 16}}


class TestOrderWaiting:
    def test_same_priority_cold_start_round_robin(self):
        # 三个 subtask 各 3 个意图，均无画像时长数据 → 轮流，每个 subtask 先出列一个
        items = []
        for sid in ("a", "b", "c"):
            for i in range(3):
                items.append(_item(f"{sid}{i}", sid, enq=f"2026-10-04T10:00:{i:02d}+08:00"))
        ordered = admission.order_waiting(items, starvation_sec=NO_BOOST,
                                          now_iso="2026-10-04T10:01:00+08:00",
                                          get_duration_sec=lambda s: None)
        sids = [it["subtask_id"] for it in ordered]
        assert sorted(sids[:3]) == ["a", "b", "c"]  # 前三个覆盖三个 subtask

    def test_same_priority_sjf_when_data_available(self):
        # 有画像时长数据 → 短作业优先
        items = [
            _item("a1", "a", enq="2026-10-04T10:00:01+08:00"),
            _item("b1", "b", enq="2026-10-04T10:00:02+08:00"),
            _item("c1", "c", enq="2026-10-04T10:00:03+08:00"),
        ]
        durations = {"a": 100.0, "b": 1.0, "c": 50.0}
        ordered = admission.order_waiting(items, starvation_sec=NO_BOOST,
                                          now_iso="2026-10-04T10:01:00+08:00",
                                          get_duration_sec=lambda s: durations.get(s))
        assert [it["subtask_id"] for it in ordered] == ["b", "c", "a"]

    def test_high_priority_always_first(self):
        items = [
            _item("low", "low", pri=10),
            _item("high", "high", pri=100),
        ]
        ordered = admission.order_waiting(items, starvation_sec=NO_BOOST,
                                          now_iso="2026-10-04T10:01:00+08:00",
                                          get_duration_sec=lambda s: None)
        assert [it["run_id"] for it in ordered] == ["high", "low"]

    def test_starvation_boosts_above_priority(self):
        # low-old 等超 60s 被提权到 101；high 只等 30s 保持原优先级 100
        items = [
            _item("low-old", "low", pri=10, enq="2026-10-04T09:00:00+08:00"),
            _item("high", "high", pri=100, enq="2026-10-04T10:00:30+08:00"),
        ]
        ordered = admission.order_waiting(items, starvation_sec=60,
                                          now_iso="2026-10-04T10:01:00+08:00",
                                          get_duration_sec=lambda s: None)
        assert ordered[0]["run_id"] == "low-old"  # 饿死提权 101 压过 priority 100