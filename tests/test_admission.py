from sched_v002 import admission, resources


def test_order_waiting_priority_then_fifo():
    items = [
        {"run_id": "a", "priority": 10, "enqueued_at": "2026-10-03T10:00:00+08:00"},
        {"run_id": "b", "priority": 50, "enqueued_at": "2026-10-03T10:00:05+08:00"},
        {"run_id": "c", "priority": 50, "enqueued_at": "2026-10-03T10:00:03+08:00"},
        {"run_id": "d", "priority": 5, "enqueued_at": "2026-10-03T10:00:00+08:00"},
    ]
    ordered = admission.order_waiting(items, 3600, "2026-10-03T10:00:10+08:00")
    ids = [i["run_id"] for i in ordered]
    # priority 降序；同层 enqueued_at 升序 → c 先于 b
    assert ids == ["c", "b", "a", "d"]


def test_order_waiting_starvation_promote():
    items = [
        {"run_id": "low", "priority": 0, "enqueued_at": "2026-10-03T08:00:00+08:00"},
        {"run_id": "high", "priority": 50, "enqueued_at": "2026-10-03T10:00:00+08:00"},
    ]
    # 低优先已等超过 1h → 提权排最前
    ordered = admission.order_waiting(items, 3600, "2026-10-03T10:00:00+08:00")
    assert [i["run_id"] for i in ordered] == ["low", "high"]


def test_estimate_job_no_lower_than_declared():
    prof = resources.Profile(cpu=0.2, memory_mb=100, declared_cpu=1.0, declared_memory_mb=256)
    est_cpu, est_mem = admission.estimate_job(prof, 1.0, 256, 1.2)
    assert est_cpu >= 1.0
    assert est_mem >= 256


def test_can_admit_fixed_memory_starved():
    cap = resources.Capacity()
    ledger = resources.Ledger()
    soft = resources.SoftResourcePool()
    # 声明超出机器上限 → 拒绝
    ok, reason = admission.can_admit(0.1, cap.total_memory_mb + 1, {}, ledger, cap, soft)
    assert not ok
    assert "内存" in reason


def test_can_admit_soft_unconfigured():
    cap = resources.Capacity()
    ledger = resources.Ledger()
    soft = resources.SoftResourcePool()
    ok, reason = admission.can_admit(0.1, 10, {"proxy": 3}, ledger, cap, soft)
    assert not ok
    assert "未配置" in reason


def test_admission_scan_blocks_and_admits():
    cap = resources.Capacity()
    ledger = resources.Ledger()
    soft = resources.SoftResourcePool()
    soft.set_capacity("proxy", 2)
    items = [
        {"run_id": "r1", "job_id": "j1"},
        {"run_id": "r2", "job_id": "j2"},
    ]
    declared = {"j1": (0.1, 10, {"proxy": 3}), "j2": (0.1, 10, {"proxy": 1})}
    res = admission.admission_scan(
        items,
        get_declared=lambda jid: declared[jid],
        get_profile=lambda jid: resources.Profile(declared_cpu=declared[jid][0], declared_memory_mb=declared[jid][1]),
        ledger=ledger, capacity=cap, soft_pool=soft, overshoot=1.2,
    )
    assert res.admitted == ["r2"]
    assert "r1" in res.blocked_soft