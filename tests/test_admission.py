from sched import admission, resources


def test_order_waiting_priority_then_fifo():
    items = [
        {"run_id": "a", "priority": 10, "enqueued_at": "2026-10-03T10:00:00+08:00"},
        {"run_id": "b", "priority": 50, "enqueued_at": "2026-10-03T10:00:05+08:00"},
        {"run_id": "c", "priority": 50, "enqueued_at": "2026-10-03T10:00:03+08:00"},
        {"run_id": "d", "priority": 50, "enqueued_at": "2026-10-03T10:00:00+08:00"},
    ]
    ordered = admission.order_waiting(items, 3600, "2026-10-03T10:00:10+08:00")
    ids = [i["run_id"] for i in ordered]
    # priority 降序；同层 enqueued_at 升序 → d/c/b 先于 a，且 d 先于 c 先于 b
    assert ids == ["d", "c", "b", "a"]


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
    # 声明超出机器上限 → 拒绝
    ok, reason = admission.can_admit(0.1, cap.total_memory_mb + 1, ledger, cap)
    assert not ok
    assert "内存" in reason


def test_can_admit_fixed_cpu_starved():
    cap = resources.Capacity()
    ledger = resources.Ledger()
    ok, reason = admission.can_admit(cap.total_cpu + 1.0, 10, ledger, cap)
    assert not ok
    assert "CPU" in reason


def test_admission_scan_blocks_and_admits():
    cap = resources.Capacity()
    ledger = resources.Ledger()
    # 先占住绝大部分内存：j1 声明为整机一半（可进），j2 声明超整机（必被挡）
    half = max(64, cap.total_memory_mb // 2)
    over = cap.total_memory_mb
    items = [
        {"run_id": "r1", "job_id": "j1"},
        {"run_id": "r2", "job_id": "j2"},
    ]
    declared = {"j1": (0.1, half), "j2": (0.1, over)}
    res = admission.admission_scan(
        items,
        get_declared=lambda jid: declared[jid],
        get_profile=lambda jid: resources.Profile(declared_cpu=declared[jid][0], declared_memory_mb=declared[jid][1]),
        ledger=ledger, capacity=cap, overshoot=1.0,
    )
    assert res.admitted == ["r1"]
    assert res.blocked == ["r2"]
    assert "内存" in res.reasons["r2"]
