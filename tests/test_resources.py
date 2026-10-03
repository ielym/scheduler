from sched import resources


def test_profile_p95():
    p = resources.Profile(declared_cpu=1.0, declared_memory_mb=100)
    for mem in [50, 60, 70, 80, 90, 100, 110, 120]:
        p.record_sample(0.5, mem, window=8)
    assert p.p95_memory_mb() == 120


def test_profile_estimate_from_declared_when_no_samples():
    p = resources.Profile(declared_cpu=0.5, declared_memory_mb=128)
    cpu, mem = p.estimate(1.2)
    assert cpu == round(0.5 * 1.2, 2)
    assert mem == int(128 * 1.2)


def test_ledger_reserve_release():
    led = resources.Ledger()
    led.reserve("r1", 1.0, 100, {"proxy": 2})
    led.reserve("r2", 2.0, 200, {"proxy": 1})
    assert led.reserved_cpu() == 3.0
    assert led.reserved_mem_mb() == 300
    assert led.reserved_soft("proxy") == 3
    led.release("r1")
    assert led.reserved_mem_mb() == 200
    assert led.reserved_soft("proxy") == 1


def test_ledger_actual():
    led = resources.Ledger()
    led.set_actual("r1", 1.5, 150)
    assert led.actual_cpu() == 1.5
    assert led.actual_mem_mb() == 150


def test_soft_pool_acquire_release():
    pool = resources.SoftResourcePool()
    pool.set_capacity("proxy", 5)
    assert pool.acquire("proxy", 3) is True
    assert pool.available("proxy") == 2
    assert pool.acquire("proxy", 3) is False
    pool.release("proxy", 3)
    assert pool.available("proxy") == 5


def test_soft_pool_zero_amount_always_ok():
    pool = resources.SoftResourcePool()
    # 容量未配置，但请求量 0 → 视为满足
    assert pool.acquire("unset", 0) is True


def test_capacity_reads_host():
    cap = resources.Capacity()
    assert cap.total_memory_mb > 0
    assert cap.total_cpu >= 1