from sched_v002 import preempt


def _ri(run_id, priority, cpu, mem, soft, manual=False, exempt=False):
    return preempt.RunningInfo(
        run_id=run_id, job_id="j", priority=priority, est_cpu=cpu, est_mem_mb=mem,
        soft=soft, manual=manual, contract_exempt=exempt,
    )


def test_select_min_priority_first():
    running = [
        _ri("high", 90, 2.0, 200, {}),
        _ri("low", 10, 1.0, 100, {}),
        _ri("mid", 50, 1.5, 150, {}),
    ]
    targets = preempt.select_preempt_targets(running, need_cpu=1.5, need_mem_mb=300, need_soft={})
    # 按 priority 升序：low(10) → mid(50) 即可满足 mem 100+150=250 <300? 需要 low+mid+? 
    # need mem 300：low(100)+mid(150)=250 不够，还需 high(200)=450 但 high 优先级高已排最后
    # 贪心顺序 low(100) -> mid(150)->250 仍 <300 -> high(200)->450 够
    assert [t.run_id for t in targets] == ["low", "mid", "high"]


def test_select_skips_manual():
    running = [_ri("manual_low", 10, 2.0, 500, {}, manual=True)]
    targets = preempt.select_preempt_targets(running, need_cpu=2.0, need_mem_mb=500, need_soft={})
    assert targets == []


def test_select_not_enough_returns_empty():
    running = [_ri("x", 10, 0.5, 10, {})]
    targets = preempt.select_preempt_targets(running, need_cpu=1.0, need_mem_mb=100, need_soft={})
    assert targets == []


def test_freed_by_sum():
    a = _ri("a", 10, 1.0, 100, {"p": 2})
    b = _ri("b", 20, 2.0, 200, {"p": 3, "q": 1})
    cpu, mem, soft = preempt.freed_by([a, b])
    assert cpu == 3.0
    assert mem == 300
    assert soft == {"p": 5, "q": 1}