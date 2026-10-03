from sched import preempt


def _ri(run_id, priority, cpu, mem, soft, manual=False, exempt=False):
    return preempt.RunningInfo(
        run_id=run_id, job_id="j", priority=priority, est_cpu=cpu, est_mem_mb=mem,
        soft=soft, manual=manual, contract_exempt=exempt,
    )


def test_select_min_priority_first():
    running = [
        _ri("low", 10, 1.0, 100, {}),
        _ri("mid", 50, 1.5, 150, {}),
    ]
    # 等待者 priority=90：low(10)→mid(50) 即可补足，按升序逐个点名
    targets = preempt.select_preempt_targets(running, need_cpu=1.5, need_mem_mb=200, need_soft={},
                                             waiter_priority=90)
    assert [t.run_id for t in targets] == ["low", "mid"]


def test_select_never_preempts_higher_or_equal_priority():
    # 等待者 priority=50，运行中 P=90（更高）与 P=50（同级）均不得入选
    running = [
        _ri("high", 90, 2.0, 200, {}),
        _ri("eq", 50, 1.5, 150, {}),
    ]
    targets = preempt.select_preempt_targets(running, need_cpu=1.0, need_mem_mb=100, need_soft={},
                                             waiter_priority=50)
    assert targets == []


def test_select_skips_contract_exempt_but_not_manual():
    # 抢占资格一律由优先级决定；manual 可恢复任务不再豁免。
    # 仅 contract_exempt（注册时声明不可断点恢复）仍不可被抢占。
    running = [
        _ri("manual_low", 10, 2.0, 500, {}, manual=True),
        _ri("exempt_low", 10, 1.0, 100, {}, exempt=True),
    ]
    targets = preempt.select_preempt_targets(running, need_cpu=2.0, need_mem_mb=500, need_soft={},
                                             waiter_priority=90)
    assert [t.run_id for t in targets] == ["manual_low"]


def test_select_not_enough_returns_empty():
    running = [_ri("x", 10, 0.5, 10, {})]
    targets = preempt.select_preempt_targets(running, need_cpu=1.0, need_mem_mb=100, need_soft={},
                                             waiter_priority=90)
    assert targets == []


def test_freed_by_sum():
    a = _ri("a", 10, 1.0, 100, {"p": 2})
    b = _ri("b", 20, 2.0, 200, {"p": 3, "q": 1})
    cpu, mem, soft = preempt.freed_by([a, b])
    assert cpu == 3.0
    assert mem == 300
    assert soft == {"p": 5, "q": 1}
