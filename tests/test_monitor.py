from sched_v002 import monitor


def _obs(run_id, mem_est, mem_actual=None, age=0.0, manual=False):
    return monitor.RunningObs(
        run_id=run_id, job_id="j", priority=0, est_mem_mb=mem_est,
        actual_mem_mb=mem_actual, heartbeat_age_sec=age, manual=manual,
    )


def test_heartbeat_kill():
    m = monitor.Monitor(heartbeat_timeout_sec=120, overload_ratio=2.0,
                        overload_sustain_ticks=3, tick_interval_sec=1.0)
    acts = m.tick([_obs("r1", 100, age=300)])
    assert len(acts) == 1
    assert acts[0].kind == "heartbeat_kill"


def test_manual_exempt_from_heartbeat():
    m = monitor.Monitor(heartbeat_timeout_sec=120, overload_ratio=2.0,
                        overload_sustain_ticks=3, tick_interval_sec=1.0)
    acts = m.tick([_obs("r1", 100, age=300, manual=True)])
    assert acts == []


def test_overload_requires_sustain():
    m = monitor.Monitor(heartbeat_timeout_sec=120, overload_ratio=2.0,
                        overload_sustain_ticks=3, tick_interval_sec=1.0)
    obs = _obs("r1", 100, mem_actual=250)  # 250 > 100*2.0
    assert m.tick([obs]) == []          # 第1次
    assert m.tick([obs]) == []          # 第2次
    acts = m.tick([obs])                # 第3次持续
    assert len(acts) == 1
    assert acts[0].kind == "overload_preempt"


def test_soft_error_rate_window():
    m = monitor.Monitor(heartbeat_timeout_sec=120, overload_ratio=2.0,
                        overload_sustain_ticks=3, tick_interval_sec=1.0)
    assert m.soft_error_rate("proxy") == 0
    m.record_soft_error("proxy")
    assert m.soft_error_rate("proxy") == 1