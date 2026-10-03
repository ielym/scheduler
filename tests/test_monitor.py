from sched_v002 import monitor


def _obs(run_id, mem_est, mem_actual=None, age=0.0, manual=False, hb_timeout=None):
    return monitor.RunningObs(
        run_id=run_id, job_id="j", priority=0, est_mem_mb=mem_est,
        actual_mem_mb=mem_actual, heartbeat_age_sec=age, manual=manual,
        heartbeat_timeout_sec=hb_timeout,
    )


def test_heartbeat_kill():
    m = monitor.Monitor(heartbeat_timeout_sec=120, overload_ratio=2.0,
                        overload_sustain_ticks=3, tick_interval_sec=1.0)
    acts = m.tick([_obs("r1", 100, age=300)])
    assert len(acts) == 1
    assert acts[0].kind == "heartbeat_kill"


def test_manual_heartbeat_relaxed_not_exempt():
    # D8：manual 任务不再豁免心跳，仅放宽阈值（默认 ×2）
    m = monitor.Monitor(heartbeat_timeout_sec=120, overload_ratio=2.0,
                        overload_sustain_ticks=3, tick_interval_sec=1.0)
    # 放宽阈值 240 内不杀
    acts = m.tick([_obs("r1", 100, age=200, manual=True, hb_timeout=240)])
    assert acts == []
    # 超过放宽阈值仍会杀（杜绝无限占用）
    acts = m.tick([_obs("r1", 100, age=300, manual=True, hb_timeout=240)])
    assert len(acts) == 1
    assert acts[0].kind == "heartbeat_kill"


def test_manual_exempt_from_overload():
    # manual 任务仍豁免超限退让（由触发者负责）
    m = monitor.Monitor(heartbeat_timeout_sec=120, overload_ratio=2.0,
                        overload_sustain_ticks=1, tick_interval_sec=1.0)
    obs = _obs("r1", 100, mem_actual=300, manual=True)
    assert m.tick([obs]) == []


def test_overload_requires_sustain():
    m = monitor.Monitor(heartbeat_timeout_sec=120, overload_ratio=2.0,
                        overload_sustain_ticks=3, tick_interval_sec=1.0)
    obs = _obs("r1", 100, mem_actual=250)  # 250 > 100*2.0
    assert m.tick([obs]) == []          # 第1次
    assert m.tick([obs]) == []          # 第2次
    acts = m.tick([obs])                # 第3次持续
    assert len(acts) == 1
    assert acts[0].kind == "overload_preempt"


def test_soft_error_count_window():
    m = monitor.Monitor(heartbeat_timeout_sec=120, overload_ratio=2.0,
                        overload_sustain_ticks=3, tick_interval_sec=1.0)
    assert m.soft_error_count("proxy") == 0
    m.record_soft_error("proxy")
    assert m.soft_error_count("proxy") == 1