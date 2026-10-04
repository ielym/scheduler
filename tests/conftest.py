"""测试共享夹具。只保留通用 SCHED_HOME 隔离和 Settings 工厂。"""

from __future__ import annotations

import threading
import time

import pytest


@pytest.fixture
def home(tmp_path, monkeypatch):
    """每个测试独立的 SCHED_HOME 根目录，隔离运行时状态。

    fixture 撤销前强制 join 测试期间新建的全部线程——防止调度器后台线程
    在 fixture 撤销后继续写盘污染下一个测试的 tmp_path。
    """
    before = set(threading.enumerate())
    monkeypatch.setenv("SCHED_HOME", str(tmp_path))
    yield tmp_path
    from sched.service import SchedulerService

    for svc in list(SchedulerService._instances):
        try:
            svc._stopping.set()
        except Exception:
            continue
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        alive = [t for t in threading.enumerate()
                 if t is not threading.current_thread() and t not in before and t.is_alive()]
        if not alive:
            break
        for t in alive:
            t.join(timeout=max(0.1, deadline - time.monotonic()))


@pytest.fixture
def make_settings():
    from sched.config import Settings

    def _make(**kw):
        base = {"run_user": ""}  # 测试环境不降权
        base.update(kw)
        return Settings(**base)

    return _make
