"""测试共享夹具：SCHED_HOME 隔离 + cgroup 隔离 + 残留线程清理（v3）。"""

from __future__ import annotations

import os
import signal
import sys
import threading
import time
from pathlib import Path

import pytest

ROOT = str(Path(__file__).resolve().parent.parent)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def _reap_children(marker: str) -> None:
    """回收本测试泄漏的调度器子进程（生产者/执行进程）。

    调度器 stop() 按设计第十三节不杀生产者（仅重启调度器保持生产者、整套关闭由外部
    生命周期负责）；测试环境无外部生命周期，故在夹具撤销时按 cmdline 归属清扫本测试
    tmp_path 下的所有子进程，保持套件封闭、避免跨运行积累孤儿进程。
    """
    try:
        for pid_dir in os.listdir("/proc"):
            if not pid_dir.isdigit():
                continue
            pid = int(pid_dir)
            if pid == os.getpid():
                continue
            try:
                with open(f"/proc/{pid_dir}/cmdline", "rb") as f:
                    cmd = f.read().decode("utf-8", "replace")
            except OSError:
                continue
            if marker in cmd:
                try:
                    os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass
    except OSError:
        pass


@pytest.fixture
def home(tmp_path, monkeypatch):
    """每个测试独立的 SCHED_HOME 根目录，隔离运行时状态。

    fixture 撤销前强制 join 测试期间新建的非 daemon 线程——防止后台线程
    在 fixture 撤销后继续写盘污染下一个测试的 tmp_path。
    """
    before = set(threading.enumerate())
    monkeypatch.setenv("SCHED_HOME", str(tmp_path))
    # cgroup 隔离：把 FALLBACK_ROOT 指向 tmp 目录，防止测试触碰
    # 生产 /sys/fs/cgroup/sched 子树（生产调度器正在运行）
    from sched import cgroup as _cg
    monkeypatch.setattr(_cg, "FALLBACK_ROOT", tmp_path / "cg")
    # 接收器的新文件发现内存态与增量目录缓存跨测试隔离
    from sched import receiver as _receiver
    _receiver._seen_files.clear()
    _receiver._dir_cache.clear()
    yield tmp_path
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        alive = [t for t in threading.enumerate()
                 if t is not threading.current_thread() and t not in before and t.is_alive()]
        if not alive:
            break
        for t in alive:
            t.join(timeout=max(0.1, deadline - time.monotonic()))
    _reap_children(str(tmp_path))
