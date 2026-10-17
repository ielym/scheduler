"""进程身份核实（PID + /proc 启动时间 + 僵尸态）。

runner（执行进程）与 producer（生产者守护）此前各自复刻同一套逻辑，此处统一：
- 执行身份 = PID + 进程启动时间联合核实，防 PID 复用误判（设计第十三节）。
- 僵尸/退出中进程不算存活（父进程未 wait 时退出进程以僵尸态留在 /proc）。
"""

from __future__ import annotations

import os

# 僵尸/退出中状态字符（/proc/<pid>/stat 第 3 字段；bytes 比较）
_ZOMBIE_STATES = frozenset((b"Z", b"x", b"X"))


def start_time(pid: int) -> float:
    """/proc/<pid> 的创建时间（进程启动时刻，用于 PID 复用核实）；失败返回 0.0。"""
    try:
        return os.stat(f"/proc/{pid}").st_ctime
    except OSError:
        return 0.0


def never_runs_again(pid: int) -> bool:
    """进程是否已不可能再执行代码（僵尸/退出中/不存在）。

    整树退出核实用它过滤 cgroup.procs 里的残留 pid：僵尸进程不消耗资源、
    信号杀不掉，等待它被收割不应阻塞 finalize。
    """
    try:
        data = open(f"/proc/{pid}/stat", "rb").read()
    except OSError:
        return True
    try:
        # stat 第 3 字段是状态字符，位于最后一个 ')' 之后（进程名可含括号）
        state = data.rsplit(b")", 1)[1].split()[0][:1]
        return state in _ZOMBIE_STATES
    except (IndexError, ValueError):
        return False


def alive(pid: int, expect_start: float | None = None) -> bool:
    """PID 是否存活（可选校验启动时间）；僵尸/退出中不算存活。"""
    if not pid:
        return False
    try:
        st = os.stat(f"/proc/{pid}")
    except OSError:
        return False
    if expect_start and expect_start != st.st_ctime:
        return False  # PID 复用：启动时间不一致即非本人
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass
    return not never_runs_again(pid)


__all__ = ["start_time", "never_runs_again", "alive"]
