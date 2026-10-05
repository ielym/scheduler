"""cgroup v2 生命周期管理（独立复刻）。

为每个 run 建立独立 cgroup，通过 memory.high（软限节流）+ memory.max（硬兜底）双水位
实现资源硬边界，并读取 memory.current / memory.peak / cpu.stat / memory.events 作为
monitor 与画像的唯一权威数据源。无法使用 cgroup 时降级为 no-op（硬限额相应不生效）。
"""

from __future__ import annotations

import os
import time
from pathlib import Path

CGROUP_ROOT = Path("/sys/fs/cgroup")
FALLBACK_ROOT = CGROUP_ROOT / "sched"


def _cgroup_fs_mounted() -> bool:
    return (CGROUP_ROOT / "cgroup.controllers").exists()


def _writable(path: Path) -> bool:
    return os.access(path, os.W_OK)


class Cgroup:
    """一个 run 的 cgroup 控制句柄。所有写操作失败时静默降级（不影响调度主流程）。"""

    def __init__(self, name: str):
        self.name = name
        self.path = FALLBACK_ROOT / name if _cgroup_fs_mounted() else None
        self.available = False
        if self.path is not None:
            try:
                self.path.mkdir(parents=True, exist_ok=True)
                # 三级嵌套（sched/<job>/<run>）：中间层 cgroup 必须显式向下委派
                # cpu+memory，否则叶子 cgroup 不会生成 memory.current/memory.max，
                # 内存硬限与画像读取静默失效（cpu 控制器在本机恰好默认出现，不能依赖）。
                self._delegate_to_nested_path()
                self.available = _writable(self.path)
            except OSError:
                self.available = False

    def _delegate_to_nested_path(self) -> None:
        """沿 name 的每一级中间目录，自上而下向其 subtree_control 写 +cpu +memory。

        cgroup v2 规则：父 cgroup 的 subtree_control 决定其子代可用哪些控制器；
        且父 cgroup 内不能直接驻留进程（no internal process）。sched/<job>/ 层
        从不直接放进程（run 都在叶子），故可以安全委派。失败静默降级。
        """
        parts = [p for p in self.name.split("/") if p and p != "."]
        # 需要委派的所有层：sched 根层 + 每个中间层（不含叶子 rid 层）。
        # 关键：必须先从 sched 根层开始委派，否则下面的 subtask/叶子收不到 memory 控制器。
        levels = [FALLBACK_ROOT]
        for part in parts[:-1]:
            levels.append(levels[-1] / part)
        for ancestor in levels:
            sc = ancestor / "cgroup.subtree_control"
            if not sc.exists():
                continue
            try:
                cur = sc.read_text().strip()
                val = "".join(f" +{c}" for c in ("cpu", "memory") if f"+{c}" not in cur)
                if val:
                    sc.write_text(val.strip())
            except OSError:
                pass

    # ---- 资源上限 ----
    def set_limits(self, memory_high: int | None, memory_max: int | None, cpu_quota: float | None) -> None:
        if not self.available:
            return
        if memory_high is not None:
            self._write("memory.high", str(int(memory_high)))
        if memory_max is not None:
            self._write("memory.max", str(int(memory_max)))
        if cpu_quota is not None:
            # cpu.max: "QUOTA PERIOD"，quota 微秒；"max" = 不限
            if cpu_quota <= 0:
                self._write("cpu.max", "max")
            else:
                self._write("cpu.max", f"{int(cpu_quota * 100000)} 100000")

    def add_process(self, pid: int) -> None:
        if not self.available:
            return
        self._write("cgroup.procs", str(int(pid)))

    def kill_all(self) -> None:
        if not self.available:
            return
        self._write("cgroup.kill", "1")

    # ---- 观测 ----
    def read_current_mem(self) -> int | None:
        return self._read_int("memory.current")

    def read_peak_mem(self) -> int | None:
        return self._read_int("memory.peak")

    def read_oom_kill_count(self) -> int | None:
        return self._read_int("memory.events", key="oom_kill")

    def read_cpu_usage_usec(self) -> int | None:
        v = self._read_int("cpu.stat", key="usage_usec")
        return v

    def procs(self) -> list[int]:
        if not self.available:
            return []
        try:
            txt = (self.path / "cgroup.procs").read_text()
        except OSError:
            return []
        return [int(x) for x in txt.split()]

    def remove(self) -> None:
        if not self.path or not self.path.exists():
            return
        try:
            self.kill_all()
            time.sleep(0.05)
            self.path.rmdir()
        except OSError:
            pass

    # ---- 内部 ----
    def _write(self, fname: str, value: str) -> None:
        try:
            (self.path / fname).write_text(value)
        except OSError:
            pass

    def _read_int(self, fname: str, key: str | None = None) -> int | None:
        if not self.available:
            return None
        try:
            txt = (self.path / fname).read_text()
        except OSError:
            return None
        if key is None:
            try:
                return int(txt.strip())
            except ValueError:
                return None
        for line in txt.splitlines():
            k, _, v = line.partition(" ")
            if k == key:
                try:
                    return int(v)
                except ValueError:
                    return None
        return None


def _delegate_controllers() -> bool:
    """在 cgroup2 里为调度器子树启用 memory/cpu 控制器（需要 root）。"""
    if not _cgroup_fs_mounted():
        return False
    try:
        FALLBACK_ROOT.mkdir(parents=True, exist_ok=True)
        subtree = CGROUP_ROOT / "cgroup.subtree_control"
        cur = subtree.read_text().strip() if subtree.exists() else ""
        for c in ("memory", "cpu"):
            if f"+{c}" not in cur:
                subtree.write_text(cur + f" +{c}" if cur else f"+{c}")
        return True
    except OSError:
        return False


def ensure_scheduler_subtree() -> bool:
    if not _cgroup_fs_mounted():
        return False
    if _writable(FALLBACK_ROOT) or FALLBACK_ROOT.exists():
        return True
    return _delegate_controllers()


def reap_orphans() -> int:
    """启动时回收 sched 子树下残留的进程（上个调度器实例崩溃/异常退出遗留的孤儿）。

    只在调度器刚启动、尚未拉起任何任务进程时调用——此刻子树内的一切进程都属于
    已消亡的旧实例。通过根 cgroup 的 cgroup.kill 一次性整组杀死（含所有后代），
    返回回收前统计到的进程数（供日志）。当前 serve 进程位于 systemd 服务 cgroup，
    不在本子树内，因此不受影响。
    """
    if not _cgroup_fs_mounted() or not FALLBACK_ROOT.exists():
        return 0
    total = 0
    for procs_file in FALLBACK_ROOT.rglob("cgroup.procs"):
        try:
            total += len([x for x in procs_file.read_text().split() if x.strip()])
        except OSError:
            continue
    try:
        (FALLBACK_ROOT / "cgroup.kill").write_text("1")
    except OSError:
        pass
    return total


def new_cgroup(name: str) -> Cgroup:
    return Cgroup(name)