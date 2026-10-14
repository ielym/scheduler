"""cgroup v2 生命周期管理（独立复刻）。

为每个 run 建立独立 cgroup 做身份管理与资源观测：读取 memory.current /
memory.peak / cpu.stat / io.stat / memory.events 作为 monitor 与画像的唯一权威
数据源，并支持 cgroup.kill 整组回收进程树。注意：本调度器不对执行进程配置
预设 CPU/内存/IO 强制配额（只观测不设限，见设计文档第八节）。无法使用 cgroup
时降级为 no-op（观测与整组回收相应不生效，调度器进入观测不足模式）。
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
                # cpu+memory，否则叶子 cgroup 不会生成 memory.current/cpu.stat
                # 等观测文件，画像读取静默失效（cpu 控制器在本机恰好默认出现，
                # 不能依赖）。本调度器只观测不设限，不写 memory.max。
                self._delegate_to_nested_path()
                self.available = _writable(self.path)
            except OSError:
                self.available = False

    # 向下委派的控制器（设计第八节 L127：I/O 计数也是准入门禁的观测输入）
    _DELEGATE_CONTROLLERS = ("cpu", "memory", "io")

    def _delegate_to_nested_path(self) -> None:
        """沿 name 的每一级中间目录，自上而下向其 subtree_control 写 +cpu +memory +io。

        cgroup v2 规则：父 cgroup 的 subtree_control 决定其子代可用哪些控制器；
        且父 cgroup 内不能直接驻留进程（no internal process）。sched/<job>/ 层
        从不直接放进程（run 都在叶子），故可以安全委派。失败静默降级。
        审查修复（io 委派缺失）：此前只委派 cpu+memory，io 控制器从未到位，
        叶子 cgroup 的 io.stat 永远不存在、I/O 计数观测恒为空。现委派 io；
        宿主根未启用 io 控制器时按其 cgroup.controllers 过滤，避免无效写。
        """
        parts = [p for p in self.name.split("/") if p and p != "."]
        # 需要委派的所有层：sched 根层 + 每个中间层（不含叶子 rid 层）。
        # 关键：必须先从 sched 根层开始委派，否则下面的 subtask/叶子收不到 memory 控制器。
        levels = [FALLBACK_ROOT]
        for part in parts[:-1]:
            levels.append(levels[-1] / part)
        for ancestor in levels:
            # 只委派该层实际启用的控制器（写未启用的控制器会失败被静默吞掉）
            try:
                avail = set((ancestor / "cgroup.controllers")
                            .read_text().split())
            except OSError:
                avail = set()
            todo = [c for c in self._DELEGATE_CONTROLLERS if c in avail]
            sc = ancestor / "cgroup.subtree_control"
            if not sc.exists() or not todo:
                continue
            try:
                cur = sc.read_text().strip()
                val = "".join(f" +{c}" for c in todo if f"+{c}" not in cur)
                if val:
                    sc.write_text(val.strip())
            except OSError:
                pass

    # ---- 进程管理 ----
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

    def read_oom_kill_count(self) -> int | None:
        return self._read_int("memory.events", key="oom_kill")

    def read_cpu_usage_usec(self) -> int | None:
        v = self._read_int("cpu.stat", key="usage_usec")
        return v

    def read_io_bytes(self) -> int | None:
        """读取 io.stat 的 rbytes+wbytes 总和（I/O 计数观测，设计文档第八节）。"""
        if not self.available:
            return None
        try:
            txt = (self.path / "io.stat").read_text()
        except OSError:
            return None
        total = 0
        found = False
        for line in txt.splitlines():
            for field in line.split()[1:]:
                k, _, v = field.partition("=")
                if k in ("rbytes", "wbytes"):
                    try:
                        total += int(v)
                        found = True
                    except ValueError:
                        pass
        return total if found else None

    def procs(self) -> list[int]:
        if not self.available:
            return []
        try:
            txt = (self.path / "cgroup.procs").read_text()
        except OSError:
            return []
        return [int(x) for x in txt.split()]

    def remove(self) -> None:
        """删除本 cgroup（先整组终止，再有界等待 rmdir）。

        一次性 rmdir 在进程树收尾稍慢（僵尸/收尾中）时必然失败且被静默
        吞掉——每次执行泄漏一个目录，kernfs inode 随时间累积。此处有界
        重试（2s）：仅在 cgroup 内仍有存活 pid（D 态等）时继续等待，
        pid 已消失仍 rmdir 失败（子 cgroup/非 cgroupfs 目录）则立即放弃，
        交给启动期 reap_orphans 的空目录清理兜底。
        """
        if not self.path or not self.path.exists():
            return
        try:
            self.kill_all()
        except OSError:
            pass
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            try:
                self.path.rmdir()
                return
            except OSError:
                pass
            if not _procs_alive(self.procs()):
                break
            time.sleep(0.05)

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


def reap_orphans(exclude: set[str] | None = None) -> int:
    """启动时回收 sched 子树下残留的进程（上个调度器实例崩溃/异常退出遗留的孤儿）。

    只在调度器刚启动、尚未拉起任何任务进程时调用——此刻子树内的一切进程都属于
    已消亡的旧实例。exclude 为需要保留的 cgroup 相对路径（如 "task/attempt"，
    协作退出宽限中的存活 worker）：这些 cgroup 及其祖先/后代都不写 cgroup.kill
    （cgroup.kill 会连带杀死所有后代，祖先目录有进程时也必须跳过）。对其余有进程
    的 cgroup 逐个整组回收，返回回收前统计到的进程数（供日志）。当前 serve 进程
    位于 systemd 服务 cgroup，不在本子树内，因此不受影响。
    """
    if not _cgroup_fs_mounted() or not FALLBACK_ROOT.exists():
        return 0
    keep = {e for e in (exclude or set()) if e}

    def _protected(rel: str) -> bool:
        # rel 与任一保留路径构成 祖先/自身/后代 关系 → 不回收
        for e in keep:
            if rel == e or rel.startswith(e + "/") or e.startswith(rel + "/"):
                return True
        return False

    total = 0
    targets: list[Path] = []
    for procs_file in FALLBACK_ROOT.rglob("cgroup.procs"):
        try:
            n = len([x for x in procs_file.read_text().split() if x.strip()])
        except OSError:
            continue
        if not n:
            continue
        rel = procs_file.parent.relative_to(FALLBACK_ROOT).as_posix()
        if _protected(rel):
            continue
        total += n
        targets.append(procs_file.parent)
    for d in targets:
        try:
            (d / "cgroup.kill").write_text("1")
        except OSError:
            pass
    _prune_empty_dirs()
    return total


def _prune_empty_dirs() -> None:
    """自底向上删除 sched 子树中的空 cgroup 目录。

    历史上 remove() 一次性 rmdir 失败被静默吞掉、优雅停机不清理，导致空
    目录持续累积（实测 1472 个目录 vs 76 个存活 attempt）。进程已清空的
    目录在此处回收；仍有进程/子目录的 rmdir 自然失败、原样保留。
    """
    try:
        dirs = sorted((p for p in FALLBACK_ROOT.rglob("*") if p.is_dir()),
                      key=lambda p: len(p.parts), reverse=True)
    except OSError:
        return
    for d in dirs:
        try:
            d.rmdir()
        except OSError:
            continue


def new_cgroup(name: str) -> Cgroup:
    return Cgroup(name)


def _procs_alive(procs: list[int]) -> bool:
    """cgroup 内是否仍有存活 pid（信号 0 探测；僵尸/已消亡的不算）。"""
    for pid in procs:
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            continue
        except PermissionError:
            return True
    return False


def read_subtree_pressure() -> dict:
    """读取 sched 子树的 cpu/memory/io 压力指标（psi，full avg10，监测用）。

    这是"执行范围的压力指标"（设计第八节）：只统计 sched 子树（执行进程），
    不含生产者或整机其它进程。控制器未委派/文件缺失时相应键缺省，不报错。
    """
    if not _cgroup_fs_mounted() or not FALLBACK_ROOT.exists():
        return {}
    out: dict[str, float] = {}
    for name in ("cpu", "memory", "io"):
        try:
            txt = (FALLBACK_ROOT / f"{name}.pressure").read_text()
        except OSError:
            continue
        for line in txt.splitlines():
            parts = line.split()
            if not parts or parts[0] != "full":
                continue
            for tok in parts[1:]:
                k, _, v = tok.partition("=")
                if k == "avg10":
                    try:
                        out[name] = float(v)
                    except ValueError:
                        pass
                    break
            break
    return out