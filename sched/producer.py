"""生产者独立守护（设计文档第三、十三节）。

- 每个启用任务常驻一个生产者；生产者启动后不因调度压力被回退/暂停/限额。
- 任务级单实例：生产者进程自身持有 producer_state/producer.lock 的 flock；
  调度器重启时若锁仍被持有且持有者存活，则接管跟踪而不重复拉起。
- 生产者崩溃用短退避重启（1s 起步，几十秒封顶），不因累计次数永久停用。
- 生产者与执行进程分属不同管理范围，禁止把控制命令发给生产者。
- 资源调度不停止生产者；仅调度器重启时默认保持/接管生产者（确认任务级单实例）。
"""

from __future__ import annotations

import fcntl
import logging
import os
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from . import config

log = logging.getLogger("sched.producer")

ENV_PASSTHROUGH = ("PATH", "HOME", "LANG", "TZ", "LC_ALL", "PYTHONUNBUFFERED")
ENV_BLACKLIST = frozenset({
    "LD_PRELOAD", "LD_LIBRARY_PATH", "LD_AUDIT", "LD_DEBUG",
    "PYTHONSTARTUP", "PYTHONINSPECT", "BASH_ENV", "ENV", "IFS", "PATH",
})
PROJECT_ROOT = Path(__file__).resolve().parent.parent

LOG_ROTATE_BYTES = 16 * 1024 * 1024  # 生产者日志轮转阈值


@dataclass
class ProducerProc:
    task_id: str
    proc: subprocess.Popen | None   # None = 接管的外部（上个调度器遗留）生产者
    pid: int
    start_time: float               # /proc 启动时间（身份核实）
    started_monotonic: float
    backoff_sec: float = 0.0
    crash_count: int = 0
    last_crash_monotonic: float = 0.0
    last_crash_wall: str = ""            # 最近一次崩溃墙钟时间（监控视图用，B2）
    external: bool = False
    dead_at_mono: float = 0.0            # 首次观测到死亡的时刻（0=存活/未知）
    restart_not_before_mono: float = 0.0  # 退避窗口结束（重启最早允许时刻）

    def alive(self) -> bool:
        if self.proc is not None:
            return self.proc.poll() is None
        return _pid_alive(self.pid, self.start_time)


def _build_argv(task_root: Path, entry: config.EntrySpec) -> list[str]:
    argv = ([entry.interpreter] if entry.interpreter else [])
    argv.append(str((task_root / entry.file).resolve()))
    argv.extend(list(entry.args))
    return argv


def _build_env(task_id: str, task_root: Path) -> dict[str, str]:
    env = {k: os.environ[k] for k in ENV_PASSTHROUGH if k in os.environ}
    env.update({
        "SCHED_TASK_ID": task_id,
        "SCHED_WORKSPACE": str(task_root),
        "SCHED_PRODUCER": "1",
    })
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(PROJECT_ROOT) + (os.pathsep + existing if existing else "")
    env["PYTHONUNBUFFERED"] = "1"
    return env


def _proc_start_time(pid: int) -> float:
    try:
        return os.stat(f"/proc/{pid}").st_ctime
    except OSError:
        return 0.0


def _pid_alive(pid: int, expect_start_time: float) -> bool:
    try:
        if os.stat(f"/proc/{pid}").st_ctime != expect_start_time:
            return False  # PID 复用：启动时间不一致即非本人
    except OSError:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _producer_lock_path(task_id: str) -> Path:
    return config.producer_state_dir(task_id) / "producer.lock"


def _read_lock_holder(task_id: str) -> tuple[int, float] | None:
    """读锁文件内容 'pid start_time'（仅当锁被持有时有意义）。"""
    try:
        parts = _producer_lock_path(task_id).read_text().split()
    except OSError:
        return None
    if len(parts) != 2:
        return None
    try:
        return int(parts[0]), float(parts[1])
    except ValueError:
        return None


def _lock_busy(task_id: str) -> bool:
    """探测任务级单实例锁是否被某个存活进程持有（try-lock 后立即释放）。"""
    p = _producer_lock_path(task_id)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(p), os.O_CREAT | os.O_RDWR, 0o644)
    except OSError:
        return False
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, fcntl.LOCK_UN)
            return False
        except OSError:
            return True
    finally:
        os.close(fd)


class ProducerGuard:
    """守护所有启用任务的生产者。每个任务最多一个存活生产者（跨调度器重启有效）。"""

    def __init__(self, cfg: config.SchedulerConfig):
        self.cfg = cfg
        self._lock = threading.RLock()
        self._procs: dict[str, ProducerProc] = {}
        self._logs = config.logs_dir()

    def launch(self, task_id: str, spec: config.TaskSpec) -> bool:
        """拉起生产者；若任务级单实例锁被存活旧生产者持有则接管跟踪并返回 False。"""
        with self._lock:
            existing = self._procs.get(task_id)
            if existing is not None and existing.alive():
                return False  # 已在运行，确保单实例
            self._procs.pop(task_id, None)
        # 接管：上个调度器遗留的生产者仍持有任务级单实例锁
        if _lock_busy(task_id):
            holder = _read_lock_holder(task_id)
            if holder and _pid_alive(holder[0], holder[1]):
                with self._lock:
                    self._procs[task_id] = ProducerProc(
                        task_id=task_id, proc=None, pid=holder[0],
                        start_time=holder[1], started_monotonic=time.monotonic(),
                        external=True)
                return False
        task_root = config.task_dir(task_id)
        argv = _build_argv(task_root, spec.producer_entry)
        env = _build_env(task_id, task_root)
        log_dir = self._logs / task_id
        log_dir.mkdir(parents=True, exist_ok=True)
        # 生产者日志 16MB 轮转（.1 覆盖旧档），防常驻进程日志无限增长
        for name in ("producer.stdout.log", "producer.stderr.log"):
            lp = log_dir / name
            try:
                if lp.exists() and lp.stat().st_size >= LOG_ROTATE_BYTES:
                    os.replace(lp, lp.with_name(name + ".1"))
            except OSError:
                pass
        stdout = open(log_dir / "producer.stdout.log", "ab")
        stderr = open(log_dir / "producer.stderr.log", "ab")
        proc = subprocess.Popen(
            argv, cwd=str(task_root), env=env,
            stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
            start_new_session=True, close_fds=True,
        )
        pp = ProducerProc(task_id=task_id, proc=proc, pid=proc.pid,
                          start_time=_proc_start_time(proc.pid),
                          started_monotonic=time.monotonic())
        with self._lock:
            self._procs[task_id] = pp
        return True

    def ensure(self, task_id: str, spec: config.TaskSpec) -> bool:
        """确保生产者存活；崩溃按退避重启（对外部接管的生产者同样生效）。

        语义：每观测到一次"从存活到死亡"的迁移才计一次崩溃并排定重启时刻；
        退避窗口内不重启、不重复计数；复活后死亡门控复位。
        返回 True 表示本轮首次观测到一次崩溃（供上层持续报警）。
        """
        with self._lock:
            pp = self._procs.get(task_id)
        if pp is None:
            self.launch(task_id, spec)
            return False
        if pp.alive():
            pp.dead_at_mono = 0.0  # 复活：复位死亡门控
            if self._logs_oversized(task_id):
                # 常驻生产者日志超限：copytruncate 原地轮转，进程保持运行。
                # 严禁对存活生产者发停止/重启命令（设计第三节：生产者不因调度
                # 压力被回退/暂停；日志 fd 以 O_APPEND 打开，截断后继续追加）
                log.info("producer %s log exceeds %d bytes, rotating in place",
                         task_id, LOG_ROTATE_BYTES)
                self._rotate_logs_inplace(task_id)
            return False
        now = time.monotonic()
        newly_crashed = False
        if pp.dead_at_mono == 0.0:
            # 首次观测到死亡：计一次崩溃 + 排定退避重启时刻
            pp.dead_at_mono = now
            if now - pp.last_crash_monotonic > self.cfg.control.producer_crash_reset_after_sec:
                pp.crash_count = 0  # 连续无崩溃期已过，退避序列复位
            pp.crash_count += 1
            pp.last_crash_monotonic = now
            from datetime import datetime, timezone
            pp.last_crash_wall = datetime.now(timezone.utc).isoformat(
                timespec="seconds")
            base = self.cfg.control.producer_restart_base_sec
            cap = self.cfg.control.producer_restart_cap_sec
            pp.backoff_sec = min(cap, base * (2 ** (pp.crash_count - 1)))
            pp.restart_not_before_mono = now + pp.backoff_sec
            newly_crashed = True
            # 反复崩溃持续报警（设计第三节），不因累计次数永久停用
            log.warning("producer %s crashed (#%d), restart not before +%.1fs",
                        task_id, pp.crash_count, pp.backoff_sec)
        if now < pp.restart_not_before_mono:
            return newly_crashed  # 退避窗口内
        self.launch(task_id, spec)
        with self._lock:
            new_pp = self._procs.get(task_id)
            if new_pp is not None and new_pp is not pp:
                # 保留连续崩溃计数（新对象死亡门控/重启时刻已复位）
                new_pp.crash_count = pp.crash_count
                new_pp.last_crash_wall = pp.last_crash_wall
        return newly_crashed

    def _logs_oversized(self, task_id: str) -> bool:
        log_dir = self._logs / task_id
        for name in ("producer.stdout.log", "producer.stderr.log"):
            try:
                if (log_dir / name).stat().st_size >= LOG_ROTATE_BYTES:
                    return True
            except OSError:
                continue
        return False

    def _rotate_logs_inplace(self, task_id: str) -> None:
        """copytruncate 原地轮转：当前内容拷贝为 .1，再把原文件截断为空。

        生产者进程的日志 fd 以 O_APPEND 打开，截断后续写仍落到文件末尾，
        因此无需重启进程（轮转与写入之间的极小窗口可能丢几行日志，
        对日志而言可接受；设计仅要求日志有界、进程常驻不被打扰）。
        """
        log_dir = self._logs / task_id
        for name in ("producer.stdout.log", "producer.stderr.log"):
            src = log_dir / name
            try:
                if not src.exists() or src.stat().st_size < LOG_ROTATE_BYTES:
                    continue
                dst = log_dir / (name + ".1")
                with open(src, "rb") as f:
                    data = f.read()
                with open(dst, "wb") as f:
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
                with open(src, "r+b") as f:
                    f.truncate(0)
                    os.fsync(f.fileno())
            except OSError as e:
                log.warning("rotate producer log %s/%s failed: %s", task_id, name, e)

    def stop_task(self, task_id: str) -> None:
        with self._lock:
            pp = self._procs.pop(task_id, None)
        if pp is None:
            return
        try:
            os.killpg(pp.pid, 15)  # SIGTERM（生产者以 start_new_session 启动，pgid==pid）
        except (ProcessLookupError, PermissionError, OSError):
            pass
        if pp.proc is not None:
            try:
                pp.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        else:
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and _pid_alive(pp.pid, pp.start_time):
                time.sleep(0.1)

    def list(self) -> list[dict]:
        with self._lock:
            out = []
            for tid, pp in self._procs.items():
                out.append({"task_id": tid, "pid": pp.pid,
                            "alive": pp.alive(),
                            "external": pp.external,
                            "crash_count": pp.crash_count,
                            "last_crash_wall": pp.last_crash_wall})
            return out
