"""生产者独立守护（设计文档第三、十三节）。

- 每个启用任务常驻一个生产者；生产者启动后不因调度压力被回退/暂停/限额。
- 生产者崩溃用短退避重启（1s 起步，几十秒封顶），不因累计次数永久停用。
- 生产者与执行进程分属不同管理范围，禁止把控制命令发给生产者。
- 资源调度不停止生产者；仅调度器重启时默认保持/接管生产者（确认任务级单实例）。
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

from . import config

ENV_PASSTHROUGH = ("PATH", "HOME", "LANG", "TZ", "LC_ALL", "PYTHONUNBUFFERED")
ENV_BLACKLIST = frozenset({
    "LD_PRELOAD", "LD_LIBRARY_PATH", "LD_AUDIT", "LD_DEBUG",
    "PYTHONSTARTUP", "PYTHONINSPECT", "BASH_ENV", "ENV", "IFS", "PATH",
})
PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class ProducerProc:
    task_id: str
    proc: subprocess.Popen
    pid: int
    started_monotonic: float
    backoff_sec: float = 0.0
    crash_count: int = 0
    last_crash_monotonic: float = 0.0


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


class ProducerGuard:
    """守护所有启用任务的生产者。每个任务最多一个存活生产者。"""

    def __init__(self, cfg: config.SchedulerConfig):
        self.cfg = cfg
        self._lock = threading.RLock()
        self._procs: dict[str, ProducerProc] = {}
        self._logs = config.logs_dir()

    def launch(self, task_id: str, spec: config.TaskSpec) -> bool:
        with self._lock:
            existing = self._procs.get(task_id)
            if existing is not None and existing.proc.poll() is None:
                return False  # 已在运行，确保单实例
            self._procs.pop(task_id, None)
        task_root = config.task_dir(task_id)
        argv = _build_argv(task_root, spec.producer_entry)
        env = _build_env(task_id, task_root)
        log_dir = self._logs / task_id
        log_dir.mkdir(parents=True, exist_ok=True)
        stdout = open(log_dir / "producer.stdout.log", "ab")
        stderr = open(log_dir / "producer.stderr.log", "ab")
        proc = subprocess.Popen(
            argv, cwd=str(task_root), env=env,
            stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
            start_new_session=True, close_fds=True,
        )
        pp = ProducerProc(task_id=task_id, proc=proc, pid=proc.pid,
                          started_monotonic=time.monotonic())
        with self._lock:
            self._procs[task_id] = pp
        return True

    def ensure(self, task_id: str, spec: config.TaskSpec) -> None:
        """确保生产者存活；崩溃则按退避重启。"""
        with self._lock:
            pp = self._procs.get(task_id)
        if pp is None:
            self.launch(task_id, spec)
            return
        if pp.proc.poll() is None:
            return
        # 已崩溃 → 计算退避
        now = time.monotonic()
        if now - pp.last_crash_monotonic > self.cfg.control.producer_crash_reset_after_sec:
            pp.crash_count = 0
        pp.crash_count += 1
        pp.last_crash_monotonic = now
        base = self.cfg.control.producer_restart_base_sec
        cap = self.cfg.control.producer_restart_cap_sec
        pp.backoff_sec = min(cap, base * (2 ** (pp.crash_count - 1)))
        if now - pp.started_monotonic < pp.backoff_sec:
            return  # 还在退避窗口内
        # 退避期已过 → 重启
        self.launch(task_id, spec)
        with self._lock:
            new_pp = self._procs.get(task_id)
            if new_pp is not None and pp is not new_pp:
                # 保留连续崩溃计数
                new_pp.crash_count = pp.crash_count

    def stop_task(self, task_id: str) -> None:
        with self._lock:
            pp = self._procs.pop(task_id, None)
        if pp is None:
            return
        try:
            os.killpg(pp.pid, 15)  # SIGTERM
        except (ProcessLookupError, PermissionError, OSError):
            pass
        try:
            pp.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass

    def stop_all(self) -> None:
        for task_id in list(self._procs):
            self.stop_task(task_id)

    def alive_tasks(self) -> dict[str, bool]:
        with self._lock:
            return {tid: (pp.proc.poll() is None) for tid, pp in self._procs.items()}

    def list(self) -> list[dict]:
        with self._lock:
            out = []
            for tid, pp in self._procs.items():
                out.append({"task_id": tid, "pid": pp.pid,
                            "alive": pp.proc.poll() is None,
                            "crash_count": pp.crash_count})
            return out