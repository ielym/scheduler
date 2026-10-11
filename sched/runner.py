"""执行进程拉起 + 身份握手 + 进程树回收 + 退出码归因。

执行身份（设计文档第十三节）：PID + 进程启动时间 + attempt_id + 执行令牌 + cgroup。
每次执行使用独立 cgroup 做统计与身份管理，不配置预设 CPU/内存/IO 强制配额（只观测）。
"""

from __future__ import annotations

import json
import os
import secrets
import signal
import subprocess
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from . import cgroup as cg
from . import config
from .config import EventKind

PROJECT_ROOT = Path(__file__).resolve().parent.parent
POLL_INTERVAL_SEC = 0.25
KILL_WAIT_SEC = 5

ENV_PASSTHROUGH = ("PATH", "HOME", "LANG", "TZ", "LC_ALL", "PYTHONUNBUFFERED")
ENV_BLACKLIST = frozenset({
    "LD_PRELOAD", "LD_LIBRARY_PATH", "LD_AUDIT", "LD_DEBUG",
    "PYTHONSTARTUP", "PYTHONINSPECT", "BASH_ENV", "ENV", "IFS", "PATH",
})


def new_id(prefix: str) -> str:
    return f"{prefix}-{secrets.token_hex(8)}"


@dataclass
class AttemptSpec:
    attempt_id: str
    job_id: str
    task_id: str
    token: str
    task_root: Path
    executor_entry: config.EntrySpec
    entry_args: list[str]
    workdir: str
    env: dict[str, str]
    progress_semver: str


class AttemptRun:
    """一个存活执行尝试的运行句柄。"""

    def __init__(self, spec: AttemptSpec, proc: subprocess.Popen,
                 run_dir: Path, cgrp: "cg.Cgroup", input_refs: list[str]):
        self.spec = spec
        self.proc = proc
        self.run_dir = run_dir
        self.cgrp = cgrp
        self.input_refs = input_refs
        self.pid = proc.pid
        self.pgid = proc.pid
        self.start_time = _proc_start_time(self.pid)
        self.started_at = now_iso()
        self._pause: dict | None = None
        self._terminating = threading.Event()

    @property
    def event_file(self) -> Path:
        return self.run_dir / "events.jsonl"

    @property
    def control_file(self) -> Path:
        return self.run_dir / "control.json"

    @property
    def checkpoint_file(self) -> Path:
        return self.run_dir / "checkpoint.json"

    def is_alive(self) -> bool:
        if self.proc.poll() is not None:
            return False
        return _pid_alive(self.pid)

    def alive_procs(self) -> list[int]:
        procs = self.cgrp.procs()
        return procs if procs else ([self.pid] if self.is_alive() else [])

    # ---- 事件 ----
    def read_events(self, after_seq: int = 0) -> list[dict]:
        """读 events.jsonl 中 seq > after_seq 的事件。"""
        out = []
        if not self.event_file.exists():
            return out
        try:
            with open(self.event_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(obj, dict) and int(obj.get("seq") or 0) > after_seq:
                        out.append(obj)
        except OSError:
            pass
        return out

    def read_checkpoint(self) -> dict | None:
        p = self.checkpoint_file
        if not p.exists():
            return None
        try:
            obj = json.loads(p.read_text("utf-8"))
            return obj if isinstance(obj, dict) else None
        except (json.JSONDecodeError, OSError):
            return None

    # ---- 控制 ----
    def write_pause(self, command_id: str, reason: str, deadline: str) -> None:
        self._pause = {"command_id": command_id, "action": "pause",
                       "job_id": self.spec.job_id, "attempt_id": self.spec.attempt_id,
                       "token": self.spec.token, "reason": reason, "deadline": deadline}
        _atomic_write(self.control_file, self._pause)

    @property
    def pause_command(self) -> dict | None:
        return self._pause

    def signal_pause(self) -> None:
        """协作暂停：先写控制文件，再发 SIGTERM 让 worker 进入安全边界落断点。"""
        try:
            os.killpg(self.pgid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError, OSError):
            pass

    def terminate(self, grace_sec: float) -> int | None:
        """整棵进程树 SIGTERM → 等 grace → SIGKILL。返回退出码。"""
        self._terminating.set()
        try:
            os.killpg(self.pgid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError, OSError):
            pass
        try:
            self.proc.wait(timeout=grace_sec)
            return self.proc.returncode
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(self.pgid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            pass
        try:
            self.proc.wait(timeout=KILL_WAIT_SEC)
        except subprocess.TimeoutExpired:
            pass
        return self.proc.returncode

    def cleanup(self) -> None:
        self.cgrp.remove()


def _atomic_write(path: Path, obj: Any) -> None:
    import tempfile
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=path.name + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _proc_start_time(pid: int) -> float:
    try:
        return os.stat(f"/proc/{pid}").st_ctime
    except OSError:
        return 0.0


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def build_argv(task_root: Path, entry: config.EntrySpec, entry_args: list[str]) -> list[str]:
    argv: list[str] = []
    if entry.interpreter:
        argv.append(entry.interpreter)
    argv.append(str((task_root / entry.file).resolve()))
    argv.extend(list(entry.args))
    argv.extend(list(entry_args))
    return argv


def build_env(spec: AttemptSpec, input_refs: list[str]) -> dict[str, str]:
    env = {k: os.environ[k] for k in ENV_PASSTHROUGH if k in os.environ}
    for k, v in spec.env.items():
        if k not in ENV_BLACKLIST:
            env[k] = v
    env.update({
        "SCHED_ATTEMPT_ID": spec.attempt_id,
        "SCHED_JOB_ID": spec.job_id,
        "SCHED_TASK_ID": spec.task_id,
        "SCHED_TOKEN": spec.token,
        "SCHED_RUN_DIR": str(config.run_dir(spec.attempt_id)),
        "SCHED_EVENT_FILE": str(config.run_dir(spec.attempt_id) / "events.jsonl"),
        "SCHED_CONTROL_FILE": str(config.run_dir(spec.attempt_id) / "control.json"),
        "SCHED_CHECKPOINT_FILE": str(config.run_dir(spec.attempt_id) / "checkpoint.json"),
        "SCHED_WORKSPACE": str(spec.task_root),
        "SCHED_PROGRESS_SEMVER": spec.progress_semver,
    })
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(PROJECT_ROOT) + (os.pathsep + existing if existing else "")
    env["PYTHONUNBUFFERED"] = "1"
    return env


def launch(spec: AttemptSpec, input_refs: list[str] | None = None) -> AttemptRun:
    run_dir = config.run_dir(spec.attempt_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    input_refs = input_refs or []
    workdir = spec.task_root if spec.workdir in (".", "") else (spec.task_root / spec.workdir)
    workdir.mkdir(parents=True, exist_ok=True)

    cgrp = cg.new_cgroup(f"{spec.task_id}/{spec.attempt_id}")
    argv = build_argv(spec.task_root, spec.executor_entry, spec.entry_args)
    env = build_env(spec, input_refs)

    proc = subprocess.Popen(
        argv,
        cwd=str(workdir),
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
        close_fds=True,
    )
    if cgrp.available:
        cgrp.add_process(proc.pid)
    return AttemptRun(spec, proc, run_dir, cgrp, input_refs)


@dataclass
class AttemptOutcome:
    status: str
    exit_code: int | None
    reason: str
    completed: bool = False
    shortage: bool = False


def decide_outcome(status_ctx: str, exit_code: int | None, had_completed: bool,
                   had_shortage: bool) -> AttemptOutcome:
    """把进程退出 + 事件证据归因为作业状态。

    status_ctx: scheduler 发起 PAUSING（协作暂停）或 CANCELLED（显式取消）。
    """
    from .config import JobStatus

    if status_ctx == JobStatus.CANCELLED:
        return AttemptOutcome(JobStatus.CANCELLED, exit_code, "显式取消")
    if status_ctx == JobStatus.PAUSING:
        return AttemptOutcome(JobStatus.PAUSED, exit_code, "协作暂停，断点已保存")
    if had_shortage:
        return AttemptOutcome(JobStatus.BACKOFF, exit_code, "统一外部不足")
    if exit_code == 0 and had_completed:
        return AttemptOutcome(JobStatus.SUCCEEDED, exit_code, "完成", completed=True)
    if exit_code == 0 and not had_completed:
        return AttemptOutcome(JobStatus.RECOVERING, exit_code, "退出 0 但无完成记录，待核实")
    return AttemptOutcome(JobStatus.FAILED, exit_code, f"业务/未知失败（退出码 {exit_code}）")