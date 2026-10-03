"""【协议 B】入口调用 + cgroup 容器 + 抢占/终结 + 退出码归因。

调度器只拉起子进程、把 stdout/stderr 落盘、按退出码与交付物判定终态；
cgroup 负责内存软硬水位；抢占/取消走整组 SIGTERM/SIGKILL。
"""

from __future__ import annotations

import os
import pwd
import signal
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from . import cgroup as cg
from . import errors, runlog

# SDK 根目录（物理项目根）：任务进程通过 PYTHONPATH 引入 sched_task_sdk。
# 用本文件位置推导，不依赖 SCHED_HOME（测试用临时 HOME 覆盖时仍需能导入 SDK）。
_PROJECT_ROOT = Path(__file__).resolve().parent.parent

POLL_INTERVAL_SEC = 0.25
KILL_WAIT_SEC = 5
TRUNCATE_HEAD = "[truncated]\n".encode("utf-8")


def build_argv(task_dir: Path, entry_file: str, interpreter: str | None, args: list[str]) -> list[str]:
    entry = str((task_dir / entry_file).resolve())
    return ([interpreter] if interpreter else []) + [entry] + list(args)


def build_env(
    task_dir: Path,
    job_id: str,
    run_id: str,
    trigger: str,
    attempt: int,
    scheduled_at: str,
    extra: dict[str, str] | None = None,
    dry_run: bool = False,
) -> dict[str, str]:
    env = dict(os.environ)
    env.update(extra or {})
    env.update(
        {
            "SCHED_JOB_ID": job_id,
            "SCHED_RUN_ID": run_id,
            "SCHED_TRIGGER": trigger,
            "SCHED_ATTEMPT": str(attempt),
            "SCHED_SCHEDULED_AT": scheduled_at,
            "SCHED_WORKSPACE": str(task_dir),
            "SCHED_DRY_RUN": "1" if dry_run else "0",
        }
    )
    # 让任务进程可 import sched_task_sdk（与 SCHED_HOME 解耦）
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(_PROJECT_ROOT) + (os.pathsep + existing if existing else "")
    return env


def _drop_privileges(user: str | None):
    """preexec_fn：把子进程降权到 run_user（v04 R31）。用户不存在/已是当前用户则跳过。"""
    if not user:
        return
    try:
        pw = pwd.getpwnam(user)
    except KeyError:
        return
    if pw.pw_uid == os.getuid():
        return
    try:
        os.setgid(pw.pw_gid)
        os.setuid(pw.pw_uid)
    except OSError:
        pass


@dataclass
class RunSpec:
    job_id: str
    run_id: str
    task_dir: Path
    entry_file: str
    interpreter: str | None
    args: list[str]
    cwd: str
    env: dict[str, str]
    trigger: str
    attempt: int
    scheduled_at: str
    expect_outputs: list[str] = field(default_factory=list)
    cancel: threading.Event = field(default_factory=threading.Event)
    preempt: threading.Event = field(default_factory=threading.Event)
    dry_run: bool = False
    run_user: str | None = None
    cgroup_name: str | None = None
    memory_high: int | None = None
    memory_max: int | None = None
    cpu_quota: float | None = None
    cancel_grace_sec: int = 10    # 手动终止宽限期（settings.runner.kill_grace_sec）
    preempt_grace_sec: int = 40   # 抢占断点保存宽限期（settings.runner.grace_sec）

    @property
    def workdir(self) -> Path:
        rel = self.cwd or "."
        return self.task_dir if rel == "." else (self.task_dir / rel)

    def argv(self) -> list[str]:
        return build_argv(self.task_dir, self.entry_file, self.interpreter, self.args)

    def environ(self) -> dict[str, str]:
        return build_env(
            self.task_dir, self.job_id, self.run_id, self.trigger, self.attempt,
            self.scheduled_at, self.env, dry_run=self.dry_run,
        )


@dataclass
class RunOutcome:
    status: str
    exit_code: int | None
    reason: str
    pid: int | None
    pgid: int | None
    started_at: str
    finished_at: str
    duration_ms: int
    outputs: list[dict]
    stdout_log: str
    stderr_log: str
    scheduler_signaled: bool = False
    memory_peak_mb: int = 0
    cpu_usage_usec: int = 0
    oom_killed: bool = False


def _terminate(proc: subprocess.Popen, grace_sec: int) -> int | None:
    """整组 SIGTERM → 等 grace → SIGKILL；返回最终退出码。"""
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError, OSError):
        pass
    try:
        proc.wait(timeout=grace_sec)
        return proc.returncode
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        pass
    try:
        proc.wait(timeout=KILL_WAIT_SEC)
    except subprocess.TimeoutExpired:
        pass
    return proc.returncode


def execute(spec: RunSpec, on_started: Callable[[int, int], None] | None = None) -> RunOutcome:
    stdout = runlog.stdout_path(spec.job_id, spec.run_id)
    stderr = runlog.stderr_path(spec.job_id, spec.run_id)
    stdout.parent.mkdir(parents=True, exist_ok=True)

    started_at = runlog.now_iso()
    t0 = time.monotonic()
    pid = pgid = exit_code = None
    kind = "exited"
    reason = ""
    scheduler_signaled = False
    cgrp = cg.new_cgroup(spec.cgroup_name or f"{spec.job_id}/{spec.run_id}")
    if cgrp.available:
        cgrp.set_limits(spec.memory_high, spec.memory_max, spec.cpu_quota)

    try:
        workdir = spec.workdir
        workdir.mkdir(parents=True, exist_ok=True)
        with open(stdout, "wb") as fout, open(stderr, "wb") as ferr:
            proc = subprocess.Popen(
                spec.argv(),
                cwd=str(workdir),
                env=spec.environ(),
                stdin=subprocess.DEVNULL,
                stdout=fout,
                stderr=ferr,
                start_new_session=True,
                close_fds=True,
                preexec_fn=(lambda: _drop_privileges(spec.run_user)),
            )
            pid, pgid = proc.pid, proc.pid
            if cgrp.available:
                cgrp.add_process(pid)
            if on_started:
                try:
                    on_started(pid, pgid)
                except Exception:
                    pass

            while True:
                rc = proc.poll()
                if rc is not None:
                    exit_code = rc
                    break
                if spec.cancel.is_set():
                    scheduler_signaled = True
                    _terminate(proc, spec.cancel_grace_sec)
                    exit_code = proc.returncode
                    kind, reason = "killed", "被手动终止"
                    break
                if spec.preempt.is_set():
                    # 优雅抢占：SIGTERM 让任务托管断点后自行退出 99；超时再 SIGKILL
                    scheduler_signaled = True
                    _terminate(proc, spec.preempt_grace_sec)
                    exit_code = proc.returncode
                    break
                time.sleep(POLL_INTERVAL_SEC)
    except FileNotFoundError as e:
        kind, reason = "failed", f"无法拉起入口：{e}"
    except Exception as e:
        kind, reason = "failed", f"执行异常：{type(e).__name__}: {e}"
    finally:
        mem_peak = (cgrp.read_peak_mem() or 0) // (1024 * 1024)
        cpu_usec = cgrp.read_cpu_usage_usec() or 0
        oom = (cgrp.read_oom_kill_count() or 0) > 0
        # 不在此处 remove：monitor 需要在 run 存活期间读取 live 数据；由 service 在 finalize 后清理

    finished_at = runlog.now_iso()
    duration_ms = int((time.monotonic() - t0) * 1000)

    # 任务 SDK 写入的精确错误分类（state.json.last_error.category），优先于退出码兜底
    state_category = _read_last_error_category(spec.task_dir)

    # 终态判定
    if kind == "exited":
        if exit_code == 0:
            status = runlog.STATUS_SUCCESS
        elif exit_code == errors.EXIT_PREEMPTED:
            status = runlog.STATUS_PREEMPTED
            reason = "抢占：断点已保存"
        elif exit_code in (errors.EXIT_RESOURCE, errors.EXIT_SOFT_RESOURCE):
            status = runlog.STATUS_RESOURCE
            reason = f"资源错误（退出码 {exit_code}）"
        elif exit_code == errors.EXIT_DATA_RISK:
            status = runlog.STATUS_FAILED
            reason = "data_risk"
        elif exit_code == errors.EXIT_SIGKILL:
            # 137：优先归因 OOM；若非 OOM 且调度器发过信号，则为抢占/终结超时
            if oom:
                status, reason = runlog.STATUS_RESOURCE, "cgroup OOM（超限被内核杀）"
            elif scheduler_signaled:
                status, reason = runlog.STATUS_PREEMPT_FAILED, "终结超时被 SIGKILL（断点未确认）"
            else:
                status, reason = runlog.STATUS_FAILED, "被 SIGKILL（外部）"
        else:
            # 其余非 0 退出码：优先用任务自报分类；无自报时按退出码兜底。
            # G5：未分类（unclassified）一律按资源错误处理（可重试），而非转人工
            if state_category == errors.Category.LOGIC.value:
                status = runlog.STATUS_FAILED
                reason = f"逻辑错误（退出码 {exit_code}，任务自报 logic）"
            elif state_category in (errors.Category.RESOURCE.value, errors.Category.UNCLASSIFIED.value):
                status = runlog.STATUS_RESOURCE
                g5 = "，G5 未分类按资源处理" if state_category == errors.Category.UNCLASSIFIED.value else ""
                reason = f"资源错误（退出码 {exit_code}，任务自报 {state_category}{g5}）"
            else:
                cat, cat_reason = errors.classify_exit_code(exit_code)
                if errors.is_retryable_category(cat):
                    status = runlog.STATUS_RESOURCE
                    reason = f"资源错误兜底（退出码 {exit_code}，{cat_reason}）"
                else:
                    status = runlog.STATUS_FAILED
                    reason = f"退出码 {exit_code}"
    elif kind == "killed":
        status = runlog.STATUS_KILLED
    else:
        status = runlog.STATUS_FAILED

    missing = [p for p in spec.expect_outputs if not (spec.task_dir / p).exists()]
    if status == runlog.STATUS_SUCCESS and missing:
        status = runlog.STATUS_FAILED
        reason = "missing_output: " + ", ".join(missing)

    return RunOutcome(
        status=status,
        exit_code=exit_code,
        reason=reason,
        pid=pid,
        pgid=pgid,
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=duration_ms,
        outputs=scan_outputs(spec.task_dir),
        stdout_log=runlog.rel_to_runs(stdout),
        stderr_log=runlog.rel_to_runs(stderr),
        scheduler_signaled=scheduler_signaled,
        memory_peak_mb=mem_peak,
        cpu_usage_usec=cpu_usec,
        oom_killed=oom,
    )


def _read_last_error_category(task_dir: Path) -> str:
    """读取任务 SDK 写入 state.json 的 last_error.category（精确分类通道）。"""
    import json

    p = task_dir / "cache" / "state.json"
    if not p.exists():
        return ""
    try:
        st = json.loads(p.read_text("utf-8"))
    except (json.JSONDecodeError, OSError):
        return ""
    if not isinstance(st, dict):
        return ""
    last_error = st.get("last_error")
    if isinstance(last_error, dict):
        cat = last_error.get("category")
        if isinstance(cat, str):
            return cat
    return ""


def scan_outputs(task_dir: Path) -> list[dict]:
    d = task_dir / "results"
    if not d.is_dir():
        return []
    out = []
    for p in sorted(d.iterdir()):
        try:
            st = p.stat()
        except OSError:
            continue
        out.append({"path": str(p.relative_to(task_dir)), "size": st.st_size if p.is_file() else None, "is_dir": p.is_dir()})
    return out


def pgid_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def kill_process_group(pgid: int, grace_sec: int = 10) -> bool:
    try:
        os.killpg(pgid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError, OSError):
        return False
    deadline = time.monotonic() + grace_sec
    while time.monotonic() < deadline:
        if not pgid_alive(pgid):
            return True
        time.sleep(POLL_INTERVAL_SEC)
    try:
        os.killpg(pgid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        pass
    return True