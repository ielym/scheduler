"""进程拉起 + cgroup + 信号 + 退出码归因。

启动协议（调度器注入环境变量）：
  SCHED_SUBTASK_ID / SCHED_REQUEST_ID / SCHED_RUN_ID / SCHED_CONFIG /
  SCHED_TRIGGER / SCHED_WORKSPACE / SCHED_ATTEMPT

退出码统一（复用 errors.py）：
  0 成功 → 意图 done
  99 抢占/停止（断点已保存）→ 续跑同意图
  100/101 资源错误 → 重试同意图
  110 数据风险 → 二刷
  其他非 0 / SIGKILL → 兜底按资源错误

支持的生命周期信号：
  - cancel（SIGTERM → 优雅停止；进程走 stop/exit_skip 控制则终结）
  - preempt（SIGTERM → 抢占，期待 exit 99）
"""

from __future__ import annotations

import json
import os
import pwd
import signal
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import cgroup as cg
from . import errors, runlog


POLL_INTERVAL_SEC = 0.25
KILL_WAIT_SEC = 5
TRUNCATE_HEAD = "[truncated]\n".encode("utf-8")

# SDK 根目录（让任务进程可 import sched_task_sdk）
_PROJECT_ROOT = Path(__file__).resolve().parent.parent

ENV_PASSTHROUGH = ("PATH", "HOME", "LANG", "TZ", "LC_ALL", "PYTHONPATH", "PYTHONUNBUFFERED")
ENV_BLACKLIST = frozenset({
    "LD_PRELOAD", "LD_LIBRARY_PATH", "LD_AUDIT", "LD_DEBUG",
    "PYTHONSTARTUP", "PYTHONINSPECT", "BASH_ENV", "ENV", "IFS",
    "PATH",  # 防注入伪造解释器/工具链劫持
})


def build_argv(task_dir: Path, entry_file: str, interpreter: str | None, args: list[str],
               config: str | None = None) -> list[str]:
    argv: list[str] = []
    if interpreter:
        argv.append(interpreter)
    argv.append(str((task_dir / entry_file).resolve()))
    argv.extend(list(args))
    if config:
        argv.extend(["--config", config])
    return argv


def build_env(
    task_dir: Path,
    subtask_id: str,
    request_id: str,
    run_id: str,
    trigger: str,
    attempt: int,
    scheduled_at: str,
    config: str | None,
    extra: dict[str, str] | None = None,
    run_user: str | None = None,
    error_rules: dict[str, Any] | None = None,
) -> dict[str, str]:
    import json as _json

    env = {k: os.environ[k] for k in ENV_PASSTHROUGH if k in os.environ}
    if extra:
        env.update({k: v for k, v in extra.items() if k not in ENV_BLACKLIST})
    env.update({
        "SCHED_SUBTASK_ID": subtask_id,
        "SCHED_REQUEST_ID": request_id,
        "SCHED_RUN_ID": run_id,
        "SCHED_TRIGGER": trigger,
        "SCHED_ATTEMPT": str(attempt),
        "SCHED_SCHEDULED_AT": scheduled_at,
        "SCHED_WORKSPACE": str(task_dir),
    })
    if config:
        env["SCHED_CONFIG"] = config
    if run_user:
        try:
            env["HOME"] = pwd.getpwnam(run_user).pw_dir
        except KeyError:
            pass
    if error_rules:
        env["SCHED_ERROR_RULES"] = _json.dumps(error_rules, ensure_ascii=False)
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(_PROJECT_ROOT) + (os.pathsep + existing if existing else "")
    env["PYTHONUNBUFFERED"] = "1"
    return env


def _drop_privileges(user: str | None):
    if not user:
        return
    try:
        pw = pwd.getpwnam(user)
    except KeyError:
        raise
    if pw.pw_uid == os.getuid():
        return
    os.setgid(pw.pw_gid)
    os.setuid(pw.pw_uid)


@dataclass
class RunSpec:
    subtask_id: str
    request_id: str
    run_id: str
    task_dir: Path
    entry_file: str
    interpreter: str | None
    args: list[str]
    cwd: str
    env: dict[str, str]
    trigger: str              # manual | auto | retry | second_pass
    attempt: int
    scheduled_at: str
    config: str | None = None
    cancel: threading.Event = field(default_factory=threading.Event)
    cancel_reason: str = ""
    preempt: threading.Event = field(default_factory=threading.Event)
    run_user: str | None = None
    cgroup_name: str | None = None
    memory_high: int | None = None
    memory_max: int | None = None
    cpu_quota: float | None = None
    cancel_grace_sec: int = 10
    preempt_grace_sec: int = 40
    error_rules: dict[str, Any] = field(default_factory=dict)
    max_log_bytes: int = 32 * 1024 * 1024

    pid: int | None = None
    pgid: int | None = None

    @property
    def workdir(self) -> Path:
        rel = self.cwd or "."
        return self.task_dir if rel == "." else (self.task_dir / rel)

    def argv(self) -> list[str]:
        return build_argv(self.task_dir, self.entry_file, self.interpreter, self.args, self.config)

    def environ(self) -> dict[str, str]:
        return build_env(
            self.task_dir, self.subtask_id, self.request_id, self.run_id,
            self.trigger, self.attempt, self.scheduled_at, self.config,
            self.env, run_user=self.run_user, error_rules=self.error_rules,
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


def _pump_stream(src, dst_path: Path, max_bytes: int) -> None:
    written = 0
    marked = False
    try:
        with open(dst_path, "wb") as out:
            while True:
                chunk = src.read(65536)
                if not chunk:
                    break
                if written < max_bytes:
                    room = max_bytes - written
                    out.write(chunk[:room])
                    out.flush()
                    written += min(len(chunk), room)
                    if len(chunk) > room and not marked:
                        out.write(TRUNCATE_HEAD)
                        marked = True
    except (OSError, ValueError):
        pass
    finally:
        try:
            src.close()
        except (OSError, ValueError):
            pass


def _read_control_from_state(task_dir: Path, run_id: str) -> str:
    """读 state.json 的 control.action（进程可能主动 stop/no_retry/reschedule）。"""
    import json as _json
    candidates = [
        task_dir / "cache" / "runs" / run_id / "state.json",
        task_dir / "cache" / "state.json",
    ]
    for p in candidates:
        try:
            st = _json.loads(p.read_text("utf-8"))
        except (json.JSONDecodeError, OSError):  # noqa: F821
            continue
        ctrl = st.get("control") if isinstance(st, dict) else None
        if isinstance(ctrl, dict) and ctrl.get("action"):
            return str(ctrl.get("action"))
    return ""


def _read_last_error_category(task_dir: Path, run_id: str) -> str:
    import json as _json
    candidates = [
        task_dir / "cache" / "runs" / run_id / "state.json",
        task_dir / "cache" / "state.json",
    ]
    for p in candidates:
        try:
            st = _json.loads(p.read_text("utf-8"))
        except (json.JSONDecodeError, OSError):  # noqa: F821
            continue
        if isinstance(st, dict):
            last = st.get("last_error")
            if isinstance(last, dict) and isinstance(last.get("category"), str):
                return last["category"]
    return ""


def execute(spec: RunSpec, on_started=None) -> RunOutcome:
    stdout = runlog.stdout_path(spec.subtask_id, spec.run_id)
    stderr = runlog.stderr_path(spec.subtask_id, spec.run_id)
    stdout.parent.mkdir(parents=True, exist_ok=True)

    started_at = runlog.now_iso()
    t0 = time.monotonic()
    pid = pgid = exit_code = None
    kind = "exited"
    reason = ""
    scheduler_signaled = False
    cgrp = cg.new_cgroup(spec.cgroup_name or f"{spec.subtask_id}/{spec.request_id}")
    if cgrp.available:
        cgrp.set_limits(spec.memory_high, spec.memory_max, spec.cpu_quota)

    try:
        workdir = spec.workdir
        workdir.mkdir(parents=True, exist_ok=True)
        proc = subprocess.Popen(
            spec.argv(),
            cwd=str(workdir),
            env=spec.environ(),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
            close_fds=True,
            preexec_fn=(lambda: _drop_privileges(spec.run_user)) if spec.run_user else None,
        )
        pump_out = threading.Thread(target=_pump_stream, args=(proc.stdout, stdout, spec.max_log_bytes), daemon=True)
        pump_err = threading.Thread(target=_pump_stream, args=(proc.stderr, stderr, spec.max_log_bytes), daemon=True)
        pump_out.start()
        pump_err.start()
        pid, pgid = proc.pid, proc.pid
        if cgrp.available:
            cgrp.add_process(pid)
        spec.pid = pid
        spec.pgid = pgid
        if on_started:
            try:
                on_started(pid, pgid)
            except Exception:
                pass

        while True:
            # 进程主动请求 stop/no_retry → 调度器优雅 stop
            active = _read_control_from_state(spec.task_dir, spec.run_id)
            if active == "stop":
                scheduler_signaled = True
                _terminate(proc, spec.cancel_grace_sec)
                exit_code = proc.returncode
                kind, reason = "killed", ("进程请求 stop：" + (spec.cancel_reason or ""))
                break
            if active == "no_retry":
                # no_retry 不发信号，进程会自退出
                pass

            if spec.cancel.is_set():
                scheduler_signaled = True
                _terminate(proc, spec.cancel_grace_sec)
                exit_code = proc.returncode
                kind, reason = "killed", (spec.cancel_reason or "被手动终止")
                break
            if spec.preempt.is_set():
                scheduler_signaled = True
                _terminate(proc, spec.preempt_grace_sec)
                exit_code = proc.returncode
                break
            rc = proc.poll()
            if rc is not None:
                exit_code = rc
                if spec.cancel.is_set() or active == "stop":
                    kind, reason, scheduler_signaled = "killed", (spec.cancel_reason or "被终止"), True
                elif spec.preempt.is_set() and rc not in (0, errors.EXIT_PREEMPTED):
                    scheduler_signaled = True
                    exit_code = errors.EXIT_PREEMPTED
                break
            time.sleep(POLL_INTERVAL_SEC)

        pump_out.join(timeout=5)
        pump_err.join(timeout=5)
    except FileNotFoundError as e:
        kind, reason = "failed", f"无法拉起入口：{e}"
    except Exception as e:  # noqa: BLE001
        kind, reason = "failed", f"执行异常：{type(e).__name__}: {e}"
    finally:
        mem_peak = (cgrp.read_peak_mem() or 0) // (1024 * 1024)
        cpu_usec = cgrp.read_cpu_usage_usec() or 0
        oom = (cgrp.read_oom_kill_count() or 0) > 0

    finished_at = runlog.now_iso()
    duration_ms = int((time.monotonic() - t0) * 1000)

    # 退出码归因
    if kind == "exited":
        if exit_code == 0:
            status = "success"
        elif exit_code == errors.EXIT_PREEMPTED:
            status = "preempted"
            reason = "抢占/停止：断点已保存"
        elif exit_code in (errors.EXIT_RESOURCE, errors.EXIT_EXTERNAL_RESOURCE):
            status = "resource"
            reason = f"资源错误（退出码 {exit_code}）"
        elif exit_code == errors.EXIT_DATA_RISK:
            status = "data_risk"
            reason = "数据风险（跳过项）"
        elif exit_code == errors.EXIT_SIGKILL or exit_code == -signal.SIGKILL:
            if oom:
                status, reason = "resource", "cgroup OOM（超限被内核杀）"
            elif scheduler_signaled:
                status, reason = "preempted", "抢占超时未落断点被 SIGKILL（回退重跑）"
            else:
                status, reason = "failed", f"被 SIGKILL（rc={exit_code}）"
        else:
            cat = _read_last_error_category(spec.task_dir, spec.run_id)
            if cat == "logic":
                status, reason = "failed", f"逻辑错误（退出码 {exit_code}，任务自报 logic）"
            else:
                # 兜底按资源错误
                status, reason = "resource", f"资源错误兜底（退出码 {exit_code}）"
    elif kind == "killed":
        status = "interrupted"
    else:
        status = "failed"

    return RunOutcome(
        status=status, exit_code=exit_code, reason=reason,
        pid=pid, pgid=pgid,
        started_at=started_at, finished_at=finished_at, duration_ms=duration_ms,
        scheduler_signaled=scheduler_signaled,
        memory_peak_mb=mem_peak, cpu_usage_usec=cpu_usec, oom_killed=oom,
    )


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


def scan_outputs(task_dir: Path) -> list[dict]:
    """扫描 results/ 下的交付物（用于 UI 展示，不再做强绑定验收）。"""
    d = task_dir / "results"
    if not d.is_dir():
        return []
    out = []
    for p in sorted(d.iterdir()):
        try:
            st = p.stat()
        except OSError:
            continue
        out.append({"path": str(p.relative_to(task_dir)), "size": st.st_size if p.is_file() else None,
                    "is_dir": p.is_dir()})
    return out