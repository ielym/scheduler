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


# 环境变量白名单透传（防调度器机密泄露到任务进程）；任务 env 黑名单（防注入提权/库劫持）
ENV_PASSTHROUGH = ("PATH", "HOME", "LANG", "TZ", "LC_ALL", "PYTHONPATH", "PYTHONUNBUFFERED")
ENV_BLACKLIST = frozenset({
    "LD_PRELOAD", "LD_LIBRARY_PATH", "LD_AUDIT", "LD_DEBUG",
    "PYTHONSTARTUP", "PYTHONINSPECT", "BASH_ENV", "ENV", "IFS",
    "PATH",  # 防注入伪造解释器/工具链劫持
})


def build_env(
    task_dir: Path,
    job_id: str,
    run_id: str,
    trigger: str,
    attempt: int,
    scheduled_at: str,
    extra: dict[str, str] | None = None,
    dry_run: bool = False,
    run_user: str | None = None,
    error_rules: dict | None = None,
) -> dict[str, str]:
    env = {k: os.environ[k] for k in ENV_PASSTHROUGH if k in os.environ}
    if extra:  # 黑名单键在注册期（taskdoc 校验）已被拒绝，此处兜底过滤
        env.update({k: v for k, v in extra.items() if k not in ENV_BLACKLIST})
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
    # 降权任务的 HOME 应为目标用户家目录（透传调度器的 /root 会造成权限异常）
    if run_user:
        try:
            env["HOME"] = pwd.getpwnam(run_user).pw_dir
        except KeyError:
            pass
    # 把 task.json 的 error_rules 注入任务进程，SDK 据此设置分类正则
    if error_rules:
        import json as _json
        env["SCHED_ERROR_RULES"] = _json.dumps(error_rules, ensure_ascii=False)
    # 让任务进程可 import sched_task_sdk（与 SCHED_HOME 解耦）
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(_PROJECT_ROOT) + (os.pathsep + existing if existing else "")
    return env


def _drop_privileges(user: str | None):
    """preexec_fn：把子进程降权到 run_user。用户不存在/已是当前用户则跳过。

    F6：setuid 失败改为上抛（通过 preexec_fn 机制暴露为 OSError 到 Popen），
    绝不静默以调度器身份执行任务代码。
    """
    if not user:
        return
    try:
        pw = pwd.getpwnam(user)
    except KeyError:
        # 上抛 KeyError（Popen 以 SubprocessError 暴露），测试环境用空 run_user 跳过
        raise
    if pw.pw_uid == os.getuid():
        return
    os.setgid(pw.pw_gid)
    os.setuid(pw.pw_uid)


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
    # task.json 的 error_rules，经环境变量注入任务进程
    error_rules: dict = field(default_factory=dict)
    # stdout/stderr 落盘上限（字节）；超出保留头部并写截断标记（默认 32MB）
    max_log_bytes: int = 32 * 1024 * 1024
    # 心跳超时等非人工取消的原因（随 cancel 置位前写入，用于终态归因与重入队判定）
    cancel_reason: str = ""

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
            run_user=self.run_user, error_rules=self.error_rules,
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


def _pump_stream(src, dst_path: Path, max_bytes: int) -> None:
    """把子进程 stdout/stderr 落盘，超过 max_bytes 丢弃后续并写截断标记（磁盘保护）。"""
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
        # stdout/stderr 走管道 + 泵线程落盘，超出上限截断（防日志打爆磁盘）
        proc = subprocess.Popen(
            spec.argv(),
            cwd=str(workdir),
            env=spec.environ(),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
            close_fds=True,
            preexec_fn=(lambda: _drop_privileges(spec.run_user)),
        )
        pump_out = threading.Thread(target=_pump_stream, args=(proc.stdout, stdout, spec.max_log_bytes), daemon=True)
        pump_err = threading.Thread(target=_pump_stream, args=(proc.stderr, stderr, spec.max_log_bytes), daemon=True)
        pump_out.start()
        pump_err.start()
        pid, pgid = proc.pid, proc.pid
        if cgrp.available:
            cgrp.add_process(pid)
        if on_started:
            try:
                on_started(pid, pgid)
            except Exception:
                pass

        while True:
            # 先查调度信号再查进程状态：避免 SIGTERM 后进程先退出、
            # rc 先到会让 cancel/preempt 语义被退出码兜底覆盖
            if spec.cancel.is_set():
                scheduler_signaled = True
                _terminate(proc, spec.cancel_grace_sec)
                exit_code = proc.returncode
                kind, reason = "killed", (spec.cancel_reason or "被手动终止")
                break
            if spec.preempt.is_set():
                # 优雅抢占：SIGTERM 让任务托管断点后自行退出 99；超时再 SIGKILL
                scheduler_signaled = True
                _terminate(proc, spec.preempt_grace_sec)
                exit_code = proc.returncode
                break
            rc = proc.poll()
            if rc is not None:
                exit_code = rc
                if spec.cancel.is_set():
                    # 竞态：stop()/cancel 的 SIGTERM 与进程退出几乎同时发生
                    kind, reason, scheduler_signaled = "killed", (spec.cancel_reason or "被手动终止"), True
                elif spec.preempt.is_set() and rc not in (0, errors.EXIT_PREEMPTED):
                    scheduler_signaled = True
                    exit_code = errors.EXIT_PREEMPTED  # 抢占窗口内退出按抢占语义归因
                break
            time.sleep(POLL_INTERVAL_SEC)
        # 进程已终结：等待泵线程把管道排干（孙进程持有管道时超时放弃，泵为 daemon）
        pump_out.join(timeout=5)
        pump_err.join(timeout=5)
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
    state_category = _read_last_error_category(spec.task_dir, spec.run_id)

    # 终态判定
    if kind == "exited":
        if exit_code == 0:
            status = runlog.STATUS_SUCCESS
        elif exit_code == errors.EXIT_PREEMPTED:
            status = runlog.STATUS_PREEMPTED
            reason = "抢占：断点已保存"
        elif exit_code in (errors.EXIT_RESOURCE, errors.EXIT_EXTERNAL_RESOURCE):
            status = runlog.STATUS_RESOURCE
            reason = f"资源错误（退出码 {exit_code}）"
        elif exit_code == errors.EXIT_DATA_RISK:
            status = runlog.STATUS_FAILED
            reason = "data_risk"
        elif exit_code == errors.EXIT_SIGKILL or exit_code == -signal.SIGKILL:
            # SIGKILL：Popen 直接 exec 时返回 -9，经 shell 包装才是 137；
            # 两种形态都要识别，否则 OOM/外部 SIGKILL 归因会失效
            if oom:
                status, reason = runlog.STATUS_RESOURCE, "cgroup OOM（超限被内核杀）"
            elif scheduler_signaled:
                status, reason = runlog.STATUS_PREEMPT_FAILED, "终结超时被 SIGKILL（断点未确认）"
            else:
                status, reason = runlog.STATUS_FAILED, f"被 SIGKILL（外部，rc={exit_code}）"
        else:
            # 其余非 0 退出码：优先用任务自报分类；无自报时按退出码兜底。
            # 未分类（unclassified）一律按资源错误处理（可重试），而非转人工
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


def _read_last_error_category(task_dir: Path, run_id: str = "") -> str:
    """读取任务 SDK 写入的 last_error.category（精确分类通道）。

    F4：优先读实例级 cache/runs/<run_id>/state.json，回落 legacy cache/state.json。
    """
    import json

    candidates = []
    if run_id:
        candidates.append(task_dir / "cache" / "runs" / run_id / "state.json")
    candidates.append(task_dir / "cache" / "state.json")
    for p in candidates:
        if not p.exists():
            continue
        try:
            st = json.loads(p.read_text("utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if not isinstance(st, dict):
            continue
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