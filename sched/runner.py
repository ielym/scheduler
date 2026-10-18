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
from datetime import datetime, timezone
from pathlib import Path

from . import cgroup as cg
from . import config, store
from . import proc as procmod

PROJECT_ROOT = Path(__file__).resolve().parent.parent
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
    heartbeat_interval_sec: float = 5.0


class AttemptRun:
    """一个存活执行尝试的运行句柄（调度器直接拉起的子进程）。

    重启不做接管：旧实例遗留的执行进程在下次启动时被统一终止清除
    （见 SchedulerService._kill_previous_runs），因此这里始终持有一个可 wait()
    的子进程，退出码可从 proc.returncode 取得。
    """

    def __init__(self, spec: AttemptSpec, proc: subprocess.Popen,
                 run_dir: Path, cgrp: "cg.Cgroup", input_refs: list[str]):
        self.spec = spec
        self.proc = proc
        self.run_dir = run_dir
        self.cgrp = cgrp
        self.input_refs = input_refs
        self.pid = proc.pid
        self.pgid = proc.pid
        self.start_time = procmod.start_time(self.pid)
        self.started_at = now_iso()
        self._pause: dict | None = None
        self._terminating = threading.Event()
        # 非阻塞终止状态机：TERM 已发出（记录升级 KILL 的截止）与 KILL 是否已发出
        self._kill_at_mono: float | None = None
        self._kill_sent = False
        # events.jsonl 增量读游标：已消费到的字节偏移与最大 seq（避免每轮全量重读）
        self._read_offset = 0
        self._read_seq = 0

    @property
    def event_file(self) -> Path:
        return self.run_dir / "events.jsonl"

    @property
    def control_file(self) -> Path:
        return self.run_dir / "control.json"

    def is_alive(self) -> bool:
        if self.proc is not None and self.proc.poll() is not None:
            return False
        # PID + 启动时间联合核实，僵尸/退出中进程不算存活（proc.alive 内含僵尸检查）
        return procmod.alive(self.pid, self.start_time)

    # ---- 事件 ----
    def read_events(self, after_seq: int = 0) -> list[dict]:
        """增量读 events.jsonl 中 seq > after_seq 的事件。

        记忆上次读取的字节偏移与最大 seq，本轮只读新增尾部；文件被轮转/截断
        （size < 偏移）时重置游标从头读，保证不漏。尾部不完整的半行不推进
        偏移，下轮重读。
        """
        out: list[dict] = []
        p = self.event_file
        try:
            size = p.stat().st_size
        except OSError:
            self._read_offset = 0
            self._read_seq = 0
            return out
        if size < self._read_offset:
            # 轮转/截断 → 重置增量游标
            self._read_offset = 0
            self._read_seq = 0
        start = self._read_offset if self._read_seq >= after_seq else 0
        try:
            with open(p, "rb") as f:
                f.seek(start)
                data = f.read()
        except OSError:
            return out
        if not data:
            return out
        offset = start
        parts = data.split(b"\n")
        for raw in parts[:-1]:  # 最后一段可能是半行，不消费
            offset += len(raw) + 1
            s = raw.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            try:
                seq = int(obj.get("seq") or 0)
            except (TypeError, ValueError):
                # 畸形 seq：跳过该行（偏移已推进，不会反复重读，也不拖垮整轮）
                continue
            if seq > self._read_seq:
                self._read_seq = seq
            if seq > after_seq:
                out.append(obj)
        self._read_offset = offset
        return out

    # ---- 控制 ----
    def write_pause(self, command_id: str, reason: str, deadline: str) -> None:
        self._pause = {"command_id": command_id, "action": "pause",
                       "job_id": self.spec.job_id, "attempt_id": self.spec.attempt_id,
                       "token": self.spec.token, "reason": reason, "deadline": deadline}
        store.atomic_write_json(self.control_file, self._pause)

    def write_permit(self) -> None:
        """写入启动许可（设计第十三节）：调度器确认 STARTED/RUNNING 后授予，
        worker 在许可确认前不进行业务副作用。"""
        store.atomic_write_json(self.run_dir / "permit.json", {
            "action": "permit",
            "job_id": self.spec.job_id,
            "attempt_id": self.spec.attempt_id,
            "token": self.spec.token,
            "granted_at": now_iso(),
        })

    @property
    def pause_command(self) -> dict | None:
        return self._pause

    def signal_pause(self) -> None:
        """协作暂停：先写控制文件，再发 SIGTERM 让 worker 进入安全边界落断点。"""
        try:
            os.killpg(self.pgid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError, OSError):
            pass

    # ---- 非阻塞终止状态机（主循环内使用） ----
    def begin_terminate(self, now_mono: float, grace_sec: float) -> bool:
        """第一阶段：发 SIGTERM 并记录升级 KILL 的截止时刻，不等待。

        幂等：重复调用不重发信号、不改动截止时刻。返回是否本次首次发出。"""
        if self._kill_at_mono is not None:
            return False
        self._terminating.set()
        self._kill_at_mono = now_mono + max(0.0, float(grace_sec))
        try:
            os.killpg(self.pgid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError, OSError):
            pass
        return True

    def poll_terminate(self, now_mono: float) -> str:
        """按时刻推进终止状态机（不阻塞）：
        - waiting：TERM 已发，等宽限到期或进程自行退出；
        - killed：宽限已到，已升级 SIGKILL + cgroup.kill（含刚升级的这一次）；
        - gone：KILL 后仍存活超过 KILL_WAIT_SEC（不可中断 D 态兜底），
          调用方应按未知退出码直接收尾，不再等待。"""
        if self._kill_at_mono is None:
            return "waiting"
        if now_mono < self._kill_at_mono:
            return "waiting"
        if not self._kill_sent:
            self._kill_sent = True
            try:
                os.killpg(self.pgid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError, OSError):
                pass
            if self.cgrp.available:
                try:
                    self.cgrp.kill_all()
                except OSError:
                    pass
            return "killed"
        if now_mono >= self._kill_at_mono + KILL_WAIT_SEC:
            return "gone"
        return "killed"

    def cleanup(self) -> None:
        self.cgrp.remove()


def pid_alive_with_start(pid: int, start_time: float) -> bool:
    """对外暴露的身份核实（重启接管/直连取消使用）：PID+启动时间，僵尸不算存活。

    委托 sched.proc 统一实现（与生产者守护共用同一套逻辑）。
    """
    return procmod.alive(pid, start_time)


def proc_never_runs_again(pid: int) -> bool:
    """进程是否已不可能再执行代码（退出中/僵尸/不存在）；委托 sched.proc。"""
    return procmod.never_runs_again(pid)


def terminate_external(pid: int, pgid: int, pid_start_time: float,
                       grace_sec: float, cgroup: str = "") -> int | None:
    """终止不由当前进程管理的执行进程树（重启恢复 / serve 不在时的直连取消）。

    A1：setsid 的孙进程脱离进程组，killpg 杀不到；cgroup 可用时以其
    cgroup.kill 无差别终止整树（覆盖漏网后代，设计第七节"确认整个执行
    进程树退出"），不可用时回退 killpg。
    """
    if not pid_alive_with_start(pid, pid_start_time):
        return None
    try:
        os.killpg(pgid or pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError, OSError):
        pass
    deadline = time.monotonic() + max(0.5, grace_sec)
    while time.monotonic() < deadline:
        if not pid_alive_with_start(pid, pid_start_time):
            return 0
        time.sleep(0.1)
    try:
        os.killpg(pgid or pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        pass
    if cgroup:
        try:
            cg.Cgroup(cgroup).kill_all()
        except Exception:
            pass
    time.sleep(0.3)
    return 0 if not pid_alive_with_start(pid, pid_start_time) else -1


def now_iso() -> str:
    """统一 UTC 时间戳（带 tz 后缀，可与任意本地时区时间正确比较）。"""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def build_argv(task_root: Path, entry: config.EntrySpec, entry_args: list[str]) -> list[str]:
    argv: list[str] = []
    if entry.interpreter:
        argv.append(entry.interpreter)
    argv.append(str((task_root / entry.file).resolve()))
    argv.extend(list(entry.args))
    argv.extend(list(entry_args))
    return argv


def build_env(spec: AttemptSpec) -> dict[str, str]:
    env = {k: os.environ[k] for k in ENV_PASSTHROUGH if k in os.environ}
    for k, v in spec.env.items():
        if k not in ENV_BLACKLIST:
            env[k] = v
    env.update({
        "SCHED_ATTEMPT_ID": spec.attempt_id,
        "SCHED_JOB_ID": spec.job_id,
        "SCHED_TASK_ID": spec.task_id,
        "SCHED_TOKEN": spec.token,
        # 代码（只读）与运行态（可写）分离：
        "SCHED_TASK_DIR": str(config.task_dir(spec.task_id)),
        "SCHED_TASK_STATE_DIR": str(config.task_state_dir(spec.task_id)),
        "SCHED_ATTEMPT_DIR": str(config.run_dir(spec.attempt_id)),
        "SCHED_ATTEMPT_WORK_DIR": str(config.attempt_work_dir(spec.attempt_id)),
        # 兼容旧 SDK/任务写法：可写工作区 = 任务级运行态目录
        "SCHED_WORKSPACE": str(config.task_state_dir(spec.task_id)),
        "SCHED_RUN_DIR": str(config.run_dir(spec.attempt_id)),
        "SCHED_EVENT_FILE": str(config.run_dir(spec.attempt_id) / "events.jsonl"),
        "SCHED_CONTROL_FILE": str(config.run_dir(spec.attempt_id) / "control.json"),
        "SCHED_PERMIT_FILE": str(config.run_dir(spec.attempt_id) / "permit.json"),
        "SCHED_HEARTBEAT_INTERVAL_SEC": str(spec.heartbeat_interval_sec),
    })
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(PROJECT_ROOT) + (os.pathsep + existing if existing else "")
    env["PYTHONUNBUFFERED"] = "1"
    return env


def launch(spec: AttemptSpec, input_refs: list[str] | None = None) -> AttemptRun:
    run_dir = config.run_dir(spec.attempt_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    # attempt 级任务临时区（随 attempt 回收；任务自由布局）
    config.attempt_work_dir(spec.attempt_id).mkdir(parents=True, exist_ok=True)
    input_refs = input_refs or []
    # 工作目录（可写）：落在任务**运行态**目录 runtime/tasks/<id>/ 内——
    # entry.cwd 与就绪请求 workdir 均相对它解析（代码在 tasks/，产物在 runtime/）
    base = config.task_state_dir(spec.task_id)
    entry_cwd = getattr(spec.executor_entry, "cwd", ".") or "."
    if entry_cwd not in (".", ""):
        base = base / entry_cwd
    workdir = base if spec.workdir in (".", "") else (base / spec.workdir)
    workdir.mkdir(parents=True, exist_ok=True)

    cgrp = cg.new_cgroup(f"{spec.task_id}/{spec.attempt_id}")
    argv = build_argv(spec.task_root, spec.executor_entry, spec.entry_args)
    env = build_env(spec)

    # 执行日志：重定向到该 attempt 的 run_dir（诊断证据，随工件周期清理——
    # SUCCEEDED/CANCELLED 的 run_dir 7 天后清理，FAILED 永久保留；不进调度器
    # 数据库/元数据，与设计第六节"原始输出不自动归档"的边界一致）。
    # 缓冲模式行刷新：worker 崩溃时已打印内容仍在（O_APPEND 无需 drain）。
    log_fh = open(run_dir / "worker.log", "ab", buffering=0)
    try:
        proc = subprocess.Popen(
            argv,
            cwd=str(workdir),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log_fh,
            stderr=log_fh,
            start_new_session=True,
            close_fds=True,
        )
    except BaseException:
        log_fh.close()  # 拉起失败：关闭父侧日志 fd，避免每次失败泄漏一个句柄
        raise
    log_fh.close()  # 子进程已继承 fd，父进程副本立即关闭
    if cgrp.available:
        cgrp.add_process(proc.pid)
    return AttemptRun(spec, proc, run_dir, cgrp, input_refs)


@dataclass
class AttemptOutcome:
    status: str
    exit_code: int | None
    reason: str
    # attempt 行的落库状态：缺省与 status 相同；"作业回 QUEUED 重跑"时
    # attempt 以 FAILED 收尾（本次执行未按协议完成）而作业状态为 QUEUED
    attempt_status: str | None = None


def decide_outcome(status_ctx: str, exit_code: int | None, had_completed: bool
                   ) -> AttemptOutcome:
    """把进程退出 + 事件证据归因为作业状态。

    status_ctx: scheduler 发起 PAUSING（协作暂停）或 CANCELLED（显式取消）。
    归因优先级：显式控制 > 退出 0 且有持久化完成记录 > 不足（仅退出码 75）
    > 失败；退出 0 但未提交完成记录、或退出码未知 → 作业回 QUEUED 重跑。

    设计第六节：只有约定退出码 75 才是"资源不足"——运行期曾报过不足
    事件不得把其它非零退出扩大为外部争用（业务逻辑错误必须归为 FAILED）。
    """
    from .config import JobStatus, EXIT_SHORTAGE

    if status_ctx == JobStatus.CANCELLED:
        if had_completed:
            # 取消请求到达时完成记录已持久化：完成证据优先于取消意图
            return AttemptOutcome(JobStatus.SUCCEEDED, exit_code,
                                  "取消请求到达前已完成并持久化完成记录")
        return AttemptOutcome(JobStatus.CANCELLED, exit_code, "显式取消")
    if status_ctx == JobStatus.PAUSING:
        # 完成证据优先于暂停意图：暂停请求到达时若已发 done（业务已完成），
        # 不得误标为 PAUSED 再排队重跑已完成的作业。
        if exit_code == 0 and had_completed:
            return AttemptOutcome(JobStatus.SUCCEEDED, exit_code,
                                  "暂停请求到达前已完成并持久化完成记录")
        if exit_code == EXIT_SHORTAGE:
            return AttemptOutcome(JobStatus.BACKOFF, exit_code,
                                  "资源不足（暂停请求中退出）")
        if exit_code == 0:
            return AttemptOutcome(JobStatus.PAUSED, exit_code, "协作暂停")
        # 非 0/75 退出（崩溃/被杀）：保守按 PAUSED 重新排队
        code = exit_code if exit_code is not None else "未知"
        return AttemptOutcome(JobStatus.PAUSED, exit_code,
                              f"暂停请求中退出（码 {code}），重新排队")
    if exit_code == 0 and had_completed:
        return AttemptOutcome(JobStatus.SUCCEEDED, exit_code, "完成")
    if exit_code == EXIT_SHORTAGE:
        return AttemptOutcome(JobStatus.BACKOFF, exit_code, "资源不足")
    if exit_code == 0:
        # 退出 0 但未显式提交完成记录：本次执行不算完成，作业重新排队重跑
        # （attempt 以 FAILED 收尾留证据，作业状态为 QUEUED）
        return AttemptOutcome(JobStatus.QUEUED, exit_code,
                              "退出 0 但未提交完成记录，重新排队重跑",
                              attempt_status=JobStatus.FAILED)
    if exit_code is None:
        # 退出码未知（无法 wait 的异常情形）：按"重新开始"回队列重跑
        return AttemptOutcome(JobStatus.QUEUED, None,
                              "退出码未知，重新排队重跑",
                              attempt_status=JobStatus.FAILED)
    return AttemptOutcome(JobStatus.FAILED, exit_code, f"业务/未知失败（退出码 {exit_code}）")