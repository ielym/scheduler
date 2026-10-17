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
from typing import Any

from . import cgroup as cg
from . import config

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
    """一个存活执行尝试的运行句柄。

    proc 为 None 时表示"接管句柄"（adopted）：调度器重启后对身份核实仍存活
    的旧 worker 重新挂接事件/控制文件继续观测（设计第十三节 L221"存活 worker
    可以通过事件与控制协议接管"）。接管进程不是当前实例的子进程，无法
    wait() 拿退出码——退出码未知时由 decide_outcome 归入 RECOVERING 待核实。
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
        self.start_time = _proc_start_time(self.pid)
        self.started_at = now_iso()
        self._pause: dict | None = None
        self._terminating = threading.Event()
        # 非阻塞终止状态机：TERM 已发出（记录升级 KILL 的截止）与 KILL 是否已发出
        self._kill_at_mono: float | None = None
        self._kill_sent = False
        # events.jsonl 增量读游标：已消费到的字节偏移与最大 seq（避免每轮全量重读）
        self._read_offset = 0
        self._read_seq = 0

    @classmethod
    def adopted(cls, spec: "AttemptSpec", run_dir: Path, cgrp: "cg.Cgroup",
                input_refs: list[str], pid: int, pgid: int,
                start_time: float, last_seq: int = 0) -> "AttemptRun":
        """构造接管句柄：沿用持久化的执行身份（PID/PGID/启动时间/cgroup）。"""
        self = cls.__new__(cls)
        self.spec = spec
        self.proc = None
        self.run_dir = run_dir
        self.cgrp = cgrp
        self.input_refs = input_refs
        self.pid = int(pid)
        self.pgid = int(pgid)
        self.start_time = float(start_time)
        self.started_at = now_iso()
        self._pause = None
        self._terminating = threading.Event()
        self._kill_at_mono = None
        self._kill_sent = False
        self._read_offset = 0
        self._read_seq = int(last_seq)
        return self

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
        if self.proc is not None and self.proc.poll() is not None:
            return False
        if not _pid_alive(self.pid, self.start_time):
            return False
        # 僵尸/退出中进程不算存活（审查修复）：接管句柄的 worker 退出后，
        # 若其父进程（旧调度器）仍存活而未 wait，会以僵尸态留在 /proc——
        # PID+启动时间核实通过但进程已不可能再执行代码或产生事件
        return not proc_never_runs_again(self.pid)

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
        _atomic_write(self.control_file, self._pause)

    def write_permit(self) -> None:
        """写入启动许可（设计第十三节）：调度器确认 STARTED/RUNNING 后授予，
        worker 在许可确认前不进行业务副作用。"""
        _atomic_write(self.run_dir / "permit.json", {
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

    def terminate(self, grace_sec: float) -> int | None:
        """整棵进程树 SIGTERM → 等 grace → SIGKILL → cgroup.kill 兜底（防 setsid 漏杀）。

        仅用于停机/启动恢复等一次性路径；主循环内请用 begin_terminate/poll_terminate
        非阻塞状态机——同步等待会冻结整个控制平面（快照/命令/事件/准入）。
        接管句柄（proc=None）走外部终止路径（无子进程 wait 权限，按身份核实轮询）。
        """
        self._terminating.set()
        try:
            os.killpg(self.pgid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError, OSError):
            pass
        if self.proc is None:
            return terminate_external(self.pid, self.pgid, self.start_time,
                                      max(0.5, grace_sec),
                                      cgroup=self.cgrp.name if self.cgrp.available else "")
        try:
            self.proc.wait(timeout=grace_sec)
            return self.proc.returncode
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(self.pgid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            pass
        if self.cgrp.available:
            try:
                self.cgrp.kill_all()  # cgroup v2 无差别终止，覆盖自行 setsid 的后代
            except OSError:
                pass
            for _ in range(4):
                if not self.cgrp.procs():
                    break
                time.sleep(0.1)
        try:
            self.proc.wait(timeout=KILL_WAIT_SEC)
        except subprocess.TimeoutExpired:
            pass
        return self.proc.returncode

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


def _pid_alive(pid: int, start_time: float | None = None) -> bool:
    """PID + 进程启动时间联合身份核实（防 PID 复用误判，设计文档第十三节）。"""
    if not pid:
        return False
    try:
        st = os.stat(f"/proc/{pid}")
        if start_time and start_time != st.st_ctime:
            return False
    except OSError:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def pid_alive_with_start(pid: int, start_time: float) -> bool:
    """对外暴露的身份核实（重启接管/直连取消使用）。

    僵尸/退出中进程不算存活（审查修复）：接管场景下旧 worker 退出后可能以
    僵尸态留在 /proc（父进程未 wait），PID+启动时间核实通过但进程已死。
    """
    if not _pid_alive(pid, start_time):
        return False
    return not proc_never_runs_again(pid)


_ZOMBIE_STATES = frozenset((b"Z", b"x", b"X"))


def proc_never_runs_again(pid: int) -> bool:
    """进程是否已不可能再执行代码（退出中/僵尸/不存在）。

    整树退出核实用它过滤 cgroup.procs 里的残留 pid：僵尸进程不消耗 CPU、
    不写任何资源、信号杀不掉，等待它被父进程/init 收割不应阻塞 finalize；
    cgroup 不可用降级为静态文件时（写入即不再更新）同理。
    审查修复：/proc/<pid>/stat 读出的是 bytes，状态字符必须与 bytes 集合
    比较（此前 b"Z" in frozenset("ZxX") 恒 False，僵尸检测从未生效）。
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
        "SCHED_PERMIT_FILE": str(config.run_dir(spec.attempt_id) / "permit.json"),
        "SCHED_WORKSPACE": str(spec.task_root),
        "SCHED_HEARTBEAT_INTERVAL_SEC": str(spec.heartbeat_interval_sec),
    })
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(PROJECT_ROOT) + (os.pathsep + existing if existing else "")
    env["PYTHONUNBUFFERED"] = "1"
    return env


def launch(spec: AttemptSpec, input_refs: list[str] | None = None) -> AttemptRun:
    run_dir = config.run_dir(spec.attempt_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    input_refs = input_refs or []
    # 工作目录：task.json 的 entry.cwd 为基，就绪请求的 workdir 相对它解析
    # （两者均默认 "."，与既有行为一致；路径合法性已在 parse_task/parse_ready 校验）
    base = spec.task_root
    entry_cwd = getattr(spec.executor_entry, "cwd", ".") or "."
    if entry_cwd not in (".", ""):
        base = spec.task_root / entry_cwd
    workdir = base if spec.workdir in (".", "") else (base / spec.workdir)
    workdir.mkdir(parents=True, exist_ok=True)

    cgrp = cg.new_cgroup(f"{spec.task_id}/{spec.attempt_id}")
    argv = build_argv(spec.task_root, spec.executor_entry, spec.entry_args)
    env = build_env(spec, input_refs)

    # 执行日志：重定向到该 attempt 的 run_dir（诊断证据，随工件周期清理——
    # SUCCEEDED/CANCELLED 的 run_dir 7 天后清理，FAILED 永久保留；不进调度器
    # 数据库/元数据，与设计第六节"原始输出不自动归档"的边界一致）。
    # 缓冲模式行刷新：worker 崩溃时已打印内容仍在（O_APPEND 无需 drain）。
    log_fh = open(run_dir / "worker.log", "ab", buffering=0)

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
    log_fh.close()  # 子进程已继承 fd，父进程副本立即关闭
    if cgrp.available:
        cgrp.add_process(proc.pid)
    return AttemptRun(spec, proc, run_dir, cgrp, input_refs)


@dataclass
class AttemptOutcome:
    status: str
    exit_code: int | None
    reason: str


def decide_outcome(status_ctx: str, exit_code: int | None, had_completed: bool
                   ) -> AttemptOutcome:
    """把进程退出 + 事件证据归因为作业状态。

    status_ctx: scheduler 发起 PAUSING（协作暂停）或 CANCELLED（显式取消）。
    归因优先级：显式控制 > 退出 0 且有持久化完成记录 > 不足（仅退出码 75）>
    退出 0 无完成记录（待核实）> 失败。

    设计第六节：只有约定退出码 75 才是"资源不足"——运行期曾报过不足
    事件不得把其它非零退出扩大为外部争用（业务逻辑错误必须归为 FAILED）。
    """
    from .config import JobStatus, EXIT_SHORTAGE

    if status_ctx == JobStatus.CANCELLED:
        if had_completed:
            # 取消请求到达时完成记录已持久化：完成证据优先于取消意图
            # （设计第十二节：退出 0 + 持久化完成记录 → SUCCEEDED）
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
            # 暂停请求中以外不足退出：断点未必已保存，语义是不足而非已暂停
            return AttemptOutcome(JobStatus.BACKOFF, exit_code,
                                  "资源不足（暂停请求中退出）")
        if exit_code == 0:
            return AttemptOutcome(JobStatus.PAUSED, exit_code, "协作暂停，断点已保存")
        # 非 0/75 退出（崩溃/被杀）：断点可能未保存，保守按 PAUSED 重新排队核实，
        # 不得冒充"断点已保存"（跨 attempt 恢复依赖该语义，设计第七节）
        code = exit_code if exit_code is not None else "未知"
        return AttemptOutcome(JobStatus.PAUSED, exit_code,
                              f"暂停请求中退出（码 {code}），断点可能未保存，重新排队")
    if exit_code == 0 and had_completed:
        return AttemptOutcome(JobStatus.SUCCEEDED, exit_code, "完成")
    if exit_code == EXIT_SHORTAGE:
        return AttemptOutcome(JobStatus.BACKOFF, exit_code, "资源不足")
    if exit_code == 0 and not had_completed:
        return AttemptOutcome(JobStatus.RECOVERING, exit_code, "退出 0 但无完成记录，待核实")
    if exit_code is None:
        # 接管句柄（非子进程）无法获取退出码：状态歧义按 RECOVERING 待核实
        # （设计第十二节 L203"状态歧义为 RECOVERING"），由恢复协议核实完成记录，
        # 不简单从头执行（设计第十三节 L223）。
        return AttemptOutcome(JobStatus.RECOVERING, exit_code,
                              "退出码未知（接管进程退出），待核实")
    return AttemptOutcome(JobStatus.FAILED, exit_code, f"业务/未知失败（退出码 {exit_code}）")