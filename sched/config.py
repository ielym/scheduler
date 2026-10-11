"""sched 路径常量、全局配置（config.json）、任务清单（task.json）与就绪请求（outbox JSON）协议。

目录布局（设计文档第二节）：
    SCHED_HOME/
      config.json          全局配置
      tasks/<task_id>/     task.json + 生产者入口 + 执行入口 + producer_state + outbox + receipts
      runtime/             单实例锁、执行数据库、请求归档、隔离、执行事件、控制文件、检查点、状态日志

任务不填写最大运行/排队数量或默认资源申请；生产者由调度器独立守护且不施加执行资源控制。
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

# sched/config.py -> sched/ -> 项目根
DEFAULT_HOME = Path(__file__).resolve().parent.parent

# 协议版本
TASK_SCHEMA_VERSION = 1
READY_SCHEMA_VERSION = 1
CONFIG_SCHEMA_VERSION = 1

TASK_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
JOB_ID_RE = re.compile(r"^[a-zA-Z0-9._:-]{1,256}$")


# ---------- 作业 / attempt 状态（设计文档第十二节） ----------
class JobStatus:
    QUEUED = "QUEUED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    PAUSING = "PAUSING"
    PAUSED = "PAUSED"
    BACKOFF = "BACKOFF"          # 外部不足已退出、等待到期探测
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"            # 业务逻辑 / 未知非资源失败
    RECOVERING = "RECOVERING"
    RECOVERY_BLOCKED = "RECOVERY_BLOCKED"
    CANCELLED = "CANCELLED"      # 显式取消

    # 存活状态：STARTING/RUNNING/PAUSING 及尚未确认退出的状态都算存活
    LIVE = {STARTING, RUNNING, PAUSING, RECOVERING, RECOVERY_BLOCKED}
    TERMINAL = {SUCCEEDED, FAILED, CANCELLED}
    RESUMPTABLE = {PAUSED, BACKOFF}


# 执行事件种类（设计文档第六节）
class EventKind:
    STARTED = "started"
    HEARTBEAT = "heartbeat"
    PROGRESS = "progress"
    SHORTAGE = "external_shortage"   # 统一外部不足
    CONTROL_ACK = "control_ack"
    CHECKPOINT = "checkpoint"
    COMPLETED = "completed"


# 退出码（与 sched_task_sdk/errors.py 镜像）
EXIT_OK = 0
EXIT_SHORTAGE = 75   # 统一外部不足


def home() -> Path:
    return Path(os.environ.get("SCHED_HOME") or DEFAULT_HOME)


def config_file() -> Path:
    return home() / "config.json"


def tasks_dir() -> Path:
    return home() / "tasks"


def runtime_dir() -> Path:
    return home() / "runtime"


def lock_file() -> Path:
    return runtime_dir() / "scheduler.lock"


def db_file() -> Path:
    return runtime_dir() / "exec.db"


def logs_dir() -> Path:
    return runtime_dir() / "logs"


def runs_root() -> Path:
    return runtime_dir() / "runs"


def status_log() -> Path:
    return runtime_dir() / "status.jsonl"


def task_dir(task_id: str) -> Path:
    return tasks_dir() / task_id


def producer_state_dir(task_id: str) -> Path:
    return task_dir(task_id) / "producer_state"


def outbox_dir(task_id: str) -> Path:
    return task_dir(task_id) / "outbox"


def receipts_dir(task_id: str) -> Path:
    return task_dir(task_id) / "receipts"


def run_dir(attempt_id: str) -> Path:
    return runs_root() / attempt_id


# ---------- 全局配置 config.json ----------
class ControlConfig(BaseModel):
    loop_tick_sec: float = 1.0              # 主循环 + 事件消费
    sample_interval_sec: float = 2.0        # 执行资源采样
    heartbeat_interval_sec: float = 5.0     # 心跳初值
    producer_restart_base_sec: float = 1.0  # 生产者崩溃退避起点
    producer_restart_cap_sec: float = 30.0  # 生产者崩溃退避上限
    producer_crash_reset_after_sec: float = 300.0  # 崩溃退避连续无崩溃后的复位
    start_interval_sec: float = 1.0         # 全局启动间隔（错开启动，防突发）
    grow_observe_sec: float = 30.0          # 扩容观察期初值
    pause_grace_base_sec: float = 10.0      # 暂停宽限节奏初值
    pause_grace_max_sec: float = 60.0
    pulse_wait_sec: float = 1.0             # 暂停确认轮询间隔
    shortage_retry_base_sec: float = 5.0    # 不足重试指数退避起点
    shortage_retry_cap_sec: float = 900.0
    protect_deadline_sec: float = 300.0     # 整轮保护截止时间初值
    protect_round_backoff_sec: float = 60.0 # 失败保护轮间等待
    slow_confidence_sec: float = 120.0      # 变慢判定连续证据窗口
    recovery_probe_sec: float = 60.0        # 恢复探测最小机会间隔


class SchedulerConfig(BaseModel):
    schema_version: int = CONFIG_SCHEMA_VERSION
    timezone: str = "Asia/Shanghai"
    control: ControlConfig = Field(default_factory=ControlConfig)

    model_config = {"extra": "ignore"}

    @field_validator("timezone")
    @classmethod
    def _tz_ok(cls, v: str) -> str:
        try:
            from zoneinfo import ZoneInfo
            ZoneInfo(v)
        except Exception as e:
            raise ValueError(f"无效时区：{v!r}") from e
        return v


# ---------- 任务清单 task.json ----------
class EntrySpec(BaseModel):
    file: str
    interpreter: str = "python3"
    args: list[str] = Field(default_factory=list)
    cwd: str = "."

    model_config = {"extra": "ignore"}


class RecoverySpec(BaseModel):
    """生产者恢复方式说明：数据源如何重放、保留多久、如何识别遗漏。"""
    replay: str = "outbox"       # outbox | source | none
    retain: str = ""             # 数据源保留描述
    replay_note: str = ""        # 如何识别遗漏

    model_config = {"extra": "ignore"}


class TaskSpec(BaseModel):
    schema_version: int = TASK_SCHEMA_VERSION
    task_id: str
    enabled: bool = True
    priority: int = 0
    producer_entry: EntrySpec      # 生产者入口（常驻）
    executor_entry: EntrySpec      # 执行入口
    workdir: str = "."
    progress_semver: str = "1.0.0"
    recovery: RecoverySpec = Field(default_factory=RecoverySpec)

    model_config = {"extra": "ignore"}

    @field_validator("task_id")
    @classmethod
    def _task_id_ok(cls, v: str) -> str:
        if not TASK_ID_RE.match(v or ""):
            raise ValueError(f"task_id 非法：{v!r}")
        if v.startswith("_") or v.startswith("."):
            raise ValueError(f"task_id 不能以 '_' 或 '.' 开头：{v!r}")
        return v

    @field_validator("priority")
    @classmethod
    def _pri_ok(cls, v: int) -> int:
        if isinstance(v, bool):
            raise ValueError("priority 必须是整数")
        return v


class TaskError(Exception):
    def __init__(self, errors: list[str] | str):
        self.errors = [errors] if isinstance(errors, str) else errors
        super().__init__("; ".join(self.errors))


def _rel_within(base: Path, rel: str, field: str) -> Path:
    p = Path(rel or ".")
    if p.is_absolute():
        raise TaskError(f"{field} 必须是相对路径：{rel}")
    if ".." in p.parts:
        raise TaskError(f"{field} 不能包含 '..'：{rel}")
    target = (base / p).resolve()
    if target != base.resolve() and base.resolve() not in target.parents:
        raise TaskError(f"{field} 越出了任务目录：{rel}")
    return target


def parse_task(raw: Any, task_id: str) -> TaskSpec:
    if not isinstance(raw, dict):
        raise TaskError("task.json 顶层必须是对象")
    try:
        doc = TaskSpec.model_validate(raw)
    except Exception as e:
        raise TaskError(str(e)) from e
    if doc.task_id != task_id:
        raise TaskError(f"task.json 内 task_id '{doc.task_id}' 与目录名 '{task_id}' 不一致")
    base = task_dir(task_id)
    for field, entry in (("producer_entry", doc.producer_entry),
                         ("executor_entry", doc.executor_entry)):
        p = _rel_within(base, entry.file, f"{field}.file")
        if not p.exists():
            raise TaskError(f"{field}.file 不存在：{entry.file}")
    _rel_within(base, doc.workdir or ".", "workdir")
    return doc


def load_task(task_id: str) -> TaskSpec:
    if not TASK_ID_RE.match(task_id or ""):
        raise TaskError(f"task_id 非法：{task_id!r}")
    td = task_dir(task_id)
    if not td.is_dir():
        raise TaskError(f"任务目录不存在：{td}")
    tj = td / "task.json"
    if not tj.exists():
        raise TaskError(f"缺少 task.json：{tj}")
    try:
        import json
        raw = json.loads(tj.read_text("utf-8"))
    except json.JSONDecodeError as e:
        raise TaskError(f"task.json 不是合法 JSON：{e}") from e
    except OSError as e:
        raise TaskError(f"读取 task.json 失败：{e}") from e
    return parse_task(raw, task_id)


# ---------- 就绪请求 outbox JSON ----------
class ReadyJob(BaseModel):
    schema_version: int = READY_SCHEMA_VERSION
    job_id: str
    task_id: str
    entry_args: list[str] = Field(default_factory=list)
    workdir: str = "."
    input_refs: list[str] = Field(default_factory=list)
    idempotency_key: str = ""
    progress_semver: str = "1.0.0"
    env: dict[str, str] = Field(default_factory=dict)

    model_config = {"extra": "ignore"}

    @field_validator("job_id")
    @classmethod
    def _job_id_ok(cls, v: str) -> str:
        if not JOB_ID_RE.match(v or ""):
            raise ValueError(f"job_id 非法：{v!r}")
        return v


class ReadyError(Exception):
    def __init__(self, errors: list[str] | str):
        self.errors = [errors] if isinstance(errors, str) else errors
        super().__init__("; ".join(self.errors))


def parse_ready(raw: Any) -> ReadyJob:
    if not isinstance(raw, dict):
        raise ReadyError("就绪 JSON 顶层必须是对象")
    try:
        return ReadyJob.model_validate(raw)
    except Exception as e:
        raise ReadyError(str(e)) from e


# ---------- 全局配置读写 ----------
def ensure_layout() -> None:
    for d in (tasks_dir(), runtime_dir(), runs_root(), logs_dir()):
        d.mkdir(parents=True, exist_ok=True)
    cf = config_file()
    if not cf.exists():
        from . import store
        store.save_json(cf, SchedulerConfig().model_dump())


def load_config() -> SchedulerConfig:
    from . import store
    raw = store.load_json(config_file(), default=None)
    if not isinstance(raw, dict):
        return SchedulerConfig()
    try:
        return SchedulerConfig.model_validate(raw)
    except Exception:
        return SchedulerConfig()


def save_config(c: SchedulerConfig) -> SchedulerConfig:
    from . import store
    store.save_json(config_file(), c.model_dump())
    return c