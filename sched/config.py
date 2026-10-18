"""sched 路径常量、全局配置（config.json）、任务清单（task.json）与就绪请求（outbox JSON）协议。

目录布局（设计文档第二节）：
    SCHED_HOME/
      config.json          全局配置（全机唯一）
      tasks/<task_id>/     ★只读定义：task.json（含静态 params）+ 生产者入口 + 执行入口
      runtime/             ★全部运行态（可整体删除）
        scheduler.lock
        state/             exec.db(+wal/shm) / snapshot.json / timing.json
        ipc/               commands.jsonl / commands_ack.jsonl
        logs/              status.jsonl* / web_metrics.jsonl* / <task_id>/producer.*
        artifacts/         runs/<attempt_id>/（含 work/ 随 attempt 回收）/ archive/<task_id>/ / isolation/<task_id>/
        tasks/<task_id>/   任务级运行态：outbox/ receipts/ producer_state/（契约固定）+ 任务自由布局

目录契约：
- tasks/<task_id>/ 只放代码与静态定义，**不存放任何运行时产物**；
- 任务级运行态（生产者游标/outbox/receipts/任务自建缓存等）统一在 runtime/tasks/<task_id>/；
- attempt 级临时区 runtime/artifacts/runs/<attempt_id>/work/ 随 attempt 回收。

任务不填写最大运行/排队数量或默认资源申请；生产者由调度器独立守护且不施加执行资源控制。
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

# sched/config.py -> sched/ -> 项目根
DEFAULT_HOME = Path(__file__).resolve().parent.parent

# 协议版本
TASK_SCHEMA_VERSION = 1
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
    BACKOFF = "BACKOFF"          # 资源不足已退出、等待到期探测
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"            # 业务逻辑 / 未知非资源失败
    CANCELLED = "CANCELLED"      # 显式取消

    # 存活状态：STARTING/RUNNING/PAUSING 及尚未确认退出的状态都算存活
    LIVE = {STARTING, RUNNING, PAUSING}
    TERMINAL = {SUCCEEDED, FAILED, CANCELLED}
    RESUMPTABLE = {PAUSED, BACKOFF}


# 执行事件种类（统一 6 种：通信格式统一，type 只区分语义）
class EventKind:
    STARTED = "started"
    HEARTBEAT = "heartbeat"
    PROGRESS = "progress"
    ERROR = "error"
    DONE = "done"
    ACK = "ack"


# 退出码（与 sched_task_sdk/errors.py 镜像）
EXIT_OK = 0
EXIT_SHORTAGE = 75   # 资源不足（资源类错误，可恢复退让）


def home() -> Path:
    return Path(os.environ.get("SCHED_HOME") or DEFAULT_HOME)


def config_file() -> Path:
    return home() / "config.json"


def tasks_dir() -> Path:
    return home() / "tasks"


def runtime_dir() -> Path:
    return home() / "runtime"


def lock_file() -> Path:
    """调度器单实例锁。"""
    return runtime_dir() / "scheduler.lock"


# ---------- runtime/state：权威状态与快照 ----------
def state_dir() -> Path:
    return runtime_dir() / "state"


def db_file() -> Path:
    return state_dir() / "exec.db"


def snapshot_file() -> Path:
    """serve 进程每轮发布的运行时快照（CLI status 读取）。"""
    return state_dir() / "snapshot.json"


def timing_file() -> Path:
    """任务级反馈节奏 EMA 持久化。"""
    return state_dir() / "timing.json"


# ---------- runtime/ipc：CLI/Web ↔ serve 控制通道 ----------
def ipc_dir() -> Path:
    return runtime_dir() / "ipc"


def commands_file() -> Path:
    """CLI → serve 控制命令队列（JSONL，serve 消费后写入 ack 文件）。"""
    return ipc_dir() / "commands.jsonl"


def commands_ack_file() -> Path:
    return ipc_dir() / "commands_ack.jsonl"


# ---------- runtime/logs：审计/指标/生产者日志 ----------
def logs_dir() -> Path:
    return runtime_dir() / "logs"


def status_log() -> Path:
    """调度器状态/审计流水（web 控制台读取末尾展示）。"""
    return logs_dir() / "status.jsonl"


def web_metrics_file() -> Path:
    """Web 控制台图表历史采样流。"""
    return logs_dir() / "web_metrics.jsonl"


# ---------- runtime/artifacts：执行工件 / 归档 / 隔离 ----------
def artifacts_dir() -> Path:
    return runtime_dir() / "artifacts"


def runs_root() -> Path:
    return artifacts_dir() / "runs"


def run_dir(attempt_id: str) -> Path:
    return runs_root() / attempt_id


def attempt_work_dir(attempt_id: str) -> Path:
    """attempt 级任务临时区（随 attempt 回收；任务自由布局）。"""
    return run_dir(attempt_id) / "work"


def archive_root() -> Path:
    return artifacts_dir() / "archive"


def isolation_root() -> Path:
    return artifacts_dir() / "isolation"


# ---------- runtime/tasks：任务级运行态（契约固定 + 任务自由布局） ----------
def tasks_state_root() -> Path:
    return runtime_dir() / "tasks"


def task_dir(task_id: str) -> Path:
    """任务定义目录（tasks/<task_id>/，只读代码与静态定义）。"""
    return tasks_dir() / task_id


def task_state_dir(task_id: str) -> Path:
    """任务级运行态目录（runtime/tasks/<task_id>/）。"""
    return tasks_state_root() / task_id


def producer_state_dir(task_id: str) -> Path:
    """契约固定：生产者游标/未确认账目 + 单实例锁。"""
    return task_state_dir(task_id) / "producer_state"


def outbox_dir(task_id: str) -> Path:
    """契约固定：生产者发布的就绪 JSON。"""
    return task_state_dir(task_id) / "outbox"


def receipts_dir(task_id: str) -> Path:
    """契约固定：接收方回执（生产者轮询消费）。"""
    return task_state_dir(task_id) / "receipts"


# ---------- 全局配置 config.json ----------
class ControlConfig(BaseModel):
    loop_tick_sec: float = 1.0              # 主循环 + 事件消费
    sample_interval_sec: float = 2.0        # 执行资源采样
    feedback_interval_init_sec: float = 5.0     # 业务进展节奏 EMA 初值（只服务慢检测/暂停宽限/保护窗，不参与存活）
    producer_restart_base_sec: float = 1.0  # 生产者崩溃退避起点
    producer_restart_cap_sec: float = 30.0  # 生产者崩溃退避上限
    producer_crash_reset_after_sec: float = 300.0  # 崩溃退避连续无崩溃后的复位
    # 启动节奏（方案 A′：反馈轴限速，取代时间轴令牌制）
    admit_budget_init: int = 1              # 冷启动并发预算：全局“在途未验证新启动数”上限（AIMD 自稳，无硬上限）
    pending_verify_flat_samples: int = 2    # 次级验证：全局执行内存连续不增长的采样数
    task_backoff_restore_steps: int = 2     # VERIFY/EMPTY → NORMAL 需连续 N 步且期间无新 75
    exec_mem_cap_ratio: float = 0.80        # 资源水位 SHED 线：执行内存占机器 ≥ 此值即停新增+撤内存（单采样）
    grow_observe_sec: float = 30.0          # 扩容观察期初值
    pause_grace_base_sec: float = 10.0      # 暂停宽限节奏初值
    pause_grace_max_sec: float = 60.0
    shortage_retry_base_sec: float = 5.0    # 不足重试指数退避起点
    shortage_retry_cap_sec: float = 900.0
    protect_deadline_sec: float = 300.0     # 整轮保护截止时间初值
    protect_round_backoff_sec: float = 60.0 # 失败保护轮间等待（起点）
    protect_round_backoff_cap_sec: float = 600.0  # 失败保护轮间等待上限（逐渐增加）
    yield_observe_sec: float = 10.0         # 保护轮每次退让后需观察的间隔（确认退出+观察 A）
    slow_confidence_sec: float = 120.0      # 变慢判定连续证据窗口
    recovery_probe_sec: float = 60.0        # 恢复探测最小机会间隔
    scan_batch_size: int = 200              # 每任务每轮接收时间片（文件数上限）
    heartbeat_interval_sec: float = 5.0     # SDK 心跳发送间隔（注入 worker；调度器据此判活）
    heartbeat_stale_multiplier: float = 6.0 # 心跳超时 = 心跳间隔 × 该倍数（丢失该数量心跳判死）
    resource_unhealthy_ratio: float = 0.9   # 持续超过判定不健康
    resource_healthy_ratio: float = 0.7     # 持续低于判定恢复健康（滞回）
    resource_stable_samples: int = 3        # 滞回连续样本数
    mem_growth_ratio: float = 0.05          # 单采样区间执行内存净增占容量比例，判"快速增长"
    observability_slow_factor: float = 3.0  # 观测不足时扩容观察期放大倍数
    task_backoff_probe_base_sec: float = 30.0   # 任务全退避后的首次探测间隔
    task_backoff_probe_cap_sec: float = 600.0   # 探测间隔上限（按级别翻倍递增）
    cmd_ack_wait_sec: float = 6.0           # CLI 控制命令等待确认超时
    stale_pause_cooldown_sec: float = 60.0  # 心跳超时/OOM/内存增长退让等暂停的作业恢复排队前冷却，防紧密重启循环
    cleanup_terminal_after_sec: float = 604800.0  # 已完结作业（成功/取消）attempt 工件保留时长

    model_config = {"extra": "forbid"}


class SchedulerConfig(BaseModel):
    schema_version: int = CONFIG_SCHEMA_VERSION
    timezone: str = "Asia/Shanghai"
    control: ControlConfig = Field(default_factory=ControlConfig)

    model_config = {"extra": "forbid"}

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
    cwd: str = "."    # 入口工作目录（相对任务目录；workdir 在此基础上再解析）

    model_config = {"extra": "forbid"}

    @field_validator("interpreter")
    @classmethod
    def _interp_ok(cls, v: str) -> str:
        # interpreter 只能是命令名（经 PATH 解析），不得带路径：契约要求
        # 解释器版本由环境统一管理，路径写法会绑定部署位置绕过该约定
        if not v or "/" in v or v.startswith("."):
            raise ValueError(f"interpreter 必须是无路径的命令名：{v!r}")
        return v


class RecoverySpec(BaseModel):
    """生产者恢复方式说明：数据源如何重放、保留多久、如何识别遗漏。"""
    replay: str = "outbox"       # outbox | source | none
    retain: str = ""             # 数据源保留描述
    replay_note: str = ""        # 如何识别遗漏

    model_config = {"extra": "forbid"}


class TaskSpec(BaseModel):
    schema_version: int = TASK_SCHEMA_VERSION
    task_id: str
    enabled: bool = True
    priority: int = 0
    producer_entry: EntrySpec      # 生产者入口（常驻）
    executor_entry: EntrySpec      # 执行入口
    workdir: str = "."
    recovery: RecoverySpec = Field(default_factory=RecoverySpec)
    # 作业级优先级授权上限（审查修复，设计第四节 L61"任务授权范围内设置"）：
    # 该任务作业级 priority 的最高允许值。缺省 = 任务优先级（全局硬规则：
    # 作业不得越过任务优先级，避免执行程序自行改变全局优先级）；任务可
    # 声明更窄的授权（更低的 cap），不可声明比任务优先级更高的 cap。
    job_priority_cap: int | None = Field(None)
    # 任务静态参数（统一到 task.json）：topic/query/日期范围等由任务自解释，
    # 调度器不解释其内容，仅保证"静态定义集中在一处、tasks/ 无独立配置文件"。
    params: dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "forbid"}

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
    # 作业级优先级授权只能收窄（不得越过任务优先级，设计第四节 L61）
    if doc.job_priority_cap is not None and doc.job_priority_cap > doc.priority:
        raise TaskError(
            f"job_priority_cap ({doc.job_priority_cap}) 不得高于任务优先级 "
            f"({doc.priority})：授权只能收窄，不能放宽")
    # B4（设计第三节 L51）：恢复方式必须可解释——数据源保留多久（retain）与
    # 如何识别遗漏（replay_note）缺失时任务清单不合规，拒绝加载。
    if not (str(doc.recovery.retain).strip()
            and str(doc.recovery.replay_note).strip()):
        raise TaskError(
            "recovery.retain 与 recovery.replay_note 必须是非空说明"
            "（数据源保留多久/如何识别遗漏，设计第三节）")
    code_base = task_dir(task_id)           # 代码：tasks/ 下（只读）
    state_base = task_state_dir(task_id)    # 运行态：runtime/tasks/ 下（可写）
    for field, entry in (("producer_entry", doc.producer_entry),
                         ("executor_entry", doc.executor_entry)):
        p = _rel_within(code_base, entry.file, f"{field}.file")
        if not p.exists():
            raise TaskError(f"{field}.file 不存在：{entry.file}")
        # 入口工作目录落在任务**运行态**目录内（B11：cwd 声明必须被校验且生效；
        # 代码在 tasks/，可写产物一律在 runtime/）
        _rel_within(state_base, entry.cwd or ".", f"{field}.cwd")
    _rel_within(state_base, doc.workdir or ".", "workdir")
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
    # 协议版本与幂等键必填（审查修复，设计第四节 L63"每份就绪 JSON 包含协议
    # 版本……幂等键"）：此前两者均可缺省（默认 1 / 空串），缺失版本号的
    # JSON 被静默当 v1 解析、空幂等键让"幂等提交"协议基础全链路落空。
    schema_version: int
    job_id: str
    task_id: str
    entry_args: list[str] = Field(default_factory=list)
    workdir: str = "."
    input_refs: list[str] = Field(default_factory=list)
    idempotency_key: str = Field(min_length=1)
    env: dict[str, str] = Field(default_factory=dict)
    priority: int | None = None  # 作业级优先级：仅允许不高于任务授权上限（接收时钳制）

    model_config = {"extra": "forbid"}

    @field_validator("job_id")
    @classmethod
    def _job_id_ok(cls, v: str) -> str:
        if not JOB_ID_RE.match(v or ""):
            raise ValueError(f"job_id 非法：{v!r}")
        return v

    @field_validator("task_id")
    @classmethod
    def _task_id_ok(cls, v: str) -> str:
        if not TASK_ID_RE.match(v or ""):
            raise ValueError(f"task_id 非法：{v!r}")
        return v

    @field_validator("priority")
    @classmethod
    def _job_pri_ok(cls, v: int | None) -> int | None:
        if v is None or isinstance(v, bool):
            if isinstance(v, bool):
                raise ValueError("priority 必须是整数")
            return v
        return v


class ReadyError(Exception):
    def __init__(self, errors: list[str] | str):
        self.errors = [errors] if isinstance(errors, str) else errors
        super().__init__("; ".join(self.errors))


def parse_ready(raw: Any) -> ReadyJob:
    if not isinstance(raw, dict):
        raise ReadyError("就绪 JSON 顶层必须是对象")
    try:
        doc = ReadyJob.model_validate(raw)
    except Exception as e:
        raise ReadyError(str(e)) from e
    # workdir 必须落在其声明任务的**运行态**目录内（与 task.json 的 workdir
    # 同一包含校验）：执行进程以此目录为 cwd，越出即越权访问任务边界之外
    try:
        _rel_within(task_state_dir(doc.task_id), doc.workdir or ".", "workdir")
    except TaskError as e:
        raise ReadyError(str(e)) from e
    return doc


# ---------- 全局配置读写 ----------
def ensure_layout() -> None:
    for d in (tasks_dir(), runtime_dir(), state_dir(), ipc_dir(), logs_dir(),
              artifacts_dir(), runs_root(), archive_root(), isolation_root(),
              tasks_state_root()):
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
    except Exception as e:
        # 不静默回退：明确告知运维全局配置无效，正在使用默认控制参数
        print(f"[sched] 全局配置 config.json 无效（{e}），使用默认控制参数",
              file=sys.stderr, flush=True)
        return SchedulerConfig()