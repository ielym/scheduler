"""【协议 A】任务目录与 task.json（schema v2）的解析与校验。

v2 在 v1 基础上新增调度体系所需的声明：resources（资源预估）、error_rules（错误分类补充）、
heartbeat（心跳超时）、smoke（冒烟参数）、runtime（非 root 执行）、contract_exempt。
保持纯手动模型：调度器绝不自动发现、绝不生成骨架。
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

from . import store

SCHEMA_VERSION = 2
SUPPORTED_VERSIONS = {1, 2}
TASK_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
INTERPRETER_RE = re.compile(r"^[A-Za-z0-9_.+-]+$")
TIME_OF_DAY_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d(:[0-5]\d)?$")


class TaskDocError(Exception):
    def __init__(self, errors: list[str] | str):
        self.errors = [errors] if isinstance(errors, str) else errors
        super().__init__("; ".join(self.errors))


class EntrySpec(BaseModel):
    file: str
    interpreter: str | None = "python3"
    args: list[str] = Field(default_factory=list)
    cwd: str = "."

    @field_validator("file")
    @classmethod
    def _file_ok(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("entry.file 不能为空")
        return v

    @field_validator("interpreter")
    @classmethod
    def _interp_ok(cls, v: str | None) -> str | None:
        if v is None:
            return None
        if not INTERPRETER_RE.match(v):
            raise ValueError(f"entry.interpreter 非法：{v!r}（只能是命令名，不能带路径）")
        return v

    @field_validator("args")
    @classmethod
    def _args_ok(cls, v: list[str]) -> list[str]:
        for a in v:
            if not isinstance(a, str):
                raise ValueError("entry.args 只能是字符串数组")
        return v


class ScheduleSpec(BaseModel):
    type: Literal["interval", "once", "manual"] = "manual"
    interval: int | None = None
    once_at: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    weekdays: list[int] = Field(default_factory=list)
    allow_overrun: bool = False
    max_instances: int = 1
    overflow: Literal["skip", "queue"] = "skip"

    @model_validator(mode="after")
    def _check(self) -> "ScheduleSpec":
        if self.type == "interval":
            if not self.interval or self.interval <= 0:
                raise ValueError("schedule.type=interval 时 schedule.interval 必须为正整数秒")
            if (self.start_time is None) != (self.end_time is None):
                raise ValueError("schedule.start_time 与 schedule.end_time 必须同时提供；都不填 = 全天")
            if self.start_time is not None:
                if not TIME_OF_DAY_RE.match(str(self.start_time)) or not TIME_OF_DAY_RE.match(str(self.end_time)):
                    raise ValueError("schedule.start_time/end_time 必须是 HH:MM 或 HH:MM:SS")
            for d in self.weekdays:
                if not isinstance(d, int) or isinstance(d, bool) or d < 1 or d > 7:
                    raise ValueError("schedule.weekdays 只能是 1..7 的整数（1=周一，7=周日）")
            if self.weekdays:
                self.weekdays = sorted(set(self.weekdays))
        elif self.type == "once" and not self.once_at:
            raise ValueError("schedule.type=once 时必须提供 schedule.once_at（ISO8601）")
        if not isinstance(self.max_instances, int) or isinstance(self.max_instances, bool):
            raise ValueError("schedule.max_instances 必须是整数（1~20）")
        if not 1 <= self.max_instances <= 20:
            raise ValueError(f"schedule.max_instances 必须在 1~20 之间，当前 {self.max_instances!r}")
        return self


class OutputsSpec(BaseModel):
    expect: list[str] = Field(default_factory=list)


class ResourceDecl(BaseModel):
    """任务声明的资源预估（gate 4 容量判定 + 账本预留）。0 = 未知，交给冒烟/画像补全。"""

    cpu: float = 0.0
    memory_mb: int = 0

    @model_validator(mode="after")
    def _check(self) -> "ResourceDecl":
        if self.cpu < 0 or (isinstance(self.memory_mb, bool) or self.memory_mb < 0):
            raise ValueError("resources.cpu / resources.memory_mb 不能为负")
        return self


class ErrorRules(BaseModel):
    resource_regex: list[str] = Field(default_factory=list)
    logic_regex: list[str] = Field(default_factory=list)


class HeartbeatSpec(BaseModel):
    timeout_sec: int = 120

    @field_validator("timeout_sec")
    @classmethod
    def _hb_ok(cls, v: int) -> int:
        if isinstance(v, bool) or v < 5:
            raise ValueError("heartbeat.timeout_sec 必须 >= 5")
        return v


class SmokeSpec(BaseModel):
    mode: Literal["dry", "real"] = "dry"
    max_runtime_sec: int = 60
    signal_after_sec: float = 1.5  # 断点闸首次运行后多久发 SIGTERM（重导入任务可调大）


# 重试时效截止允许的两种写法：
#   1) ISO8601 绝对时间：2026-10-04T15:00:00+08:00
#   2) 每日时刻：HH:MM 或 HH:MM:SS（按调度时区解释，"每天此刻之后不再自动重试"）
NOT_AFTER_DAILY_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d(:[0-5]\d)?$")


class RetrySpec(BaseModel):
    """任务自治的可恢复失败重试策略（资源错误 100/101、未分类、超时、中断后自动重排队）。

    调度器不设全局冷却/退避/重试上限：资源不足时任务在 WQ 排队，等其它任务释放
    固定资源后自然获得准出；是否重试、重试节奏、何时因时效性放弃，一律由任务声明。
    """

    # 自动重入队后最早可再次准出的间隔（秒）。0 = 立即重排队（默认：固定资源不足靠
    # 队列排队即可，任务内部的就地重试/换连接由任务自己在进程内完成）。
    delay_sec: float = 0.0
    # 最大尝试次数（含首次）：0 = 不放弃（默认，有资源必须跑完）；
    # 正整数 N = 第 N 次仍失败则转人工。新计划触发（interval/once/手动）的 run 计数重置。
    max_attempts: int = 0
    # 时效性截止：超过该时刻后不再自动重试（转人工），防止收盘后/过期后无意义空跑。
    # 空串 = 不设时效；支持 ISO8601 绝对时间或每日 HH:MM（每日时刻按调度时区解释）。
    not_after: str = ""

    @field_validator("delay_sec")
    @classmethod
    def _delay_ok(cls, v: float) -> float:
        if v < 0:
            raise ValueError("retry.delay_sec 不能为负")
        return v

    @field_validator("max_attempts")
    @classmethod
    def _max_ok(cls, v: int) -> int:
        if isinstance(v, bool) or v < 0:
            raise ValueError("retry.max_attempts 必须是 >= 0 的整数（0=不放弃）")
        return v

    @field_validator("not_after")
    @classmethod
    def _not_after_ok(cls, v: str) -> str:
        v = (v or "").strip()
        if v and not NOT_AFTER_DAILY_RE.match(v):
            try:
                datetime.fromisoformat(v)
            except ValueError as e:
                raise ValueError(
                    "retry.not_after 必须是 ISO8601 时间（2026-10-04T15:00:00+08:00）"
                    "或每日时刻 HH:MM（如 15:00）") from e
        return v


class RuntimeSpec(BaseModel):
    user: str = "sched-run"  # 非 root 执行用户；空 = 以调度器运行用户执行


class TaskDoc(BaseModel):
    schema_version: int = SCHEMA_VERSION
    name: str = ""
    description: str = ""
    entry: EntrySpec
    schedule: ScheduleSpec = Field(default_factory=ScheduleSpec)
    priority: int = 0
    env: dict[str, str] = Field(default_factory=dict)
    outputs: OutputsSpec = Field(default_factory=OutputsSpec)
    resources: ResourceDecl = Field(default_factory=ResourceDecl)
    error_rules: ErrorRules = Field(default_factory=ErrorRules)
    heartbeat: HeartbeatSpec = Field(default_factory=HeartbeatSpec)
    smoke: SmokeSpec = Field(default_factory=SmokeSpec)
    runtime: RuntimeSpec = Field(default_factory=RuntimeSpec)
    retry: RetrySpec = Field(default_factory=RetrySpec)
    contract_exempt: bool = False  # True：只能手动触发，禁止自动调度（不可断点恢复的任务）

    @field_validator("schema_version")
    @classmethod
    def _version_ok(cls, v: int) -> int:
        if v not in SUPPORTED_VERSIONS:
            raise ValueError(f"schema_version 不支持：{v}（支持 {sorted(SUPPORTED_VERSIONS)}）")
        return v

    @field_validator("priority")
    @classmethod
    def _priority_ok(cls, v: int) -> int:
        if isinstance(v, bool) or not (0 <= v <= 100):
            raise ValueError("priority 必须在 0~100 之间")
        return v

    @field_validator("env")
    @classmethod
    def _env_ok(cls, v: dict[str, str]) -> dict[str, str]:
        from .runner import ENV_BLACKLIST
        for k, val in v.items():
            if not isinstance(k, str) or not isinstance(val, str):
                raise ValueError("env 必须是字符串到字符串的映射")
            if k in ENV_BLACKLIST:
                raise ValueError(f"env 键在安全黑名单中（防注入提权）：{k}")
        return v

    @model_validator(mode="after")
    def _check_exempt(self) -> "TaskDoc":
        if self.contract_exempt and self.schedule.type != "manual":
            raise ValueError("contract_exempt=true 的任务只能 schedule.type=manual（禁止自动触发）")
        return self

    def display_name(self) -> str:
        return self.name or self.entry.file


def _rel_within(task_dir: Path, rel: str, field: str) -> Path:
    if not rel:
        raise TaskDocError(f"{field} 不能为空")
    p = Path(rel)
    if p.is_absolute():
        raise TaskDocError(f"{field} 必须是相对路径：{rel}")
    if ".." in p.parts:
        raise TaskDocError(f"{field} 不能包含 '..'：{rel}")
    base = task_dir.resolve()
    target = (base / p).resolve()
    if target != base and base not in target.parents:
        raise TaskDocError(f"{field} 越出了任务目录：{rel}")
    return target


def validate_task_id(task_id: str) -> str:
    if not TASK_ID_RE.match(task_id or ""):
        raise TaskDocError(f"任务 id 非法：{task_id!r}（字母数字下划线短横，1-64 位）")
    if task_id.startswith("_") or task_id.startswith("."):
        raise TaskDocError(f"任务 id 不能以 '_' 或 '.' 开头：{task_id!r}")
    return task_id


def parse_task_json(raw: Any, task_dir: Path) -> TaskDoc:
    if not isinstance(raw, dict):
        raise TaskDocError("task.json 顶层必须是对象")
    try:
        doc = TaskDoc.model_validate(raw)
    except ValidationError as e:
        msgs = []
        for err in e.errors():
            loc = ".".join(str(x) for x in err["loc"]) or "(root)"
            msgs.append(f"{loc}: {err['msg']}")
        raise TaskDocError(msgs) from e

    entry_path = _rel_within(task_dir, doc.entry.file, "entry.file")
    if not entry_path.exists():
        raise TaskDocError(f"entry.file 不存在：{doc.entry.file}")
    if doc.entry.interpreter is None and not os.access(entry_path, os.X_OK):
        raise TaskDocError(f"entry.interpreter 为 null 时 entry.file 必须具备可执行权限")
    _rel_within(task_dir, doc.entry.cwd or ".", "entry.cwd")
    for i, item in enumerate(doc.outputs.expect):
        _rel_within(task_dir, item, f"outputs.expect[{i}]")
    return doc


def load_task_doc(task_dir: Path) -> TaskDoc:
    validate_task_id(task_dir.name)
    if not task_dir.is_dir():
        raise TaskDocError(f"任务目录不存在：{task_dir}")
    tj = task_dir / "task.json"
    if not tj.exists():
        raise TaskDocError(f"缺少 task.json：{tj}")
    try:
        raw = json.loads(store.read_text(tj))
    except json.JSONDecodeError as e:
        raise TaskDocError(f"task.json 不是合法 JSON：{e}") from e
    return parse_task_json(raw, task_dir)


def effective(task_dir: Path, doc: TaskDoc) -> dict[str, Any]:
    return {
        "schedule": doc.schedule.model_dump(),
        "priority": doc.priority,
        "resources": doc.resources.model_dump(),
    }


# ================= 任务自治重试策略（task.json retry 段 + 运行时 state.json 覆盖） =================

def retry_deadline(not_after: str, now: datetime) -> datetime | None:
    """把 retry.not_after 解析为具体截止时刻；空串/非法（schema 已校验）返回 None。

    每日 HH:MM 取"今天的该时刻"；若今天该时刻已过，即代表截止已到（调用方用 now > dl 判定）。
    """
    not_after = (not_after or "").strip()
    if not not_after:
        return None
    if NOT_AFTER_DAILY_RE.match(not_after):
        parts = [int(x) for x in not_after.split(":")]
        h, m = parts[0], parts[1]
        s = parts[2] if len(parts) > 2 else 0
        tz = now.tzinfo
        return now.replace(hour=h, minute=m, second=s, microsecond=0) if tz is not None \
            else datetime(now.year, now.month, now.day, h, m, s)
    try:
        dl = datetime.fromisoformat(not_after)
    except ValueError:
        return None
    if dl.tzinfo is None and now.tzinfo is not None:
        dl = dl.replace(tzinfo=now.tzinfo)
    return dl


def resolve_retry(doc: TaskDoc | None, state_snapshot: dict | None, now: datetime) -> dict[str, Any]:
    """合并任务静态声明（task.json retry）与运行时覆盖（state.json.retry_hint）。

    运行时 hint 由任务 SDK 在本次运行中按当天实际情况写入（如半日市的收盘时刻），
    仅允许收紧/指定 retry 三要素；字段缺省回落 task.json 声明。
    返回 {delay_sec, max_attempts, not_after, deadline}。
    """
    r = doc.retry if doc is not None else RetrySpec()
    delay, max_attempts, not_after = r.delay_sec, r.max_attempts, r.not_after
    hint = (state_snapshot or {}).get("retry_hint")
    if isinstance(hint, dict):
        try:
            h = RetrySpec.model_validate({
                "delay_sec": hint.get("delay_sec", delay),
                "max_attempts": hint.get("max_attempts", max_attempts),
                "not_after": hint.get("not_after", not_after),
            })
        except ValidationError:
            pass  # 任务写坏 hint 不影响调度：回落静态声明
        else:
            delay, max_attempts, not_after = h.delay_sec, h.max_attempts, h.not_after
    return {"delay_sec": float(delay), "max_attempts": int(max_attempts),
            "not_after": not_after, "deadline": retry_deadline(not_after, now)}