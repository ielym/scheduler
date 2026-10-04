"""【协议 B v1】意图文件（Intent File）的解析与校验。

意图文件是调度器与子任务之间的最小通信单元：
- 子任务在 tasks/<id>/requests/<request_id>.json 写入意图 = 声明“要启动一个进程”
- 调度器扫描、入库、拉起、维护生命周期
- 写后不可变：不提供“改参数”语义；换日期/换参数 = 写新文件
- 不自行删除：调度器在持久态记录消费进度（pending/running/done/archived）

启动协议：调度器对每个意图执行 run.py --config <config>，注入
SCHED_SUBTASK_ID / SCHED_REQUEST_ID / SCHED_RUN_ID / SCHED_CONFIG / SCHED_TRIGGER / SCHED_WORKSPACE。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator


SCHEMA_VERSION = 1
REQUEST_ID_RE = re.compile(r"^[a-zA-Z0-9._-]{1,128}$")


class IntentError(Exception):
    def __init__(self, errors: list[str] | str):
        self.errors = [errors] if isinstance(errors, str) else errors
        super().__init__("; ".join(self.errors))


class EntryOverride(BaseModel):
    file: str | None = None
    interpreter: str | None = None
    args: list[str] = Field(default_factory=list)
    cwd: str = "."

    model_config = {"extra": "ignore"}


class LifetimeSpec(BaseModel):
    completion: Literal["terminal", "indefinite"] = "terminal"
    # 意图完成后的自动重试策略（资源错误/抢占导致的退出）
    retry: dict[str, Any] = Field(default_factory=lambda: {
        "backoff_base_sec": 5,    # 资源不足回退 pending 的固定退避（防热循环）
        "backoff_cap_sec": 300,
        "not_after": "",          # ISO8601 绝对时间或每日 HH:MM；空=无截止
    })

    model_config = {"extra": "ignore"}


class IntentFile(BaseModel):
    schema_version: int = SCHEMA_VERSION
    request_id: str = ""
    kind: Literal["daemon", "oneshot"] = "oneshot"
    config: str = ""
    entry: EntryOverride = Field(default_factory=EntryOverride)
    priority: int | None = None
    resources: dict[str, Any] = Field(default_factory=lambda: {"cpu": 0.0, "memory_mb": 0})
    lifetime: LifetimeSpec = Field(default_factory=LifetimeSpec)

    model_config = {"extra": "ignore"}

    @field_validator("schema_version")
    @classmethod
    def _ver_ok(cls, v: int) -> int:
        if v != SCHEMA_VERSION:
            raise ValueError(f"schema_version 不支持：{v}（仅支持 {SCHEMA_VERSION}）")
        return v

    @field_validator("request_id")
    @classmethod
    def _rid_ok(cls, v: str) -> str:
        if not REQUEST_ID_RE.match(v or ""):
            raise ValueError(f"request_id 非法：{v!r}")
        return v

    @field_validator("priority")
    @classmethod
    def _pri_ok(cls, v: int | None) -> int | None:
        if v is None:
            return v
        if isinstance(v, bool) or not (0 <= v <= 100):
            raise ValueError("priority 必须在 0~100 之间（或不填）")
        return v

    @model_validator(mode="after")
    def _kind_lifetime(self) -> "IntentFile":
        # kind=daemon 默认 indefinite
        if self.kind == "daemon" and self.lifetime.completion != "indefinite":
            self.lifetime.completion = "indefinite"
        return self

    def is_daemon(self) -> bool:
        return self.kind == "daemon"


def _rel_within(task_dir: Path, rel: str, field: str) -> Path:
    if not rel:
        raise IntentError(f"{field} 不能为空")
    p = Path(rel)
    if p.is_absolute():
        raise IntentError(f"{field} 必须是相对路径：{rel}")
    if ".." in p.parts:
        raise IntentError(f"{field} 不能包含 '..'：{rel}")
    base = task_dir.resolve()
    target = (base / p).resolve()
    if target != base and base not in target.parents:
        raise IntentError(f"{field} 越出了任务目录：{rel}")
    return target


def _canonical_resources(raw: Any) -> dict:
    if not isinstance(raw, dict):
        return {"cpu": 0.0, "memory_mb": 0}
    cpu = raw.get("cpu", 0.0) if isinstance(raw.get("cpu"), (int, float)) else 0.0
    mem = raw.get("memory_mb", 0) if isinstance(raw.get("memory_mb"), int) and not isinstance(raw.get("memory_mb"), bool) else 0
    return {"cpu": float(cpu), "memory_mb": int(mem)}


def parse_intent(raw: Any, task_dir: Path) -> IntentFile:
    if not isinstance(raw, dict):
        raise IntentError("意图文件顶层必须是对象")
    try:
        doc = IntentFile.model_validate(raw)
    except ValidationError as e:
        msgs = []
        for err in e.errors():
            loc = ".".join(str(x) for x in err["loc"]) or "(root)"
            msgs.append(f"{loc}: {err['msg']}")
        raise IntentError(msgs) from e

    # 校验 config 路径（若提供，必须在任务目录内）
    if doc.config:
        _rel_within(task_dir, doc.config, "config")
    # entry.file 若提供，也必须在任务目录内
    if doc.entry.file:
        _rel_within(task_dir, doc.entry.file, "entry.file")
        _rel_within(task_dir, doc.entry.cwd or ".", "entry.cwd")

    # 规范化 resources（pydantic 默认的 schema 字段类型放这里兜底）
    doc.resources = _canonical_resources(raw.get("resources"))
    return doc


def load_intent(path: Path, task_dir: Path) -> IntentFile:
    try:
        raw = json.loads(path.read_text("utf-8"))
    except json.JSONDecodeError as e:
        raise IntentError(f"意图文件不是合法 JSON：{e}") from e
    # request_id 若未在 JSON 里填，从文件名导出
    if isinstance(raw, dict) and not raw.get("request_id"):
        raw["request_id"] = path.stem
    doc = parse_intent(raw, task_dir)
    # request_id 必须与文件名一致（append-only 不可变契约）
    if doc.request_id != path.stem:
        raise IntentError(f"request_id '{doc.request_id}' 与文件名 '{path.stem}' 不一致（append-only 契约）")
    return doc


def fingerprint(path: Path) -> str:
    """意图文件内容的哈希，用于检测 append-only 违规（调度器侧对比是否被改写）。"""
    h = hashlib.sha256()
    try:
        h.update(path.read_bytes())
    except OSError:
        return ""
    return h.hexdigest()[:16]