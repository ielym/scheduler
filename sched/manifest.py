"""manifest.json（schema_version=3）解析与校验。

唯一子任务清单——不含调度时机/并发开关/资源豁免字段。
时机 = 驱动进程写意图文件的时机；并发协调由子任务通过 SDK emit_intent 自行管理。
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator


SCHEMA_VERSION = 3
TASK_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
INTERPRETER_RE = re.compile(r"^[A-Za-z0-9_.+-]+$")


class ManifestError(Exception):
    def __init__(self, errors: list[str] | str):
        self.errors = [errors] if isinstance(errors, str) else errors
        super().__init__("; ".join(self.errors))


class EntrySpec(BaseModel):
    file: str
    interpreter: str | None = "python3"
    args: list[str] = Field(default_factory=list)
    cwd: str = "."

    model_config = {"extra": "ignore"}

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


class ResourceDecl(BaseModel):
    cpu: float = 0.0
    memory_mb: int = 0

    model_config = {"extra": "ignore"}

    @model_validator(mode="after")
    def _check(self) -> "ResourceDecl":
        if self.cpu < 0 or (isinstance(self.memory_mb, bool) or self.memory_mb < 0):
            raise ValueError("resources.cpu / resources.memory_mb 不能为负")
        return self


class ErrorRules(BaseModel):
    resource_regex: list[str] = Field(default_factory=list)
    logic_regex: list[str] = Field(default_factory=list)

    model_config = {"extra": "ignore"}


class HeartbeatSpec(BaseModel):
    timeout_sec: int = 120

    model_config = {"extra": "ignore"}

    @field_validator("timeout_sec")
    @classmethod
    def _hb_ok(cls, v: int) -> int:
        if isinstance(v, bool) or v < 5:
            raise ValueError("heartbeat.timeout_sec 必须 >= 5")
        return v


class RuntimeSpec(BaseModel):
    user: str = "sched-run"

    model_config = {"extra": "ignore"}


class TaskManifest(BaseModel):
    schema_version: int = SCHEMA_VERSION
    name: str = ""
    description: str = ""
    run_mode: Literal["auto", "manual"] = "auto"
    entry: EntrySpec
    priority: int = 0
    resources: ResourceDecl = Field(default_factory=ResourceDecl)
    error_rules: ErrorRules = Field(default_factory=ErrorRules)
    heartbeat: HeartbeatSpec = Field(default_factory=HeartbeatSpec)
    runtime: RuntimeSpec = Field(default_factory=RuntimeSpec)

    model_config = {"extra": "ignore"}

    @field_validator("schema_version")
    @classmethod
    def _version_ok(cls, v: int) -> int:
        if v != SCHEMA_VERSION:
            raise ValueError(f"schema_version 不支持：{v}（仅支持 {SCHEMA_VERSION}）")
        return v

    @field_validator("priority")
    @classmethod
    def _priority_ok(cls, v: int) -> int:
        if isinstance(v, bool) or not (0 <= v <= 100):
            raise ValueError("priority 必须在 0~100 之间")
        return v

    def display_name(self) -> str:
        return self.name or self.entry.file


def _rel_within(task_dir: Path, rel: str, field: str) -> Path:
    if not rel:
        raise ManifestError(f"{field} 不能为空")
    p = Path(rel)
    if p.is_absolute():
        raise ManifestError(f"{field} 必须是相对路径：{rel}")
    if ".." in p.parts:
        raise ManifestError(f"{field} 不能包含 '..'：{rel}")
    base = task_dir.resolve()
    target = (base / p).resolve()
    if target != base and base not in target.parents:
        raise ManifestError(f"{field} 越出了任务目录：{rel}")
    return target


def validate_task_id(task_id: str) -> str:
    if not TASK_ID_RE.match(task_id or ""):
        raise ManifestError(f"任务 id 非法：{task_id!r}（字母数字下划线短横，1-64 位）")
    if task_id.startswith("_") or task_id.startswith("."):
        raise ManifestError(f"任务 id 不能以 '_' 或 '.' 开头：{task_id!r}")
    return task_id


def parse_manifest(raw: Any, task_dir: Path) -> TaskManifest:
    if not isinstance(raw, dict):
        raise ManifestError("manifest.json 顶层必须是对象")
    try:
        doc = TaskManifest.model_validate(raw)
    except ValidationError as e:
        msgs = []
        for err in e.errors():
            loc = ".".join(str(x) for x in err["loc"]) or "(root)"
            msgs.append(f"{loc}: {err['msg']}")
        raise ManifestError(msgs) from e

    entry_path = _rel_within(task_dir, doc.entry.file, "entry.file")
    if not entry_path.exists():
        raise ManifestError(f"entry.file 不存在：{doc.entry.file}")
    if doc.entry.interpreter is None and not os.access(entry_path, os.X_OK):
        raise ManifestError(f"entry.interpreter 为 null 时 entry.file 必须具备可执行权限")
    _rel_within(task_dir, doc.entry.cwd or ".", "entry.cwd")
    return doc


def load_manifest(task_dir: Path) -> TaskManifest:
    validate_task_id(task_dir.name)
    if not task_dir.is_dir():
        raise ManifestError(f"任务目录不存在：{task_dir}")
    mj = task_dir / "manifest.json"
    if not mj.exists():
        raise ManifestError(f"缺少 manifest.json：{mj}")
    try:
        raw = json.loads(mj.read_text("utf-8"))
    except json.JSONDecodeError as e:
        raise ManifestError(f"manifest.json 不是合法 JSON：{e}") from e
    except OSError as e:
        raise ManifestError(f"读取 manifest.json 失败：{e}") from e
    return parse_manifest(raw, task_dir)