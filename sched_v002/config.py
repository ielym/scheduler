"""scheduler_v002 路径常量与全局设置（独立于现有 scheduler，默认根 = /mnt/data01/projects/scheduler_v002）。

与现有实现保持协议面兼容：SCHED_HOME 环境变量可覆盖根目录，便于测试隔离。
"""

from __future__ import annotations

import os
import secrets
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field, field_validator

DEFAULT_HOME = Path("/mnt/data01/projects/scheduler_v002")


def home() -> Path:
    """项目根目录。测试通过 SCHED_HOME 覆盖。"""
    return Path(os.environ.get("SCHED_HOME") or DEFAULT_HOME)


def tasks_dir() -> Path:
    return home() / "tasks"


def data_dir() -> Path:
    return home() / "data"


def runs_dir() -> Path:
    return data_dir() / "runs"


def runtime_dir() -> Path:
    return data_dir() / "runtime"


def jobs_file() -> Path:
    return data_dir() / "jobs.json"


def settings_file() -> Path:
    return data_dir() / "settings.json"


def scheduler_state_file() -> Path:
    return runtime_dir() / "scheduler_state.json"


def decisions_log() -> Path:
    return runtime_dir() / "decisions.jsonl"


def profiles_file() -> Path:
    return runtime_dir() / "profiles.json"


def ledger_file() -> Path:
    return runtime_dir() / "ledger.json"


def sched_user_conf() -> Path:
    return home() / "deploy" / "sched-run.conf"


class HttpConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8787


class AuthConfig(BaseModel):
    enabled: bool = False
    token: str = ""
    allow_loopback_without_token: bool = True


class RunnerConfig(BaseModel):
    max_workers: int = 20
    kill_grace_sec: int = 10
    heartbeat_timeout_sec: int = 120
    wait_max_sec: int = 300
    cooldown_sec: int = 60
    grace_sec: int = 40


# 资源阈值阶梯（v04 R29，并经评估建议改为 memory.high 软限 + memory.max 硬限双水位）
class ResourceConfig(BaseModel):
    # 画像三级窗口
    window_samples: int = 8          # 最近 N 次正式 run
    decay: float = 0.5               # 三级画像迭代权重（越个例权重越低）
    overshoot: float = 1.2           # 画像上浮系数（P95 ×1.2）
    # cgroup 双水位（进程内软限 + 硬兜底，比例基于画像预估值）
    memory_high_ratio: float = 1.5   # memory.high = 预估值 ×1.5（软限节流）
    memory_max_ratio: float = 2.5    # memory.max = 预估值 ×2.5（硬兜底）
    # monitor 超限判定
    overload_ratio: float = 2.0      # 实际/预估 >2.0 视为超限
    overload_sustain_sec: int = 60   # 持续判定窗口
    sample_interval_sec: int = 3     # monitor 采样间隔
    # 准出时对非调度进程占用的感知（R14）：est_mem > MemAvailable - margin → 退让等待
    external_safety_margin_mb: int = 512


class Settings(BaseModel):
    timezone: str = "Asia/Shanghai"
    http: HttpConfig = Field(default_factory=HttpConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    runner: RunnerConfig = Field(default_factory=RunnerConfig)
    resource: ResourceConfig = Field(default_factory=ResourceConfig)
    retention_days: int = 90
    max_log_mb: int = 32
    # 非 root 执行用户（v04 R31）；为空 = 以调度器运行用户直接执行
    run_user: str = "sched-run"

    @field_validator("timezone")
    @classmethod
    def _tz_ok(cls, v: str) -> str:
        try:
            ZoneInfo(v)
        except (ZoneInfoNotFoundError, ValueError, KeyError) as e:
            raise ValueError(f"无效时区：{v!r}（例如 Asia/Shanghai）") from e
        return v


def default_settings() -> Settings:
    s = Settings()
    s.auth.token = secrets.token_urlsafe(32)
    return s


def ensure_layout() -> None:
    from . import store

    for d in (tasks_dir(), data_dir(), runs_dir(), runtime_dir()):
        d.mkdir(parents=True, exist_ok=True)

    if not settings_file().exists():
        store.save_json(settings_file(), default_settings().model_dump())


def load_settings() -> Settings:
    from . import store

    raw = store.load_json(settings_file(), default=None)
    if not isinstance(raw, dict) or not raw:
        s = default_settings()
        store.save_json(settings_file(), s.model_dump())
        return s
    try:
        s = Settings.model_validate(raw)
    except Exception:
        keep = ((raw.get("auth") or {}).get("token") or "") if isinstance(raw.get("auth"), dict) else ""
        s = Settings()
        s.auth.token = keep or secrets.token_urlsafe(32)
        store.save_json(settings_file(), s.model_dump())
    if not s.auth.token:
        s.auth.token = secrets.token_urlsafe(32)
        store.save_json(settings_file(), s.model_dump())
    return s


def save_settings(s: Settings) -> Settings:
    from . import store

    store.save_json(settings_file(), s.model_dump())
    return s