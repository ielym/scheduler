"""scheduler_v002 路径常量与全局设置（独立于现有 scheduler，默认根 = /mnt/data01/projects/scheduler_v002）。

与现有实现保持协议面兼容：SCHED_HOME 环境变量可覆盖根目录，便于测试隔离。
"""

from __future__ import annotations

import os
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


class RunnerConfig(BaseModel):
    kill_grace_sec: int = 10
    heartbeat_timeout_sec: int = 120
    cooldown_sec: int = 60
    grace_sec: int = 40
    # 资源类错误最大重试次数；0 = 不设上限（R17 退让而非扑杀，默认永不放弃）。
    # 仅当运维确认某任务的"资源错误"实际不可恢复时，才显式配置封顶转人工。
    max_resource_attempts: int = 0


# 资源阈值阶梯（v04 R29，并经评估建议改为 memory.high 软限 + memory.max 硬限双水位）
class ResourceConfig(BaseModel):
    # 画像三级窗口
    window_samples: int = 8          # 最近 N 次正式 run
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
    runner: RunnerConfig = Field(default_factory=RunnerConfig)
    resource: ResourceConfig = Field(default_factory=ResourceConfig)
    max_log_mb: int = 32
    # 非 root 执行用户（v04 R31）；为空 = 以调度器运行用户直接执行
    run_user: str = "sched-run"
    # ---- 调度行为参数（原为模块级硬编码，第三轮审查后统一入 Settings）----
    starvation_sec: float = 3600.0        # 饿死看门狗阈值（1h）
    preempt_exempt_sec: float = 60.0      # 任务启动后的抢占豁免期
    data_risk_max_reschedules: int = 3    # 110 数据风险二刷轮数上限
    soft_degrade_threshold: int = 3       # 软资源错误计数达到该值触发降额
    soft_degrade_window_sec: float = 600.0
    soft_degrade_cooldown_sec: float = 300.0
    hot_reload_sec: float = 30.0          # serve 主循环周期性热加载 jobs.json

    @field_validator("timezone")
    @classmethod
    def _tz_ok(cls, v: str) -> str:
        try:
            ZoneInfo(v)
        except (ZoneInfoNotFoundError, ValueError, KeyError) as e:
            raise ValueError(f"无效时区：{v!r}（例如 Asia/Shanghai）") from e
        return v


def default_settings() -> Settings:
    return Settings()


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
        s = default_settings()
        store.save_json(settings_file(), s.model_dump())
    return s


def save_settings(s: Settings) -> Settings:
    from . import store

    store.save_json(settings_file(), s.model_dump())
    return s