"""sched 路径常量与全局设置。

项目根（tasks/、data/ 的父目录）默认取本包所在目录的上一级——代码可整体 clone/搬迁到
任意路径，无需改配置。SCHED_HOME 环境变量可覆盖根目录（多实例/测试隔离用）。
"""

from __future__ import annotations

import os
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field, field_validator

# sched/config.py -> sched/ -> 项目根
DEFAULT_HOME = Path(__file__).resolve().parent.parent


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


def admission_state_file() -> Path:
    return runtime_dir() / "admission.json"


def decisions_log() -> Path:
    return runtime_dir() / "decisions.jsonl"


def profiles_file() -> Path:
    return runtime_dir() / "profiles.json"


def ledger_file() -> Path:
    return runtime_dir() / "ledger.json"


def sched_user_conf() -> Path:
    return home() / "deploy" / "sched-run.conf"


class RunnerConfig(BaseModel):
    # 这些都是引擎内部参数，不在管理台暴露；重试节奏/放弃策略由意图文件 lifetime.retry 段声明
    # 调度器不设全局冷却/退避/重试上限。
    kill_grace_sec: int = 10
    heartbeat_timeout_sec: int = 120
    grace_sec: int = 40


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
    # 运行中动态画像：每隔这么多秒把 cgroup 实测（CPU 速率/内存）落入画像滑窗并落盘
    profile_live_interval_sec: int = 60
    # 准出时对非调度进程占用的感知（R14）：est_mem > MemAvailable - margin → 退让等待
    external_safety_margin_mb: int = 512
    # 最大并发运行进程数：CPU/内存再空闲也不超过此上限，防文件描述符/进程数耗尽
    max_running: int = 50


class WebConfig(BaseModel):
    # 内嵌管理台：serve 进程内起 uvicorn，所有写操作与同一个
    # SchedulerService 实例（另开进程写 scheduler_state.json 会与 serve 内存态互相覆盖，故只允许内嵌）。
    enabled: bool = True
    host: str = "127.0.0.1"   # 默认仅本机；对外暴露必须配 auth_token（或显式风险开关，见下）
    port: int = 8765
    # HTTP Basic Auth 密码（用户名任意，只校验密码）。非空即对全部页面/接口启用。
    # 管理台可在线改任务 .py 并注册执行（等同 RCE），绑非回环地址时必须配 token。
    auth_token: str = ""
    # 显式承认"无密码暴露公网"的风险开关：host 非回环且 auth_token 为空时，
    # 必须显式置 true 才允许启动，防止改 host 后不知不觉裸奔。
    allow_public_no_auth: bool = False


class Settings(BaseModel):
    timezone: str = "Asia/Shanghai"
    runner: RunnerConfig = Field(default_factory=RunnerConfig)
    resource: ResourceConfig = Field(default_factory=ResourceConfig)
    web: WebConfig = Field(default_factory=WebConfig)
    max_log_mb: int = 32
    # 非 root 执行用户；为空 = 以调度器当前用户运行；系统无此用户时自动降级为空
    run_user: str = ""
    # ---- 调度行为参数（引擎内部，不进管理台；任务自治参数在意图文件 lifetime.retry 段）----
    starvation_sec: float = 60.0          # 饿死看门狗阈值（60s，同优先级冷启动轮转的兜底）
    preempt_exempt_sec: float = 60.0      # 任务启动后的抢占豁免期
    data_risk_max_reschedules: int = 3    # 110 数据风险二刷轮数上限
    hot_reload_sec: float = 30.0          # serve 主循环周期性热加载 jobs.json/tasks 目录

    @field_validator("timezone")
    @classmethod
    def _tz_ok(cls, v: str) -> str:
        try:
            ZoneInfo(v)
        except (ZoneInfoNotFoundError, ValueError, KeyError) as e:
            raise ValueError(f"无效时区：{v!r}（例如 Asia/Shanghai）") from e
        return v

    @field_validator("run_user")
    @classmethod
    def _run_user_ok(cls, v: str) -> str:
        if not v:
            return v
        import pwd
        try:
            pwd.getpwnam(v)
        except KeyError:
            # 用户不存在时降级为空（以调度器当前用户运行），避免 preexec_fn raise 导致进程 exit 2
            import warnings
            warnings.warn(f"run_user='{v}' 不存在，降级为空（以当前用户运行）")
            return ""
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