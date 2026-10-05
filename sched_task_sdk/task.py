"""SDK 基类：SchedTask（daemon 与 oneshot 统一基类）+ DaemonTask + OneshotTask。

差别仅在 steps() 生成器是否终止：
  - oneshot: steps() 有限迭代（跑完最后一步自然退出）
  - daemon: steps() 无限迭代（内层 while True），靠 SIGTERM/request_stop 落断点退出 99

共用：心跳线程、断点契约、错误分类、控制指令、信号处理。
"""

from __future__ import annotations

import signal
import sys
import time
from typing import Any, Iterable, Iterator

from .errors import (EXIT_DATA_RISK, EXIT_LOGIC, EXIT_NO_RETRY, EXIT_OK,
                     EXIT_PREEMPTED, EXIT_RESOURCE, EXIT_EXTERNAL_RESOURCE,
                     EXIT_STOP_REQUESTED)
from .state import (DEFAULT_HEARTBEAT_INTERVAL_SEC, DEFAULT_STEP_TIMEOUT_SEC,
                    TaskState, _global_state)
from .intent import emit_intent, list_intents


class SchedTask:
    """统一任务基类（daemon 与 oneshot 共用）。

    子类实现：
      - steps() → 迭代器/生成器：每个 yield 返回一个"步骤"（可是 dict/对象/None）
        每个步骤内自己做业务逻辑、调用 self.state.save_checkpoint(marker) 落断点。
      - process_step(step) → 实际处理一个步骤（可选，默认 passthrough）
      - on_error(exc) → 可选：分类错误 category（resource / logic / unclassified）
    """

    kind: str = "oneshot"
    name: str = "task"

    def __init__(
        self,
        name: str | None = None,
        state: TaskState | None = None,
        heartbeat_interval_sec: int = DEFAULT_HEARTBEAT_INTERVAL_SEC,
        step_timeout_sec: int = DEFAULT_STEP_TIMEOUT_SEC,
    ):
        if name is not None:
            self.name = name
        self.state = state if state is not None else TaskState()
        self.state.heartbeat_interval_sec = heartbeat_interval_sec
        self.state.step_timeout_sec = step_timeout_sec
        self.state.on_sigterm = self._on_sigterm
        self._suspended_yield = None  # 被抢占时保存断点标记

    # ---- 子类 override ----

    def steps(self) -> Iterator[Any]:
        """迭代器：yield 每个 step（daemon 子类应无限迭代）。"""
        raise NotImplementedError

    def process_step(self, step: Any) -> None:
        """可选：对 yield 出来的 step 做实际处理。默认无操作。"""
        pass

    def on_error(self, exc: BaseException) -> str:
        """可选：返回错误 category（resource / logic / unclassified）。默认 unclassified。"""
        return "unclassified"

    # ---- 生命周期 ----

    def run(self) -> int:
        """主循环：初始化 state → 消费 steps() → 返回退出码。"""
        self.state.start()
        self.state.enter_step("init")

        resume = self.state.resume_point()
        marker = None
        if resume and resume.get("resume_point"):
            marker = resume["resume_point"]
            self._log(f"[SDK] resume from checkpoint: {marker}")

        exit_code = EXIT_OK
        try:
            for step in self.steps():
                if self.state._preempted or self.state._stop_requested:
                    break
                self.state.enter_step(f"step", progress=0.0)
                try:
                    self.process_step(step)
                except Exception as exc:  # noqa: BLE001
                    cat = self.on_error(exc)
                    self.state.error(cat, f"{type(exc).__name__}: {exc}")
                    if cat == "logic":
                        exit_code = EXIT_LOGIC
                    else:
                        exit_code = EXIT_RESOURCE
                    break
        except Exception as exc:  # noqa: BLE001  捕获 steps() 迭代器在 yield 前抛的异常（如初始化失败）
            cat = self.on_error(exc)
            self.state.error(cat, f"{type(exc).__name__}: {exc}")
            if cat == "logic":
                exit_code = EXIT_LOGIC
            else:
                exit_code = EXIT_RESOURCE
        finally:
            # 落断点 → 退出
            if self.state._preempted:
                exit_code = EXIT_PREEMPTED
            elif self.state._stop_requested:
                exit_code = EXIT_STOP_REQUESTED
            elif self.state._no_retry and exit_code != EXIT_OK:
                exit_code = EXIT_NO_RETRY
            self.state.finalize()
        return exit_code

    # ---- 信号 ----

    def _on_sigterm(self) -> None:
        """SIGTERM 钩子：让 steps() 生成器能在当前 step 落断点后退出。"""
        self.save_checkpoint(None)

    def save_checkpoint(self, marker: Any) -> None:
        """公开的断点落盘（子任务在业务步骤结束时主动调用）。"""
        self.state.save_checkpoint(marker)

    def resume_point(self) -> dict | None:
        """读取当前断点（等价于 self.state.resume_point()，便于子类直读）。"""
        return self.state.resume_point()

    def heartbeat(self) -> None:
        """显式写一次心跳（正常由后台心跳线程自动写）。"""
        self.state.heartbeat()

    # ---- 控制 ----

    def request_stop(self, reason: str = "") -> None:
        self.state.request_stop(reason)

    def no_retry(self, reason: str = "") -> None:
        self.state.no_retry(reason)

    def reschedule(self, delay_sec: float, reason: str = "") -> None:
        self.state.reschedule(delay_sec, reason)

    # ---- 意图 ----

    def emit_intent(self, **kwargs) -> str:
        return emit_intent(**kwargs)

    def list_intents(self) -> list[dict]:
        return list_intents()

    # ---- 工具 ----

    def _log(self, msg: str) -> None:
        print(msg, flush=True, file=sys.stderr)


class OneshotTask(SchedTask):
    """有限迭代任务：跑完 steps() 自然 exit 0。"""
    kind = "oneshot"

    def steps(self) -> Iterator[Any]:  # noqa: D401
        yield None  # 子类覆写


class DaemonTask(SchedTask):
    """常驻任务：steps() 无限迭代，靠 SIGTERM/request_stop 落断点 exit 99。"""
    kind = "daemon"

    def steps(self) -> Iterator[Any]:  # noqa: D401
        while True:
            yield None  # 子类覆写


# ---- run.py 入口便捷函数 ----

def run_task(task: SchedTask) -> int:
    """最简单的 run.py 入口：构造 task → task.run() → os._exit(rc)。"""
    rc = task.run()
    sys.exit(rc)