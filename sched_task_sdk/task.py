"""任务基类：托管断点/心跳/抢占退出(99)/错误分类/跳过项。

路径 B 的约定：任务收到 SIGTERM 即「托管断点 → 退出 99」，调度器据此判定为干净抢占可恢复。
SDK 把这一约定封装进 SchedTask.run()，任务作者只需实现 steps() 生成器并维护 resume_point。

阻止信号：SIGTERM 只置位 _stop_requested，不打断正在执行的步；长步靠后台心跳线程维持存活。
"""

from __future__ import annotations

import os
import re
import signal
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generator, Iterator

from .errors import Category, classify_exception, logic, resource
from .state import TaskState

PREEMPT_EXIT_CODE = 99
DRY_RUN_ENV = "SCHED_DRY_RUN"

_pending_skipped_path = "cache/pending_skipped.jsonl"
_dead_letter_path = "cache/dead_letter.jsonl"


@dataclass
class Step:
    """yield 出的一个可观测步。name 用于展示，payload 透传到指标。"""

    name: str
    payload: dict[str, Any] | None = None


class SchedTask:
    # 心跳线程刷新间隔（秒）；步内无 flush 也保持心跳，避免长步被误判卡死
    HEARTBEAT_INTERVAL_SEC = 30

    def __init__(self, workspace: Path | None = None):
        self.state = TaskState(workspace)
        self.workspace = Path(os.environ.get("SCHED_WORKSPACE") or (workspace or os.getcwd()))
        self.resource_regex: list[str] = []
        self.logic_regex: list[str] = []
        self._stop_requested = False
        self._heartbeat_thread: threading.Thread | None = None
        self._exit_code = 0

    def contract_probe(self) -> int:
        """契约自检（闸2）：只写出合法 state.json 即退出 0，不发任何业务副作用。"""
        self._install_signal_handlers()
        self.state.status = "running"
        self.state.current_step = "contract_probe"
        self.state.resume_point = self.state.resume_point or {}
        self.state.flush()
        self.state.mark_success()
        self.state.flush()
        return 0

    # ---- 用户实现 ----
    def steps(self, resume: dict[str, Any]) -> Iterator[Step]:
        """任务主体：生成器。resume 为上次的 resume_point（首次为空 dict）。

        典型写法：
            for date in self.pending_dates(resume):
                yield Step("fetch", date=date)
                self.fetch(date)
                self.state.resume_point = {"date": date}
                self.state.flush()
        """
        raise NotImplementedError
        yield Step("noop")  # pragma: no cover

    # ---- 生命周期（run.py 的 main 调用）----
    def run(self) -> int:
        self._install_signal_handlers()
        self._start_heartbeat()
        self.state.status = "running"
        self.state.started_at = self.state.started_at or self.state.updated_at
        self.state.flush()
        try:
            for step in self.steps(resume=self.state.resume_point):
                self.state.loop_count += 1
                if isinstance(step, Step):
                    self.state.current_step = step.name
                self.state.flush()
                if self._stop_requested:
                    self.state.mark_preempted()
                    self.state.flush()
                    return self._exit(PREEMPT_EXIT_CODE)
        except SystemExit as e:
            code = e.code if isinstance(e.code, int) else 0
            if code == PREEMPT_EXIT_CODE:
                self.state.mark_preempted()
                self.state.flush()
                return self._exit(PREEMPT_EXIT_CODE)
            return self._exit(code)
        except BaseException as e:  # noqa: BLE001 —— 任务任何异常都要归类落盘
            classified = classify_exception(e, self.resource_regex, self.logic_regex)
            self.state.mark_failed(classified["category"], classified["message"])
            self.state.flush()
            code = self._code_for_category(classified["category"])
            return self._exit(code)

        self.state.mark_success()
        self.state.flush()
        return self._exit(0)

    # ---- 意图退出（如日期不命中，正常结束）----
    def finish(self, code: int = 0) -> int:
        self.state.mark_success()
        self.state.flush()
        return self._exit(code)

    # ---- 错误分类辅助 ----
    def fail(self, message: str, category: str | None = None) -> int:
        cat = category or logic()
        self.state.mark_failed(cat, message)
        self.state.flush()
        return self._exit(self._code_for_category(cat))

    def skip_item(self, item_id: str, reason: str) -> None:
        """把一条数据处理失败记入 pending_skipped（二刷），不因单条数据卡住整体。"""
        append_jsonl(self.workspace / _pending_skipped_path, {"item": item_id, "reason": reason})

    def dead_letter(self, item_id: str, reason: str) -> None:
        """明确判死的数据进 dead_letter，避免无限重试。"""
        append_jsonl(self.workspace / _dead_letter_path, {"item": item_id, "reason": reason})

    @property
    def dry_run(self) -> bool:
        return os.environ.get(DRY_RUN_ENV) == "1"

    # ---- 内部 ----
    def _install_signal_handlers(self) -> None:
        def _on_term(signum, frame):  # noqa: ARG001
            self._stop_requested = True

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, _on_term)
            except ValueError:
                pass

    def _start_heartbeat(self) -> None:
        def _beat() -> None:
            while True:
                if self._heartbeat_thread is None:
                    return
                # 仅当主线程未在 flush 时这里自身 flush 就足够了；周期刷新 updated_at
                self.state.flush()
                threading.Event().wait(self.HEARTBEAT_INTERVAL_SEC)

        self._heartbeat_thread = threading.Thread(target=_beat, name="sched-task-heartbeat", daemon=True)
        self._heartbeat_thread.start()

    @staticmethod
    def _code_for_category(category: str) -> int:
        if category == Category.RESOURCE:
            return 100
        if category == Category.UNCLASSIFIED:
            return 1  # 交给调度器 G5 按资源处理
        return 1

    def _exit(self, code: int) -> int:
        self._exit_code = int(code)
        self._stop_heartbeat()
        return self._exit_code

    def _stop_heartbeat(self) -> None:
        t, self._heartbeat_thread = self._heartbeat_thread, None
        # daemon 线程，直接置空标记即可让循环退出；不 join 避免阻塞


def append_jsonl(path: Path, obj: dict[str, Any]) -> None:
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")
        f.flush()
        os.fsync(f.fileno())


def run_task(task: SchedTask) -> int:
    """run.py 的 main() 一句话入口。"""
    if os.environ.get("SCHED_CONTRACT_PROBE") == "1":
        return task.contract_probe()
    code = task.run()
    return code


__all__ = ["SchedTask", "Step", "PREEMPT_EXIT_CODE", "run_task", "resource", "logic", "Category"]