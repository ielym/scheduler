"""持久化：调度态原子快照 + 决策审计。

主循环每 tick 维护：
- scheduler_state.json：WQ 快照（纯 dict 列表，字段由 service._enqueue_intent 决定）
- active.json：在途运行快照（崩溃恢复用）
- decisions.jsonl：每次调度决策的可追溯审计（append-only）
- intents_state.json：意图消费状态（由 scanner.py 独立维护，本模块不再插手）
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

from . import config, store


@dataclass
class SchedulerState:
    wq: list[dict] = field(default_factory=list)

    def waiting_items(self) -> list[dict]:
        return self.wq

    def enqueue(self, item: dict) -> None:
        self.wq.append(item)

    def pop_run(self, run_id: str) -> dict | None:
        for i, it in enumerate(self.wq):
            if it.get("run_id") == run_id:
                return self.wq.pop(i)
        return None


def load_scheduler_state() -> SchedulerState:
    raw = store.load_json(config.scheduler_state_file(), default=None)
    if not isinstance(raw, dict):
        return SchedulerState()
    return SchedulerState(wq=raw.get("wq") or [])


def save_scheduler_state(st: SchedulerState) -> None:
    store.save_json(config.scheduler_state_file(), asdict(st))


def load_admission_state() -> dict[str, dict]:
    raw = store.load_json(config.admission_state_file(), default={})
    return raw if isinstance(raw, dict) else {}


def save_admission_state(state: dict[str, dict]) -> None:
    store.save_json(config.admission_state_file(), state)


def load_active() -> list[dict]:
    raw = store.load_json(config.runtime_dir() / "active.json", default=[])
    return raw if isinstance(raw, list) else []


def save_active(snapshot: list[dict]) -> None:
    store.save_json(config.runtime_dir() / "active.json", snapshot)


def load_recover_queue() -> list[dict]:
    raw = store.load_json(config.runtime_dir() / "recover_queue.json", default=[])
    return raw if isinstance(raw, list) else []


def save_recover_queue(records: list[dict]) -> None:
    store.save_json(config.runtime_dir() / "recover_queue.json", records)


def append_decision(record: dict) -> None:
    store.append_jsonl(config.decisions_log(), record)