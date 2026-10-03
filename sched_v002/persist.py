"""L3 持久化：调度态原子快照 + 决策审计日志 + 运行态(active.json) 读写。

- scheduler_state.json：全局队列 WQ / 冷却 / 退避 / 软资源容量 的原子快照（先落快照后执行）。
- decisions.jsonl：每次调度决策的可追溯审计（append-only）。
- active.json：在途运行（pid/pgid）供崩溃恢复 reconcile 用。
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from pathlib import Path

from . import config, store, runlog


@dataclass
class WaitingRun:
    run_id: str
    job_id: str
    priority: int
    trigger: str
    attempt: int
    scheduled_at: str
    enqueued_at: str
    resume_point_ref: bool = False   # 是否为恢复运行（来自之前的抢占/中断）
    missed_count: int = 0
    contract_exempt: bool = False
    manual: bool = False


@dataclass
class SchedulerState:
    wq: list[dict] = field(default_factory=list)
    cooldowns: dict[str, str] = field(default_factory=dict)     # key(job 或 job/soft) -> iso 截止
    backoffs: dict[str, dict] = field(default_factory=dict)     # job_id -> {"until": iso, "attempts": n}
    soft_capacities: dict[str, int] = field(default_factory=dict)

    def waiting_items(self) -> list[dict]:
        return self.wq

    def enqueue(self, item: dict) -> None:
        self.wq.append(item)

    def pop_run(self, run_id: str) -> dict | None:
        for i, it in enumerate(self.wq):
            if it["run_id"] == run_id:
                return self.wq.pop(i)
        return None


def load_scheduler_state() -> SchedulerState:
    raw = store.load_json(config.scheduler_state_file(), default=None)
    if not isinstance(raw, dict):
        return SchedulerState()
    return SchedulerState(
        wq=raw.get("wq") or [],
        cooldowns=raw.get("cooldowns") or {},
        backoffs=raw.get("backoffs") or {},
        soft_capacities=raw.get("soft_capacities") or {},
    )


def save_scheduler_state(st: SchedulerState) -> None:
    store.save_json(config.scheduler_state_file(), asdict(st))


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


def snapshot_running_from(active_records: list[dict]) -> list[dict]:
    return [
        {
            "run_id": r.get("run_id"),
            "job_id": r.get("job_id"),
            "pid": r.get("pid"),
            "pgid": r.get("pgid"),
            "status": r.get("status"),
            "task_dir": r.get("task_dir"),
            "started_at": r.get("started_at"),
            "scheduled_at": r.get("scheduled_at"),
            "queued_at": r.get("queued_at"),
        }
        for r in active_records
    ]