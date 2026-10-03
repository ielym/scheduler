"""注册表：data/jobs.json 是唯一真源（纯手动注册）。

任务目录 tasks/<name>/ 由用户自行创建（含 task.json 与入口），调度器绝不自动发现。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import config, store, taskdoc


@dataclass
class JobEntry:
    job_id: str
    task_dir: Path
    doc: taskdoc.TaskDoc | None = None
    error: str = ""
    enabled: bool = True

    @property
    def ok(self) -> bool:
        return self.doc is not None and not self.error

    def effective(self) -> dict[str, Any]:
        if self.doc is None:
            return {}
        return taskdoc.effective(self.task_dir, self.doc)

    def schedule(self) -> dict[str, Any]:
        return self.effective().get("schedule") or {"type": "manual"}


def load_state() -> dict[str, dict]:
    raw = store.load_json(config.jobs_file(), default={})
    return raw if isinstance(raw, dict) else {}


def save_state(state: dict[str, dict]) -> None:
    store.save_json(config.jobs_file(), state)


def upsert_state(job_id: str, patch: dict[str, Any]) -> dict:
    state = load_state()
    entry = state.get(job_id) or {"enabled": True}
    for k, v in patch.items():
        entry[k] = v
    state[job_id] = entry
    save_state(state)
    return entry


def drop_state(job_id: str) -> None:
    state = load_state()
    if job_id in state:
        state.pop(job_id)
        save_state(state)


def load_registered() -> dict[str, JobEntry]:
    state = load_state()
    root = config.tasks_dir()
    entries: dict[str, JobEntry] = {}
    for jid, st in sorted(state.items()):
        task_dir = root / jid
        e = JobEntry(job_id=jid, task_dir=task_dir, enabled=bool(st.get("enabled", True)))
        if not task_dir.is_dir():
            e.error = f"任务目录不存在：{task_dir}"
        else:
            try:
                e.doc = taskdoc.load_task_doc(task_dir)
            except taskdoc.TaskDocError as ex:
                e.error = "; ".join(ex.errors)
        entries[jid] = e
    return entries