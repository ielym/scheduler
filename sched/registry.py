"""注册表：data/jobs.json 是唯一真源。

tasks/<subtask_id>/ 下必须有 manifest.json（唯一清单格式）。
jobs.json 只记录 enabled 状态；其他信息从 manifest.json 解析。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import config, store, manifest


@dataclass
class JobEntry:
    subtask_id: str
    task_dir: Path
    doc: manifest.TaskManifest | None = None
    error: str = ""
    enabled: bool = True

    @property
    def ok(self) -> bool:
        return self.doc is not None and not self.error


def load_state() -> dict[str, dict]:
    raw = store.load_json(config.jobs_file(), default={})
    return raw if isinstance(raw, dict) else {}


def save_state(state: dict[str, dict]) -> None:
    store.save_json(config.jobs_file(), state)


def upsert_state(subtask_id: str, patch: dict[str, Any]) -> dict:
    state = load_state()
    entry = state.get(subtask_id) or {"enabled": True}
    for k, v in patch.items():
        entry[k] = v
    state[subtask_id] = entry
    save_state(state)
    return entry


def drop_state(subtask_id: str) -> None:
    state = load_state()
    if subtask_id in state:
        state.pop(subtask_id)
    save_state(state)


def load_registered() -> dict[str, JobEntry]:
    state = load_state()
    root = config.tasks_dir()
    entries: dict[str, JobEntry] = {}
    for sid, st in sorted(state.items()):
        task_dir = root / sid
        e = JobEntry(subtask_id=sid, task_dir=task_dir, enabled=bool(st.get("enabled", True)))
        if not task_dir.is_dir():
            e.error = f"任务目录不存在：{task_dir}"
        else:
            try:
                e.doc = manifest.load_manifest(task_dir)
            except manifest.ManifestError as ex:
                e.error = "; ".join(ex.errors)
        entries[sid] = e
    return entries
