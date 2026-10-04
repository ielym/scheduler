"""请求收件箱扫描 + append-only 校验 + 意图消费状态持久化。

调度器每 tick 调用 scan_subtask() 得到「新意图 / 已完成意图 / 已改写违规」三组 diff。
消费状态存 data/runtime/intents_state.json：
  { "<subtask_id>": { "<request_id>": {"status": "pending|running|done|archived|rejected",
                                        "fingerprint": "...", "run_id": "...", "first_seen": "ISO",
                                        "finished_at": "ISO", "rejected_reason": ""} } }

不可变契约（append-only）：
- 同一 request_id 指纹一旦记录，后续若文件指纹变化 → 标记 rejected（子任务改写意图=违规，手动处理）
- 子任务自行删意图文件 → 调度器在消费状态里标记 done（文件不再存在 = 意图终结）
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from . import config, store, runlog
from .intent import fingerprint as intent_fingerprint

log = logging.getLogger("sched.scanner")

INTENT_FILENAME_RE = __import__("re").compile(r"^[a-zA-Z0-9._-]{1,128}\.json$")
# requests/ 目录里只允许意图文件（<request_id>.json）；以下文件名是 task 根目录/legacy 文件，
# 即使误放到 requests/ 里也绝不当意图扫描。
IGNORE_FILENAMES = {"manifest.json", "task.json"}


@dataclass
class DiffResult:
    new_requests: list[dict] = field(default_factory=list)          # [{request_id, path, fingerprint}]
    modified_requests: list[dict] = field(default_factory=list)     # [{request_id, reason, old_fp, new_fp}]  违规
    disappeared_requests: list[dict] = field(default_factory=list)  # [{request_id, reason}]  文件没了 = 终结
    total_visible: int = 0


def _intents_state_path() -> Path:
    return config.runtime_dir() / "intents_state.json"


def load_intents_state() -> dict[str, dict[str, dict]]:
    """{subtask_id: {request_id: state_dict}}。"""
    raw = store.load_json(_intents_state_path(), default={})
    return raw if isinstance(raw, dict) else {}


def save_intents_state(state: dict[str, dict[str, dict]]) -> None:
    store.save_json(_intents_state_path(), state)


def _visible_intent_files(requests_dir: Path) -> list[Path]:
    if not requests_dir.is_dir():
        return []
    out = []
    for p in sorted(requests_dir.iterdir()):
        if not p.is_file():
            continue
        if p.name in IGNORE_FILENAMES:
            continue
        if not p.name.endswith(".json"):
            continue
        if not INTENT_FILENAME_RE.match(p.name):
            continue
        out.append(p)
    return out


def scan_subtask(subtask_id: str, task_dir: Path, persisted: dict[str, dict[str, dict]] | None = None) -> DiffResult:
    """扫描一个子任务的 requests/，返回增量 diff 并同步持久态。

    调用方若给了 persisted（= load_intents_state() 的子树）则使用，避免重复 IO；
    否则本函数自己 load。持久态的 save 由调用方统一控制（减少每 tick 多次落盘）。
    """
    requests_dir = task_dir / "requests"
    files = _visible_intent_files(requests_dir)
    fp_map: dict[str, str] = {f.stem: intent_fingerprint(f) for f in files}

    if persisted is None:
        persisted = load_intents_state().get(subtask_id, {})

    diff = DiffResult(total_visible=len(files))
    now_iso = runlog.now_iso()

    existing_ids = set(persisted.keys())
    visible_ids = set(fp_map.keys())

    # 新意图：文件存在 + 持久态里没记录
    for rid in sorted(visible_ids - existing_ids):
        diff.new_requests.append({
            "request_id": rid,
            "path": str(requests_dir / (rid + ".json")),
            "fingerprint": fp_map[rid],
        })

    # 已消失：持久态记录在 pending/running，但文件没了 → 终结
    for rid in sorted(existing_ids - visible_ids):
        st = persisted.get(rid) or {}
        status = st.get("status", "")
        if status in ("pending", "running", ""):
            diff.disappeared_requests.append({
                "request_id": rid,
                "reason": "文件不存在（意图终结）",
            })

    # 已改写违规：文件在，但指纹变了且原状态不是 pending（pending 时改写也算违规——
    # 子任务应该写新 request_id）
    for rid in sorted(visible_ids & existing_ids):
        st = persisted.get(rid) or {}
        old_fp = st.get("fingerprint", "")
        if old_fp and old_fp != fp_map[rid]:
            diff.modified_requests.append({
                "request_id": rid,
                "reason": f"意图指纹变更（append-only 违规）：{old_fp[:16]}→{fp_map[rid][:16]}",
                "old_fp": old_fp,
                "new_fp": fp_map[rid],
            })

    # 把新意图登记为 pending；已改写 → rejected；已消失（running 态）→ done
    for r in diff.new_requests:
        rid = r["request_id"]
        persisted[rid] = {
            "status": "pending",
            "fingerprint": r["fingerprint"],
            "run_id": "",
            "first_seen": now_iso,
            "finished_at": "",
            "rejected_reason": "",
        }
    for r in diff.modified_requests:
        rid = r["request_id"]
        st = persisted.get(rid, {})
        st["status"] = "rejected"
        st["rejected_reason"] = r["reason"]
        persisted[rid] = st
    for r in diff.disappeared_requests:
        rid = r["request_id"]
        st = persisted.get(rid, {})
        if st.get("status") in ("pending", "running", ""):
            st["status"] = "done"
            st["finished_at"] = now_iso
            st["rejected_reason"] = r["reason"]
            persisted[rid] = st

    return diff


def mark_running(subtask_id: str, request_id: str, run_id: str) -> None:
    """意图被准出拉起后持久态标记 running。"""
    state = load_intents_state()
    st = state.setdefault(subtask_id, {}).setdefault(request_id, {})
    st["status"] = "running"
    st["run_id"] = run_id
    save_intents_state(state)


def mark_terminal(subtask_id: str, request_id: str, status: str, reason: str = "", finished_at: str | None = None) -> None:
    """意图终结：done / archived。"""
    state = load_intents_state()
    st = state.setdefault(subtask_id, {}).setdefault(request_id, {})
    st["status"] = status  # done | archived
    st["finished_at"] = finished_at or runlog.now_iso()
    if reason:
        st["rejected_reason"] = reason
    save_intents_state(state)


def mark_retry_same(subtask_id: str, request_id: str) -> None:
    """同一意图的续跑/重试：状态回 pending，run_id 清空，保留指纹。"""
    state = load_intents_state()
    st = state.setdefault(subtask_id, {}).setdefault(request_id, {})
    st["status"] = "pending"
    st["run_id"] = ""
    save_intents_state(state)


def pending_items(subtask_id: str) -> list[dict]:
    """拿到子任务下所有 pending 意图的摘要（供 service 组装 WQ 项）。"""
    state = load_intents_state()
    sub = state.get(subtask_id, {})
    out = []
    for rid, st in sorted(sub.items()):
        if st.get("status") != "pending":
            continue
        out.append({"request_id": rid, "fingerprint": st.get("fingerprint", ""),
                    "first_seen": st.get("first_seen", "")})
    return out