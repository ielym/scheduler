"""SDK 意图 API：emit_intent（原子写新意图文件）+ list_intents（读当前意图清单）。

不提供 update/remove——append-only 契约：新执行 = 新 request_id 文件，生命周期靠 state.json 的 control 段 + scheduler 侧 scanner 协调。
"""

from __future__ import annotations

import json
import os
import secrets
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from .errors import EXIT_OK, EXIT_PREEMPTED


_intent_lock = threading.Lock()


def _workspace() -> Path:
    v = os.environ.get("SCHED_WORKSPACE")
    if v:
        return Path(v)
    raise RuntimeError("SCHED_WORKSPACE 未设置（非调度器启动，无法使用 emit_intent）")


def _requests_dir() -> Path:
    d = _workspace() / "requests"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _atomic_write(path: Path, data: dict) -> None:
    import os as _os
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    _os.replace(tmp, path)


def _make_request_id(prefix: str = "") -> str:
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    rand = secrets.token_hex(4)
    base = f"{prefix}-{ts}-{rand}" if prefix else f"{ts}-{rand}"
    return base[:128]  # 128 是 scanner 的 request_id 长度上限


def emit_intent(
    config: str = "",
    kind: str = "daemon",
    entry: dict | None = None,
    priority: int | None = None,
    resources: dict | None = None,
    lifetime: dict | None = None,
    request_id: str | None = None,
    **_unused,
) -> str:
    """原子写一个新意图文件（写后不可变，append-only）。

    返回写入的 request_id。子任务后续可按需多次调用（多次新进程 = 多次调用）。
    """
    if kind not in ("daemon", "oneshot"):
        raise ValueError(f"kind 必须是 'daemon' 或 'oneshot'：{kind!r}")
    if entry is None:
        entry = {}
    if resources is None:
        resources = {"cpu": 0.0, "memory_mb": 0}
    if lifetime is None:
        lifetime = {
            "completion": "indefinite" if kind == "daemon" else "terminal",
            "retry": {"backoff_base_sec": 5, "backoff_cap_sec": 300, "not_after": ""},
        }
    rid = request_id or _make_request_id()
    if not rid:
        raise ValueError("request_id 不能为空")
    payload = {
        "schema_version": 1,
        "request_id": rid,
        "kind": kind,
        "config": config or "",
        "entry": entry,
        "priority": priority if priority is not None else 0,
        "resources": resources,
        "lifetime": lifetime,
    }
    with _intent_lock:
        target = _requests_dir() / (rid + ".json")
        if target.exists():
            # 已存在 → 幂等（完全相同则跳过，不同则报错——append-only 契约）
            try:
                prev = json.loads(target.read_text("utf-8"))
                if prev == payload:
                    return rid
            except (json.JSONDecodeError, OSError):
                pass
            raise FileExistsError(f"意图文件已存在且内容不同：{target}")
        _atomic_write(target, payload)
    return rid


def list_intents() -> list[dict]:
    """读当前 requests/ 下所有意图文件的清单（按 request_id 排序）。"""
    d = _requests_dir()
    out: list[dict] = []
    for p in sorted(d.glob("*.json")):
        if not p.is_file():
            continue
        try:
            obj = json.loads(p.read_text("utf-8"))
            if isinstance(obj, dict):
                out.append(obj)
        except (json.JSONDecodeError, OSError):
            continue
    return out