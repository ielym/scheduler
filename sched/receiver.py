"""接收器：outbox 增量扫描、去重、索引、回执、归档、异常隔离（设计文档第三、五节）。

- 持续、增量扫描各任务 outbox/ 下的就绪 JSON，写入磁盘索引（SQLite jobs）。
- 去重：相同 job_id + 相同内容 → 已有状态并回执；相同 job_id 内容不同 → 隔离并给原因。
- 接收时先提交作业记录及内容摘要，再发布回执并归档请求。
- 不设接收上限；不向生产者发背压；数据库暂不可写时不删文件。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
from pathlib import Path

from . import config
from .config import ReadyError, parse_ready

log = logging.getLogger("sched.receiver")


def _content_hash(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()[:32]


def _archive_dir(task_id: str) -> Path:
    return config.runtime_dir() / "archive" / task_id


def _isolation_dir(task_id: str) -> Path:
    return config.runtime_dir() / "isolation" / task_id


def _write_receipt(task_id: str, job_id: str, content_hash: str, now: str,
                   status: str, reason: str = "") -> None:
    rd = config.receipts_dir(task_id)
    rd.mkdir(parents=True, exist_ok=True)
    from . import store
    store.save_json(rd / f"{job_id}.json", {
        "job_id": job_id, "task_id": task_id, "content_hash": content_hash,
        "received_at": now, "status": status, "reason": reason,
    })


def _isolate(task_id: str, path: Path, reason: str) -> None:
    dst_dir = _isolation_dir(task_id)
    dst_dir.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(str(path), str(dst_dir / path.name))
    except OSError:
        pass
    log.warning("isolate ready json %s/%s: %s", task_id, path.name, reason)


def _archive(task_id: str, path: Path) -> None:
    dst_dir = _archive_dir(task_id)
    dst_dir.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(str(path), str(dst_dir / path.name))
    except OSError:
        # 归档失败不影响接收正确性（回执已写），文件留在 outbox 下轮重享幂等
        pass


def receive_outbox(task_id: str, spec: config.TaskSpec, db, now: str) -> dict:
    out = config.outbox_dir(task_id)
    summary = {"task_id": task_id, "seen": 0, "created": 0, "existing": 0,
               "conflict": 0, "invalid": 0}
    if not out.is_dir():
        return summary
    for p in sorted(out.glob("*.json")):
        if not p.is_file():
            continue
        summary["seen"] += 1
        try:
            raw = p.read_bytes()
        except OSError:
            continue
        try:
            doc = parse_ready(json.loads(raw.decode("utf-8")))
        except (json.JSONDecodeError, UnicodeDecodeError, ReadyError) as e:
            _isolate(task_id, p, f"无效 JSON：{e}")
            summary["invalid"] += 1
            _write_receipt(task_id, p.stem, "", now, "invalid", str(e))
            continue
        if doc.task_id != task_id:
            _isolate(task_id, p, f"task_id 不匹配：{doc.task_id}")
            summary["invalid"] += 1
            continue
        ch = _content_hash(raw)
        priority = spec.priority
        status, created = db.upsert_job(doc, ch, now, priority)
        if status == "conflict":
            _isolate(task_id, p, "job_id 已存在但内容不同（不覆盖原作业）")
            summary["conflict"] += 1
            _write_receipt(task_id, doc.job_id, ch, now, "conflict",
                           "job_id 已存在但内容不同")
            continue
        if created:
            summary["created"] += 1
        else:
            summary["existing"] += 1
        _write_receipt(task_id, doc.job_id, ch, now, "received")
        _archive(task_id, p)
    return summary


def receive_all(db, now: str, specs: dict[str, config.TaskSpec]) -> dict[str, dict]:
    out = {}
    for task_id, spec in specs.items():
        if not spec.enabled:
            continue
        try:
            out[task_id] = receive_outbox(task_id, spec, db, now)
        except Exception as e:  # noqa: BLE001
            log.warning("receive_outbox %s failed: %s", task_id, e)
            out[task_id] = {"task_id": task_id, "error": str(e)}
    return out