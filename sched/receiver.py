"""接收器：outbox 增量扫描、去重、索引、回执、归档、异常隔离（设计文档第三、五节）。

- 持续、增量扫描各任务 outbox/ 下的就绪 JSON，写入磁盘索引（SQLite jobs）。
  增量列举：目录 mtime 未变化的子目录沿用缓存清单，只重扫变化的目录
  （设计第五节"流式分页"），每轮全量列举仅发生在冷启动。
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
import time
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
    # 保留 outbox 内的相对子目录结构（设计第二节：outbox 可按发布批次/文件名
    # 散列分目录），避免不同分目录的同名文件在隔离区互相覆盖
    rel = path.relative_to(config.outbox_dir(task_id))
    dst_dir = _isolation_dir(task_id) / rel.parent
    dst_dir.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(str(path), str(dst_dir / path.name))
    except OSError:
        pass
    log.warning("isolate ready json %s/%s: %s", task_id, rel.as_posix(), reason)


def _archive(task_id: str, path: Path, job_id: str) -> None:
    """按 job_id 归档（与归档对账的检查路径 archive/<task>/<job_id>.json 一致；
    outbox 分目录不改变归档布局，job_id 全局唯一不会冲突）。"""
    dst_dir = _archive_dir(task_id)
    dst_dir.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(str(path), str(dst_dir / f"{job_id}.json"))
    except OSError:
        # 归档失败不影响接收正确性（回执已写），文件留在 outbox 下轮重享幂等
        pass


# B3：新文件优先发现的内存态（task_id -> 上轮扫描时见到的文件相对路径集合）。
# 不持久化：重启冷启动首轮全部视为新文件（行为与现状一致，仅排序变化）。
# 接收器在调度器主循环内单线程调用，无需加锁。
_seen_files: dict[str, set[str]] = {}

# 增量目录缓存（审查问题二修复，设计第五节"流式增量发现"）：
# task_id -> {rel_dir: (mtime_ns, frozenset(文件名))}。
# 目录 mtime 在条目增删/替换时变化：未变化的目录沿用缓存的文件清单，
# 只有变化的目录才重新 scandir——避免每轮对每任务全量 stat+排序。
# 目录树本身每轮遍历（目录数 << 文件数），删除的目录随缓存重建自然清除。
# 与 _seen_files 一样不持久化，重启后冷启动做一次全量列举。
_dir_cache: dict[str, dict[str, tuple[int, frozenset[str]]]] = {}


def _list_json_rels(out: Path, task_id: str) -> list[str]:
    """列出 outbox 下全部 *.json 的相对路径（排序确定性不变，增量列举）。"""

    cache = _dir_cache.get(task_id) or {}
    new_cache: dict[str, tuple[int, frozenset[str]]] = {}
    rels: list[str] = []

    def _scan_dir(rel: str, d: Path) -> None:
        try:
            mtime = d.stat().st_mtime_ns
        except OSError:
            return
        rel_key = rel or "."
        cached = cache.get(rel_key)
        if cached is not None and cached[0] == mtime:
            names = cached[1]
        else:
            found: set[str] = set()
            try:
                with os.scandir(d) as it:
                    for e in it:
                        if e.name.endswith(".json") and e.is_file(follow_symlinks=False):
                            found.add(e.name)
            except OSError:
                pass
            names = frozenset(found)
        new_cache[rel_key] = (mtime, names)
        prefix = rel + "/" if rel else ""
        rels.extend(prefix + n for n in names)
        try:
            with os.scandir(d) as it:
                for e in it:
                    if e.is_dir(follow_symlinks=False):
                        _scan_dir(prefix + e.name, Path(e.path))
        except OSError:
            pass

    _scan_dir("", out)
    _dir_cache[task_id] = new_cache
    rels.sort()
    return rels


def receive_outbox(task_id: str, spec: config.TaskSpec, db, now: str,
                   batch_size: int | None = None) -> dict:
    """扫描一个任务的 outbox（含分目录，设计第二节 L23"可以按发布批次或
    文件名散列分目录"）。

    batch_size：本轮最多处理的文件数（时间片），剩余文件下轮继续（deferred 计数）。
    B3（设计第五节"发现即接收"）：新发布的文件优先发现——半数配额给新文件、
    半数保底给旧文件（防积压场景下旧文件饿死），处理顺序新文件在前。
    不设接收上限；不向生产者发背压；数据库暂不可写时不删文件。
    """
    out = config.outbox_dir(task_id)
    summary = {"task_id": task_id, "seen": 0, "created": 0, "existing": 0,
               "conflict": 0, "invalid": 0, "deferred": 0}
    latencies: list[float] = []
    if not out.is_dir():
        return summary

    def _rel(p: Path) -> str:
        return p.relative_to(out).as_posix()

    # 递归扫描分目录（增量：仅重扫 mtime 变化的目录，设计第五节"流式增量发现"）；
    # 按相对路径排序保证确定性（不把文件名顺序当业务时间，设计第五节），
    # 去重/seen 记忆按相对路径区分不同分目录的同名文件
    all_rels = _list_json_rels(out, task_id)
    all_files = [out / r for r in all_rels]
    seen = _seen_files.get(task_id, set())
    new_files = [p for p in all_files if _rel(p) not in seen]
    old_files = [p for p in all_files if _rel(p) in seen]
    if batch_size is not None:
        new_quota = max(1, batch_size // 2)
        old_quota = max(0, batch_size - new_quota)
        sel_new = new_files[:new_quota]
        sel_old = old_files[:old_quota]
        files = sel_new + sel_old
        summary["deferred"] = ((len(new_files) - len(sel_new)) +
                               (len(old_files) - len(sel_old)))
    else:
        files = new_files + old_files
    for p in files:
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
            # 回执闭环（设计第三节）：生产者只轮询自己任务的 receipts/，
            # 不写回执会让错误投递的作业在消费端永远无反馈
            _write_receipt(task_id, doc.job_id, _content_hash(raw), now,
                           "invalid", f"task_id 不匹配：{doc.task_id}")
            continue
        ch = _content_hash(raw)
        # 作业级优先级：仅允许在任务授权范围内降低/保持，不得越过任务优先级
        priority = spec.priority
        if doc.priority is not None:
            if doc.priority > spec.priority:
                log.warning("job %s priority %d exceeds task %s priority %d, clamped",
                            doc.job_id, doc.priority, task_id, spec.priority)
            priority = min(doc.priority, spec.priority)
        status, created = db.upsert_job(doc, ch, now, priority)
        if status == "conflict":
            _isolate(task_id, p, "job_id 已存在但内容不同（不覆盖原作业）")
            summary["conflict"] += 1
            _write_receipt(task_id, doc.job_id, ch, now, "conflict",
                           "job_id 已存在但内容不同")
            continue
        if created:
            summary["created"] += 1
            # 发现延迟监控：就绪文件落盘到被接收发现的耗时（文件 mtime → now）
            try:
                latencies.append(max(0.0, time.time() - p.stat().st_mtime))
            except OSError:
                pass
        else:
            summary["existing"] += 1
        _write_receipt(task_id, doc.job_id, ch, now, "received")
        _archive(task_id, p, doc.job_id)
    # 轮末更新内存态：本轮见到的全部文件相对路径（已归档/隔离的自动出集合）
    _seen_files[task_id] = set(all_rels)
    if latencies:
        summary["discovery_latencies"] = latencies
    return summary


def receive_all(db, now: str, specs: dict[str, config.TaskSpec],
                order: list[str] | None = None,
                batch_size: int | None = None) -> dict[str, dict]:
    """按 order 顺序扫描（调度器传入优先级降序 + 同级轮转后的顺序）。

    每任务每轮受 batch_size 时间片约束，保证公平与可中断。
    """
    out = {}
    for task_id in (order if order is not None else list(specs.keys())):
        spec = specs.get(task_id)
        if spec is None or not spec.enabled:
            continue
        try:
            out[task_id] = receive_outbox(task_id, spec, db, now, batch_size)
        except Exception as e:  # noqa: BLE001
            log.warning("receive_outbox %s failed: %s", task_id, e)
            out[task_id] = {"task_id": task_id, "error": str(e)}
    return out