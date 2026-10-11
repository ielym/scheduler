"""生产者 SDK：发现游标 + 待发布记录持久化 + 无丢失发布 + 稳定作业标识 + 重放。

生产者与调度器解耦（设计文档第三节）：
- 发现一个业务事件后，先把稳定事件标识、输入引用、业务版本与待发布作业记录持久化，
  再推进发现游标（两者同一原子写入）。
- 作业标识由 (task_id, 稳定事件标识, 业务版本) 确定，重放得到相同结果。
- 发布：写临时文件 → fsync → 同文件系统原子 rename → fsync 目录，成功后才标记已发布。
- 未收到接收确认的作业保持可重放。
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any


class ProducerError(Exception):
    pass


def _task_id() -> str:
    return os.environ.get("SCHED_TASK_ID") or ""


def _workspace() -> Path:
    v = os.environ.get("SCHED_WORKSPACE")
    if not v:
        raise ProducerError("SCHED_WORKSPACE 未设置（非生产者启动？）")
    return Path(v)


def _state_dir() -> Path:
    d = _workspace() / "producer_state"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _outbox_dir() -> Path:
    d = _workspace() / "outbox"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _receipts_dir() -> Path:
    d = _workspace() / "receipts"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _state_file() -> Path:
    return _state_dir() / "state.json"


def job_id_of(task_id: str, event_id: str, business_version: str) -> str:
    """稳定作业标识：由任务标识 + 稳定事件标识 + 业务版本确定。"""
    digest = hashlib.sha256(
        f"{task_id}\0{event_id}\0{business_version}".encode("utf-8")
    ).hexdigest()[:32]
    return f"{task_id}-{digest}"


def _atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=path.name + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        _fsync_dir(path.parent)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _fsync_dir(d: Path) -> None:
    try:
        fd = os.open(str(d), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


class Producer:
    """常驻生产者的基类。子类实现 discover(cursor) → (new_cursor, [publish_spec...])。"""

    def __init__(self):
        self._lock = threading.RLock()
        self._state = self._load()

    # ---------- 状态 ----------
    def _load(self) -> dict:
        p = _state_file()
        if p.exists():
            try:
                obj = json.loads(p.read_text("utf-8"))
                if isinstance(obj, dict):
                    return {"cursor": obj.get("cursor"),
                            "unacked": obj.get("unacked") or {}}
            except (json.JSONDecodeError, OSError):
                pass
        return {"cursor": None, "unacked": {}}

    def _save(self) -> None:
        with self._lock:
            _atomic_write_json(_state_file(), self._state)

    @property
    def cursor(self) -> Any:
        with self._lock:
            return self._state["cursor"]

    def advance(self, new_cursor: Any, jobs: list[dict]) -> None:
        """持久化游标与待发布记录（同一原子写）。jobs 每项含 event_id/business_version 与
        正式的就绪 JSON 载荷字段。"""
        with self._lock:
            task_id = _task_id()
            for j in jobs:
                event_id = str(j.get("event_id") or "")
                business_version = str(j.get("business_version") or "")
                job_id = j.get("job_id") or job_id_of(task_id, event_id, business_version)
                self._state["unacked"][job_id] = self._ready_payload(job_id, j)
            self._state["cursor"] = new_cursor
        self._save()

    def _ready_payload(self, job_id: str, spec: dict) -> dict:
        return {
            "schema_version": 1,
            "job_id": job_id,
            "task_id": _task_id(),
            "entry_args": list(spec.get("entry_args") or []),
            "workdir": spec.get("workdir") or ".",
            "input_refs": list(spec.get("input_refs") or []),
            "idempotency_key": spec.get("idempotency_key") or "",
            "progress_semver": spec.get("progress_semver") or "1.0.0",
            "env": dict(spec.get("env") or {}),
        }

    # ---------- 发布 ----------
    def publish_all(self) -> list[str]:
        """把未发布/未确认的作业写到 outbox（幂等，丢失可重发）。返回写入的 job_id 列表。"""
        published = []
        with self._lock:
            items = list(self._state["unacked"].items())
        out = _outbox_dir()
        for job_id, payload in items:
            target = out / f"{job_id}.json"
            # 幂等：内容一致就跳过；不一致（文件被破坏/半写）则重写
            content = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
            if target.exists():
                try:
                    if target.read_text("utf-8") == content:
                        published.append(job_id)
                        continue
                except OSError:
                    pass
            fd, tmp = tempfile.mkstemp(dir=str(out), prefix=job_id + ".", suffix=".tmp")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    f.write(content)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, target)
                _fsync_dir(out)
                published.append(job_id)
            except BaseException:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
        return published

    # ---------- 接收确认 ----------
    def consume_receipts(self) -> int:
        """扫描 receipts/ 下的回执，把已确认作业从 unacked 移除。"""
        rd = _receipts_dir()
        acked = []
        if rd.is_dir():
            for p in rd.glob("*.json"):
                try:
                    obj = json.loads(p.read_text("utf-8"))
                except (json.JSONDecodeError, OSError):
                    continue
                if isinstance(obj, dict) and obj.get("job_id"):
                    acked.append(obj["job_id"])
        if not acked:
            return 0
        with self._lock:
            removed = 0
            for job_id in acked:
                if job_id in self._state["unacked"]:
                    del self._state["unacked"][job_id]
                    removed += 1
        if removed:
            self._save()
        return removed

    # ---------- 子类实现 ----------
    def discover(self, cursor: Any) -> tuple[Any, list[dict]]:
        raise NotImplementedError


def run_producer(producer: Producer, tick_sec: float = 60.0, replay_only: bool = False) -> int:
    """生产者主循环：重放未确认 → 循环发现/发布/消费回执。"""
    import time

    producer.publish_all()
    while True:
        try:
            producer.consume_receipts()
            cursor = producer.cursor
            new_cursor, jobs = producer.discover(cursor)
            if jobs:
                producer.advance(new_cursor, jobs)
            published = producer.publish_all()
            if published:
                print(f"[producer] published {len(published)} jobs", flush=True)
        except Exception as e:  # noqa: BLE001  生产者不因单轮异常退出
            print(f"[producer] 轮询异常：{type(e).__name__}: {e}", flush=True)
        if replay_only:
            break
        time.sleep(tick_sec)
    return 0


__all__ = ["Producer", "ProducerError", "job_id_of", "run_producer"]