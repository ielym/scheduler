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


# 公共导出：业务生产者（如按日期落 JSON 记录）复用同一原子写实现（C4），
# 杜绝半写文件被调度器当成就绪请求解析。
atomic_write_json = _atomic_write_json


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
                            "unacked": obj.get("unacked") or {},
                            "conflicted": obj.get("conflicted") or {}}
            except (json.JSONDecodeError, OSError):
                pass
        return {"cursor": None, "unacked": {}, "conflicted": {}}

    def _save(self) -> None:
        with self._lock:
            _atomic_write_json(_state_file(), self._state)

    @property
    def cursor(self) -> Any:
        with self._lock:
            return self._state["cursor"]

    def advance(self, new_cursor: Any, jobs: list[dict]) -> None:
        """持久化游标与待发布记录（同一原子写）。jobs 每项含 event_id/business_version 与
        正式的就绪 JSON 载荷字段；可选 subdir 指定 outbox 分目录（大规模任务，
        设计第二节 L23"可以按发布批次或文件名散列分目录"）。

        先构造新状态并落盘，成功后才替换内存游标：持久化失败时内存与磁盘都停留在
        旧游标，下轮从旧位置重扫，事件不丢失（游标先推进会把未发布记录永久跳过）。
        """
        with self._lock:
            task_id = _task_id()
            new_state = {"cursor": self._state["cursor"],
                         "unacked": dict(self._state["unacked"]),
                         "conflicted": dict(self._state.get("conflicted") or {})}
            for j in jobs:
                event_id = str(j.get("event_id") or "")
                business_version = str(j.get("business_version") or "")
                job_id = j.get("job_id") or job_id_of(task_id, event_id, business_version)
                new_state["unacked"][job_id] = self._ready_payload(job_id, j)
            new_state["cursor"] = new_cursor
            _atomic_write_json(_state_file(), new_state)
            self._state = new_state

    def _ready_payload(self, job_id: str, spec: dict) -> dict:
        payload = {
            "schema_version": 1,
            "job_id": job_id,
            "task_id": _task_id(),
            "entry_args": list(spec.get("entry_args") or []),
            "workdir": spec.get("workdir") or ".",
            "input_refs": list(spec.get("input_refs") or []),
            # 幂等键兜底（审查修复，设计第四节 L63"每份就绪 JSON 包含……幂等键"）：
            # 业务未显式提供时以稳定事件标识兜底（同一事件重放得到同一键），
            # 保证幂等键全链路非空——接收端对空幂等键的请求按无效载荷隔离。
            "idempotency_key": str(spec.get("idempotency_key")
                                   or spec.get("event_id") or job_id),
            "progress_semver": spec.get("progress_semver") or "1.0.0",
            "env": dict(spec.get("env") or {}),
        }
        # 作业级优先级（可选，覆盖任务默认值）
        pri = spec.get("priority")
        if pri is not None:
            payload["priority"] = int(pri)
        # outbox 分目录（可选，协议外字段：不进入就绪 JSON 本体——接收端
        # extra=forbid 会拒绝未定义字段；由 publish_all 发布时定位使用）
        subdir = str(spec.get("subdir") or "").strip().strip("/")
        if subdir and ".." not in Path(subdir).parts and not subdir.startswith("/"):
            payload["__outbox_subdir__"] = subdir
        return payload

    # ---------- 发布 ----------
    def publish_all(self) -> list[str]:
        """把未发布/未确认的作业写到 outbox（幂等，丢失可重发）。返回写入的 job_id 列表。

        审查修复：支持 outbox 分目录（payload 的 __outbox_subdir__ 协议外字段
        指定相对子目录，发布时剥离该字段——正式 JSON 仍是纯协议载荷，接收端
        递归扫描分目录，设计第二节 L23 / 第五节 L83）。
        """
        published = []
        with self._lock:
            items = list(self._state["unacked"].items())
        out = _outbox_dir()
        for job_id, payload in items:
            # 协议外字段剥离在副本上进行（不修改 unacked 内存/磁盘记录），
            # 重发时仍能定位同一分目录
            subdir = ""
            body = payload
            if isinstance(payload, dict) and "__outbox_subdir__" in payload:
                body = {k: v for k, v in payload.items()
                        if k != "__outbox_subdir__"}
                subdir = str(payload["__outbox_subdir__"] or "").strip("/")
            target_dir = out / subdir if subdir else out
            target_dir.mkdir(parents=True, exist_ok=True)
            target = target_dir / f"{job_id}.json"
            # 幂等：内容一致就跳过；不一致（文件被破坏/半写）则重写
            content = json.dumps(body, ensure_ascii=False, indent=2, sort_keys=True)
            if target.exists():
                try:
                    if target.read_text("utf-8") == content:
                        published.append(job_id)
                        continue
                except OSError:
                    pass
            fd, tmp = tempfile.mkstemp(dir=str(target_dir),
                                       prefix=job_id + ".", suffix=".tmp")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    f.write(content)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, target)
                _fsync_dir(target_dir)
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
        """扫描 receipts/ 下的回执，按状态处理未确认作业。

        - received：接收方明确确认 → 从 unacked 移除（同时清掉 conflicted 隔离账）；
        - conflict / invalid：请求被隔离（内容冲突/非法载荷）→ 从 unacked 移入
          conflicted 桶并大声告警（C1：此前按"未确认"永远重放，隔离事实上失效、
          冲突文件每轮重发刷屏；现在隔离可见、不再重复发布，留待人工处理）。

        C1：回执是一次性确认凭证——处理账目成功落盘（_save）后删除回执文件；
        不在 unacked 中的孤儿回执（重复确认/账目已迁移）同样删除，防止回执
        目录无限累积拖慢每轮 O(N) 扫描。_save 失败时异常冒泡、回执保留，
        下轮重试（不丢确认）。删除失败忽略，下轮重试。
        """
        rd = _receipts_dir()
        acked: list[str] = []
        conflicts: list[tuple[str, str]] = []
        files: list[Path] = []
        if rd.is_dir():
            for p in rd.glob("*.json"):
                files.append(p)
                try:
                    obj = json.loads(p.read_text("utf-8"))
                except (json.JSONDecodeError, OSError):
                    continue
                if not (isinstance(obj, dict) and obj.get("job_id")):
                    continue
                status = str(obj.get("status") or "")
                if status == "received":
                    acked.append(str(obj["job_id"]))
                elif status in ("conflict", "invalid"):
                    conflicts.append((str(obj["job_id"]), status))
        if not files:
            return 0
        with self._lock:
            removed = 0
            changed = False
            # 先备份再变更：_save 落盘失败时回滚内存账目，保证"下轮重试"仍能
            # 检测到变更并重新落盘（否则内存已改、重试轮 changed=False 会直接
            # 删回执，确认在重启前永远到不了磁盘）
            backup = (dict(self._state["unacked"]), dict(self._state["conflicted"]))
            try:
                for job_id in acked:
                    if job_id in self._state["unacked"]:
                        del self._state["unacked"][job_id]
                        removed += 1
                        changed = True
                    if self._state["conflicted"].pop(job_id, None) is not None:
                        changed = True
                for job_id, status in conflicts:
                    payload = self._state["unacked"].pop(job_id, None)
                    if payload is not None:
                        self._state["conflicted"][job_id] = payload
                        changed = True
                        print(f"[producer] 回执 {status}：作业 {job_id} 已被接收方隔离"
                              f"（内容冲突/非法载荷），停止重发，等待人工处理", flush=True)
                if changed:
                    self._save()  # 账目先落盘；失败异常冒泡，本轮不删回执
            except BaseException:
                self._state["unacked"], self._state["conflicted"] = backup
                raise
        # 账目已持久化（或无需变更）→ 删除本轮全部回执（孤儿/损坏的一并清理）
        for p in files:
            try:
                os.unlink(p)
            except OSError:
                pass
        return removed

    # ---------- 子类实现 ----------
    def discover(self, cursor: Any) -> tuple[Any, list[dict]]:
        raise NotImplementedError


def _acquire_instance_lock():
    """任务级单实例锁：生产者进程自身持有 flock（设计文档第三节）。

    锁被其他存活进程持有时返回 None（已有生产者实例）；正常持锁返回 fd，
    进程退出时由 OS 自动释放，供新调度器/新生产者接管。
    """
    import fcntl
    p = _state_dir() / "producer.lock"
    try:
        fd = os.open(str(p), os.O_CREAT | os.O_RDWR, 0o644)
    except OSError:
        return None
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        return None
    try:
        os.ftruncate(fd, 0)
        start_time = os.stat(f"/proc/{os.getpid()}").st_ctime
        os.write(fd, f"{os.getpid()} {start_time}".encode())
    except OSError:
        pass
    return fd


def run_producer(producer: Producer, tick_sec: float = 60.0, replay_only: bool = False) -> int:
    """生产者主循环：任务级单实例锁 → 重放未确认 → 循环发现/发布/消费回执。"""
    import fcntl
    import time

    lock_fd = _acquire_instance_lock()
    if lock_fd is None:
        print("[producer] 任务级单实例锁被持有，已有生产者实例在运行，退出", flush=True)
        return 0
    try:
        producer.publish_all()
        while True:
            try:
                producer.consume_receipts()
                cursor = producer.cursor
                new_cursor, jobs = producer.discover(cursor)
                if jobs or new_cursor != cursor:
                    # 空事件轮次游标前进同样要持久化，否则每轮重扫
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
    finally:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)
        except OSError:
            pass


__all__ = ["Producer", "ProducerError", "job_id_of", "run_producer",
           "atomic_write_json"]