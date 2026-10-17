"""文件存储唯一入口：flock 排他锁 + 原子替换写。

所有对 config.json / task.json / snapshot.json / *.jsonl（事件、状态、控制命令）
及运行时 JSON 的读写都必须走这里。v3 的持久化状态以 SQLite（exec.db）为主。
"""

from __future__ import annotations

import fcntl
import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


@contextmanager
def file_lock(path: Path, exclusive: bool = True) -> Iterator[None]:
    """对 path 加 flock（锁挂在 .lock 旁路文件上，保证原子替换期间锁不丢）。"""
    lock_path = path.with_name(path.name + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with file_lock(path, exclusive=False):
        try:
            return json.loads(path.read_text("utf-8"))
        except (json.JSONDecodeError, OSError, UnicodeDecodeError):
            return default


def atomic_write_json(path: Path, obj: Any) -> None:
    """写临时文件 → fsync → os.replace，读者永远看不到半截文件。

    不持锁：并发安全由调用方保证（save_json 持 flock；run_dir 下的控制/许可
    文件按 attempt 独占写入，无需跨进程锁）。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=path.name + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def save_json(path: Path, obj: Any) -> None:
    """持有 flock 的原子 JSON 写（多进程可能并发写同一文件的场景）。"""
    with file_lock(path):
        atomic_write_json(path, obj)


# ---------- jsonl 尺寸轮转（防止 decisions/runs 审计日志无限增长打爆磁盘） ----------
ROTATE_MAX_BYTES = 16 * 1024 * 1024   # 单档上限 16MB
ROTATE_GENERATIONS = 3                # 保留 .1/.2/.3 共 3 个历史档


def _rotate(path: Path) -> None:
    """把 path 轮转为 path.1（旧档依次后移，超出代数即丢弃）。"""
    for i in range(ROTATE_GENERATIONS - 1, 0, -1):
        src = path.with_name(f"{path.name}.{i}")
        if src.exists():
            src.replace(path.with_name(f"{path.name}.{i + 1}"))
    path.replace(path.with_name(f"{path.name}.1"))


def append_jsonl(path: Path, obj: Any, max_bytes: int | None = None) -> None:
    """追加一行 JSON。O_APPEND 单次写原子，再加 flock 兜跨进程。

    超过 max_bytes（默认 ROTATE_MAX_BYTES）先轮转再追加，磁盘占用有界。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n"
    limit = ROTATE_MAX_BYTES if max_bytes is None else max_bytes
    with file_lock(path):
        try:
            if limit > 0 and path.exists() and path.stat().st_size >= limit:
                _rotate(path)
        except OSError:
            pass
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())


def iter_jsonl(path: Path) -> Iterator[dict]:
    """逐行解析，跳过损坏行。"""
    if not path.exists():
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                yield obj


def read_jsonl(path: Path) -> list[dict]:
    return list(iter_jsonl(path))


def _atomic_write_unlocked(path: Path, text: str) -> None:
    """临时文件 + fsync + os.replace（假定调用方已持有 file_lock）。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=path.name + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def remove_jsonl(path: Path, should_remove) -> int:
    """在单次排他锁内「读 → 过滤 → 原子重写」jsonl，避免与 append_jsonl 竞争丢新行。

    返回移除条数。损坏行原样保留（宁可保留也不静默丢审计）。
    """
    removed = 0
    with file_lock(path):
        if not path.exists():
            return 0
        kept_lines: list[str] = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                try:
                    obj = json.loads(s)
                except json.JSONDecodeError:
                    kept_lines.append(line)
                    continue
                if isinstance(obj, dict) and should_remove(obj):
                    removed += 1
                    continue
                kept_lines.append(line)
        _atomic_write_unlocked(path, "".join(kept_lines))
    return removed