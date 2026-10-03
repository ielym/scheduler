"""文件存储唯一入口：flock 排他锁 + 原子替换写（独立复刻，不依赖现有 scheduler）。

所有对 settings.json / jobs.json / *.jsonl / 运行时快照的读写都必须走这里。
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


def save_json(path: Path, obj: Any) -> None:
    """写临时文件 → fsync → os.replace，读者永远看不到半截文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with file_lock(path):
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


def append_jsonl(path: Path, obj: Any) -> None:
    """追加一行 JSON。O_APPEND 单次写原子，再加 flock 兜跨进程。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n"
    with file_lock(path):
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


def write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with file_lock(path):
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


def read_text(path: Path, default: str = "") -> str:
    if not path.exists():
        return default
    try:
        return path.read_text("utf-8")
    except (OSError, UnicodeDecodeError):
        return default