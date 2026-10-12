"""检查点存储（worker 侧）。

调度器不理解业务检查点内容；SDK 只负责原子读写与提交编号递增。
检查点结构（设计文档第七节）：
  {format_version, input_ref, business_version, progress, commit_seq, checksum, data}

写入：临时文件 + fsync + os.replace，保证调度器永远读不到半写检查点。
checksum（B4）：业务数据规范化 JSON 的 sha256 前 32 位——读回校验不一致
（磁盘损坏/半写被外部替换）时丢弃断点并大声告警，宁可从零重放也不带病恢复。
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import threading
from pathlib import Path
from typing import Any


def checkpoint_file() -> Path:
    v = os.environ.get("SCHED_CHECKPOINT_FILE")
    if not v:
        raise RuntimeError("SCHED_CHECKPOINT_FILE 未设置（非调度器启动？）")
    return Path(v)


def _data_checksum(data: Any) -> str:
    blob = json.dumps(data, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


def _fsync_dir(d: Path) -> None:
    """目录条目 fsync（C2）：让 os.replace 后的目录项落盘，防断电丢文件名。"""
    try:
        fd = os.open(str(d), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


class CheckpointStore:
    def __init__(self):
        self._lock = threading.Lock()
        self._commit_seq = 0
        self._loaded = False
        self._current: dict | None = None

    def load(self) -> dict | None:
        """进程启动时读取断点（恢复最近提交点）；checksum 不一致按损坏丢弃。"""
        with self._lock:
            if self._loaded:
                return self._current
            self._loaded = True
            p = checkpoint_file()
            self._current = None
            self._commit_seq = 0
            if p.exists():
                try:
                    obj = json.loads(p.read_text("utf-8"))
                except (json.JSONDecodeError, OSError, ValueError):
                    obj = None
                if isinstance(obj, dict):
                    self._current = obj
                    self._commit_seq = int(obj.get("commit_seq") or 0)
                    ck = str(obj.get("checksum") or "")
                    if ck and ck != _data_checksum(obj.get("data")):
                        print(f"[sched-sdk] 检查点 checksum 不一致，按损坏丢弃：{p}",
                              file=sys.stderr, flush=True)
                        self._current = None
                        self._commit_seq = 0
            return self._current

    @property
    def last(self) -> dict | None:
        """最近一次已知检查点（未 load 过时先读盘）。"""
        if not self._loaded:
            self.load()
        return self._current

    @property
    def commit_seq(self) -> int:
        return self._commit_seq

    def save(self, data: Any, *, input_ref: str = "", business_version: str = "",
             progress: Any = None, format_version: int = 1) -> dict:
        """原子提交一个检查点，提交编号递增。返回完整检查点对象。"""
        with self._lock:
            self._commit_seq += 1
            cp = {
                "format_version": format_version,
                "input_ref": input_ref or _env_str("SCHED_JOB_ID"),
                "business_version": business_version,
                "progress": progress,
                "commit_seq": self._commit_seq,
                "checksum": _data_checksum(data),
                "data": data,
                "saved_at": _now_iso(),
            }
            p = checkpoint_file()
            p.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=str(p.parent), prefix=p.name + ".", suffix=".tmp")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(cp, f, ensure_ascii=False, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, p)
                # C2：目录条目也落盘——os.replace 只保证文件数据持久，目录项
                # 不 fsync 时断电可能丢失刚 rename 的检查点文件名
                _fsync_dir(p.parent)
            except BaseException:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
            self._current = cp
            return cp


def _now_iso() -> str:
    from datetime import datetime
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _env_str(k: str) -> str:
    return os.environ.get(k) or ""


__all__ = ["CheckpointStore", "checkpoint_file"]