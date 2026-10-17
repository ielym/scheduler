"""CLI / Web 与 serve 之间的共享 IPC 工具（设计第十四节）。

此前 `sched/__main__.py` 与 `sched/web.py` 各自复刻了同一套：单实例锁存活探测、
快照新鲜度校验、控制命令写入与回执等待。此处统一为唯一实现，两处复用。

约定：
- serve 每轮把运行时快照写入 runtime/snapshot.json，读取方用双时钟（monotonic +
  墙钟）校验新鲜度；
- 控制命令写入 runtime/commands.jsonl，serve 消费后回执 commands_ack.jsonl；
- serve 不在（单实例锁空闲）时由调用方降级直连数据库处理。
"""

from __future__ import annotations

import fcntl
import os
import time
from datetime import datetime, timedelta, timezone

from . import config, runner, store

SNAPSHOT_FRESH_SEC = 10.0


def serve_alive() -> bool:
    """serve 是否持有单实例锁（flock 探测）。"""
    p = config.lock_file()
    if not p.exists():
        return False
    try:
        fd = os.open(str(p), os.O_RDWR)
    except OSError:
        return False
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, fcntl.LOCK_UN)
            return False
        except OSError:
            return True
    finally:
        os.close(fd)


def fresh_snapshot() -> dict | None:
    """读取 serve 快照并校验新鲜度（双时钟，防重启后误报 serve=alive）。

    单调时钟只在同一次开机内可比：系统重启后 monotonic 归零，旧快照的差值
    为大负数、永不过阈值，`sched status` 会一直显示重启前的 serve=alive。
    故同时校验墙钟 ts——serve 每 loop_tick 秒写快照，ts 超过 SNAPSHOT_FRESH_SEC
    即为陈旧或来自上一次开机（设计第十四节：时钟跳变保守核实）。
    """
    snap = store.load_json(config.snapshot_file(), default=None)
    if not isinstance(snap, dict):
        return None
    mono = snap.get("mono")
    if not isinstance(mono, (int, float)):
        return None
    if time.monotonic() - mono > SNAPSHOT_FRESH_SEC:
        return None
    ts = snap.get("ts")
    if not isinstance(ts, str):
        return None
    try:
        t = datetime.fromisoformat(ts)
    except ValueError:
        return None
    if t.tzinfo is None:
        t = t.astimezone()
    if datetime.now(timezone.utc) - t > timedelta(seconds=SNAPSHOT_FRESH_SEC):
        return None
    return snap


def enqueue(kind: str, target: str, reason: str) -> dict:
    """写入一条控制命令，返回命令对象（含 id）。"""
    cmd = {"id": runner.new_id("cmd"), "kind": kind, "target": target,
           "reason": reason, "ts": runner.now_iso()}
    store.append_jsonl(config.commands_file(), cmd)
    return cmd


def enqueue_many(kind: str, targets: list[str], reason: str) -> list[dict]:
    """批量写入控制命令，返回命令对象列表。"""
    cmds = [{"id": runner.new_id("cmd"), "kind": kind, "target": t,
             "reason": reason, "ts": runner.now_iso()} for t in targets]
    for c in cmds:
        store.append_jsonl(config.commands_file(), c)
    return cmds


def wait_answers(ids: set[str], wait_sec: float) -> dict[str, dict]:
    """等待给定命令 id 的回执，返回 {id: ack}；超时未到的缺失。"""
    got: dict[str, dict] = {}
    if not ids:
        return got
    deadline = time.monotonic() + max(0.0, wait_sec)
    while time.monotonic() < deadline and len(got) < len(ids):
        for ack in store.read_jsonl(config.commands_ack_file()):
            cid = ack.get("id")
            if cid in ids:
                got[cid] = ack
        if len(got) < len(ids):
            time.sleep(0.15)
    return got


def send_command(kind: str, target: str, reason: str,
                 wait_sec: float) -> dict | None:
    """写控制命令并等待 serve 回执；超时返回 None。"""
    cmd = enqueue(kind, target, reason)
    return wait_answers({cmd["id"]}, wait_sec).get(cmd["id"])


__all__ = ["SNAPSHOT_FRESH_SEC", "serve_alive", "fresh_snapshot", "enqueue",
           "enqueue_many", "wait_answers", "send_command"]
