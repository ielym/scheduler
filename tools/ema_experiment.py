#!/usr/bin/env python3
"""EMA 退避闸网格实验驱动器（实验分支专用，无人值守）。

对每组 step_* 配置：改 config.json → 重启 scheduler.service → 静置 SETTLE 秒
→ 采集 WINDOW 秒内的指标 → 记录一行 JSON。全部跑完恢复初始配置并重启。

优化目标（用户口径）：
- 总吞吐 ↑           → thru_per_min（成功的日期作业/分钟）
- 不频繁启停         → fin_per_min / launch_per_min / 退避动作次数
- 不误杀老作业       → long_paused（寿命 ≥120s 却被 PAUSED/BACKOFF 收尾的 attempt 数）
- 强杀应保持 0       → kill_grace（pause_grace_exceeded 次数）

用法：
  python3 tools/ema_experiment.py [--settle 150] [--window 510] [--out /tmp/ema_results.jsonl]
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

HOME = Path("/mnt/data01/projects/scheduler")
CONFIG = HOME / "config.json"
DB = HOME / "runtime" / "state" / "exec.db"
STATUS = HOME / "runtime" / "logs" / "status.jsonl"

# (名称, config.json 覆盖项)。第一组是"恒判偏离"的对照组 = 改动前行为。
CONFIGS: list[tuple[str, dict]] = [
    ("ctrl_always_backoff",
     {"step_deviation_k": 0.0}),
    ("median50_trim10_recMed10_k3",
     {"step_baseline_method": "median", "step_baseline_samples": 50,
      "step_baseline_trim_ratio": 0.1, "step_current_method": "recent_median",
      "step_current_samples": 10, "step_deviation_k": 3.0}),
    ("median30_trim25_recMed5_k2",
     {"step_baseline_method": "median", "step_baseline_samples": 30,
      "step_baseline_trim_ratio": 0.25, "step_current_method": "recent_median",
      "step_current_samples": 5, "step_deviation_k": 2.0}),
    ("slowEma_a02_fastEma_a3_k3",
     {"step_baseline_method": "slow_ema", "step_baseline_ema_alpha": 0.02,
      "step_current_method": "fast_ema", "step_current_alpha": 0.3,
      "step_deviation_k": 3.0}),
]


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def load_cfg() -> dict:
    return json.loads(CONFIG.read_text("utf-8"))


def save_cfg(obj: dict) -> None:
    # 预检：写盘前先用真实 schema 校验，避免"写进去但服务读成无效→回落默认值"
    # 这种静默失效（配置额外字段受 extra=forbid 约束，写错层级就会被整体拒绝）。
    sys.path.insert(0, str(HOME))
    from sched import config as _cfg            # noqa: PLC0415
    _cfg.SchedulerConfig.model_validate(obj)    # 不合法直接抛错，绝不静默
    CONFIG.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def restart() -> None:
    subprocess.run(["systemctl", "restart", "scheduler.service"], check=False)


def _parse(ts: str) -> datetime | None:
    try:
        d = datetime.fromisoformat(ts)
    except (ValueError, TypeError):
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def _status_counts(t0: datetime, t1: datetime) -> dict:
    c: dict[str, int] = {}
    try:
        for line in STATUS.read_text("utf-8", errors="replace").splitlines():
            if not line.strip():
                continue
            try:
                o = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = _parse(o.get("ts") or "")
            if ts is None or not (t0 <= ts < t1):
                continue
            k = o.get("kind") or ""
            c[k] = c.get(k, 0) + 1
    except OSError:
        pass
    return c


def evaluate(t0: datetime, t1: datetime) -> dict:
    mins = max(1e-6, (t1 - t0).total_seconds() / 60.0)
    a0 = t0.isoformat(timespec="seconds")
    a1 = t1.isoformat(timespec="seconds")
    res: dict = {}
    try:
        db = sqlite3.connect(str(DB))
        db.row_factory = sqlite3.Row
        rows = db.execute(
            "SELECT status, started_at, finished_at FROM attempts "
            "WHERE finished_at >= ? AND finished_at < ?", (a0, a1)).fetchall()
        by: dict[str, int] = {}
        long_paused = 0
        for r in rows:
            by[r["status"]] = by.get(r["status"], 0) + 1
            s, f = _parse(r["started_at"]), _parse(r["finished_at"])
            if s and f and (f - s).total_seconds() >= 120 and \
                    r["status"] in ("PAUSED", "BACKOFF"):
                long_paused += 1
        live = db.execute(
            "SELECT started_at FROM attempts WHERE status IN "
            "('STARTING','RUNNING','PAUSING')").fetchall()
        now = datetime.now(timezone.utc)
        ages = [(now - _parse(r["started_at"])).total_seconds() / 60.0
                for r in live if _parse(r["started_at"])]
        db.close()
        res["finalized"] = len(rows)
        res["succeeded"] = by.get("SUCCEEDED", 0)
        res["backoff75"] = by.get("BACKOFF", 0)
        res["paused"] = by.get("PAUSED", 0)
        res["long_paused"] = long_paused
        res["max_live_age_min"] = round(max(ages), 1) if ages else 0.0
        res["live_now"] = len(live)
    except sqlite3.Error as e:
        res["db_error"] = str(e)
    sc = _status_counts(t0, t1)
    for k in ("attempt_launched", "pause_grace_exceeded", "task_backoff_step",
              "task_backoff_empty", "backoff_deviated", "backoff_suppressed",
              "protect_started", "protect_released"):
        res[k] = sc.get(k, 0)
    res["thru_per_min"] = round(res.get("succeeded", 0) / mins, 3)
    res["fin_per_min"] = round(res.get("finalized", 0) / mins, 2)
    res["launch_per_min"] = round(res.get("attempt_launched", 0) / mins, 2)
    res["ratio75"] = round(res.get("backoff75", 0) / max(1, res.get("finalized", 0)), 3)
    return res


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--settle", type=float, default=150.0)
    ap.add_argument("--window", type=float, default=510.0)
    ap.add_argument("--out", default="/tmp/ema_results.jsonl")
    args = ap.parse_args()

    base = load_cfg()
    out_lines = []
    try:
        for name, over in CONFIGS:
            cfg = load_cfg()
            cfg.setdefault("control", {}).update(over)   # 覆盖项必须落在 control 段内
            save_cfg(cfg)                                # 内含 schema 预检，非法即抛错
            log(f"=== {name}: 应用配置 {over} ===")
            restart()
            log(f"静置 {args.settle:.0f}s（冷启动重排+爬坡）...")
            time.sleep(args.settle)
            t0 = datetime.now(timezone.utc)
            log(f"采集窗口 {args.window:.0f}s ...")
            time.sleep(args.window)
            t1 = datetime.now(timezone.utc)
            res = evaluate(t0, t1)
            row = {"config": name, "overrides": over,
                   "window_sec": round((t1 - t0).total_seconds()),
                   **res}
            out_lines.append(row)
            with open(args.out, "a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
            log(f"  结果: {json.dumps(res, ensure_ascii=False)}")
    finally:
        log("恢复初始配置并重启")
        save_cfg(base)
        restart()
    log("=== 汇总 ===")
    for r in out_lines:
        log(json.dumps(r, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
