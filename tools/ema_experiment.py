#!/usr/bin/env python3
"""EMA 退避闸网格实验驱动器（实验分支专用，无人值守）。

对每组 step_* 配置：**热更新** config.json（scheduler 按 mtime 1s 内生效，不重启）
→ 静置 SETTLE 秒 → 采集 WINDOW 秒内的指标 → 记录一行 JSON。支持 --rounds 交替采样。
全部跑完热更新恢复初始配置。

为什么必须避免重启：重启会清空未终结 attempt/事件、把全部作业重新入队，并触发
全量"重扫 OSS"风暴（实测重启后 progress 事件量暴涨约百倍），测量窗口被严重污染。

优化目标（用户口径）：
- 总吞吐 ↑           → thru_per_min（成功的日期作业/分钟）
- 不频繁启停         → fin_per_min / launch_per_min / 退避动作次数
- 不误杀老作业       → long_paused（寿命 ≥120s 却被 PAUSED/BACKOFF 收尾的 attempt 数）
- 强杀应保持 0       → kill_grace（pause_grace_exceeded 次数）

用法：
  python3 tools/ema_experiment.py [--settle 60] [--window 420] [--rounds 4]
                                 [--grid grid.json] [--out /tmp/ema_results.jsonl]
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

HOME = Path("/mnt/data01/projects/scheduler")
CONFIG = HOME / "config.json"
DB = HOME / "runtime" / "state" / "exec.db"
STATUS = HOME / "runtime" / "logs" / "status.jsonl"

# (名称, config.json 覆盖项)。ctrl 是"恒判偏离"对照组（= 闸门前的行为）。
# 下面三组是交替采样（--rounds）下验证过的组合；落选的中位数变体已移除。
CONFIGS: list[tuple[str, dict]] = [
    ("ctrl_K0",
     {"step_deviation_k": 0.0}),
    ("gate_K3",
     {"step_baseline_method": "slow_ema", "step_baseline_ema_alpha": 0.02,
      "step_current_method": "fast_ema", "step_current_alpha": 0.3,
      "step_deviation_k": 3.0}),
    ("gate_K2",
     {"step_baseline_method": "slow_ema", "step_baseline_ema_alpha": 0.02,
      "step_current_method": "fast_ema", "step_current_alpha": 0.3,
      "step_deviation_k": 2.0}),
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
    CONFIG.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n",
                      encoding="utf-8")


def _wait_reload(since_epoch: float, timeout: float = 30.0) -> bool:
    """等 status.jsonl 出现 gate_reloaded 事件（loop_tick=1s，约 1-2s 生效）。

    用状态审计流而非日志：服务未配置 logging handler（root=WARNING），info 级
    不回显；status.jsonl 才是可靠的可观测通道。
    """
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            for line in STATUS.read_text("utf-8", errors="replace").splitlines()[-200:]:
                if not line.strip():
                    continue
                try:
                    o = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if o.get("kind") == "gate_reloaded":
                    ts = _parse(o.get("ts") or "")
                    if ts is None or ts.timestamp() >= since_epoch - 5:
                        return True
        except OSError:
            pass
        time.sleep(1.0)
    return False


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
        # 真实工作量（论文级 progress 事件数）比 "attempt 数" 更能代表吞吐：
        # 每个 attempt 处理的论文数差异极大（1 篇 vs 上百篇），用 attempt 数
        # 计数会被"这批作业恰好内容多少"淹没，这正是前两轮噪声巨大的主因。
        papers = db.execute(
            "SELECT COUNT(*) FROM events WHERE kind='progress' "
            "AND at >= ? AND at < ?", (a0, a1)).fetchone()[0]
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
        res["papers"] = int(papers)
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
              "protect_started", "protect_released",
              "congestion_shrink", "admit_budget_shrink"):
        res[k] = sc.get(k, 0)
    res["papers_per_min"] = round(res.get("papers", 0) / mins, 2)
    res["thru_per_min"] = round(res.get("succeeded", 0) / mins, 3)
    res["fin_per_min"] = round(res.get("finalized", 0) / mins, 2)
    res["launch_per_min"] = round(res.get("attempt_launched", 0) / mins, 2)
    res["ratio75"] = round(res.get("backoff75", 0) / max(1, res.get("finalized", 0)), 3)
    res["exit75_rate"] = round(
        res.get("backoff75", 0) / max(1, res.get("backoff75", 0) + res.get("succeeded", 0)), 3)
    return res


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--settle", type=float, default=60.0)
    ap.add_argument("--window", type=float, default=420.0)
    ap.add_argument("--out", default="/tmp/ema_results.jsonl")
    ap.add_argument("--grid", default=None,
                    help="自定义网格 JSON：[{\"name\":..,\"overrides\":{..}}, ..]；缺省用内置 CONFIGS")
    ap.add_argument("--rounds", type=int, default=1,
                    help="轮数：按网格顺序循环 N 轮（交替采样，抵消时间漂移与冷启动偏置）")
    args = ap.parse_args()

    grid = CONFIGS
    if args.grid:
        obj = json.loads(Path(args.grid).read_text("utf-8"))
        grid = [(g["name"], g["overrides"]) for g in obj]

    base = load_cfg()
    out_lines = []
    try:
        for rnd in range(1, max(1, args.rounds) + 1):
            for name, over in grid:
                cfg = load_cfg()
                cfg.setdefault("control", {}).update(over)   # 覆盖项必须落在 control 段内
                log(f"=== [第{rnd}轮] {name}: 热更新配置 {over} ===")
                t_write = time.time()
                save_cfg(cfg)                                # 内含 schema 预检，非法即抛错
                ok = _wait_reload(t_write)
                log(f"  热更新{'已确认' if ok else '未确认（继续，留意日志）'}")
                log(f"静置 {args.settle:.0f}s ...")
                time.sleep(args.settle)
                t0 = datetime.now(timezone.utc)
                log(f"采集窗口 {args.window:.0f}s ...")
                time.sleep(args.window)
                t1 = datetime.now(timezone.utc)
                res = evaluate(t0, t1)
                row = {"round": rnd, "config": name, "overrides": over,
                       "window_sec": round((t1 - t0).total_seconds()),
                       **res}
                out_lines.append(row)
                with open(args.out, "a", encoding="utf-8") as f:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
                log(f"  结果: {json.dumps(res, ensure_ascii=False)}")
    finally:
        log("恢复初始配置（热更新，无需重启）")
        save_cfg(base)
        _wait_reload(time.time())
    log("=== 汇总（按组聚合 papers/min 中位数，抗漂移） ===")
    import statistics as _st
    by_cfg: dict[str, list[dict]] = {}
    for r in out_lines:
        by_cfg.setdefault(r["config"], []).append(r)
    for name, rs in by_cfg.items():
        med = lambda key: _st.median(x.get(key, 0) for x in rs)  # noqa: E731
        log(f"{name}: papers/min 中位={med('papers_per_min')}  "
            f"thru/min 中位={med('thru_per_min')}  "
            f"exit75率 中位={med('exit75_rate')}  "
            f"paused 中位={med('paused')}  "
            f"suppressed 中位={med('backoff_suppressed')}")
    for r in out_lines:
        log(json.dumps(r, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
