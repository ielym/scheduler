"""em_calendar_backfill 执行者：单个市场/资产键的日历（含交易时段）全量回溯。

- 单元 = 一个市场/资产键（每市场一个作业）。
- 存储：`calendar/market=<key>/date=<D>/data.parquet`（D = 市场**当地自然日**），每份 = 截至 D 的全量快照。
- 列：`date` + `is_trade_day` + `tz_local`(IANA) + `sessions`(list<struct>：当地/北京双墙钟)。
- 交易日按**交易所当地日期**键控；sessions 由 `emlib/sessions.py` 规则表构造（DST/夜盘/跨日见其文档）。
- 流程：取候选交易日 → 按「交易日集合相同」归并 → 逐自然日写快照（已存在跳过 → 断点续跑）
  → 写 `_meta/<key>.v<VERSION>.meta.json` → complete。
- 完整性校验：非空、严格递增、无重复；不过不落盘。
- 网络纪律：默认走隧道；资源错误 R3（2×3s）→ exit 75；不设预防性限速（R1）。
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time
from pathlib import Path

_SCHED_SRC = os.environ.get("SCHED_SRC", "/mnt/data01/projects/scheduler")
if _SCHED_SRC and os.path.isdir(_SCHED_SRC) and _SCHED_SRC not in sys.path:
    sys.path.insert(0, _SCHED_SRC)

from sched_task_sdk import ExecutorTask, run_task  # noqa: E402

from emlib import crawler, oss, calendar_util, sessions  # noqa: E402
from emlib.version import VERSION  # noqa: E402

# 进展上报节流（每 N 个分区上报一次，避免事件刷屏；仍单调递增、反映真实进展）
_PROGRESS_EVERY = 100


def _log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | {msg}", flush=True)


def _work_dir() -> Path:
    v = os.environ.get("SCHED_ATTEMPT_WORK_DIR")
    return Path(v) if v else Path("/tmp/sched_em_calendar")


def _load_config() -> dict:
    rel = os.environ.get("SCHED_CONFIG") or ""
    ws = Path(os.environ.get("SCHED_WORKSPACE") or Path.cwd())
    p = ws / rel if rel else (ws / "config.json")
    if p.is_file():
        return json.loads(p.read_text("utf-8"))
    return {}


def _meta_obj(key: str) -> str:
    return oss.obj(f"_meta/{key}.v{VERSION}.meta.json")


def _atomic_put(local: Path, final_obj: str, tmp_dir: str, pause_check) -> None:
    """临时对象上传 → copy 到最终分区 → 删临时对象（原子；失败不落最终对象）。"""
    tmp_obj = f"{tmp_dir}/{local.name}"
    oss.upload(str(local), tmp_dir, pause_check=pause_check)
    oss.copy(tmp_obj, final_obj, pause_check=pause_check)
    oss.remove(tmp_obj, pause_check=pause_check)


class CalendarBackfillExecutor(ExecutorTask):
    def execute(self):
        cfg = _load_config()
        key = str(cfg.get("key") or "").strip()
        targets = cfg.get("targets") or []
        start_date = str(cfg.get("start_date") or "1990-01-01")
        if not key or not targets:
            raise RuntimeError(f"配置缺失 key/targets：{cfg}")
        _log(f"[{key}] 启动：{len(targets)} 个候选标的（schema v{VERSION}）")

        # 幂等：已存在本版本 `_meta` ⇒ 整个市场已完成
        self.check_pause()
        if oss.exists(_meta_obj(key), pause_check=self.check_pause):
            _log(f"[{key}] 已完成（_meta v{VERSION} 存在），跳过")
            self.complete({"key": key, "skipped": "already_done"})
            return

        # 逐候选取交易日（每个候选一次全历史 K 线）
        by_set: dict[frozenset, list[dict]] = {}
        for t in targets:
            self.check_pause()
            secid = str(t.get("secid") or "").strip()
            if not secid:
                continue
            days = crawler.trading_days(secid, pause_check=self.check_pause)
            if not days:
                _log(f"[{key}] 标的 {secid} 无 K 线，跳过（业务事实）")
                continue
            _validate_days(key, secid, days)
            by_set.setdefault(frozenset(days), []).append(t)
            _log(f"[{key}] {secid} 交易日 {len(days)} 天（{days[0]}..{days[-1]}）")

        scratch = _work_dir() / key
        scratch.mkdir(parents=True, exist_ok=True)

        if not by_set:
            # 全部候选无 K 线：业务事实（R6）。写 no-data 完成标记并 complete。
            meta = {"key": key, "schema_version": VERSION, "markets": [],
                    "reason": "no_kline_data",
                    "targets": [str(t.get("secid")) for t in targets],
                    "done_at": time.strftime("%Y-%m-%dT%H:%M:%S")}
            mp = scratch / f"{key}.meta.json"
            mp.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
            oss.upload(str(mp), oss.obj("_meta"), pause_check=self.check_pause)
            _log(f"[{key}] 全部候选无 K 线，标记 no-data 完成")
            self.complete({"key": key, "files": 0, "reason": "no_kline_data"})
            return

        markets: list[dict] = []
        sources: set[str] = set()
        total_parts = 0
        for dayset, ts in by_set.items():
            days = sorted(dayset)                      # ['YYYY-MM-DD', ...] 升序
            day_objs = [dt.date.fromisoformat(x) for x in days]
            tset = set(days)
            mkt = key if len(by_set) == 1 else f"{key}__{_rep_code(ts)}"
            earliest, latest = days[0], days[-1]
            start = start_date if start_date and start_date < earliest else earliest
            tz_local = sessions.market_tz(key)
            tz_tok = sessions.tz_token(key)

            # 预构造「交易日 → sessions」（夜盘需完整交易日集合做 prev_trade 锚定）
            sessions_by_day: dict[str, list[dict]] = {}
            for x in days:
                _tz, sess, src = sessions.sessions_for(key, dt.date.fromisoformat(x), day_objs)
                sessions_by_day[x] = sess
                sources.add(src)
            _log(f"[{key}] 市场目录 market={mkt}（date=<D>_{tz_tok}）：{earliest}..{latest}（起点 {start}，tz={tz_local}）")

            # 逐自然日写「截至当日」的全量快照分区（acc 即快照，无需切片）
            acc: list[dict] = []
            written = 0
            for x in calendar_util.natural_days(start, latest):
                self.check_pause()
                acc.append({"date": x, "is_trade_day": x in tset, "tz_local": tz_local,
                            "sessions": sessions_by_day.get(x, [])})
                final_obj = oss.obj(f"market={mkt}/date={x}_{tz_tok}/data.parquet")
                if oss.exists(final_obj, pause_check=self.check_pause):
                    continue  # 分区已存在 → 幂等跳过（断点续跑）
                local = scratch / f"{mkt}__{x}.parquet"
                calendar_util.write_parquet(acc, local)
                _atomic_put(local, final_obj, oss.obj(f"_tmp/{mkt}"), self.check_pause)
                written += 1
                total_parts += 1
                if written % _PROGRESS_EVERY == 0:
                    self.report_progress(total_parts)
                    _log(f"[{key}] {mkt} 已写 {written} 分区（截至 {x}）")

            markets.append({"market": mkt, "targets": [t["secid"] for t in ts],
                            "tz_local": tz_local,
                            "trade_days": len(days), "date_min": earliest, "date_max": latest,
                            "partitions_written": written})
            self.report_progress(total_parts)

        # 完成标记（幂等真源，随版本命名）
        meta = {"key": key, "schema_version": VERSION, "markets": markets,
                "sessions_source": sorted(sources),
                "partitions": total_parts, "done_at": time.strftime("%Y-%m-%dT%H:%M:%S")}
        mp = scratch / f"{key}.meta.json"
        mp.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        oss.upload(str(mp), oss.obj("_meta"), pause_check=self.check_pause)
        _log(f"[{key}] 完成：{len(markets)} 个市场目录，{total_parts} 个分区")
        self.complete({"key": key, "markets": len(markets), "partitions": total_parts})


def _rep_code(ts: list[dict]) -> str:
    return min((str(t.get("code") or t.get("secid") or "x").lower()) for t in ts)


def _validate_days(key: str, secid: str, days: list[str]) -> None:
    """完整性校验：非空、严格递增、无重复。"""
    if not days:
        raise RuntimeError(f"{key}/{secid} 交易日为空")
    if any(days[i] >= days[i + 1] for i in range(len(days) - 1)):
        raise RuntimeError(f"{key}/{secid} 交易日序列非严格递增/含重复：疑似残缺")


def main() -> int:
    return run_task(CalendarBackfillExecutor(name="em_calendar_backfill"))


if __name__ == "__main__":
    sys.exit(main())
