"""共享调度实现：每个 topic 一份 controller（emit 所有日期 intent）+ 每个日期一份 run（下载论文）。

8 个 topic 目录的 run.py / controller.py 只是对这里的薄封装。
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

# 确保能 import sched_task_sdk
_SCHED_SRC = os.environ.get("SCHED_SRC", "/mnt/data01/projects/scheduler")
if _SCHED_SRC and os.path.isdir(_SCHED_SRC) and _SCHED_SRC not in sys.path:
    sys.path.insert(0, _SCHED_SRC)

from sched_task_sdk import OneshotTask, DaemonTask, emit_intent, list_intents  # noqa: E402
from sched_task_sdk.task import run_task  # noqa: E402

# 确保能 import 共享模块
_TASKS_ROOT = Path(__file__).resolve().parent.parent
if str(_TASKS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TASKS_ROOT))

from shared.arxiv_fetcher import (  # noqa: E402
    ProxySession,
    fetch_records,
    strip_version,
    download_pdf,
    local_pdf_dir,
    already_downloaded,
)

# ---------------------------------------------------------------- 常量
TOPIC_CONFIG_FILENAME = "config.json"
MAX_INNER_RETRIES = 5       # 单篇下载在 run.py 内部的额外重试轮次
MAX_TOTAL_RETRIES = 3       # run.py 整体循环重试（所有论文跑完后仍有失败时）


# ---------------------------------------------------------------- 日志辅助
def _log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} | {msg}", flush=True)


# ---------------------------------------------------------------- 日期范围
def date_range(start: date, end: date) -> list[date]:
    """生成 [start, end] 闭区间的所有日期。"""
    out = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


# ---------------------------------------------------------------- 读 topic config
def load_topic_config() -> dict:
    """从 SCHED_CONFIG 指向的 JSON（或 WORKSPACE/config.json）读 topic 配置。"""
    ws = Path(os.environ.get("SCHED_WORKSPACE") or Path(__file__).resolve().parent)
    # 优先读 SCHED_CONFIG（调度器注入，可能指向 config.json 或 config/dates/*.json）
    cfg_path = os.environ.get("SCHED_CONFIG") or TOPIC_CONFIG_FILENAME
    # 如果 SCHED_CONFIG 指向 config/dates/*.json，那里面只有 topic 和 date
    abs_cfg = ws / cfg_path if cfg_path else ws / TOPIC_CONFIG_FILENAME
    if abs_cfg.is_file():
        cfg = json.loads(abs_cfg.read_text("utf-8"))
    else:
        # fallback 到默认 config.json
        cfg = json.loads((ws / TOPIC_CONFIG_FILENAME).read_text("utf-8"))
    # topic 级 config 可能也在 config.json 里，再尝试读一次合并
    topic_cfg_path = ws / TOPIC_CONFIG_FILENAME
    if topic_cfg_path.is_file() and topic_cfg_path != abs_cfg:
        topic_cfg = json.loads(topic_cfg_path.read_text("utf-8"))
        # 覆盖（以 dates config 里的字段为准）
        merged = {**topic_cfg, **cfg}
        cfg = merged
    return cfg


# ================================================================
# Controller：一次性 emit 所有日期 intent
# ================================================================

def request_id_for(topic: str, date_str: str) -> str:
    return f"arxiv-{topic.lower()}-{date_str}"


class TopicController(OneshotTask):
    """一次性驱动：为 2026-01-01 → 2026-09-30 的每一天 emit 一个下载 intent。

    设计要点：
    - 继承 OneshotTask（有限迭代，emit 完退出）
    - 已存在的 intent 跳过（append-only 契约）
    - 进度 checkpoint：每 emit N 个落一次，被抢占后续跑继续
    """

    def __init__(self, topic: str, **kw):
        super().__init__(name=f"arxiv_controller_{topic.lower()}", **kw)
        self.topic = topic

    def steps(self):
        cfg = load_topic_config()
        start_str = cfg.get("start_date", "2026-01-01")
        end_str = cfg.get("end_date", "2026-09-30")
        start_d = date.fromisoformat(start_str)
        end_d = date.fromisoformat(end_str)

        # 拿到已存在的 intent（append-only）
        existing = {i["request_id"] for i in list_intents()}

        all_dates = date_range(start_d, end_d)
        total = len(all_dates)
        _log(f"[{self.topic}] controller 启动：{start_str} → {end_str} 共 {total} 天，已存在 {len(existing)} 个 intent")

        # resume 点：上次 emit 到了哪一天
        resume = self.state.resume_point()
        last_emitted: str = (resume or {}).get("last_emitted", "") if resume else ""
        resume_idx = 0
        if last_emitted:
            for i, d in enumerate(all_dates):
                if d.isoformat() <= last_emitted:
                    resume_idx = i + 1

        ws = Path(os.environ.get("SCHED_WORKSPACE") or Path.cwd())
        topic_config_rel = TOPIC_CONFIG_FILENAME  # 所有日期 intent 共用 topic 级 config

        emitted, skipped = 0, 0
        for i, d in enumerate(all_dates):
            if i < resume_idx:
                continue
            date_str = d.isoformat()
            rid = request_id_for(self.topic, date_str)
            if rid in existing:
                skipped += 1
                continue

            # 每个 intent entry 覆盖成 run.py（覆盖 manifest 默认 entry=controller.py）
            emit_intent(
                request_id=rid,
                kind="oneshot",
                entry={"file": "run.py", "interpreter": "python3", "args": [], "cwd": "."},
                config=topic_config_rel,
                priority=30,
                resources={"cpu": 1.0, "memory_mb": 512},
                lifetime={
                    "completion": "terminal",
                    "retry": {
                        "max_attempts": 3,
                        "backoff_base_sec": 10,
                        "backoff_cap_sec": 600,
                        "not_after": "",
                    },
                },
            )
            emitted += 1
            # 每 10 个落一次断点 + 进度
            if emitted % 10 == 0 or i == total - 1:
                self.save_checkpoint({"last_emitted": date_str})
                self.state.enter_step(
                    f"emit intents",
                    progress=round((i + 1) / total * 100, 1),
                )
                _log(f"[{self.topic}] emit {emitted} 个（跳过 {skipped}），进度 {i+1}/{total}")

        _log(f"[{self.topic}] controller 完成：emitted={emitted}, skipped={skipped}")
        yield None  # steps 结束


def run_controller() -> int:
    cfg = load_topic_config()
    topic = cfg["topic"]
    return run_task(TopicController(topic=topic))


# ================================================================
# Run：下载单个日期的论文
# ================================================================

class DateFetcher(OneshotTask):
    """下载某一天的 arXiv 论文 PDF 到本地目录。

    断点续跑：每篇下载成功后把 source_id 写入 per-request_id 的 checkpoint 文件；
    重入时从 checkpoint 恢复，跳过已下载的（本地文件校验 %%PDF + %%EOF 双重保险）。

    ⚠️ 不使用 SDK 的 state.save_checkpoint()（它写 cache/resume_point.json，
    但同一 task 下 273 个 date intent 共享一个 resume_point.json，会互相覆盖）。
    本类改为自管理 per-request_id 的 checkpoint 文件。

    错误处理：
    - 单篇失败不终止整体 → 进入 failed_ids
    - 第一轮完后统计 → 内部重试循环（MAX_INNER_RETRIES 轮）
    - 全部循环仍失败 → 写 checkpoint + exit 101（让 sched 重新调起整个 intent）
    """

    def __init__(self, topic: str, target_date: str, **kw):
        super().__init__(name=f"arxiv_fetch_{topic.lower()}_{target_date}", **kw)
        self.topic = topic
        self.target_date = target_date
        # 每个 intent 独立 checkpoint 文件（run_id 不同 → 文件不同）
        rid = os.environ.get("SCHED_REQUEST_ID") or "no_request"
        ws = Path(os.environ.get("SCHED_WORKSPACE") or Path(__file__).resolve().parent.parent.parent)
        self._ckpt_file = ws / "cache" / f"checkpoint_{rid}.json"

    # ---------- per-request_id checkpoint ----------
    def _read_checkpoint(self) -> dict:
        try:
            if self._ckpt_file.is_file():
                return json.loads(self._ckpt_file.read_text("utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
        return {"done_ids": []}

    def _write_checkpoint(self, done_ids: list) -> None:
        try:
            self._ckpt_file.parent.mkdir(parents=True, exist_ok=True)
            self._ckpt_file.write_text(
                json.dumps({"done_ids": sorted(done_ids), "saved_at": time.time()}, ensure_ascii=False),
                encoding="utf-8",
            )
        except OSError:
            pass

    def _clear_checkpoint(self) -> None:
        try:
            self._ckpt_file.unlink()
        except OSError:
            pass

    # ---------- steps ----------
    def steps(self):
        cfg = load_topic_config()
        query = cfg["query"]
        date_compact = self.target_date.replace("-", "")
        full_query = f"{query} AND submittedDate:[{date_compact}0000 TO {date_compact}2359]"

        _log(f"[{self.topic} {self.target_date}] 启动，query={query[:80]}...")

        # 从 per-request_id checkpoint 恢复（避开共享 resume_point.json 的污染）
        ckpt = self._read_checkpoint()
        done_ids: set[str] = set(ckpt.get("done_ids", []))
        _log(f"[{self.topic} {self.target_date}] 断点恢复，已完成 {len(done_ids)} 篇")

        # 先检索所有论文
        session = ProxySession()
        try:
            records = fetch_records(session, full_query, log_fn=_log)
        except Exception as e:  # noqa: BLE001
            _log(f"[{self.topic} {self.target_date}] 检索失败：{e}")
            self.state.error("resource", f"检索失败：{e}")
            sys.exit(101)

        total = len(records)
        _log(f"[{self.topic} {self.target_date}] 命中 {total} 篇论文")
        local_dir = local_pdf_dir(self.topic, self.target_date)

        # 第一轮：逐篇下载（跳过 checkpoint + 本地已存在）
        failed_ids: set[str] = set()
        first_pass = True
        retry_round = 0

        while True:
            if first_pass:
                todo_records = records
                first_pass = False
            else:
                todo_records = [r for r in records if strip_version(r.get("source_id", "")) in failed_ids]
                retry_round += 1
                _log(f"[{self.topic} {self.target_date}] 第 {retry_round} 轮重试，剩余 {len(todo_records)} 篇")

            if not todo_records:
                break

            for i, rec in enumerate(todo_records, 1):
                source_id = (rec.get("source_id") or "").strip()
                if not source_id:
                    continue
                base_id = strip_version(source_id)
                file_id = base_id.replace("/", "_")

                if base_id in done_ids:
                    continue
                if already_downloaded(local_dir, file_id):
                    done_ids.add(base_id)
                    self._write_checkpoint(list(done_ids))
                    continue

                pdf_path = local_dir / f"{file_id}.pdf"
                self.state.enter_step(
                    f"下载 {base_id}",
                    progress=round(len(done_ids) / max(total, 1) * 100, 1),
                )
                try:
                    download_pdf(session, base_id, pdf_path, log_fn=_log)
                    done_ids.add(base_id)
                    failed_ids.discard(base_id)
                    self._write_checkpoint(list(done_ids))
                except Exception as e:  # noqa: BLE001
                    failed_ids.add(base_id)
                    _log(f"  [{file_id}] 下载失败：{e}")

            if not failed_ids:
                break

            if retry_round >= MAX_INNER_RETRIES:
                _log(f"[{self.topic} {self.target_date}] 重试 {retry_round} 轮仍有 {len(failed_ids)} 篇失败")
                break

            _log(f"[{self.topic} {self.target_date}] 本轮剩余 {len(failed_ids)} 篇失败，sleep 10s 后继续")
            time.sleep(10)

        self.state.enter_step(
            "完成",
            progress=100.0 if not failed_ids else round(len(done_ids) / max(total, 1) * 100, 1),
        )
        _log(f"[{self.topic} {self.target_date}] 完成：done={len(done_ids)}/{total}, failed={len(failed_ids)}")

        if failed_ids:
            self._write_checkpoint(list(done_ids))
            self.state.error("resource", f"{len(failed_ids)} 篇失败")
            _log(f"[{self.topic} {self.target_date}] 退出码 101，等待 sched 重试")
            sys.exit(101)

        _log(f"[{self.topic} {self.target_date}] 全部完成 ✅")
        self._clear_checkpoint()
        yield None


def run_date_fetcher() -> int:
    """run.py 入口：解析 env 里的 SCHED_REQUEST_ID（arxiv-<topic>-<date>）启动对应 fetcher。"""
    rid = os.environ.get("SCHED_REQUEST_ID", "")
    topic = date_str = ""

    if rid.startswith("arxiv-") and len(rid) > 14:
        # arxiv-<topic_lower>-<YYYY-MM-DD>
        parts = rid.split("-", 2)  # ["arxiv", "topic", "YYYY-MM-DD"]
        if len(parts) == 3:
            topic_lower = parts[1]
            date_str = parts[2]
            # 从 config.json 里还原大小写 topic
            cfg = load_topic_config()
            topic = cfg.get("topic", topic_lower)

    if not topic or not date_str:
        # fallback：从 SCHED_CONFIG 读（可能是 config/dates/*.json 里含 date 字段）
        cfg = load_topic_config()
        topic = topic or cfg.get("topic", "")
        date_str = date_str or cfg.get("date", "")

    if not topic or not date_str:
        _log(f"无法确定 topic 或 date（REQUEST_ID={rid}），退出")
        sys.exit(1)

    return run_task(DateFetcher(topic=topic, target_date=date_str))
