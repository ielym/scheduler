"""sched Web 控制台（v3，stdlib-only，无新增依赖）。

  sched web [--host 127.0.0.1] [--port 8765] [--token XXXX]

数据面（全部只读，不干扰 serve 主循环）：
- runtime/state/snapshot.json —— serve 每轮发布的运行时快照（双时钟新鲜度校验）
- runtime/state/exec.db (WAL) —— 作业 / attempt / 事件（只读连接）

控制面（复用 CLI 与 serve 的既有 IPC 通道，设计第十四节）：
- runtime/ipc/commands.jsonl  —— cancel_job / requeue_job / cancel_attempt
- runtime/ipc/commands_ack.jsonl —— serve 回执（等待至 cmd_ack_wait_sec）
- serve 不在（单实例锁空闲）时直连数据库处理（与 CLI 相同语义）

页面为 sched/web_static/ 下的自包含静态资源（无构建步骤、无外部 CDN）。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from . import __version__, config, db as dbmod, ipc, runner, scheduler as schedmod, store

STATIC_DIR = Path(__file__).resolve().parent / "web_static"
_JOB_FIELDS = ("job_id", "task_id", "priority", "status", "reason", "queued_at",
               "started_at", "finished_at", "receive_seq", "retry_count",
               "next_retry_at", "status_seq", "idempotency_key")

# 任务动作（enable/disable）与命令 POST 的互斥：避免并发写 task.json
_task_lock = threading.Lock()


# ---------------------------------------------------------------- 工具

def _serve_alive() -> bool:
    """serve 是否持有单实例锁（flock 探测）。"""
    return ipc.serve_alive()


def _fresh_snapshot() -> dict | None:
    """读取 serve 快照并校验新鲜度（双时钟，与 CLI status 相同规则）。"""
    return ipc.fresh_snapshot()


def _ro_conn() -> sqlite3.Connection:
    """只读连接（WAL 并发读安全；serve 不在且 WAL 干净时同样可用）。"""
    try:
        conn = sqlite3.connect(f"file:{config.db_file()}?mode=ro", uri=True)
        conn.execute("SELECT 1 FROM jobs LIMIT 1")
    except sqlite3.Error:
        conn = sqlite3.connect(str(config.db_file()))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=3000")
    return conn


def _rw_conn() -> sqlite3.Connection:
    """读写连接（删除路径专用；WAL 与 serve 并发写安全，busy_timeout 兜竞争）。"""
    conn = sqlite3.connect(str(config.db_file()))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=8000")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _machine() -> dict:
    mem_total = 0
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                mem_total = int(line.split()[1]) // 1024
                break
    except (OSError, ValueError, IndexError):
        pass
    return {"cpu_count": os.cpu_count() or 1, "mem_total_mb": mem_total}


def _parse_json_cols(rows: list[dict], *cols: str) -> list[dict]:
    for r in rows:
        for c in cols:
            v = r.get(c)
            if isinstance(v, str) and v:
                try:
                    r[c] = json.loads(v)
                except json.JSONDecodeError:
                    pass
    return rows


# ---------------------------------------------------------------- API

def api_overview() -> dict:
    snap = _fresh_snapshot()
    specs = schedmod.load_all_specs()
    counts: dict[str, int] = {}
    try:
        conn = _ro_conn()
        try:
            rows = conn.execute(
                "SELECT status, COUNT(*) n FROM jobs WHERE deleted=0 GROUP BY status").fetchall()
            counts = {r["status"]: int(r["n"]) for r in rows}
        finally:
            conn.close()
    except sqlite3.Error:
        pass
    cfg = config.load_config()
    return {
        "serve_alive": _serve_alive(),
        "version": __version__,
        "timezone": cfg.timezone,
        "now": runner.now_iso(),
        "machine": _machine(),
        "snapshot": snap,
        "job_counts": counts,
        "task_specs": {tid: {"priority": s.priority, "enabled": s.enabled}
                       for tid, s in specs.items()},
    }


def api_tasks() -> dict:
    snap = _fresh_snapshot()
    specs = schedmod.load_all_specs()
    items: dict[str, dict] = {}
    for tid, spec in specs.items():
        items[tid] = {
            "task_id": tid,
            "priority": spec.priority,
            "enabled": spec.enabled,
            "workdir": spec.workdir,
            "job_priority_cap": spec.job_priority_cap,
            "recovery": spec.recovery.model_dump(),
            "producer_entry": spec.producer_entry.model_dump(),
            "executor_entry": spec.executor_entry.model_dump(),
        }
    if snap:
        for tid, mon in (snap.get("task_monitor") or {}).items():
            if tid in items:
                items[tid]["monitor"] = mon
        for tid, s in (snap.get("scale") or {}).items():
            if tid in items:
                items[tid]["scale"] = s
    # 四分法大类计数（运维口径，无论 serve 是否在线都从 DB 聚合；
    # 已删除作业不统计）——等待中 / 需处理 / 已完成
    try:
        conn = _ro_conn()
        try:
            for r in conn.execute(
                    """SELECT task_id,
                              SUM(CASE WHEN status IN ('QUEUED','BACKOFF','PAUSED')
                                  THEN 1 ELSE 0 END) waiting,
                              SUM(CASE WHEN status IN ('FAILED')
                                  THEN 1 ELSE 0 END) attention,
                              SUM(CASE WHEN status IN ('SUCCEEDED','CANCELLED')
                                  THEN 1 ELSE 0 END) finished
                       FROM jobs WHERE deleted=0 GROUP BY task_id"""):
                if r["task_id"] in items:
                    mon = items[r["task_id"]].setdefault("monitor", {})
                    mon["waiting"] = int(r["waiting"] or 0)
                    mon["attention"] = int(r["attention"] or 0)
                    mon["finished"] = int(r["finished"] or 0)
        finally:
            conn.close()
    except sqlite3.Error:
        pass
    if not snap:
        # serve 不在：数据库兜底视图（排队数 / 存活数 / task_state 目标）
        try:
            conn = _ro_conn()
            try:
                for r in conn.execute(
                        "SELECT task_id, COUNT(*) n FROM jobs WHERE status='QUEUED' "
                        "AND deleted=0 GROUP BY task_id"):
                    if r["task_id"] in items:
                        items[r["task_id"]].setdefault("monitor", {})["queued"] = int(r["n"])
                for r in conn.execute(
                        "SELECT a.job_id, j.task_id, COUNT(*) n FROM attempts a "
                        "JOIN jobs j ON a.job_id=j.job_id "
                        "WHERE a.status IN ('STARTING','RUNNING','PAUSING') "
                        "GROUP BY j.task_id"):
                    if r["task_id"] in items:
                        items[r["task_id"]].setdefault("monitor", {})["live"] = int(r["n"])
                for r in conn.execute("SELECT task_id, run_target, mode FROM task_state"):
                    if r["task_id"] in items:
                        items[r["task_id"]]["scale"] = {
                            "run_target": int(r["run_target"]), "mode": r["mode"]}
            finally:
                conn.close()
        except sqlite3.Error:
            pass
    return {"serve_alive": snap is not None, "tasks": sorted(items.values(),
            key=lambda x: (-x["priority"], x["task_id"]))}


_VALID_STATUS = {v for v in vars(dbmod.JobStatus).values() if isinstance(v, str)}
# 运维四分法（状态大类）：内部等待细分（排队/退避/暂停）全部自动恢复，
# 归"等待中"；FAILED 不自愈，归"需处理"。
_STATUS_GROUPS = {
    "RUNNING_ACTIVE": ("STARTING", "RUNNING", "PAUSING"),
    "WAITING": ("QUEUED", "BACKOFF", "PAUSED"),
    "FINISHED": ("SUCCEEDED", "CANCELLED"),
    "ATTENTION": ("FAILED",),
}


def _status_where(status: str, where: list, args: list) -> None:
    """状态过滤：接受大类名（IN 多状态）或单状态；无效值忽略。"""
    if not status:
        return
    if status in _STATUS_GROUPS:
        sts = _STATUS_GROUPS[status]
        where.append(f"status IN ({','.join('?' * len(sts))})")
        args.extend(sts)
    elif status in _VALID_STATUS:
        where.append("status=?")
        args.append(status)


def api_jobs(params: dict) -> dict:
    status = (params.get("status") or "").strip()
    task = (params.get("task") or "").strip()
    q = (params.get("q") or "").strip()
    sort = params.get("sort", "recent")
    try:
        page = max(1, int(params.get("page", 1)))
        limit = min(200, max(10, int(params.get("limit", 50))))
    except ValueError:
        page, limit = 1, 50
    where, args = ["deleted=0"], []
    _status_where(status, where, args)
    if task:
        where.append("task_id=?")
        args.append(task)
    if q:
        where.append("(job_id LIKE ? OR idempotency_key LIKE ?)")
        args.extend([f"%{q}%", f"%{q}%"])
    wsql = " WHERE " + " AND ".join(where)
    order = {"oldest": "receive_seq ASC",
             "priority": "priority DESC, receive_seq ASC",
             "priority_asc": "priority ASC, receive_seq ASC"}.get(sort, "receive_seq DESC")
    conn = _ro_conn()
    try:
        total = conn.execute(
            f"SELECT COUNT(*) n FROM jobs{wsql}", args).fetchone()["n"]
        rows = conn.execute(
            f"SELECT {', '.join(_JOB_FIELDS)} FROM jobs{wsql} "
            f"ORDER BY {order} LIMIT ? OFFSET ?",
            args + [limit, (page - 1) * limit]).fetchall()
    finally:
        conn.close()
    return {"total": int(total), "page": page, "limit": limit,
            "items": [dict(r) for r in rows]}


def api_job_detail(job_id: str) -> tuple[dict, int]:
    conn = _ro_conn()
    try:
        row = conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            return {"error": f"job not found: {job_id}"}, 404
        job = dict(row)
        if job.get("deleted"):
            return {"error": "该作业已被删除（登记保留）"}, 404
        atts = conn.execute(
            "SELECT * FROM attempts WHERE job_id=? ORDER BY seq DESC LIMIT 100",
            (job_id,)).fetchall()
    finally:
        conn.close()
    _parse_json_cols([job], "env_json", "entry_args_json", "input_refs_json")
    return {"job": job, "attempts": [dict(a) for a in atts]}, 200


def api_attempt_events(attempt_id: str, limit: int = 300) -> tuple[dict, int]:
    conn = _ro_conn()
    try:
        att = conn.execute(
            "SELECT * FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
        if att is None:
            return {"error": f"attempt not found: {attempt_id}"}, 404
        rows = conn.execute(
            "SELECT seq, kind, payload, at FROM events WHERE attempt_id=? "
            "ORDER BY seq DESC LIMIT ?", (attempt_id, int(limit))).fetchall()
    finally:
        conn.close()
    events = _parse_json_cols([dict(r) for r in rows], "payload")
    return {"attempt": dict(att), "events": events}, 200


def api_attempt_log(attempt_id: str, tail: int = 200) -> tuple[dict, int]:
    """执行日志（worker stdout/stderr 落盘于 run_dir/worker.log）——
    读末尾 tail 行（字节级截尾，容忍半行），单行超长截断。
    先经数据库核实 attempt 存在（attempt_id 均为调度器生成，防路径穿越）。"""
    try:
        conn = _ro_conn()
        try:
            row = conn.execute(
                "SELECT 1 FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
        finally:
            conn.close()
    except sqlite3.Error as e:
        return {"error": f"database error: {e}"}, 500
    if row is None:
        return {"error": f"attempt not found: {attempt_id}"}, 404
    att_dir = config.runs_root() / attempt_id
    log_path = att_dir / "worker.log"
    if not log_path.exists():
        # 老 attempt（日志落盘功能上线前启动）没有日志文件
        return {"exists": False, "lines": [],
                "note": "该 attempt 启动于日志落盘功能上线前，无 worker.log"}, 200
    try:
        size = log_path.stat().st_size
        with open(log_path, "rb") as f:
            if size > 512 * 1024:
                f.seek(-512 * 1024, os.SEEK_END)
                f.readline()  # 丢弃截断产生的半行
            raw = f.read()
    except OSError as e:
        return {"error": f"读取日志失败：{e}"}, 500
    text = raw.decode("utf-8", errors="replace")
    lines = [ln[:2000] for ln in text.splitlines()][-max(1, int(tail)):]
    return {"exists": True, "lines": lines, "size": size}, 200


def api_live_attempts(limit: int = 800) -> dict:
    snap = _fresh_snapshot()
    conn = _ro_conn()
    try:
        rows = conn.execute(
            """SELECT a.attempt_id, a.job_id, a.seq, a.status, a.pid, a.pgid,
                      a.cgroup, a.started_at, a.token,
                      j.task_id, j.priority, j.reason AS job_reason
               FROM attempts a JOIN jobs j ON a.job_id = j.job_id
               WHERE a.status IN ('STARTING','RUNNING','PAUSING')
               ORDER BY j.priority DESC, a.started_at DESC LIMIT ?""",
            (int(limit),)).fetchall()
    finally:
        conn.close()
    items = [dict(r) for r in rows]
    # 每 attempt 资源占用（调度器 cgroup 采样经快照导出；约 2s 更新一次）
    res = (snap or {}).get("attempt_resources") or {}
    for it in items:
        r = res.get(it["attempt_id"])
        if r:
            it["cpu_pct"] = r.get("cpu_pct")
            it["mem_mb"] = r.get("mem_mb")
    return {"items": items}


# ---------------------------------------------------------------- 事件流 / 指标 / 导出

_STATUS_KIND_CN = {
    "attempt_launched": "启动执行", "attempt_finalized": "执行结束",
    "recover_requeued": "重启回收重排",
    "recover_confirmed": "恢复确认成功", "producer_crash": "生产者崩溃",
    "pause_requested": "请求暂停", "pause_grace_exceeded": "暂停宽限超时",
    "protect_released": "保护解除", "protect_ended": "保护轮结束",
    "job_requeued": "作业重排", "job_cancelled": "作业取消",
    "observability_changed": "观测可用性变化",
}


def api_events_recent(limit: int = 60) -> dict:
    """调度器状态日志末尾 N 条（runtime/logs/status.jsonl，控制动作审计流）。
    尾部 256KB 定位 + 跳过截断半行，不整文件载入。"""
    limit = max(1, min(300, int(limit)))
    p = config.status_log()
    if not p.exists():
        return {"items": []}
    try:
        size = p.stat().st_size
        with open(p, "rb") as f:
            if size > 256 * 1024:
                f.seek(-256 * 1024, os.SEEK_END)
                f.readline()  # 丢弃截断半行
            raw = f.read()
    except OSError:
        return {"items": []}
    items = []
    for line in raw.decode("utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and obj.get("kind"):
            items.append(obj)
    out = []
    for obj in items[-limit:][::-1]:
        kind = str(obj.get("kind"))
        out.append({
            "ts": obj.get("ts") or "",
            "kind": kind,
            "kind_cn": _STATUS_KIND_CN.get(kind, kind),
            "task_id": obj.get("task_id") or "",
            "job_id": obj.get("job_id") or "",
            "attempt_id": obj.get("attempt_id") or "",
            "detail": obj.get("reason") or obj.get("status") or "",
        })
    return {"items": out}


_METRICS_INTERVAL_SEC = 5.0
_METRICS_ROTATE_BYTES = 16 * 1024 * 1024
_metrics_lock = threading.Lock()


def _metrics_path() -> Path:
    return config.web_metrics_file()


def _metrics_sampler() -> None:
    """后台采样线程：每 5s 从 serve 快照记录资源/队列水位（图表历史持久化）。
    CPU/IO 为累计值差分；web 重启后首个样本无速率（只记水位）。"""
    last_t = 0.0
    last_cpu = last_io = None
    while True:
        try:
            snap = _fresh_snapshot()
            if snap:
                now = time.time()
                cpu_pct = io_bps = None
                if last_t and now > last_t and last_cpu is not None:
                    dt = now - last_t
                    cpu_pct = max(0.0, (snap["exec_cpu_usec"] - last_cpu)
                                  / 1e6 / dt / max(1, os.cpu_count() or 1) * 100)
                    io_bps = max(0.0, (snap.get("exec_io_bytes", 0) - (last_io or 0)) / dt)
                last_t = now
                last_cpu = snap["exec_cpu_usec"]
                last_io = snap.get("exec_io_bytes", 0)
                # 四分法大类计数（运维口径；已删除作业不统计）
                jc = {}
                try:
                    conn = _ro_conn()
                    try:
                        for r in conn.execute(
                                "SELECT status, COUNT(*) n FROM jobs "
                                "WHERE deleted=0 GROUP BY status"):
                            jc[r["status"]] = int(r["n"])
                    finally:
                        conn.close()
                except sqlite3.Error:
                    pass
                row = {"t": round(now, 1), "cpu": round(cpu_pct, 2) if cpu_pct is not None else None,
                       "mem": snap["exec_mem_mb"], "io": round(io_bps, 1) if io_bps is not None else None,
                       "queued": snap["queued"], "live": snap["live_attempts"],
                       "jRunning": jc.get("STARTING", 0) + jc.get("RUNNING", 0) + jc.get("PAUSING", 0),
                       "jWaiting": jc.get("QUEUED", 0) + jc.get("BACKOFF", 0) + jc.get("PAUSED", 0),
                       "jFinished": jc.get("SUCCEEDED", 0) + jc.get("CANCELLED", 0),
                       "jAttention": jc.get("FAILED", 0),
                       "producers": sum(1 for p in snap.get("producers", [])
                                        if p.get("alive"))}
                with _metrics_lock:
                    try:
                        if (_metrics_path().exists()
                                and _metrics_path().stat().st_size >= _METRICS_ROTATE_BYTES):
                            _metrics_path().replace(
                                _metrics_path().with_suffix(".jsonl.1"))
                    except OSError:
                        pass
                    store.append_jsonl(_metrics_path(), row)
        except Exception:  # noqa: BLE001 — 采样线程永不退出
            pass
        time.sleep(_METRICS_INTERVAL_SEC)


def api_metrics(rng: str = "24h") -> dict:
    """图表历史（web_metrics.jsonl）。range ∈ 1h/6h/12h/24h/48h；超过 480 点降采样。"""
    spans = {"1h": 3600, "6h": 21600, "12h": 43200, "24h": 86400, "48h": 172800}
    span = spans.get(rng, 86400)
    cutoff = time.time() - span
    rows: list[dict] = []
    p = _metrics_path()
    if p.exists():
        try:
            with open(p, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(obj, dict) and obj.get("t", 0) >= cutoff:
                        rows.append(obj)
        except OSError:
            pass
    if len(rows) > 480:
        step = len(rows) / 480.0
        rows = [rows[int(i * step)] for i in range(480)]
    return {"range": rng, "points": rows}


def _wait_ack(cmd_id: str, wait_sec: float) -> dict | None:
    return ipc.wait_answers({cmd_id}, wait_sec).get(cmd_id)


# 控制台可用命令类型（serve 侧 _exec_command 必须能处理；pause_attempt 为协作
# 暂停——任务级停止用它，此前批量入口遗漏该类型导致 Web"停止任务"恒失败）
_CMD_KINDS = ("cancel_job", "requeue_job", "cancel_attempt", "pause_attempt")


def api_command_batch(kind: str, targets: list, reason: str) -> tuple[dict, int]:
    """批量控制命令：一次写入 N 条 → serve 一轮主循环批量消费 → 回执一次收齐。
    比逐条 POST 快一个数量级（50 条 ≈ 2s vs 90s+）。requeue/cancel 幂等，全量提交安全。
    """
    if kind not in _CMD_KINDS:
        return {"error": f"未知命令类型：{kind}"}, 400
    if not isinstance(targets, list) or not targets:
        return {"error": "targets 必须是非空数组"}, 400
    if len(targets) > 500:
        return {"error": "单批上限 500 条"}, 400
    if any(not isinstance(t, str) or not t for t in targets):
        return {"error": "targets 含非法项"}, 400
    reason = (reason or "").strip() or "Web 控制台批量操作"
    if _serve_alive():
        try:
            cmds = ipc.enqueue_many(kind, targets, reason)
        except OSError as e:
            return {"error": f"写入命令通道失败：{e}"}, 500
        # serve 每 loop_tick（1s）批量消费一轮；等待时长随批量规模放宽，封顶 30s
        wait = min(30.0, config.load_config().control.cmd_ack_wait_sec + len(cmds) * 0.1)
        got = ipc.wait_answers({c["id"] for c in cmds}, wait)
        results = []
        for c in cmds:
            ack = got.get(c["id"])
            if ack is None:
                results.append({"target": c["target"], "ok": False,
                                "detail": "等待 serve 确认超时"})
            else:
                results.append({"target": c["target"], "ok": bool(ack.get("ok")),
                                "detail": "操作成功" if ack.get("ok")
                                else str(ack.get("detail") or "失败")})
        ok_n = sum(1 for r in results if r["ok"])
        return {"results": results, "ok": ok_n, "fail": len(results) - ok_n}, 200
    # serve 不在：直连逐条处理（与 CLI 相同语义）；协作暂停无 serve 无法执行
    from . import __main__ as cli
    results = []
    for t in targets:
        if kind == "pause_attempt":
            results.append({"target": t, "ok": False,
                            "detail": "serve 未在线，无法协作暂停"})
            continue
        try:
            rc = cli._requeue(t, reason) if kind == "requeue_job" else cli._cancel(kind, t, reason)
            results.append({"target": t, "ok": rc == 0,
                            "detail": "操作成功" if rc == 0 else "处理失败（详见服务日志）"})
        except Exception as e:  # noqa: BLE001
            results.append({"target": t, "ok": False, "detail": f"{type(e).__name__}: {e}"})
    ok_n = sum(1 for r in results if r["ok"])
    return {"results": results, "ok": ok_n, "fail": len(results) - ok_n}, 200


def api_command(kind: str, target: str, reason: str) -> tuple[dict, int]:
    if kind not in _CMD_KINDS:
        return {"ok": False, "detail": f"未知命令类型：{kind}"}, 400
    if not target:
        return {"ok": False, "detail": "缺少 target"}, 400
    reason = (reason or "").strip() or "Web 控制台操作"
    if _serve_alive():
        wait = config.load_config().control.cmd_ack_wait_sec
        try:
            ack = ipc.send_command(kind, target, reason, wait)
        except OSError as e:
            return {"ok": False, "detail": f"写入命令通道失败：{e}"}, 500
        if ack is None:
            return {"ok": False, "detail": "等待 serve 确认超时（serve 可能刚退出）"}, 504
        return {"ok": bool(ack.get("ok")),
                "detail": "操作成功" if ack.get("ok") else str(ack.get("detail") or "失败")}, 200
    # serve 不在：直连处理（与 CLI 相同语义）；协作暂停无 serve 无法执行
    if kind == "pause_attempt":
        return {"ok": False, "detail": "serve 未在线，无法协作暂停"}, 200
    from . import __main__ as cli
    if kind == "requeue_job":
        rc = cli._requeue(target, reason)
    else:
        rc = cli._cancel(kind, target, reason)
    return {"ok": rc == 0, "detail": "操作成功" if rc == 0 else "处理失败（详见服务日志）"}, 200


def _wipe_job_files(task_id: str, job_id: str) -> int:
    """删除作业的本地运行工件：runs/<attempt>/ 目录（worker.log / 事件流 /
    许可 / 检查点文件）。归档 JSON 保留（登记；仅任务级删除清除）。"""
    import shutil
    try:
        conn = _ro_conn()
        try:
            aids = [r["attempt_id"] for r in conn.execute(
                "SELECT attempt_id FROM attempts WHERE job_id=?", (job_id,))]
        finally:
            conn.close()
    except sqlite3.Error:
        aids = []
    n = 0
    for aid in aids:
        d = config.runs_root() / aid
        if d.is_dir():
            shutil.rmtree(d, ignore_errors=True)
            n += 1
    return n


def _ensure_job_deleted_column() -> None:
    """幂等迁移：jobs 表加 deleted 标记（逻辑删除，serve 不感知、默认 0）。

    表尚不存在（全新 SCHED_HOME、serve 未初始化过）时跳过——建表由 DB() 的
    建库流程负责，此处只补列，不对缺失的表执行 ALTER。
    """
    conn = _rw_conn()
    try:
        cols = [r["name"] for r in conn.execute("PRAGMA table_info(jobs)")]
        if cols and "deleted" not in cols:
            conn.execute("ALTER TABLE jobs ADD COLUMN deleted INTEGER NOT NULL DEFAULT 0")
    finally:
        conn.close()


def api_job_delete(job_id: str) -> tuple[dict, int]:
    """删除作业：本地工件物理删除 + 数据库行逻辑删除（前端不再显示，
    登记与去重锚点保留——生产者重发该 job_id 时 receiver 去重仍有效，
    只有任务级删除才物理清表）。存活（STARTING/RUNNING/PAUSING）拒绝。"""
    conn = _ro_conn()
    try:
        row = conn.execute(
            "SELECT job_id, task_id, status, deleted FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        live = conn.execute(
            "SELECT 1 FROM attempts WHERE job_id=? AND status IN "
            "('STARTING','RUNNING','PAUSING')", (job_id,)).fetchone()
    finally:
        conn.close()
    if row is None:
        return {"ok": False, "detail": f"job not found: {job_id}"}, 404
    if row["deleted"]:
        return {"ok": True, "detail": "该作业已删除（幂等）"}, 200
    if live is not None:
        return {"ok": False, "detail": "作业仍在执行，请先取消再删除"}, 409
    n = _wipe_job_files(row["task_id"], job_id)
    conn = _rw_conn()
    try:
        conn.execute("UPDATE jobs SET deleted=1 WHERE job_id=?", (job_id,))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True, "detail": f"已删除（清理 {n} 个执行目录，登记保留不可见）"}, 200


def api_job_delete_batch(job_ids: list) -> tuple[dict, int]:
    """批量删除（≤500）：逐个处理，跳过存活项并汇报。"""
    if not isinstance(job_ids, list) or not job_ids:
        return {"ok": False, "detail": "job_ids 必须是非空数组"}, 400
    if len(job_ids) > 500:
        return {"ok": False, "detail": "单批上限 500 条"}, 400
    results = []
    for jid in job_ids:
        try:
            obj, _code = api_job_delete(str(jid))
            results.append({"target": str(jid), "ok": bool(obj.get("ok")),
                            "detail": obj.get("detail", "")})
        except Exception as e:  # noqa: BLE001
            results.append({"target": str(jid), "ok": False,
                            "detail": f"{type(e).__name__}: {e}"})
    ok_n = sum(1 for r in results if r["ok"])
    return {"results": results, "ok": ok_n, "fail": len(results) - ok_n}, 200


def _task_live_attempt_ids(task_id: str) -> list[str]:
    conn = _ro_conn()
    try:
        return [r["attempt_id"] for r in conn.execute(
            """SELECT a.attempt_id FROM attempts a JOIN jobs j ON a.job_id=j.job_id
               WHERE j.task_id=? AND a.status IN ('STARTING','RUNNING','PAUSING')""",
            (task_id,))]
    finally:
        conn.close()


def api_task_stop(task_id: str) -> tuple[dict, int]:
    """超级管理：停止任务全部存活执行（协作暂停 + 检查点保留）。作业经
    PAUSED 自动回 QUEUED——任务禁用时被准入闸门挡住（停在排队），重新
    启用后自动从检查点续跑，不会像取消那样需要人工重排。"""
    aids = _task_live_attempt_ids(task_id)
    if not aids:
        return {"ok": True, "detail": "无存活执行", "stopped": 0}, 200
    obj, code = api_command_batch("pause_attempt", aids, "任务级停止：停止全部执行（可恢复）")
    stopped = obj.get("ok", 0) if code == 200 else 0
    return {"ok": stopped == len(aids), "stopped": stopped,
            "detail": f"已协作暂停 {stopped}/{len(aids)} 个执行（检查点保留，作业回排队）"}, code


def api_task_delete(task_id: str) -> tuple[dict, int]:
    """完全删除任务：停止执行 → 终止生产者 → 删代码/工件/归档 → 清四表 +
    task_state，等同从未创建。生产者由调用方先停或在此强制终止。"""
    import shutil
    import subprocess
    td = config.task_dir(task_id)
    if not td.is_dir():
        return {"ok": False, "detail": f"任务目录不存在：{task_id}"}, 404
    # 1. 取消全部存活执行（控制通道，serve 在/不在均可）
    aids = _task_live_attempt_ids(task_id)
    if aids:
        api_command_batch("cancel_attempt", aids, "任务删除：停止全部执行")
        deadline = time.monotonic() + 25.0
        while time.monotonic() < deadline:
            if not _task_live_attempt_ids(task_id):
                break
            time.sleep(1.0)
        left = _task_live_attempt_ids(task_id)
        if left:
            return {"ok": False,
                    "detail": f"仍有 {len(left)} 个执行未退出（宽限超时），已取消删除；请稍后重试"}, 409
    # 2. 终止该任务目录下的残留进程（生产者为 serve 子进程，spec 移除后
    #    guard 不再管理 → 须按命令行特征终止；task_id 严格 [a-zA-Z0-9_-]+）
    pattern = f"tasks/{task_id}/"
    for sig in ("TERM", "KILL"):
        try:
            subprocess.run(["pkill", f"-{sig}", "-f", pattern],
                           capture_output=True, timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            pass
        if sig == "TERM":
            time.sleep(3.0)
    # 3. 文件清理：任务定义（tasks/）、任务运行态（runtime/tasks/）、归档、隔离
    removed = {"task_dir": False, "attempts": 0}
    try:
        shutil.rmtree(td, ignore_errors=True)
        removed["task_dir"] = not td.exists()
    except OSError:
        pass
    for d in (config.task_state_dir(task_id),
              config.archive_root() / task_id,
              config.isolation_root() / task_id):
        if d.is_dir():
            shutil.rmtree(d, ignore_errors=True)
    conn = _rw_conn()
    try:
        aids = [r["attempt_id"] for r in conn.execute(
            """SELECT a.attempt_id FROM attempts a JOIN jobs j ON a.job_id=j.job_id
               WHERE j.task_id=?""", (task_id,))]
    finally:
        conn.close()
    for aid in aids:
        d = config.runs_root() / aid
        if d.is_dir():
            shutil.rmtree(d, ignore_errors=True)
            removed["attempts"] += 1
    # 4. DB 清除（四表 + task_state，子查询无变量数限制）
    try:
        conn = _rw_conn()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM events WHERE attempt_id IN (SELECT attempt_id "
                         "FROM attempts WHERE job_id IN (SELECT job_id FROM jobs WHERE task_id=?))", (task_id,))
            conn.execute("DELETE FROM attempts WHERE job_id IN (SELECT job_id FROM jobs WHERE task_id=?)", (task_id,))
            conn.execute("DELETE FROM jobs WHERE task_id=?", (task_id,))
            conn.execute("DELETE FROM task_state WHERE task_id=?", (task_id,))
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
    except sqlite3.Error as e:
        return {"ok": False, "detail": f"数据库清除失败（文件已删）：{e}"}, 500
    return {"ok": True, "detail": f"任务 {task_id} 已完全删除",
            "stopped_attempts": len(aids), "removed": removed}, 200


def api_task_action(task_id: str, action: str) -> tuple[dict, int]:
    if action in ("enable", "disable"):
        return _api_task_enable_disable(task_id, action)
    if action == "stop":
        return api_task_stop(task_id)
    if action == "delete":
        return api_task_delete(task_id)
    return {"ok": False, "detail": f"未知任务动作：{action}"}, 400


def _api_task_enable_disable(task_id: str, action: str) -> tuple[dict, int]:
    with _task_lock:
        tj = config.task_dir(task_id) / "task.json"
        if not tj.exists():
            return {"ok": False, "detail": f"task.json 不存在：{task_id}"}, 404
        try:
            raw = json.loads(tj.read_text("utf-8"))
            if not isinstance(raw, dict):
                raise ValueError("顶层必须是对象")
        except (json.JSONDecodeError, OSError, UnicodeDecodeError, ValueError) as e:
            return {"ok": False, "detail": f"读取 task.json 失败：{e}"}, 400
        if action == "enable":
            try:
                config.parse_task(raw, task_id)
            except config.TaskError as e:
                return {"ok": False, "detail": f"校验失败，未启用：{e}"}, 400
        raw["enabled"] = action == "enable"
        try:
            store.save_json(tj, raw)
        except OSError as e:
            return {"ok": False, "detail": f"写入失败：{e}"}, 500
    return {"ok": True, "detail": f"{action.upper()} {task_id}"}, 200


# ---------------------------------------------------------------- HTTP

class _Handler(BaseHTTPRequestHandler):
    server_version = f"sched-web/{__version__}"
    protocol_version = "HTTP/1.1"
    token: str | None = None

    def log_message(self, fmt, *args):  # 安静模式（访问日志交给上层）
        pass

    # -- 基础 --
    def _send(self, code: int, body: bytes, ctype: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        # 全套防缓存头：no-store 挡标准浏览器，no-cache/must-revalidate/Pragma/Expires
        # 挡忽略 no-store 的劣质代理与恢复缓存（bfcache 场景由版本化 URL 兜底）
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _json(self, obj, code: int = 200) -> None:
        self._send(code, json.dumps(obj, ensure_ascii=False).encode("utf-8"),
                   "application/json; charset=utf-8")

    def _authed(self) -> bool:
        if not self.token:
            return True
        auth = self.headers.get("Authorization", "")
        if auth == f"Bearer {self.token}":
            return True
        qs = parse_qs(urlparse(self.path).query)
        return qs.get("token", [None])[0] == self.token

    # -- GET --
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = unquote(parsed.path)
        qs = parse_qs(parsed.query)
        if path == "/api/stream":
            # SSE：EventSource 无法带 Authorization 头，token 走查询参数
            if not self._authed():
                self._json({"error": "unauthorized"}, 401)
                return
            self._sse_stream()
            return
        if path.startswith("/api/"):
            if not self._authed():
                self._json({"error": "unauthorized"}, 401)
                return
            self._route_api(path, qs)
            return
        # 静态资源不鉴权（无敏感数据）——鉴权失败时登录页才能加载
        self._serve_static(path)

    def _sse_stream(self) -> None:
        """overview 推送流（每 2s 一帧，chunked 编码）。断开即返回，线程归还。"""
        self.close_connection = True
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Transfer-Encoding", "chunked")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()

            def chunk(b: bytes) -> None:
                self.wfile.write(f"{len(b):X}\r\n".encode() + b + b"\r\n")
                self.wfile.flush()

            chunk(b"retry: 5000\n\n")
            while True:
                data = json.dumps(api_overview(), ensure_ascii=False).encode("utf-8")
                chunk(b"event: overview\ndata: " + data + b"\n\n")
                time.sleep(2.0)
        except (BrokenPipeError, ConnectionResetError, OSError):
            return

    def do_POST(self) -> None:
        if not self._authed():
            self._json({"error": "unauthorized"}, 401)
            return
        parsed = urlparse(self.path)
        path = unquote(parsed.path)
        try:
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n) or b"{}") if n else {}
        except (ValueError, json.JSONDecodeError):
            self._json({"ok": False, "detail": "请求体不是合法 JSON"}, 400)
            return
        if path == "/api/command":
            obj, code = api_command(str(body.get("kind") or ""),
                                    str(body.get("target") or ""),
                                    str(body.get("reason") or ""))
            self._json(obj, code)
        elif path == "/api/command_batch":
            obj, code = api_command_batch(str(body.get("kind") or ""),
                                          body.get("targets") or [],
                                          str(body.get("reason") or ""))
            self._json(obj, code)
        elif path == "/api/task":
            obj, code = api_task_action(str(body.get("task_id") or ""),
                                        str(body.get("action") or ""))
            self._json(obj, code)
        elif path == "/api/job_delete":
            obj, code = api_job_delete(str(body.get("job_id") or ""))
            self._json(obj, code)
        elif path == "/api/job_delete_batch":
            obj, code = api_job_delete_batch(body.get("job_ids") or [])
            self._json(obj, code)
        else:
            self._json({"error": "not found"}, 404)

    def _route_api(self, path: str, qs: dict[str, list[str]]) -> None:
        try:
            if path == "/api/overview":
                self._json(api_overview())
            elif path == "/api/tasks":
                self._json(api_tasks())
            elif path == "/api/jobs":
                self._json(api_jobs({k: v[-1] for k, v in qs.items()}))
            elif path.startswith("/api/jobs/"):
                obj, code = api_job_detail(path[len("/api/jobs/"):])
                self._json(obj, code)
            elif path.startswith("/api/attempts/") and path.endswith("/events"):
                aid = path[len("/api/attempts/"):-len("/events")]
                limit = int(qs.get("limit", ["300"])[-1])
                obj, code = api_attempt_events(aid, limit)
                self._json(obj, code)
            elif path.startswith("/api/attempts/") and path.endswith("/log"):
                aid = path[len("/api/attempts/"):-len("/log")]
                tail = int(qs.get("tail", ["200"])[-1])
                obj, code = api_attempt_log(aid, tail)
                self._json(obj, code)
            elif path == "/api/attempts/live":
                self._json(api_live_attempts())
            elif path == "/api/events/recent":
                limit = int(qs.get("limit", ["60"])[-1])
                self._json(api_events_recent(limit))
            elif path == "/api/metrics":
                rng = qs.get("range", ["24h"])[-1]
                self._json(api_metrics(rng))
            elif path == "/api/health":
                self._json({"ok": True, "ts": runner.now_iso()})
            else:
                self._json({"error": "not found"}, 404)
        except sqlite3.Error as e:
            self._json({"error": f"database error: {e}"}, 500)
        except Exception as e:  # noqa: BLE001
            self._json({"error": f"{type(e).__name__}: {e}"}, 500)

    _CTYPES = {".html": "text/html; charset=utf-8", ".css": "text/css; charset=utf-8",
               ".js": "application/javascript; charset=utf-8", ".svg": "image/svg+xml",
               ".png": "image/png", ".ico": "image/x-icon", ".woff2": "font/woff2",
               ".json": "application/json; charset=utf-8"}

    def _serve_static(self, path: str) -> None:
        if path in ("/", ""):
            path = "/index.html"
        base = STATIC_DIR.resolve()
        f = (base / path.lstrip("/")).resolve()
        # is_relative_to（而非字符串前缀）——前缀比较会放行同名前缀的兄弟目录
        # （如 web_static_x 以 "web_static" 开头），构成目录穿越
        if not f.is_relative_to(base) or not f.is_file():
            self._send(404, b"not found", "text/plain; charset=utf-8")
            return
        ctype = self._CTYPES.get(f.suffix.lower(), "application/octet-stream")
        data = f.read_bytes()
        if f.suffix.lower() == ".html":
            # cache busting：按 mtime 版本化资源引用——发版后 URL 变化，
            # 浏览器/代理的任何缓存策略都不再可能拿到旧 JS
            try:
                for asset in (b"app.js", b"app.css"):
                    mtime = int((STATIC_DIR / asset.decode()).stat().st_mtime)
                    data = data.replace(asset, asset + b"?v=" + str(mtime).encode())
            except OSError:
                pass
        self._send(200, data, ctype)


def serve(host: str = "127.0.0.1", port: int = 8765, token: str | None = None) -> None:
    _Handler.token = token or os.environ.get("SCHED_WEB_TOKEN") or None
    _ensure_job_deleted_column()  # 幂等迁移：逻辑删除标记列（先于任何查询）
    sampler = threading.Thread(target=_metrics_sampler, daemon=True,
                               name="sched-web-metrics")
    sampler.start()
    httpd = ThreadingHTTPServer((host, port), _Handler)
    httpd.daemon_threads = True
    print(f"sched web serving on http://{host}:{port} "
          f"(SCHED_HOME={config.home()}, auth={'token' if _Handler.token else 'none'})",
          flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
