"""内嵌管理台（FastAPI）。

- 只允许在 serve 进程内启动（start_in_thread），所有写操作调同一个 SchedulerService。
- 三个 tab：全局仪表盘 / 子任务 / 进程管理。
- 进程管理覆盖全部历史状态（running / waiting / done / failed / success ...）。
"""

from __future__ import annotations

import hmac
import ipaddress
import json
import threading
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from pydantic import BaseModel

import logging

from . import config, manifest, persist, registry, runlog, store, scanner

log = logging.getLogger("sched.web")


def is_loopback(host: str) -> bool:
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return host in ("localhost",)


def validate_public_listen(cfg) -> None:
    if not cfg.enabled or is_loopback(cfg.host):
        return
    if cfg.auth_token:
        return
    if cfg.allow_public_no_auth:
        return
    raise RuntimeError(
        f"管理台 host={cfg.host} 非回环且 auth_token 为空：拒绝启动。"
        f"请在 data/settings.json 配置 web.auth_token 或显式 allow_public_no_auth=true。"
    )


STATIC_DIR = Path(__file__).resolve().parent / "web_static"


class ClearAllBody(BaseModel):
    delete_requests: bool = False


def start_in_thread(svc) -> None:
    cfg = config.load_settings().web
    if not cfg.enabled:
        return
    validate_public_listen(cfg)

    app = FastAPI(title="sched")
    app.state.svc = svc

    @app.middleware("http")
    async def _auth(req: Request, call_next):
        if cfg.auth_token:
            t = req.headers.get("Authorization", "")
            if not hmac.compare_digest(t, cfg.auth_token):
                return JSONResponse({"error": "unauthorized"}, status_code=401)
        return await call_next(req)

    @app.get("/api/health")
    def health(_req: Request):
        return {"ok": True, "started_at": svc.started_at}

    @app.get("/api/status")
    def status(_req: Request):
        svc.refresh()
        cap = svc.capacity
        st = svc.status()
        st["subtasks_list"] = svc.list_subtasks()
        st["cpu_total"] = cap.total_cpu
        st["mem_total_mb"] = cap.total_memory_mb
        # 真实值（monitor 通过 cgroup 采集）—— 优先用真实值，没有时才用 reserve 估计
        st["actual_cpu"] = svc.ledger.actual_cpu()
        st["actual_mem_mb"] = svc.ledger.actual_mem_mb()
        return st

    @app.get("/api/subtasks")
    def subtasks(_req: Request, page: int | None = Query(None, ge=1),
                 page_size: int | None = Query(None, ge=1, le=200)):
        svc.refresh()
        items = svc.list_subtasks()
        if page is None and page_size is None:
            return items
        ps = page_size or 20
        p = page or 1
        total = len(items)
        start = (p - 1) * ps
        return {"total": total, "page": p, "page_size": ps,
                "items": items[start:start + ps]}

    @app.get("/api/subtasks/{name}")
    def subtask_detail(name: str, _req: Request):
        svc.refresh()
        e = svc.entries.get(name)
        if e is None:
            raise HTTPException(404, f"not registered: {name}")
        doc = e.doc
        return {
            "subtask_id": name,
            "name": doc.display_name() if doc else name,
            "run_mode": doc.run_mode if doc else "?",
            "entry": doc.entry.model_dump() if doc else {},
            "priority": doc.priority if doc else 0,
            "resources": doc.resources.model_dump() if doc else {},
            "heartbeat": doc.heartbeat.model_dump() if doc else {},
            "runtime": doc.runtime.model_dump() if doc else {},
            "enabled": e.enabled,
            "ok": e.ok,
            "error": e.error,
        }

    # ---------- enable / disable 子任务 ----------
    @app.post("/api/start/{name}")
    def start(name: str, _req: Request):
        """启用子任务：设 enabled=True + 重入队 pending 意图 + 空任务自动引导。

        任务启用状态（registry.enabled）与意图状态（intents_state）解耦：
        启用时只把 pending 意图重新入队，不触碰 done/running 等其他状态。
        空任务（requests/ 无意图文件）的引导写入由调度器 _tick_scan 统一兜底，
        这里显式调用 bootstrap 保证立即生效。
        """
        from . import registry as _reg
        st = _reg.load_state()
        if name not in st:
            raise HTTPException(404, "not registered")
        _reg.upsert_state(name, {"enabled": True})
        svc.refresh()

        # 1) 把该子任务所有 pending 意图重新入队（去重：已在 WQ 的跳过）
        intents = scanner.load_intents_state()
        wq_rids = {it.get("request_id") for it in persist.load_scheduler_state().wq
                   if it.get("subtask_id") == name}
        requeued = 0
        if name in intents:
            for rid, info in intents[name].items():
                if info.get("status") != "pending" or rid in wq_rids:
                    continue
                try:
                    svc._enqueue_intent(name, rid, trigger="resume", resume_ref=True)
                    requeued += 1
                except Exception:
                    log.warning("requeue pending intent %s/%s failed", name, rid)

        # 2) 空任务引导（复用 service.bootstrap_subtask，与 _tick_scan 同源）
        boot = svc.bootstrap_subtask(name)

        return {"ok": True, "subtask_id": name,
                "requeued": requeued, "bootstrapped": boot.get("bootstrapped", False)}

    @app.post("/api/stop/{name}")
    def stop(name: str, _req: Request):
        """禁用子任务：设 enabled=False + kill 运行中进程 + 清 WQ + running→pending。

        任务启用状态与意图状态解耦：禁用不把意图标 stopped，只把 running 回退 pending。
        pending 意图保留，下次启用时重新入队；done 意图不受影响。
        """
        from . import registry as _reg
        # 1) 关闭 registry 开关
        st = _reg.load_state()
        if name not in st:
            raise HTTPException(404, "not registered")
        _reg.upsert_state(name, {"enabled": False})

        # 2) 立即 kill 所有属于该 subtask 的运行中进程
        killed = []
        with svc._lock:
            run_ids = [rid for rid, re in svc.running.items()
                       if re.run.get("subtask_id") == name]
        for rid in run_ids:
            try:
                svc.stop_run(rid, reason=f"子任务 {name} 被禁用")
                killed.append(rid)
            except KeyError:
                pass

        # 3) 清除 WQ 中属于该 subtask 的项（改内存态 self.state，否则 _tick_admit 仍会准出）
        with svc._lock:
            before = len(svc.state.wq)
            svc.state.wq = [r for r in svc.state.wq if r.get("subtask_id") != name]
            removed_wq = before - len(svc.state.wq)
            persist.save_scheduler_state(svc.state)

        # 4) running 意图回退 pending（进程已被杀，不再是 running）
        #    不标 stopped：意图生命周期与任务启用状态解耦
        with scanner.STATE_LOCK:
            intents = scanner.load_intents_state()
            reset_pending = 0
            if name in intents:
                for info in intents[name].values():
                    if info.get("status") == "running":
                        info["status"] = "pending"
                        info["run_id"] = ""
                        reset_pending += 1
                if reset_pending:
                    scanner.save_intents_state(intents)

        svc.refresh()
        return {"ok": True, "subtask_id": name,
                "killed_running": killed, "removed_wq": removed_wq,
                "reset_pending": reset_pending}

    @app.post("/api/subtasks/{name}/priority")
    def set_priority(name: str, body: dict, _req: Request):
        """修改子任务 manifest.json 的 priority（立即生效，新 intent 入队用新值）。"""
        new_pri = int(body.get("priority", 0))
        if not (0 <= new_pri <= 100):
            raise HTTPException(400, "priority 需在 0~100 之间")
        e = svc.entries.get(name)
        if e is None:
            raise HTTPException(404, f"not registered: {name}")
        mpath = e.task_dir / "manifest.json"
        try:
            m = json.loads(mpath.read_text("utf-8"))
        except Exception as ex:
            raise HTTPException(500, f"读 manifest 失败: {ex}") from ex
        m["priority"] = new_pri
        mpath.write_text(json.dumps(m, ensure_ascii=False, indent=2), encoding="utf-8")
        svc.refresh()
        return {"ok": True, "subtask_id": name, "priority": new_pri}

    @app.delete("/api/subtasks/{name}")
    def delete_subtask(name: str, _req: Request, delete_dir: bool = Query(False)):
        """删除子任务：默认只从调度器移除记录；delete_dir=true 连本地任务目录一起删。"""
        svc.refresh()
        if name not in svc.entries:
            raise HTTPException(404, f"not registered: {name}")
        return svc.delete_subtask(name, delete_dir=delete_dir)

    @app.post("/api/admin/clear")
    def clear_all(body: ClearAllBody, _req: Request):
        """清空全部调度状态：停所有进程、清注册表/WQ/意图/运行历史；
        delete_requests=true 时同时清空所有子任务 requests/ 意图文件（保留任务代码）。"""
        return svc.clear_all(delete_requests=body.delete_requests)

    # ---------- 进程控制 ----------
    @app.post("/api/process/stop/{run_id}")
    def stop_run(run_id: str, _req: Request):
        # 1) 运行中 → cancel
        try:
            return svc.stop_run(run_id, reason="web 手动停止")
        except KeyError:
            pass
        # 2) 等待中（WQ）→ 出队取消排队：进程从未拉起，直接移除队列项即可
        with svc._lock:
            wq = svc.state.wq
            kept = [r for r in wq if r.get("run_id") != run_id]
            removed = len(wq) - len(kept)
            if removed:
                svc.state.wq = kept
                persist.save_scheduler_state(svc.state)
                return {"ok": True, "run_id": run_id, "dequeued": True,
                        "removed_wq": removed}
        raise HTTPException(404, f"run not running or waiting: {run_id}")

    @app.delete("/api/process/{run_id}")
    def delete_run(run_id: str, _req: Request, delete_files: bool = Query(False)):
        """删除单个进程：勾选 delete_files 才删日志/缓存；否则只停止并从界面移除，不影响任务/其他进程。"""
        return svc.delete_run(run_id, delete_files=delete_files)

    # ---------- 进程管理（全部状态 + 历史） ----------
    @app.get("/api/processes")
    def processes(_req: Request, subtask_id: str | None = None,
                  status: str | None = None,
                  page: int = Query(1, ge=1),
                  page_size: int = Query(50, ge=1, le=200)):
        """返回全部进程（running / waiting / done / failed / success / stopped）。

        只返回有 run_id 的条目——intent 还没被 scheduler 拉起的（pending、stopped-无runid）不算进程。
        status 参数支持逗号分隔多值，且 done 和 success 被视为同一个筛选值 "done"。
        """
        merged = []

        # 1) 运行中
        for r in persist.load_active():
            merged.append({
                "run_id": r.get("run_id"),
                "subtask_id": r.get("subtask_id"),
                "priority": r.get("priority"),
                "pid": r.get("pid"),
                "status": "running",
                "started_at": r.get("started_at"),
                "enqueued_at": None,
                "finished_at": None,
            })
        # 2) WQ 中等待
        wq_rows = []
        for r in persist.load_scheduler_state().wq:
            wq_rows.append({
                "run_id": r.get("run_id"),
                "subtask_id": r.get("subtask_id"),
                "priority": r.get("priority"),
                "pid": None,
                "status": "waiting",
                "started_at": None,
                "enqueued_at": r.get("enqueued_at"),
                "finished_at": None,
            })
        merged.extend(wq_rows)
        # 运行中/等待中已由 active/WQ 展示，intents 侧同 run_id 跳过，避免重复行
        covered_rids = {x["run_id"] for x in merged if x["run_id"]}
        # 3) intents_state 里所有状态的历史
        intents = scanner.load_intents_state()
        # 预索引 run log mtime（避免每条都 stat 目录）
        _run_log_mtime_cache: dict[tuple[str, str], str | None] = {}

        def _guess_started_at(sid: str, run_id: str) -> str | None:
            """fallback：从 data/runs/{sid}/{run_id}.stdout.log 的 mtime 推算 started_at（ISO）。"""
            if not run_id:
                return None
            key = (sid, run_id)
            if key in _run_log_mtime_cache:
                return _run_log_mtime_cache[key]
            run_root = config.runs_dir() / sid / f"{run_id}.stdout.log"
            result = None
            try:
                import datetime as _dt
                mtime = run_root.stat().st_mtime
                # 转 Asia/Shanghai（与 intents_state 其他时间串保持一致）
                try:
                    from zoneinfo import ZoneInfo
                    result = _dt.datetime.fromtimestamp(mtime, tz=_dt.timezone.utc).astimezone(ZoneInfo("Asia/Shanghai")).isoformat()
                except Exception:
                    # tzdata 缺失 → +08:00 手动近似
                    dt_utc = _dt.datetime.fromtimestamp(mtime, tz=_dt.timezone.utc)
                    result = (dt_utc + _dt.timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%S+08:00")
            except OSError:
                pass
            _run_log_mtime_cache[key] = result
            return result

        for sid, rid_map in intents.items():
            if subtask_id and sid != subtask_id:
                continue
            for rid, info in rid_map.items():
                st = info.get("status", "unknown")
                if st == "pending":
                    continue
                run_id = info.get("run_id") or ""
                if not run_id or run_id in covered_rids:
                    continue  # intent 从未被 scheduler 拉起，或已由 active/WQ 展示 → 跳过
                first_seen = info.get("first_seen") or None
                started = first_seen or _guess_started_at(sid, run_id)
                merged.append({
                    "run_id": run_id,
                    "subtask_id": sid,
                    "priority": None,
                    "pid": None,
                    "status": st,
                    "started_at": started,
                    "enqueued_at": first_seen,
                    "finished_at": info.get("finished_at") or None,
                })

        # 4) 过滤：多值 status 支持（逗号分隔），done+success 归一
        _STATUS_ALIAS = {"done": {"done", "success"}}
        if status:
            want = set(s.strip() for s in status.split(","))
            expanded: set[str] = set()
            for w in want:
                expanded |= _STATUS_ALIAS.get(w, {w})
            merged = [x for x in merged if x["status"] in expanded]
        if subtask_id:
            merged = [x for x in merged if x["subtask_id"] == subtask_id]

        # 5) 排序：状态优先级（running > waiting > 其他）然后 started_at 降序
        _PRIO = {"running": 0, "waiting": 1}
        merged.sort(key=lambda x: (
            _PRIO.get(x["status"], 2),
            x.get("started_at") or x.get("enqueued_at") or "",
        ), reverse=True)

        total = len(merged)
        start = (page - 1) * page_size
        return {"total": total, "page": page, "page_size": page_size,
                "items": merged[start:start + page_size]}

    # ---------- 进程日志 ----------
    @app.get("/api/runs/{run_id}/log")
    def run_log(run_id: str, _req: Request,
                stream: str = Query("stdout"),
                max_bytes: int = Query(2_000_000, ge=1024, le=20_000_000)):
        """返回 run 的 stdout 或 stderr 日志最后 max_bytes 字节（tail 语义）。

        stream=stdout|stderr 分开返回。遍历所有 subtask 目录找
        {run_id}.stdout.log / {run_id}.stderr.log。找不到时返回 404。
        """
        if stream not in ("stdout", "stderr"):
            raise HTTPException(400, "stream 需为 stdout 或 stderr")
        ext = ".stdout.log" if stream == "stdout" else ".stderr.log"
        run_root = config.runs_dir()
        if not run_root.is_dir():
            raise HTTPException(404, "no runs directory yet")

        found: Path | None = None
        for sub in run_root.iterdir():
            if not sub.is_dir():
                continue
            cand = sub / f"{run_id}{ext}"
            if cand.is_file():
                found = cand
                break

        if found is None:
            raise HTTPException(404, f"no log file for run_id={run_id}")

        size = found.stat().st_size
        offset = max(0, size - max_bytes)
        with open(found, "rb") as f:
            f.seek(offset)
            data = f.read()
        # 尽力解码（含少量非法 UTF-8 就替换）
        text = data.decode("utf-8", errors="replace")
        return PlainTextResponse(text, headers={
            "X-Run-Log-Path": str(found),
            "X-Run-Log-Total-Bytes": str(size),
            "X-Run-Log-Range": f"{offset}-{size}",
        })

    # ---------- 遗留端点（保留兼容） ----------
    @app.get("/api/intents")
    def intents(_req: Request, subtask_id: str | None = None,
                status: str | None = None,
                page: int | None = Query(None, ge=1),
                page_size: int | None = Query(None, ge=1, le=200)):
        raw = scanner.load_intents_state()
        if subtask_id is None and status is None and page is None and page_size is None:
            return raw
        flat = []
        for sid, states in raw.items():
            if subtask_id and sid != subtask_id:
                continue
            for rid, st in states.items():
                if status and st.get("status") != status:
                    continue
                flat.append({"subtask_id": sid, "request_id": rid, **st})
        ps = page_size or 50
        p = page or 1
        total = len(flat)
        start = (p - 1) * ps
        return {"total": total, "page": p, "page_size": ps,
                "items": flat[start:start + ps]}

    @app.get("/api/active")
    def active(_req: Request):
        return persist.load_active()

    @app.get("/api/wq")
    def wq(_req: Request):
        return persist.load_scheduler_state().wq

    @app.get("/api/discover")
    def discover(_req: Request):
        """列出 tasks/ 下所有含 manifest.json 的本地任务目录，标注是否已注册。

        供前端"注册本地任务"使用：清空/删除后任务目录仍存在，需要入口重新注册。
        """
        svc.refresh()
        registered = set(svc.entries)
        tdir = config.tasks_dir()
        out = []
        if tdir.is_dir():
            for sub in sorted(tdir.iterdir()):
                if not sub.is_dir() or sub.name.startswith("."):
                    continue
                if not (sub / "manifest.json").is_file():
                    continue
                item = {"subtask_id": sub.name, "registered": sub.name in registered}
                try:
                    item["run_mode"] = manifest.load_manifest(sub).run_mode
                except manifest.ManifestError:
                    item["run_mode"] = "?"
                out.append(item)
        return out

    @app.post("/api/register/{name}")
    def register(name: str, _req: Request):
        try:
            result = svc.register_subtask(name)
        except manifest.ManifestError as e:
            raise HTTPException(400, "; ".join(e.errors)) from e
        return result

    # ---------- 静态页面 ----------
    @app.get("/")
    def index(_req: Request):
        return FileResponse(str(STATIC_DIR / "index.html"))

    @app.get("/subtask.html")
    def subtask_page(_req: Request):
        return FileResponse(str(STATIC_DIR / "subtask.html"))

    import uvicorn

    def _serve():
        uvicorn.run(app, host=cfg.host, port=cfg.port, log_level="warning")

    svc._web_thread = threading.Thread(target=_serve, name="sched-web", daemon=True)
    svc._web_thread.start()


def stop_in_thread(svc) -> None:
    pass
