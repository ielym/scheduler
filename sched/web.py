"""内嵌管理台（FastAPI）。

设计约束（与 sched 技能的 H1 一致）：
- Web 层只允许在 serve 进程内启动（start_in_thread），所有写操作直接调用同一个
  SchedulerService 实例。禁止另起 Web 进程：WQ/账本是 serve 的内存态，
  跨进程写 scheduler_state.json 会被 serve 下一次落盘覆盖。
- 上线检查（五道闸）由调度器自动完成：tasks/ 出现新任务目录即后台检查，
  通过自动纳入调度；失败在页面展示原因，由创建者本地修改任务文件后自动重查，
  页面无注册按钮、无定时自动重试。
- 管理台不再提供任务文件查看/编辑：任务必须先在本地开发验证到生产级，再按目录名导入。
- 「立即运行一次」只入队（service.trigger），绝不直接拉起任务进程。
"""

from __future__ import annotations

import base64
import hmac
import ipaddress
import threading
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from . import config, persist, registry, runlog, store, taskdoc


def is_loopback(host: str) -> bool:
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return host in ("localhost",)


def validate_public_listen(cfg) -> None:
    """绑非回环地址前的强制鉴权检查（不满足直接拒启动，不静默裸奔）。"""
    if not cfg.enabled or is_loopback(cfg.host):
        return
    if cfg.auth_token:
        return
    if cfg.allow_public_no_auth:
        return
    raise RuntimeError(
        f"管理台 host={cfg.host} 为非回环地址且 web.auth_token 为空：拒绝启动。\n"
        "管理台可手动触发/停止任务执行。二选一：\n"
        "1) 在 data/settings.json 配置 web.auth_token（推荐）；\n"
        "2) 确需无密码公网暴露，显式设置 web.allow_public_no_auth=true（风险自负）。")

STATIC_DIR = Path(__file__).resolve().parent / "web_static"


# 请求/响应模型必须定义在模块级：create_app 内的局部类配合延迟注解会让 FastAPI 无法解析类型。
class TriggerResp(BaseModel):
    run_id: str
    job_id: str
    priority: int


class EnabledReq(BaseModel):
    enabled: bool


class ImportReq(BaseModel):
    dir_name: str


class StopReq(BaseModel):
    reason: str | None = None


def _running_snapshot(svc) -> list[dict]:
    out = []
    for re in list(svc.running.values()):
        r = re.run
        out.append({
            "run_id": r.get("run_id"), "job_id": r.get("job_id"), "pid": r.get("pid"),
            "trigger": r.get("trigger"), "attempt": r.get("attempt"),
            "started_at": r.get("started_at"), "est_cpu": re.est_cpu, "est_mem_mb": re.est_mem_mb,
            "manual": bool(r.get("manual")),
            "live_cpu_cores": round(re.live_cpu_cores, 3) if re.live_cpu_cores > 0 else None,
            "live_mem_mb": re.live_mem_mb or None,
        })
    return out


# ---------------- App 工厂 ----------------

def create_app(svc) -> FastAPI:
    app = FastAPI(title="sched 管理台", docs_url="/api/docs", version="1")

    auth_token = svc.settings.web.auth_token

    if auth_token:
        @app.middleware("http")
        async def basic_auth(request: Request, call_next):
            # HTTP Basic：用户名任意，仅对密码做常量时间比较
            header = request.headers.get("Authorization", "")
            ok = False
            if header.startswith("Basic "):
                try:
                    decoded = base64.b64decode(header[6:], validate=True).decode("utf-8")
                    _, _, pw = decoded.partition(":")
                    ok = hmac.compare_digest(pw, auth_token)
                except (ValueError, UnicodeDecodeError):
                    ok = False
            if not ok:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "需要管理台认证"},
                    headers={"WWW-Authenticate": 'Basic realm="sched", charset="UTF-8"'},
                )
            return await call_next(request)

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(STATIC_DIR / "index.html")

    # ---------- 只读 ----------

    @app.get("/api/overview")
    def overview():
        cap = svc.capacity
        st = svc.status()
        return {
            **st,
            "machine": {
                "total_memory_mb": cap.total_memory_mb,
                "available_memory_mb": cap.available_memory_mb(),
                "total_cpu": cap.total_cpu,
            },
            "started_at": svc.started_at,
        }

    @app.get("/api/jobs")
    def jobs():
        svc.refresh()
        out = svc.list_jobs()
        state = registry.load_state()
        for j in out:
            j["enabled"] = bool(state.get(j["job_id"], {}).get("enabled", True))
        return out

    @app.get("/api/queue")
    def queue():
        with svc._lock:
            waiting = [
                {
                    "run_id": it.get("run_id"), "job_id": it.get("job_id"),
                    "priority": it.get("priority"), "trigger": it.get("trigger"),
                    "attempt": it.get("attempt"), "queued_at": it.get("queued_at"),
                    "scheduled_at": it.get("scheduled_at"),
                    "resume_point_ref": bool(it.get("resume_point_ref")),
                    "manual": bool(it.get("manual")),
                    "not_before": it.get("not_before") or "",
                }
                for it in svc.state.wq
            ]
        # 给每个等待项附上"上次运行结果"：用户在队列页就能看到到底因为什么在等待
        for it in waiting:
            latest = runlog.query_runs(job_id=it["job_id"], limit=1)
            if not latest:
                continue
            rec = latest[0]
            msg = ""
            snap = rec.get("state_snapshot") or {}
            le = snap.get("last_error") or {}
            if isinstance(le, dict):
                msg = str(le.get("message") or "")
            it["last"] = {
                "status": rec.get("status"),
                "exit_code": rec.get("exit_code"),
                "reason": rec.get("reason") or "",
                "message": msg,
                "finished_at": rec.get("finished_at") or rec.get("started_at") or "",
            }
        return {"waiting": waiting, "running": _running_snapshot(svc)}

    @app.get("/api/runs")
    def runs(job_id: str | None = None, status: str | None = None, limit: int = Query(100, le=1000)):
        return runlog.query_runs(job_id=job_id, status=status, limit=limit)

    @app.get("/api/runs/{job_id}/{run_id}")
    def run_detail(job_id: str, run_id: str):
        rec = runlog.find_run(run_id)
        if not rec or rec.get("job_id") != job_id:
            raise HTTPException(404, "run 不存在")
        return rec

    @app.get("/api/logs/{job_id}/{run_id}/{stream_name}")
    def logs(job_id: str, run_id: str, stream_name: str, tail: int = Query(0, ge=0, le=5000),
             offset: int = 0):
        if stream_name not in ("stdout", "stderr"):
            raise HTTPException(400, "stream 仅支持 stdout/stderr")
        p = (runlog.stdout_path(job_id, run_id) if stream_name == "stdout"
             else runlog.stderr_path(job_id, run_id))
        if not p.exists():
            return {"content": "", "size": 0, "offset": 0, "eof": True}
        return runlog.read_log(p, tail=tail, offset=offset)

    @app.get("/api/decisions")
    def decisions(limit: int = Query(200, le=2000)):
        items = list(store.iter_jsonl(config.decisions_log()))[-limit:]
        return list(reversed(items))

    @app.get("/api/profiles")
    def profiles():
        svc.profiles.load()
        out = {}
        for jid, d in svc.profiles._data.items():
            decl_cpu, decl_mem = svc._declared(jid)
            p = svc.profiles.get(jid, decl_cpu, decl_mem)
            out[jid] = {
                "p95_cpu": p.p95_cpu(), "p95_memory_mb": p.p95_memory_mb(),
                "p50_duration_sec": p.p50_duration_sec(),
                "declared_cpu": decl_cpu, "declared_memory_mb": decl_mem,
                "samples": len(p.samples), "duration_samples": len(p.durations),
            }
        return out

    # ---------- 写操作（与 CLI 等价，全部走 service 同一实例） ----------

    @app.post("/api/jobs/{job_id}/trigger", response_model=TriggerResp)
    def trigger_job(job_id: str):
        svc.refresh()
        try:
            r = svc.trigger(job_id, trigger="manual")
        except KeyError:
            raise HTTPException(404, f"任务未注册：{job_id}")
        except Exception as e:
            raise HTTPException(400, str(e))
        return TriggerResp(run_id=r["run_id"], job_id=r["job_id"], priority=r["priority"])

    @app.post("/api/jobs/{job_id}/enabled")
    def set_enabled(job_id: str, req: EnabledReq):
        if job_id not in registry.load_state():
            raise HTTPException(404, f"任务未注册：{job_id}")
        registry.upsert_state(job_id, {"enabled": req.enabled})
        svc.refresh()
        persist.append_decision({"ts": runlog.now_iso(), "act": "web_set_enabled",
                                 "job_id": job_id, "enabled": req.enabled})
        return {"ok": True, "job_id": job_id, "enabled": req.enabled}

    @app.get("/api/jobs/{job_id}/config")
    def get_job_config(job_id: str):
        try:
            return svc._job_config_view(job_id)
        except taskdoc.TaskDocError as e:
            raise HTTPException(400, "; ".join(e.errors))
        except KeyError:
            raise HTTPException(404, f"任务未注册：{job_id}")

    @app.post("/api/jobs/{job_id}/config")
    def update_job_config(job_id: str, patch: dict):
        if not isinstance(patch, dict) or not patch:
            raise HTTPException(400, "请求体必须是非空 JSON 对象（要修改的调度参数）")
        try:
            return svc.update_job_config(job_id, patch)
        except taskdoc.TaskDocError as e:
            raise HTTPException(400, "; ".join(e.errors))
        except KeyError:
            raise HTTPException(404, f"任务未注册：{job_id}")
        except Exception as e:
            raise HTTPException(400, str(e))

    @app.post("/api/runs/{job_id}/{run_id}/stop")
    def stop_run(job_id: str, run_id: str, req: StopReq | None = None):
        try:
            reason = (req.reason if req and req.reason else "管理台手动停止")
            return svc.stop_run(run_id, reason=reason)
        except KeyError:
            raise HTTPException(404, "没有正在运行的该 run（可能已结束）")

    @app.delete("/api/runs/{job_id}/{run_id}")
    def delete_run(job_id: str, run_id: str, delete_files: bool = Query(False)):
        try:
            return svc.delete_run(job_id, run_id, delete_files=delete_files)
        except KeyError:
            raise HTTPException(404, "历史中不存在该 run")
        except RuntimeError as e:
            raise HTTPException(409, str(e))

    # ---------- 任务目录（只读列出）与导入 ----------

    @app.get("/api/tasks")
    def list_tasks():
        rows = []
        registered = registry.load_state()
        admission = svc.admission_snapshot()
        if config.tasks_dir().exists():
            for d in sorted(config.tasks_dir().iterdir()):
                if not d.is_dir() or d.name.startswith((".", "_")):
                    continue
                has_tj = (d / "task.json").exists()
                row = {
                    "id": d.name, "has_task_json": has_tj,
                    "registered": d.name in registered,
                    "enabled": bool(registered.get(d.name, {}).get("enabled", True)),
                }
                adm = admission.get(d.name)
                if not row["registered"] and adm:
                    row["admission"] = {
                        "status": adm.get("status"),
                        "reason": adm.get("reason", ""),
                        "attempts": int(adm.get("attempts") or 0),
                        "last_check": adm.get("last_check", ""),
                    }
                rows.append(row)
        return rows

    @app.post("/api/tasks")
    def import_task(req: ImportReq):
        """按子任务目录名导入一个已在本地验证的生产级任务（同步跑五道闸）。"""
        name = (req.dir_name or "").strip()
        try:
            taskdoc.validate_task_id(name)
        except taskdoc.TaskDocError as e:
            raise HTTPException(400, "; ".join(e.errors))
        task_dir = config.tasks_dir() / name
        if not task_dir.is_dir() or not (task_dir / "task.json").is_file():
            raise HTTPException(404, f"tasks/{name} 目录或 task.json 不存在")
        if name in registry.load_state():
            raise HTTPException(409, f"任务已纳入调度：{name}（无需重复导入）")
        try:
            svc.register_job(name)
        except Exception as e:
            # 五闸任一失败：把闸门原因透传给前端，本地改好文件后会自动重查/可再次导入
            raise HTTPException(400, str(e))
        persist.append_decision({"ts": runlog.now_iso(), "act": "web_import_task", "job_id": name})
        return {"ok": True, "id": name}

    return app


# ---------------- serve 内嵌启动 ----------------

def start_in_thread(svc) -> threading.Thread | None:
    import uvicorn

    cfg = svc.settings.web
    if not cfg.enabled:
        return None
    validate_public_listen(cfg)
    app = create_app(svc)
    config_uv = uvicorn.Config(app, host=cfg.host, port=cfg.port, log_level="warning",
                               access_log=False)
    server = uvicorn.Server(config_uv)
    svc._web_server = server
    t = threading.Thread(target=server.run, name="sched-web", daemon=True)
    t.start()
    # 等待端口就绪（最多 2s），失败不阻塞调度主循环
    for _ in range(20):
        if server.started:
            break
        time.sleep(0.1)
    return t


def stop_in_thread(svc) -> None:
    server = getattr(svc, "_web_server", None)
    if server is not None:
        server.should_exit = True
