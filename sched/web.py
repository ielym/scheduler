"""内嵌管理台（FastAPI）。

设计约束（与 sched 技能的 H1 一致）：
- Web 层只允许在 serve 进程内启动（start_in_thread），所有写操作直接调用同一个
  SchedulerService 实例。禁止另起 Web 进程：WQ/账本/软资源是 serve 的内存态，
  跨进程写 scheduler_state.json 会被 serve 下一次落盘覆盖。
- 触发类操作只入队（service.trigger），绝不直接拉起任务进程；注册真实跑五道闸。
- 文件编辑仅限任务目录内的源码类文件，cache/results 运行时产物禁止经 Web 改写。
"""

from __future__ import annotations

import base64
import hmac
import ipaddress
import json
import os
import threading
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel

from . import config, contract, persist, registry, runlog, store, taskdoc


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
        "管理台可在线改任务代码并注册执行（等同 RCE）。二选一：\n"
        "1) 在 data/settings.json 配置 web.auth_token（推荐）；\n"
        "2) 确需无密码公网暴露，显式设置 web.allow_public_no_auth=true（风险自负）。")

STATIC_DIR = Path(__file__).resolve().parent / "web_static"

# 同一时刻只允许一个 register（闸门会真实串行拉起最多 4 个进程，见 gates.md）
_register_lock = threading.Lock()

_EDIT_ALLOWED_SUFFIX = {".py", ".json", ".txt", ".md", ".conf", ".cfg", ".ini", ".yaml", ".yml", ".sh", ".env"}
_EDIT_DENY_PARTS = {"cache", "results", "__pycache__", ".git"}
_MAX_FILE_BYTES = 1024 * 1024


# 请求/响应模型必须定义在模块级：create_app 内的局部类配合延迟注解会让 FastAPI 无法解析类型。
class TriggerResp(BaseModel):
    run_id: str
    job_id: str
    priority: int


class EnabledReq(BaseModel):
    enabled: bool


class SoftReq(BaseModel):
    name: str
    n: int


class CreateReq(BaseModel):
    id: str


class FileReq(BaseModel):
    content: str


def _write_task_file(path: Path, text: str) -> int:
    """写任务源码并放开 0644。

    store.write_text_atomic 基于 mkstemp（0600）原子替换，会把文件改成仅属主可读；
    而闸门/正式运行要降权到 run_user（sched-run）执行，0600 会直接 Permission denied
    （闸2 退出码 2）。凡经管理台写入的任务文件必须补 chmod。
    """
    store.write_text_atomic(path, text)
    os.chmod(path, 0o644)
    return len(text.encode("utf-8"))


def _chown_tree(path: Path, user: str) -> str:
    """把任务目录树属主改为降权执行用户（等价技能部署步骤 chown -R <user> tasks/<id>）。

    任务运行时要在目录里写 cache/state 与 results；serve 非 root 时 chown 会失败，
    调用方应把失败信息透传给用户，而不是静默继续（静默则闸2 必挂）。
    """
    import pwd
    pw = pwd.getpwnam(user)
    os.chown(path, pw.pw_uid, pw.pw_gid)
    for root, dirs, files in os.walk(path):
        for name in dirs + files:
            os.chown(os.path.join(root, name), pw.pw_uid, pw.pw_gid)
    return user


# ---------------- 新建任务的最小骨架（与技能 assets/skeleton 同源思路，保持极简） ----------------

_SKELETON_TASK_JSON = """{
  "schema_version": 2,
  "name": "%s",
  "description": "",
  "entry": {"file": "run.py", "interpreter": "python3", "args": [], "cwd": "."},
  "schedule": {"type": "manual", "max_instances": 1, "overflow": "queue"},
  "priority": 40,
  "env": {},
  "outputs": {"expect": ["results/final.json"]},
  "resources": {"cpu": 0.2, "memory_mb": 128, "soft": {}},
  "error_rules": {"resource_regex": [], "logic_regex": ["LOGIC:"]},
  "heartbeat": {"timeout_sec": 120},
  "smoke": {"mode": "dry", "max_runtime_sec": 60, "signal_after_sec": 1.5},
  "runtime": {"user": "%RUN_USER%"},
  "contract_exempt": false
}
"""

_SKELETON_RUN_PY = '''"""最小任务骨架（管理台自动生成）。更丰富的场景模板见 sched 技能 assets/templates。

合规要点（勿删）：
- 每完成一个最小工作单元就写 resume_point 并 flush；抢占后从游标幂等重放。
- dry 路径每块 sleep 一小段：让注册闸5 在 signal_after_sec(默认1.5s) 发出的
  SIGTERM 命中"块处理中"，验证 99 抢占语义；真实逻辑上线后可去掉人为 sleep。
- 逻辑错误用 raise ValueError("LOGIC: ...")（配合 task.json logic_regex）；
  资源错误 flush 后 sys.exit(100|101)；数据风险 sys.exit(110) 请求二刷。
- 交付物原子写到 results/final.json（outputs.expect 缺失会被判 failed）。
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from sched_task_sdk import SchedTask, Step, run_task

TOTAL_ITEMS = 4


class MyTask(SchedTask):
    def steps(self, resume: dict):
        start = int((resume or {}).get("cursor", 0))
        for i in range(start, TOTAL_ITEMS):
            item_id = f"item-{i}"
            yield Step("process", {"item": item_id})
            if self.dry_run:
                time.sleep(0.8)   # 仅为闸5 命中块中；真实任务删除
                print(f"[DRY] would process {item_id}")
            else:
                print(f"processed {item_id}")   # TODO: 幂等真实副作用
            self.state.resume_point = {"cursor": i + 1}
            self.state.progress = int((i + 1) / TOTAL_ITEMS * 100)
            self.state.flush()

        out_dir = Path(self.workspace) / "results"
        out_dir.mkdir(parents=True, exist_ok=True)
        target = out_dir / "final.json"
        tmp = target.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({"processed": TOTAL_ITEMS}, ensure_ascii=False), encoding="utf-8")
        tmp.replace(target)   # 生成器自然结束 → SDK 自动 success、清 resume_point、exit 0


if __name__ == "__main__":
    sys.exit(run_task(MyTask()))
'''


# ---------------- 工具 ----------------

def _safe_task_path(task_id: str, rel: str) -> Path:
    task_dir = (config.tasks_dir() / task_id).resolve()
    if not task_dir.is_dir():
        raise HTTPException(404, f"任务目录不存在：{task_id}")
    p = (task_dir / rel).resolve()
    if p != task_dir and task_dir not in p.parents:
        raise HTTPException(400, "路径越出任务目录")
    return p


def _editable_path(task_id: str, rel: str) -> Path:
    rel = (rel or "").lstrip("/")
    parts = Path(rel).parts
    if not parts or any(part in _EDIT_DENY_PARTS or part == ".." for part in parts):
        raise HTTPException(400, "禁止编辑该路径（仅允许任务源码文件，cache/results 为运行时产物）")
    p = _safe_task_path(task_id, rel)
    if p.suffix.lower() not in _EDIT_ALLOWED_SUFFIX:
        raise HTTPException(400, f"不允许编辑该后缀文件：{p.suffix or '(无后缀)'}")
    return p


def _running_snapshot(svc) -> list[dict]:
    out = []
    for re in list(svc.running.values()):
        r = re.run
        out.append({
            "run_id": r.get("run_id"), "job_id": r.get("job_id"), "pid": r.get("pid"),
            "trigger": r.get("trigger"), "attempt": r.get("attempt"),
            "started_at": r.get("started_at"), "est_cpu": re.est_cpu, "est_mem_mb": re.est_mem_mb,
            "soft": re.soft, "manual": bool(r.get("manual")),
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
            "settings": {
                "timezone": svc.settings.timezone,
                "cooldown_sec": svc.settings.runner.cooldown_sec,
                "grace_sec": svc.settings.runner.grace_sec,
                "kill_grace_sec": svc.settings.runner.kill_grace_sec,
                "heartbeat_timeout_sec": svc.settings.runner.heartbeat_timeout_sec,
                "max_resource_attempts": svc.settings.runner.max_resource_attempts,
                "hot_reload_sec": svc.settings.hot_reload_sec,
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
                }
                for it in svc.state.wq
            ]
            cooldowns = dict(svc.state.cooldowns)
            backoffs = dict(svc.state.backoffs)
        return {"waiting": waiting, "running": _running_snapshot(svc),
                "cooldowns": cooldowns, "backoffs": backoffs}

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
            decl_cpu, decl_mem, _ = svc._declared(jid)
            p = svc.profiles.get(jid, decl_cpu, decl_mem)
            out[jid] = {
                "p95_cpu": p.p95_cpu(), "p95_memory_mb": p.p95_memory_mb(),
                "declared_cpu": decl_cpu, "declared_memory_mb": decl_mem,
                "samples": len(p.samples),
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

    @app.post("/api/soft")
    def set_soft(req: SoftReq):
        if req.n < 0:
            raise HTTPException(400, "容量必须是非负整数")
        svc.state.soft_capacities[req.name] = req.n
        svc.soft_pool.set_capacity(req.name, req.n)
        persist.save_scheduler_state(svc.state)
        return {"ok": True, "name": req.name, "capacity": req.n}

    # ---------- 注册（SSE 逐闸推送，等价 python3 -m sched register） ----------

    @app.post("/api/register/{job_id}")
    def register_job(job_id: str):
        if not _register_lock.acquire(blocking=False):
            raise HTTPException(409, "已有注册在进行中，请等待其结束")

        def sse():
            try:
                def emit(event: str, payload: dict) -> str:
                    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"

                try:
                    taskdoc.validate_task_id(job_id)
                    task_dir = config.tasks_dir() / job_id
                    if not task_dir.is_dir():
                        yield emit("error", {"message": f"任务目录不存在：{task_dir}"})
                        return
                    doc = taskdoc.load_task_doc(task_dir)
                    run_user = (doc.runtime.user or "") or (svc.settings.run_user or "")
                    import pwd
                    if run_user:
                        try:
                            pwd.getpwnam(run_user)
                        except KeyError:
                            yield emit("error", {"message": f"运行用户 {run_user!r} 不存在"})
                            return
                        # 与技能部署步骤等价：保证降权用户能读代码、写 cache/results
                        try:
                            _chown_tree(task_dir, run_user)
                        except (PermissionError, OSError) as e:
                            yield emit("error", {"message": f"chown {run_user} 失败（serve 是否 root？）：{e}"})
                            return
                except taskdoc.TaskDocError as e:
                    yield emit("error", {"message": "; ".join(e.errors)})
                    return

                gates = contract.ContractGates(svc.settings)
                measured_mem = 0

                # 手动逐闸（为拿到每闸事件，不能直接 run_all）
                seq = [
                    ("gate1_static", lambda: gates.gate_static(doc, task_dir)),
                    ("gate2_contract", lambda: gates.gate_contract(doc, task_dir)),
                    ("gate3_smoke", lambda: gates.gate_smoke(doc, task_dir)),
                ]
                results = []
                for name, fn in seq:
                    t0 = time.monotonic()
                    try:
                        r = fn()
                    except Exception as e:
                        yield emit("error", {"message": f"{name} 执行异常：{type(e).__name__}: {e}"})
                        return
                    elapsed = round(time.monotonic() - t0, 2)
                    results.append(r)
                    data = {"gate": r.gate, "ok": r.ok, "reason": r.reason, "elapsed_sec": elapsed}
                    smoke = (r.artifacts or {}).get("smoke")
                    if smoke:
                        measured_mem = int(smoke.get("memory_peak_mb") or 0)
                        data["smoke"] = {k: smoke.get(k) for k in
                                         ("elapsed_sec", "memory_peak_mb", "cpu_sec", "exit_code")}
                    if (r.artifacts or {}).get("est_mem_mb"):
                        data["est_mem_mb"] = r.artifacts["est_mem_mb"]
                    yield emit("gate_ok" if r.ok else "gate_failed", data)
                    if not r.ok:
                        yield emit("done", {"ok": False, "job_id": job_id})
                        return

                t0 = time.monotonic()
                g4 = gates.gate_capacity(doc, task_dir, measured_mem_mb=measured_mem)
                results.append(g4)
                d4 = {"gate": g4.gate, "ok": g4.ok, "reason": g4.reason,
                      "elapsed_sec": round(time.monotonic() - t0, 2),
                      "est_mem_mb": (g4.artifacts or {}).get("est_mem_mb")}
                yield emit("gate_ok" if g4.ok else "gate_failed", d4)
                if not g4.ok:
                    yield emit("done", {"ok": False, "job_id": job_id})
                    return

                if not doc.contract_exempt:
                    t0 = time.monotonic()
                    g5 = gates.gate_checkpoint(doc, task_dir)
                    results.append(g5)
                    d5 = {"gate": g5.gate, "ok": g5.ok, "reason": g5.reason,
                          "elapsed_sec": round(time.monotonic() - t0, 2)}
                    yield emit("gate_ok" if g5.ok else "gate_failed", d5)
                    if not g5.ok:
                        yield emit("done", {"ok": False, "job_id": job_id})
                        return

                # 全过：与 SchedulerService.register_job 相同的落库动作（本进程同一 svc）
                est_mem = int(measured_mem or doc.resources.memory_mb or 0)
                est_cpu = 0.0
                smoke_r = results[2].artifacts.get("smoke") or {}
                el = float(smoke_r.get("elapsed_sec") or 0)
                if el > 0:
                    est_cpu = round(float(smoke_r.get("cpu_sec") or 0) / el, 3)
                svc.profiles.record(job_id, est_cpu, est_mem, doc.resources.cpu,
                                    doc.resources.memory_mb,
                                    window=svc.settings.resource.window_samples)
                svc.profiles.save()
                registry.upsert_state(job_id, {"enabled": True})
                svc.refresh()
                yield emit("done", {"ok": True, "job_id": job_id,
                                    "est_mem_mb": est_mem, "est_cpu": est_cpu})
            finally:
                _register_lock.release()

        return StreamingResponse(sse(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    # ---------- 任务源码文件 ----------

    @app.get("/api/tasks")
    def list_tasks():
        rows = []
        registered = registry.load_state()
        if config.tasks_dir().exists():
            for d in sorted(config.tasks_dir().iterdir()):
                if not d.is_dir() or d.name.startswith((".", "_")):
                    continue
                has_tj = (d / "task.json").exists()
                rows.append({
                    "id": d.name, "has_task_json": has_tj,
                    "registered": d.name in registered,
                    "enabled": bool(registered.get(d.name, {}).get("enabled", True)),
                })
        return rows

    @app.post("/api/tasks")
    def create_task(req: CreateReq):
        try:
            taskdoc.validate_task_id(req.id)
        except taskdoc.TaskDocError as e:
            raise HTTPException(400, "; ".join(e.errors))
        task_dir = config.tasks_dir() / req.id
        if task_dir.exists():
            raise HTTPException(409, f"任务目录已存在：{req.id}")
        (task_dir / "results").mkdir(parents=True)
        run_user = svc.settings.run_user or ""
        tj = _SKELETON_TASK_JSON.replace("%RUN_USER%", run_user) % req.id
        _write_task_file(task_dir / "task.json", tj)
        _write_task_file(task_dir / "run.py", _SKELETON_RUN_PY)
        os.chmod(task_dir, 0o755)
        os.chmod(task_dir / "results", 0o755)
        chown_error = ""
        if run_user:
            try:
                _chown_tree(task_dir, run_user)
            except (KeyError, PermissionError, OSError) as e:
                chown_error = f"（注意：chown {run_user} 失败：{e}；闸2 前需手动处理目录属主）"
        persist.append_decision({"ts": runlog.now_iso(), "act": "web_create_task", "job_id": req.id})
        return {"ok": True, "id": req.id, "warning": chown_error}

    @app.get("/api/tasks/{job_id}/files")
    def task_files(job_id: str):
        task_dir = _safe_task_path(job_id, ".")
        files = []
        for p in sorted(task_dir.rglob("*")):
            if any(part in _EDIT_DENY_PARTS for part in p.relative_to(task_dir).parts):
                continue
            if p.is_file():
                files.append({"path": str(p.relative_to(task_dir)), "size": p.stat().st_size})
        return files

    @app.get("/api/tasks/{job_id}/file")
    def read_task_file(job_id: str, path: str):
        p = _editable_path(job_id, path)
        if not p.exists():
            raise HTTPException(404, "文件不存在")
        if p.stat().st_size > _MAX_FILE_BYTES:
            raise HTTPException(400, "文件超过 1MB，管理台不打开")
        return {"path": path, "content": p.read_text("utf-8", errors="replace"), "size": p.stat().st_size}

    @app.put("/api/tasks/{job_id}/file")
    def write_task_file(job_id: str, path: str, req: FileReq):
        p = _editable_path(job_id, path)
        data = req.content.encode("utf-8")
        if len(data) > _MAX_FILE_BYTES:
            raise HTTPException(400, "文件超过 1MB")
        n = _write_task_file(p, req.content)
        persist.append_decision({"ts": runlog.now_iso(), "act": "web_save_file",
                                 "job_id": job_id, "path": path, "bytes": n})
        return {"ok": True, "path": path, "bytes": n}

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
