"""sched CLI（新协议 v3，全新命令，不兼容旧 v2 命令）。

  sched serve         常驻调度循环（前台）
  sched web           Web 控制台（默认 http://127.0.0.1:8765，只读快照+数据库，
                      控制操作走 commands.jsonl 通道；--token 可选鉴权）
  sched status        全局状态（读 serve 快照 + 数据库；serve 不在时为数据库视图）
  sched list          已加载任务（task.json）
  sched task validate <id>      校验任务清单
  sched task enable <id>        启用任务（写 task.json enabled=true）
  sched task disable <id>       禁用任务
  sched job list [task]         作业清单（最近 N 条）
  sched job show <job_id>       作业详情 + attempts
  sched job cancel <job_id>     显式取消（经 serve 控制通道；serve 不在时直连处理）
  sched job requeue <job_id>    重排作业（RECOVERY_BLOCKED 等状态的解除出口；
                                运行中/恢复核实中的作业拒绝重排）
  sched attempt list [job]      attempt 清单
  sched attempt cancel <id>     显式取消单个 attempt
  sched version

CLI 与 serve 的 IPC（设计文档第十四节）：
- serve 每轮把运行时快照写入 runtime/snapshot.json，status 读取；
- cancel 类命令写入 runtime/commands.jsonl，serve 消费后回执 commands_ack.jsonl；
  serve 不在（单实例锁空闲）时 CLI 直连数据库 + 终止进程树完成取消。
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone

from . import config, db as dbmod, runner, scheduler as schedmod, store
from .config import JobStatus

SNAPSHOT_FRESH_SEC = 10.0


def _db():
    return dbmod.DB()


def _print_json(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def _serve_alive() -> bool:
    """serve 是否持有单实例锁（flock 探测）。"""
    import fcntl
    import os
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


def _fresh_snapshot() -> dict | None:
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


def _send_command(kind: str, target: str, reason: str = "显式取消") -> dict | None:
    """写控制命令并等待 serve 回执；超时返回 None。"""
    cfg = config.load_config()
    cmd = {"id": runner.new_id("cmd"), "kind": kind, "target": target,
           "reason": reason, "ts": runner.now_iso()}
    store.append_jsonl(config.commands_file(), cmd)
    deadline = time.monotonic() + cfg.control.cmd_ack_wait_sec
    while time.monotonic() < deadline:
        for ack in store.read_jsonl(config.commands_ack_file()):
            if ack.get("id") == cmd["id"]:
                return ack
        time.sleep(0.2)
    return None


def _direct_cancel_attempt(db, att: dict, reason: str) -> None:
    """serve 不在时：直连终止进程树 + 落终态（PID+启动时间身份核实）。"""
    pid = att.get("pid") or 0
    pst = att.get("pid_start_time") or 0.0
    if pid and pst:
        runner.terminate_external(pid, att.get("pgid") or pid, pst,
                                   config.load_config().control.pause_grace_base_sec)
    db.commit_transition(
        att["attempt_id"],
        {"status": JobStatus.CANCELLED, "reason": reason,
         "finished_at": runner.now_iso()},
        att["job_id"], JobStatus.CANCELLED, reason, runner.now_iso())


def _cancel(kind: str, target: str, reason: str) -> int:
    if _serve_alive():
        ack = _send_command(kind, target, reason)
        if ack is not None:
            if ack.get("ok"):
                print(f"CANCELLED {target}")
                return 0
            print(f"cancel failed: {ack.get('detail')}", file=sys.stderr)
            return 2
        print("等待 serve 确认超时（serve 可能刚退出）", file=sys.stderr)
        return 2
    # serve 不在：直连处理
    db = _db()
    if kind == "cancel_job":
        job = db.get_job(target)
        if job is None:
            print(f"job not found: {target}", file=sys.stderr)
            return 2
        for att in db.live_attempts():
            if att["job_id"] == target:
                _direct_cancel_attempt(db, att, reason)
        if db.get_job(target)["status"] not in JobStatus.TERMINAL:
            db.set_job_status(target, JobStatus.CANCELLED, reason, runner.now_iso())
        print(f"CANCELLED {target}")
        return 0
    # cancel_attempt
    att = db.get_attempt(target)
    if att is None:
        print(f"attempt not found: {target}", file=sys.stderr)
        return 2
    if att["status"] not in JobStatus.TERMINAL:
        _direct_cancel_attempt(db, att, reason)
    print(f"CANCELLED {target}")
    return 0


def _requeue(job_id: str, reason: str) -> int:
    """重排作业：RECOVERY_BLOCKED 等不可解状态的解除出口。

    serve 存活走命令通道；否则直连关闭遗留 attempt（进程仍在先终止）→ QUEUED。
    """
    if _serve_alive():
        ack = _send_command("requeue_job", job_id, reason)
        if ack is not None:
            if ack.get("ok"):
                print(f"REQUEUED {job_id}")
                return 0
            print(f"requeue failed: {ack.get('detail')}", file=sys.stderr)
            return 2
        print("等待 serve 确认超时（serve 可能刚退出）", file=sys.stderr)
        return 2
    # serve 不在：直连处理
    db = _db()
    job = db.get_job(job_id)
    if job is None:
        print(f"job not found: {job_id}", file=sys.stderr)
        return 2
    if job["status"] in (JobStatus.STARTING, JobStatus.RUNNING, JobStatus.PAUSING):
        print(f"job {job_id} 正在执行（{job['status']}），不能重排", file=sys.stderr)
        return 2
    for att in db.open_attempts_for_job(job_id):
        pid = att.get("pid") or 0
        pst = att.get("pid_start_time") or 0.0
        if pid and pst and runner.pid_alive_with_start(pid, pst):
            runner.terminate_external(pid, att.get("pgid") or pid, pst,
                                       config.load_config().control.pause_grace_base_sec)
        db.commit_transition(
            att["attempt_id"],
            {"status": JobStatus.CANCELLED, "reason": f"重排关闭：{reason}",
             "finished_at": runner.now_iso()},
            job_id, JobStatus.QUEUED, reason, runner.now_iso())
    db.set_job_status(job_id, JobStatus.QUEUED, reason, runner.now_iso())
    print(f"REQUEUED {job_id}")
    return 0


def cmd_serve(args) -> int:
    import signal
    import threading

    svc = schedmod.SchedulerService()
    svc.start()
    stop = threading.Event()

    def _sig(signum, frame):
        stop.set()

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)
    print(f"sched serving (SCHED_HOME={config.home()})", flush=True)
    try:
        while not stop.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    print("shutting down...", flush=True)
    svc.stop()
    return 0


def cmd_web(args) -> int:
    from . import web
    web.serve(host=args.host, port=args.port, token=args.token)
    return 0


def cmd_status(args) -> int:
    snap = _fresh_snapshot()
    # 单连接读取，用完即关（不构建 SchedulerService——那会多开一条永不关闭的
    # 数据库连接只为读 tasks/ 清单）
    db = _db()
    try:
        counts = db.status_counts()
        scale_fallback = db.all_task_states()
    finally:
        db.close()
    specs = schedmod.load_all_specs()
    if args.json:
        _print_json({"snapshot": snap, "job_counts": counts,
                     "tasks": {tid: {"priority": s.priority, "enabled": s.enabled}
                               for tid, s in specs.items()}})
        return 0
    if snap:
        print(f"serve=alive ts={snap.get('ts')}")
        print(f"tasks={snap['tasks']} queued={snap['queued']} "
              f"live_attempts={snap['live_attempts']} recovering={snap.get('recovering', 0)}")
        print(f"exec_cpu_usec={snap['exec_cpu_usec']} exec_mem_mb={snap['exec_mem_mb']} "
              f"exec_io_bytes={snap.get('exec_io_bytes', 0)} "
              f"observability={'ok' if snap.get('observability_ok', True) else 'insufficient'} "
              f"resource={'unhealthy' if snap.get('resource_unhealthy') else 'healthy'}")
        print(f"protected={snap.get('protected_task') or '-'} "
              f"yielded={snap.get('yielded')} pending_resume={snap.get('pending_resume')}")
        print("\n--- producers ---")
        for p in snap.get("producers", []):
            ext = " (external)" if p.get("external") else ""
            print(f"  {p['task_id']:<28} pid={p['pid']} alive={p['alive']} "
                  f"crash={p['crash_count']}{ext}")
        print("\n--- scale ---")
        for tid, s in sorted(snap.get("scale", {}).items()):
            print(f"  {tid:<28} run_target={s['run_target']} mode={s['mode']}")
        pend = snap.get("scan_pending") or {}
        if pend:
            print(f"\n--- scan pending ---\n  {pend}")
    else:
        print("serve=not-running (snapshot missing or stale; database view)")
        print(f"tasks={len(specs)}")
    print("\n--- jobs by status ---")
    if counts:
        for status, n in sorted(counts.items()):
            print(f"  {status:<18} {n}")
    else:
        print("  (no jobs)")
    if not snap and scale_fallback:
        print("\n--- scale (from task_state) ---")
        for s in sorted(scale_fallback, key=lambda x: x["task_id"]):
            print(f"  {s['task_id']:<28} run_target={s.get('run_target')} "
                  f"mode={s.get('mode')}")
    return 0


def cmd_list(args) -> int:
    specs = schedmod.load_all_specs()
    if args.json:
        _print_json([{"task_id": tid, "priority": s.priority, "enabled": s.enabled}
                     for tid, s in sorted(specs.items())])
        return 0
    if not specs:
        print("(no tasks)")
        return 0
    print(f"{'TASK':<30} {'PRI':>4} {'ENB':>3}")
    for tid, s in sorted(specs.items()):
        print(f"{tid:<30} {s.priority:>4} {'Y' if s.enabled else 'N':>3}")
    return 0


def cmd_task(args) -> int:
    if args.action == "validate":
        try:
            config.load_task(args.task_id)
            print(f"OK {args.task_id}")
            return 0
        except config.TaskError as e:
            print(f"FAILED {args.task_id}: {e}", file=sys.stderr)
            return 2
    if args.action in ("enable", "disable"):
        td = config.task_dir(args.task_id)
        tj = td / "task.json"
        if not tj.exists():
            print(f"task.json not found: {tj}", file=sys.stderr)
            return 2
        try:
            raw = json.loads(tj.read_text("utf-8"))
        except (json.JSONDecodeError, OSError, UnicodeDecodeError) as e:
            print(f"读取 task.json 失败：{e}", file=sys.stderr)
            return 2
        if not isinstance(raw, dict):
            print("task.json 顶层必须是对象", file=sys.stderr)
            return 2
        if args.action == "enable":
            # 启用前先完整校验：把不合规清单置 enabled=true 只会让 serve
            # 每轮加载告警且该任务永远不可用（disable 保留为运维出口不校验）
            try:
                config.parse_task(raw, args.task_id)
            except config.TaskError as e:
                print(f"校验失败，未启用：{e}", file=sys.stderr)
                return 2
        raw["enabled"] = args.action == "enable"
        store.save_json(tj, raw)
        print(f"{args.action.upper()} {args.task_id}")
        return 0
    return 2


def cmd_job(args) -> int:
    db = _db()
    if args.action == "list":
        jobs = db.list_jobs(task_id=args.task_id)
        if args.json:
            _print_json(jobs)
            return 0
        if not jobs:
            print("(no jobs)")
            return 0
        print(f"{'JOB':<40} {'TASK':<28} {'PRI':>4} {'STATUS':<18}")
        for j in jobs:
            print(f"{j['job_id']:<40} {j['task_id']:<28} {j['priority']:>4} {j['status']:<18}")
        return 0
    if args.action == "show":
        j = db.get_job(args.job_id)
        if not j:
            print(f"job not found: {args.job_id}", file=sys.stderr)
            return 2
        attempts = db.list_attempts(job_id=args.job_id)
        if args.json:
            _print_json({"job": j, "attempts": attempts})
            return 0
        _print_json(j)
        print("--- attempts ---")
        for a in attempts:
            print(f"  {a['attempt_id']} seq={a['seq']} status={a['status']} "
                  f"exit={a['exit_code']} pid={a['pid']} {a['reason']}")
        return 0
    if args.action == "cancel":
        return _cancel("cancel_job", args.job_id, getattr(args, "reason", "") or "CLI job cancel")
    if args.action == "requeue":
        return _requeue(args.job_id, getattr(args, "reason", "") or "CLI job requeue")
    return 2


def cmd_attempt(args) -> int:
    db = _db()
    if args.action == "list":
        atts = db.list_attempts(job_id=args.job_id)
        if args.json:
            _print_json(atts)
            return 0
        for a in atts:
            print(f"{a['attempt_id']} job={a['job_id']} seq={a['seq']} status={a['status']} "
                  f"exit={a['exit_code']} pid={a['pid']}")
        return 0
    if args.action == "cancel":
        return _cancel("cancel_attempt", args.attempt_id,
                       getattr(args, "reason", "") or "CLI attempt cancel")
    return 2


def cmd_version(args) -> int:
    from . import __version__
    print(f"sched {__version__} (protocol v3)")
    return 0


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="sched", description="sched 调度器 CLI (v3)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("serve", help="常驻调度循环").set_defaults(func=cmd_serve)

    p_web = sub.add_parser("web", help="Web 控制台")
    p_web.add_argument("--host", default="127.0.0.1")
    p_web.add_argument("--port", type=int, default=8765)
    p_web.add_argument("--token", default=None, help="可选 Bearer 鉴权令牌")
    p_web.set_defaults(func=cmd_web)

    p_status = sub.add_parser("status", help="全局状态")
    p_status.add_argument("--json", action="store_true")
    p_status.set_defaults(func=cmd_status)

    p_list = sub.add_parser("list", help="任务清单")
    p_list.add_argument("--json", action="store_true")
    p_list.set_defaults(func=cmd_list)

    p_task = sub.add_parser("task", help="任务管理")
    tsub = p_task.add_subparsers(dest="action", required=True)
    tsub.add_parser("validate").add_argument("task_id")
    tsub.add_parser("enable").add_argument("task_id")
    tsub.add_parser("disable").add_argument("task_id")
    p_task.set_defaults(func=cmd_task)

    p_job = sub.add_parser("job", help="作业管理")
    jsub = p_job.add_subparsers(dest="action", required=True)
    _jl = jsub.add_parser("list")
    _jl.add_argument("task_id", nargs="?", default=None)
    _jl.add_argument("--json", action="store_true")
    _js = jsub.add_parser("show")
    _js.add_argument("job_id")
    _js.add_argument("--json", action="store_true")
    _jc = jsub.add_parser("cancel")
    _jc.add_argument("job_id")
    _jc.add_argument("--reason", default="CLI job cancel")
    _jr = jsub.add_parser("requeue")
    _jr.add_argument("job_id")
    _jr.add_argument("--reason", default="CLI job requeue")
    p_job.set_defaults(func=cmd_job)

    p_att = sub.add_parser("attempt", help="attempt 管理")
    asub = p_att.add_subparsers(dest="action", required=True)
    _al = asub.add_parser("list")
    _al.add_argument("job_id", nargs="?", default=None)
    _al.add_argument("--json", action="store_true")
    _ac = asub.add_parser("cancel")
    _ac.add_argument("attempt_id")
    _ac.add_argument("--reason", default="CLI attempt cancel")
    p_att.set_defaults(func=cmd_attempt)

    p_ver = sub.add_parser("version", help="版本")
    p_ver.set_defaults(func=cmd_version)

    return p


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
