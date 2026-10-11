"""sched CLI（新协议 v3，全新命令，不兼容旧 v2 命令）。

  sched serve         常驻调度循环（前台）
  sched status        全局状态（生产者 / 动态目标 / 存活 attempt / 队列）
  sched list          已加载任务（task.json）
  sched task validate <id>      校验任务清单
  sched task enable <id>        启用任务（写 task.json enabled=true）
  sched task disable <id>       禁用任务
  sched job list [task]         作业清单（最近 N 条）
  sched job show <job_id>       作业详情 + attempts
  sched job cancel <job_id>     显式取消（运行中先撤进程树）
  sched attempt list [job]      attempt 清单
  sched attempt cancel <id>     显式取消单个 attempt
  sched version
"""

from __future__ import annotations

import argparse
import json
import sys

from . import config, db as dbmod, scheduler as schedmod
from .config import JobStatus


def _db():
    return dbmod.DB()


def _svc():
    return schedmod.SchedulerService()


def _print_json(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def cmd_serve(args) -> int:
    import signal
    import threading

    svc = _svc()
    svc.start()
    stop = threading.Event()

    def _sig(signum, frame):
        stop.set()

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)
    print(f"sched serving (SCHED_HOME={config.home()})", flush=True)
    try:
        while not stop.is_set():
            import time
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    print("shutting down...", flush=True)
    svc.stop()
    return 0


def cmd_status(args) -> int:
    svc = _svc()
    svc.refresh_specs()
    st = svc.status()
    st["tasks"] = len(svc.specs)
    if args.json:
        _print_json(st)
        return 0
    print(f"tasks={st['tasks']} queued={st['queued']} live_attempts={st['live_attempts']}")
    print(f"exec_cpu_usec={st['exec_cpu_usec']} exec_mem_mb={st['exec_mem_mb']} "
          f"protected={st['protected_task'] or '-'} yielded={st['yielded']}")
    print("\n--- producers ---")
    for p in st["producers"]:
        print(f"  {p['task_id']:<28} pid={p['pid']} alive={p['alive']} crash={p['crash_count']}")
    print("\n--- scale ---")
    for tid, s in sorted(st["scale"].items()):
        print(f"  {tid:<28} run_target={s['run_target']} mode={s['mode']}")
    return 0


def cmd_list(args) -> int:
    specs = _svc().refresh_specs()
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
        import json as _json
        raw = _json.loads(tj.read_text("utf-8"))
        raw["enabled"] = args.action == "enable"
        from . import store
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
        svc = _svc()
        svc.refresh_specs()
        svc.cancel_job(args.job_id)
        print(f"CANCELLED {args.job_id}")
        return 0
    return 2


def cmd_attempt(args) -> int:
    svc = _svc()
    svc.refresh_specs()
    if args.action == "list":
        atts = svc.db.list_attempts(job_id=args.job_id)
        if args.json:
            _print_json(atts)
            return 0
        for a in atts:
            print(f"{a['attempt_id']} job={a['job_id']} seq={a['seq']} status={a['status']} "
                  f"exit={a['exit_code']} pid={a['pid']}")
        return 0
    if args.action == "cancel":
        a = svc.attempts.get(args.attempt_id)
        if a is not None:
            a.terminate(svc.cfg.control.pause_grace_base_sec)
            print(f"CANCELLED {args.attempt_id}")
        else:
            print(f"attempt not live: {args.attempt_id}", file=sys.stderr)
            return 2
        return 0
    return 2


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="sched", description="sched 调度器 CLI (v3)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("serve", help="常驻调度循环").set_defaults(func=cmd_serve)

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
    jsub.add_parser("list").add_argument("task_id", nargs="?", default=None)
    jsub.add_parser("show").add_argument("job_id")
    jsub.add_parser("cancel").add_argument("job_id")
    p_job.set_defaults(func=cmd_job)

    p_att = sub.add_parser("attempt", help="attempt 管理")
    asub = p_att.add_subparsers(dest="action", required=True)
    asub.add_parser("list").add_argument("job_id", nargs="?", default=None)
    asub.add_parser("cancel").add_argument("attempt_id")
    p_att.set_defaults(func=cmd_attempt)

    return p


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())