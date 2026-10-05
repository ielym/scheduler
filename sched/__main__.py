"""sched CLI：register / list / status / start / serve / process。

"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def _service():
    from . import service
    return service


def _settings():
    from . import config
    return config.load_settings()


def _cmd_register(args) -> int:
    sid = args.name
    svc = _service().SchedulerService()
    try:
        svc.register_subtask(sid)
        print(f"REGISTERED {sid}")
    except Exception as e:
        print(f"FAILED {sid}: {e}", file=sys.stderr)
        return 2
    return 0


def _cmd_list(args) -> int:
    svc = _service().SchedulerService()
    svc.refresh()
    subs = svc.list_subtasks()
    if args.json:
        print(json.dumps(subs, ensure_ascii=False, indent=2))
        return 0
    if not subs:
        print("(no registered subtasks)")
        return 0
    print(f"{'SUBTASK':<24} {'RUN':<8} {'ENB':>3} {'PRI':>4} {'OK':>3} {'RES'}")
    for s in subs:
        res = s.get("resources") or {}
        res_str = f"cpu={res.get('cpu', 0)},mem={res.get('memory_mb', 0)}MB"
        if s.get("estimate_duration_sec"):
            res_str += f",dur≈{s['estimate_duration_sec']:.0f}s"
        print(f"{s['subtask_id']:<24} {s.get('run_mode','?'):<8} "
              f"{('Y' if s.get('enabled') else 'N'):>3} "
              f"{int(s.get('priority') or 0):>4} "
              f"{('Y' if s.get('ok') else 'N'):>3} {res_str}")
    return 0


def _cmd_status(args) -> int:
    svc = _service().SchedulerService()
    svc.refresh()
    st = svc.status()
    if args.json:
        print(json.dumps(st, ensure_ascii=False, indent=2))
        return 0
    print(f"subtasks={st['subtasks']} waiting={st['waiting']} running={st['running']} "
          f"ledger_mem={st['ledger_reserved_mem_mb']}MB ledger_cpu={st['ledger_reserved_cpu']}")
    # 展示 WQ
    from .service import persist
    st_obj = persist.load_scheduler_state()
    if st_obj.wq:
        print("\n--- WQ ---")
        for it in st_obj.wq[:20]:
            rid = it.get("run_id")[:12] if it.get("run_id") else ""
            print(f"  {rid}... priority={it.get('priority')} trigger={it.get('trigger')} "
                  f"attempt={it.get('attempt')} waiting={(it.get('enqueued_at') or '')[:19]}")
    # 展示 intents
    from . import scanner
    all_s = scanner.load_intents_state()
    if all_s:
        print("\n--- INTENTS STATE ---")
        for sid, states in all_s.items():
            total = len(states)
            done = sum(1 for st in states.values() if st.get("status") == "done")
            pending = sum(1 for st in states.values() if st.get("status") == "pending")
            running = sum(1 for st in states.values() if st.get("status") == "running")
            print(f"  {sid}: total={total} pending={pending} running={running} done={done}")
    return 0


def _cmd_start(args) -> int:
    """显式启动 run_mode=manual 的子任务（将 enabled=true）。"""
    from . import registry
    st = registry.load_state()
    if args.name not in st:
        print(f"not registered: {args.name}", file=sys.stderr)
        return 2
    registry.upsert_state(args.name, {"enabled": True})
    print(f"ENABLED {args.name}")
    return 0


def _cmd_serve(args) -> int:
    import signal
    import threading

    svc = _service().SchedulerService()
    if args.web:
        svc._web_enabled = True
    svc.start()

    # systemd 停止/重启发送 SIGTERM：转为优雅停机（cancel 在途子进程并落断点），
    # 避免子进程被直接遗弃为孤儿。SIGINT（Ctrl-C）保持原行为一致。
    stop_event = threading.Event()

    def _request_shutdown(signum, frame):
        stop_event.set()

    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT, _request_shutdown)

    from . import config as _config
    print(f"sched serving (SCHED_HOME={_config.home()})...", flush=True)
    try:
        while not stop_event.is_set():
            import time
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    print("shutting down...", flush=True)
    svc.stop()
    return 0


def _cmd_process(args) -> int:
    """process subcommands: list / stop / no-retry.

    当前版本依赖调度器已在跑（通过共享的 intents_state.json + active.json 通信）。
    """
    from . import scanner, persist, store, config
    all_intents = scanner.load_intents_state()
    active = persist.load_active()

    if args.action == "list":
        if args.json:
            print(json.dumps({"intents": all_intents, "active": active}, ensure_ascii=False, indent=2))
            return 0
        target = args.subtask
        for sid, states in sorted(all_intents.items()):
            if target and sid != target:
                continue
            print(f"## {sid}")
            for rid, st in sorted(states.items()):
                print(f"  {rid}  status={st.get('status')}  run={st.get('run_id', '-')}  "
                      f"fp={st.get('fingerprint', '')[:10]}")
        if args.subtask and args.subtask not in all_intents:
            print("(no intents for this subtask)")
        return 0

    if args.action in ("stop", "no-retry"):
        # 意图文件删除（=调度器 _tick_scan 发现 disappear → cancel 进程） 或 intent 文件改写（违规 rejected）
        # 我们不改写，只靠 SDK control 段——但 SDK 写 state.json control 段只在进程活着时有效。
        # 从 CLI 直接操作意图文件是硬路径（等价于子任务意图消失）：
        request_id = args.request_id
        task_dir = config.tasks_dir() / args.subtask
        intent_path = task_dir / "requests" / f"{request_id}.json"
        if not intent_path.exists():
            print(f"intent not found: {intent_path}", file=sys.stderr)
            return 2
        target = task_dir / "requests" / f"{request_id}.json.bak"
        intent_path.rename(target)
        print(f"intent 标记为消失 → 调度器将 cancel 进程 ({args.action})")
        # 也把状态标记 done
        import datetime as dt
        st = scanner.load_intents_state()
        sub = st.setdefault(args.subtask, {}).get(request_id)
        if sub and sub.get("status") == "running":
            sub["status"] = "done"
            sub["finished_at"] = dt.datetime.now().astimezone().isoformat(timespec="seconds")
            sub["rejected_reason"] = f"CLI {args.action}"
            scanner.save_intents_state(st)
        return 0

    print(f"unknown process action: {args.action}", file=sys.stderr)
    return 2


def _cmd_clear(args) -> int:
    """清空全部调度状态（建议先停服务再执行，避免与运行实例写盘竞争）。"""
    svc = _service().SchedulerService()
    result = svc.clear_all(delete_requests=args.delete_requests)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="sched", description="sched CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_register = sub.add_parser("register", help="注册子任务（跑闸）")
    p_register.add_argument("name")
    p_register.set_defaults(func=_cmd_register)

    p_list = sub.add_parser("list", help="列出已注册子任务")
    p_list.add_argument("--json", action="store_true")
    p_list.set_defaults(func=_cmd_list)

    p_status = sub.add_parser("status", help="展示全局状态（WQ + intents + ledger）")
    p_status.add_argument("--json", action="store_true")
    p_status.set_defaults(func=_cmd_status)

    p_start = sub.add_parser("start", help="显式启用 run_mode=manual 的子任务")
    p_start.add_argument("name")
    p_start.set_defaults(func=_cmd_start)

    p_serve = sub.add_parser("serve", help="启动调度器主循环（前台运行）")
    p_serve.add_argument("--web", action="store_true", help="同时启动内嵌管理台（若 settings.web.enabled=false 需手动改）")
    p_serve.set_defaults(func=_cmd_serve)

    p_clear = sub.add_parser("clear", help="清空全部调度状态（注册表/WQ/意图/运行历史；建议先停服务）")
    p_clear.add_argument("--delete-requests", action="store_true",
                        help="同时清空所有子任务 requests/ 意图文件（保留任务代码）")
    p_clear.set_defaults(func=_cmd_clear)

    p_proc = sub.add_parser("process", help="子进程管理（依赖调度器已跑）")
    proc_sub = p_proc.add_subparsers(dest="action", required=True)
    proc_list = proc_sub.add_parser("list")
    proc_list.add_argument("subtask", nargs="?", default=None)
    proc_list.add_argument("--json", action="store_true")
    proc_stop = proc_sub.add_parser("stop")
    proc_stop.add_argument("subtask")
    proc_stop.add_argument("request_id")
    proc_noretry = proc_sub.add_parser("no-retry")
    proc_noretry.add_argument("subtask")
    proc_noretry.add_argument("request_id")
    p_proc.set_defaults(func=_cmd_process)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())