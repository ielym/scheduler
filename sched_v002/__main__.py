"""命令行入口：sched-v002 <command>。

命令：
  register <name>        跑五道闸并注册任务
  run <name>             手动触发一次
  list                    列出任务与下次触发
  status                  全局队列/账本/软资源状态
  set-soft <name> <n>     设置软资源容量
  serve                   前台常驻调度循环（Ctrl-C 退出）
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from . import config, runlog
from .service import SchedulerService


def _svc() -> SchedulerService:
    return SchedulerService(config.load_settings())


def cmd_register(args) -> int:
    svc = _svc()
    try:
        r = svc.register_job(args.name)
    except Exception as e:
        print(f"注册失败：{e}")
        return 1
    print(f"注册成功：{r['job_id']}")
    for g in r["gates"]:
        print(f"  - {g}")
    return 0


def cmd_run(args) -> int:
    svc = _svc()
    svc.refresh()
    try:
        r = svc.trigger(args.name, trigger="manual")
    except Exception as e:
        print(f"触发失败：{e}")
        return 1
    print(f"已入队：{r['run_id']} ({r['job_id']}, priority={r['priority']})")
    return 0


def cmd_list(_args) -> int:
    svc = _svc()
    svc.refresh()
    for j in svc.list_jobs():
        print(f"{j['job_id']:<28} pri={j['priority']:<3} ok={j['ok']} next={j['next_run_at'] or '-'}")
    return 0


def cmd_status(_args) -> int:
    svc = _svc()
    svc.refresh()
    st = svc.status()
    print(f"jobs={st['jobs']} waiting={st['waiting']} running={st['running']}")
    print(f"ledger reserved_mem={st['ledger_reserved_mem_mb']}MB actual_mem={st['ledger_actual_mem_mb']}MB")
    for k, v in st["soft"].items():
        print(f"  soft[{k}] cap={v['cap']} used={v['used']}")
    return 0


def cmd_set_soft(args) -> int:
    svc = _svc()
    svc.state.soft_capacities[args.name] = int(args.n)
    svc.soft_pool.set_capacity(args.name, int(args.n))
    from . import persist
    persist.save_scheduler_state(svc.state)
    print(f"soft[{args.name}] = {args.n}")
    return 0


def cmd_serve(_args) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    svc = _svc()
    svc.start()
    print("调度器已启动（Ctrl-C 退出）")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n退出中…")
        svc.stop()
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="sched-v002")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("register"); sp.add_argument("name"); sp.set_defaults(fn=cmd_register)
    sp = sub.add_parser("run"); sp.add_argument("name"); sp.set_defaults(fn=cmd_run)
    sp = sub.add_parser("list"); sp.set_defaults(fn=cmd_list)
    sp = sub.add_parser("status"); sp.set_defaults(fn=cmd_status)
    sp = sub.add_parser("set-soft"); sp.add_argument("name"); sp.add_argument("n", type=int); sp.set_defaults(fn=cmd_set_soft)
    sp = sub.add_parser("serve"); sp.set_defaults(fn=cmd_serve)

    args = p.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())