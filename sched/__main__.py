"""命令行入口：sched <command>。

命令：
  register <name>        立即对一个任务跑上线检查（五道闸）并纳入调度
                         （常规无需使用：任务文件放进 tasks/ 后调度器会自动检查上线）
  run <name>             立即运行一次（含声明不可断点恢复的任务的人工触发通道）
  list                    列出任务与下次触发
  status                  队列/账本/上线检查状态
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
        print(f"上线检查未通过：{e}")
        return 1
    print(f"上线检查通过，已纳入调度：{r['job_id']}")
    for g in r["gates"]:
        print(f"  - {g}")
    return 0


def cmd_run(args) -> int:
    svc = _svc()
    svc.refresh()
    try:
        r = svc.trigger(args.name, trigger="manual")
    except Exception as e:
        print(f"运行失败：{e}")
        return 1
    print(f"已加入运行队列：{r['run_id']} ({r['job_id']}, priority={r['priority']})")
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
    print(f"任务数={st['jobs']} 等待运行={st['waiting']} 运行中={st['running']}")
    print(f"账本预留内存={st['ledger_reserved_mem_mb']}MB 实测内存={st['ledger_actual_mem_mb']}MB")
    adm = st.get("admission") or {}
    if adm.get("checking") or adm.get("failed"):
        print(f"上线检查：检查中 {adm.get('checking', 0)} 个，未通过 {adm.get('failed', 0)} 个")
        for name, rec in svc.admission_snapshot().items():
            if rec.get("status") == "failed":
                print(f"  [未通过] {name}：{rec.get('reason', '')}")
    return 0


def cmd_serve(args) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    svc = _svc()
    if getattr(args, "web", False):
        svc.settings.web.enabled = True
    if getattr(args, "no_web", False):
        svc.settings.web.enabled = False
    svc._web_enabled = bool(svc.settings.web.enabled)
    if svc._web_enabled:
        from . import web
        try:
            web.validate_public_listen(svc.settings.web)
        except RuntimeError as e:
            print(f"启动失败：{e}", file=sys.stderr)
            return 1
    try:
        svc.start()
    except RuntimeError as e:
        # 单实例锁冲突等启动期错误：面向运维的友好报错，而非 traceback
        print(f"启动失败：{e}", file=sys.stderr)
        return 1
    print("调度器已启动（Ctrl-C 退出）")
    if svc._web_enabled:
        print(f"管理台：http://{svc.settings.web.host}:{svc.settings.web.port}/")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n退出中…")
        svc.stop()
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="sched")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("register"); sp.add_argument("name"); sp.set_defaults(fn=cmd_register)
    sp = sub.add_parser("run"); sp.add_argument("name"); sp.set_defaults(fn=cmd_run)
    sp = sub.add_parser("list"); sp.set_defaults(fn=cmd_list)
    sp = sub.add_parser("status"); sp.set_defaults(fn=cmd_status)
    sp = sub.add_parser("serve")
    sp.add_argument("--web", action="store_true", help="同时启动内嵌 Web 管理台（覆盖 settings.web.enabled）")
    sp.add_argument("--no-web", dest="no_web", action="store_true", help="强制不启动管理台")
    sp.set_defaults(fn=cmd_serve)

    args = p.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())