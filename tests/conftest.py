"""测试共享夹具与任务脚手架。"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest

# 用于测试的通用任务入口：参数经 task.json 的 env 注入（build_env 会透传到子进程）。
# 故障注入一律仅在真实运行（not dry_run）生效，保证注册冒烟（闸5 dry）永远能过。
RUN_PY = '''\
import json, os, sys, time
from pathlib import Path
from sched_task_sdk import SchedTask, Step, run_task

TOTAL = int(os.environ.get("SCHED_TASK_TOTAL", "3"))
DRY_SLEEP = float(os.environ.get("SCHED_TASK_DRY_SLEEP", "0.01"))
REAL_SLEEP = float(os.environ.get("SCHED_TASK_REAL_SLEEP", "0.01"))


class Task(SchedTask):
    def steps(self, resume):
        if not self.dry_run:
            hint = os.environ.get("SCHED_TASK_RETRY_HINT")
            if hint:
                self.set_retry_hint(**json.loads(hint))
            if os.environ.get("SCHED_TASK_HB_STALL") == "1":
                self._stop_heartbeat()  # 模拟真卡死（死锁/GIL 卡死）：心跳也停
                time.sleep(30)  # 不写心跳，由调度器心跳超时兜底
            code = os.environ.get("SCHED_TASK_EXIT_CODE")
            if code:
                self.state.flush()
                sys.exit(int(code))
        if not self.dry_run and os.environ.get("SCHED_TASK_FAIL") == "1":
            raise RuntimeError("connection timeout simulated")
        start = int((resume or {}).get("index", 0))
        for i in range(start, TOTAL):
            yield Step("process", {"index": i})
            time.sleep(DRY_SLEEP if self.dry_run else REAL_SLEEP)
            self.state.resume_point = {"index": i + 1}
            self.state.flush()
        out = Path(self.workspace) / "results"
        out.mkdir(parents=True, exist_ok=True)
        (out / "final.json").write_text(json.dumps({"processed": TOTAL}))


if __name__ == "__main__":
    sys.exit(run_task(Task()))
'''


def write_task(
    root: Path,
    name: str = "sample_task",
    *,
    dry_sleep: float = 0.8,
    real_sleep: float = 0.02,
    total: int = 3,
    priority: int = 50,
    resources: dict | None = None,
    contract_exempt: bool = False,
    extra_env: dict | None = None,
    retry: dict | None = None,
    heartbeat_timeout: int | None = None,
    logic_regex: list[str] | None = None,
) -> Path:
    """在 root/tasks/<name>/ 下写一个可跑五道闸与正常运行的测试任务。"""
    task_dir = root / "tasks" / name
    task_dir.mkdir(parents=True, exist_ok=True)
    (task_dir / "run.py").write_text(RUN_PY, encoding="utf-8")

    env = {
        "SCHED_TASK_TOTAL": str(total),
        "SCHED_TASK_DRY_SLEEP": str(dry_sleep),
        "SCHED_TASK_REAL_SLEEP": str(real_sleep),
    }
    env.update(extra_env or {})

    doc = {
        "schema_version": 2,
        "name": name,
        "description": "生成于测试的示例任务",
        "entry": {"file": "run.py", "interpreter": "python3", "args": [], "cwd": "."},
        "schedule": {"type": "manual", "max_instances": 1, "overflow": "queue"},
        "priority": priority,
        "env": env,
        "outputs": {"expect": ["results/final.json"]},
        "resources": resources or {"cpu": 0.1, "memory_mb": 64},
        "error_rules": {"resource_regex": [], "logic_regex": logic_regex or []},
        "heartbeat": {"timeout_sec": heartbeat_timeout or 120},
        "smoke": {"mode": "dry", "max_runtime_sec": 60},
        "runtime": {"user": ""},  # 测试环境不降权（置空 = 以调度器身份执行）
        "contract_exempt": contract_exempt,
    }
    if retry is not None:
        doc["retry"] = retry
    (task_dir / "task.json").write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
    return task_dir


@pytest.fixture
def home(tmp_path, monkeypatch):
    """每个测试独立的 SCHED_HOME 根目录。

    fixture 撤销前强制 join 测试期间新建的全部线程——worker 在
    running.pop 之后才落盘终态（append_run/save_active），若按 running
    追踪会漏掉「已 pop 但仍在写盘」的线程；monkeypatch 撤销后这些线程
    会把测试数据写进生产 data/ 目录（本轮审查发现的实际污染来源）。
    """
    before = set(threading.enumerate())
    monkeypatch.setenv("SCHED_HOME", str(tmp_path))
    yield tmp_path
    from sched.service import SchedulerService

    for svc in list(SchedulerService._instances):
        try:
            svc._stopping.set()
        except Exception:
            continue
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        alive = [t for t in threading.enumerate()
                 if t is not threading.current_thread() and t not in before and t.is_alive()]
        if not alive:
            break
        for t in alive:
            t.join(timeout=max(0.1, deadline - time.monotonic()))


@pytest.fixture
def make_settings():
    from sched.config import Settings

    def _make(**kw):
        base = {"run_user": ""}  # 测试环境不降权
        base.update(kw)
        return Settings(**base)

    return _make