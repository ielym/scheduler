"""测试共享夹具与任务脚手架。"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

# 用于测试的通用任务入口：参数经 task.json 的 env 注入（build_env 会透传到子进程）。
RUN_PY = '''\
import json, os, sys, time
from pathlib import Path
from sched_task_sdk import SchedTask, Step, run_task

TOTAL = int(os.environ.get("SCHED_TASK_TOTAL", "3"))
DRY_SLEEP = float(os.environ.get("SCHED_TASK_DRY_SLEEP", "0.01"))
REAL_SLEEP = float(os.environ.get("SCHED_TASK_REAL_SLEEP", "0.01"))


class Task(SchedTask):
    def steps(self, resume):
        if os.environ.get("SCHED_TASK_FAIL") == "1":
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
        "resources": resources or {"cpu": 0.1, "memory_mb": 64, "soft": {}},
        "error_rules": {"resource_regex": [], "logic_regex": []},
        "heartbeat": {"timeout_sec": 120},
        "smoke": {"mode": "dry", "max_runtime_sec": 60, "sample_size": 1},
        "runtime": {"user": "sched-run"},
        "diff_check": False,
        "contract_exempt": contract_exempt,
    }
    (task_dir / "task.json").write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
    return task_dir


@pytest.fixture
def home(tmp_path, monkeypatch):
    """每个测试独立的 SCHED_HOME 根目录。"""
    monkeypatch.setenv("SCHED_HOME", str(tmp_path))
    return tmp_path


@pytest.fixture
def make_settings():
    from sched_v002.config import Settings

    def _make(**kw):
        base = {"run_user": ""}  # 测试环境不降权
        base.update(kw)
        return Settings(**base)

    return _make