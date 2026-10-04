"""双闸准入。

闸1 静态 — manifest.json schema 合法 + entry 存在 + 路径约束（在 manifest.py parse_manifest 完成）
闸2 契约可执行 — dry 跑通，exit 0

运行期断点硬契约（D1，daemon/oneshot 同等 SIGTERM → 落断点 → exit 99）由 SDK 保证。
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import cgroup as cg
from . import manifest as manifest_mod
from . import runner


CONTRACT_PROBE_ENV = "SCHED_CONTRACT_PROBE"


@dataclass
class GateResult:
    gate: str
    ok: bool
    reason: str = ""
    artifacts: dict[str, Any] = field(default_factory=dict)


def _spawn_bounded(doc: manifest_mod.TaskManifest, task_dir: Path,
                   signal_after_sec: float | None = None,
                   timeout_sec: int = 60,
                   memory_high: int | None = None,
                   memory_max: int | None = None,
                   cpu_quota: float | None = None,
                   run_user: str | None = None,
                   env_extra: dict[str, str] | None = None,
                   run_id: str = "probe") -> dict:
    """dry 跑一次 run.py，只关心 exit_code（闸2 契约可执行）。"""
    argv = runner.build_argv(task_dir, doc.entry.file, doc.entry.interpreter,
                             list(doc.entry.args))
    env = runner.build_env(
        task_dir, doc.name or task_dir.name, run_id, run_id, "contract",
        1, time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        config=None,
        extra={}, run_user=run_user,
        error_rules={"resource_regex": doc.error_rules.resource_regex,
                     "logic_regex": doc.error_rules.logic_regex},
    )
    if env_extra:
        env.update(env_extra)
    workdir = task_dir if (doc.entry.cwd or ".") == "." else task_dir / doc.entry.cwd

    cgrp = cg.new_cgroup(f"contract/{run_id}")
    if cgrp.available:
        cgrp.set_limits(memory_high, memory_max, cpu_quota)

    exit_code = None
    signaled = False
    try:
        proc = subprocess.Popen(
            argv, cwd=str(workdir), env=env, stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True, close_fds=True,
            preexec_fn=(lambda: runner._drop_privileges(run_user)) if run_user else None,
        )
        if cgrp.available:
            cgrp.add_process(proc.pid)
        if signal_after_sec is not None:
            time.sleep(signal_after_sec)
            try:
                os.killpg(proc.pid, signal.SIGTERM)
                signaled = True
            except OSError:
                pass
        try:
            exit_code = proc.wait(timeout=timeout_sec)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except OSError:
                pass
            try:
                exit_code = proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
    finally:
        cgrp.remove()
    return {"exit_code": exit_code, "signaled": signaled}


class ContractGates:
    def __init__(self, settings=None):
        self.settings = settings

    def gate_static(self, doc: manifest_mod.TaskManifest, task_dir: Path) -> GateResult:
        # parse_manifest 已做 schema + entry.file 存在 + 路径约束
        return GateResult("gate1_static", True, "manifest schema 合法（闸1）")

    def gate_contract(self, doc: manifest_mod.TaskManifest, task_dir: Path) -> GateResult:
        """闸2 契约可执行：dry 跑通 exit_code=0。"""
        run_user = (doc.runtime.user or "") if doc.runtime else ""
        run_user = run_user or (self.settings.run_user or "") if self.settings else ""
        res = _spawn_bounded(doc, task_dir, timeout_sec=30, run_user=run_user,
                             env_extra={CONTRACT_PROBE_ENV: "1"}, run_id="probe")
        if res["exit_code"] != 0:
            return GateResult("gate2_contract", False,
                              f"契约自检退出码非 0：{res['exit_code']}")
        return GateResult("gate2_contract", True, "契约自检通过（闸2）")

    def run_all_gates(self, doc: manifest_mod.TaskManifest, task_dir: Path) -> tuple[bool, list[GateResult]]:
        """跑双闸：静态 → 契约。"""
        results: list[GateResult] = []
        results.append(self.gate_static(doc, task_dir))
        results.append(self.gate_contract(doc, task_dir))
        ok = all(r.ok for r in results)
        return ok, results
