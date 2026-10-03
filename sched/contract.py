"""L0 契约层：五道闸（注册/提交前依次通过，任一未过则拒绝加入调度）。

闸1 静态闸   ：task.json schema v2 + 入口存在 + 路径约束 + contract_exempt 规则
闸2 契约闸   ：contract probe —— 入口可执行且写出合法 state.json（证明遵守 SDK 契约）
闸3 冒烟闸   ：dry/real 冒烟，测量资源峰值（防打爆的第一道预估）
闸4 容量闸   ：预估 > 机器硬上限 → 硬拒绝（需求 2.1.1）
闸5 断点闸   ：跑→SIGTERM→验证 exit 99 + resume_point → 恢复跑（证明可抢占，路径 B）
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
from . import errors, resources, runner, taskdoc

CONTRACT_PROBE_ENV = "SCHED_CONTRACT_PROBE"


@dataclass
class GateResult:
    gate: str
    ok: bool
    reason: str = ""
    artifacts: dict[str, Any] = field(default_factory=dict)


def _spawn_bounded(doc: taskdoc.TaskDoc, task_dir: Path, run_id: str, env_extra: dict[str, str], timeout_sec: int,
                   signal_after_sec: float | None = None, memory_high: int | None = None,
                   memory_max: int | None = None, cpu_quota: float | None = None,
                   run_user: str | None = None) -> dict:
    """拉起入口，返回 {exit_code, memory_peak_mb, cpu_usec, timed_out, signaled}。

    F6：注册期闸门与正式运行同等降权（run_user 非空时 preexec 降权，失败即闸失败）。
    """
    argv = runner.build_argv(task_dir, doc.entry.file, doc.entry.interpreter, list(doc.entry.args))
    env = runner.build_env(task_dir, "contract", run_id, "contract", 1, time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                           dict(doc.env), dry_run=(env_extra.get("SCHED_DRY_RUN") == "1"),
                           run_user=run_user,
                           error_rules={"resource_regex": doc.error_rules.resource_regex,
                                        "logic_regex": doc.error_rules.logic_regex})
    env.update(env_extra)
    workdir = task_dir if (doc.entry.cwd or ".") == "." else task_dir / doc.entry.cwd

    cgrp = cg.new_cgroup(f"contract/{run_id}")
    if cgrp.available:
        cgrp.set_limits(memory_high, memory_max, cpu_quota)

    exit_code = None
    timed_out = False
    signaled = False
    t0 = time.monotonic()
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
            timed_out = True
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except OSError:
                pass
            try:
                exit_code = proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        # 给 cgroup 统计一点落盘时间
        time.sleep(0.1)
    finally:
        mem_peak = (cgrp.read_peak_mem() or 0) // (1024 * 1024)
        cpu_usec = cgrp.read_cpu_usage_usec() or 0
        cgrp.remove()
    return {
        "exit_code": exit_code,
        "memory_peak_mb": mem_peak,
        "cpu_usec": cpu_usec,
        "cpu_sec": round(cpu_usec / 1_000_000, 3),
        "elapsed_sec": round(time.monotonic() - t0, 2),
        "timed_out": timed_out,
        "signaled": signaled,
    }


def _read_state(task_dir: Path, run_id: str = "") -> dict:
    """读实例级状态并叠加任务级断点（实例级 + 任务级两级布局）。

    实例级：cache/runs/<run_id>/state.json（回落 legacy cache/state.json）；
    resume_point 以 cache/resume_point.json 为准（任务级断点跨运行共享）。
    """
    import json

    def _load(p: Path) -> dict:
        if not p.exists():
            return {}
        try:
            obj = json.loads(p.read_text("utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
        return obj if isinstance(obj, dict) else {}

    st: dict = {}
    if run_id:
        st = _load(task_dir / "cache" / "runs" / run_id / "state.json")
    if not st:
        st = _load(task_dir / "cache" / "state.json")
    rp = _load(task_dir / "cache" / "resume_point.json").get("resume_point")
    if isinstance(rp, dict) and rp:
        st["resume_point"] = rp
    return st


def _state_written_since(task_dir: Path, run_id: str, since_ts: float) -> bool:
    """run 开始后任务是否写过 state.json（实例级或 legacy 任一更新即算）。

    心跳契约：区分「只在 SCHED_CONTRACT_PROBE 环境写状态的伪契约任务」——
    这类任务正式运行期不写状态，120s 后会被 heartbeat_kill 且不可恢复。
    """
    candidates = [task_dir / "cache" / "runs" / run_id / "state.json",
                  task_dir / "cache" / "state.json"]
    for p in candidates:
        try:
            if p.exists() and p.stat().st_mtime >= since_ts - 1.0:
                return True
        except OSError:
            continue
    return False


class ContractGates:
    def __init__(self, settings=None):
        from . import config

        self.settings = settings or config.load_settings()
        self.res_cfg = self.settings.resource
        self.capacity = resources.Capacity()

    def gate_static(self, doc: taskdoc.TaskDoc, task_dir: Path) -> GateResult:
        # parse_task_json 已做 schema/入口/路径校验；此处补 contract_exempt 规则复核
        if doc.contract_exempt and doc.schedule.type != "manual":
            return GateResult("gate1_static", False, "contract_exempt=true 只能 schedule.type=manual")
        return GateResult("gate1_static", True, "schema v2 合法")

    def gate_contract(self, doc: taskdoc.TaskDoc, task_dir: Path) -> GateResult:
        res = _spawn_bounded(doc, task_dir, "probe", {CONTRACT_PROBE_ENV: "1"}, timeout_sec=30,
                             run_user=self.settings.run_user or None)
        if res["exit_code"] != 0:
            return GateResult("gate2_contract", False, f"契约自检退出码非 0：{res['exit_code']}", {"probe": res})
        st = _read_state(task_dir, "probe")
        need = ("status", "current_step", "schema_version", "resume_point")
        missing = [k for k in need if k not in st]
        if missing:
            return GateResult("gate2_contract", False, f"state.json 缺失契约字段：{missing}", {"state": st})
        return GateResult("gate2_contract", True, "契约自检通过", {"state": st})

    def gate_smoke(self, doc: taskdoc.TaskDoc, task_dir: Path) -> GateResult:
        dry = "1" if doc.smoke.mode == "dry" else "0"
        res = _spawn_bounded(
            doc, task_dir, "smoke", {"SCHED_DRY_RUN": dry},
            timeout_sec=doc.smoke.max_runtime_sec,
            memory_high=int(self.capacity.total_memory_mb * 1024 * 1024 * 0.9),
            memory_max=int(self.capacity.total_memory_mb * 1024 * 1024),
            cpu_quota=self.capacity.total_cpu,
            run_user=self.settings.run_user or None,
        )
        if res["timed_out"]:
            return GateResult("gate3_smoke", False, f"冒烟超时（>{doc.smoke.max_runtime_sec}s）", {"smoke": res})
        if res["exit_code"] not in (0, errors.EXIT_PREEMPTED):
            return GateResult("gate3_smoke", False, f"冒烟退出码异常：{res['exit_code']}", {"smoke": res})
        return GateResult("gate3_smoke", True, "冒烟通过", {"smoke": res})

    def gate_capacity(self, doc: taskdoc.TaskDoc, task_dir: Path, measured_mem_mb: int = 0) -> GateResult:
        declared = doc.resources.memory_mb
        est_mem = measured_mem_mb or declared or 0
        if declared and declared > self.capacity.total_memory_mb:
            return GateResult("gate4_capacity", False,
                              f"声明内存 {declared}MB > 机器上限 {self.capacity.total_memory_mb}MB，拒绝加入")
        if measured_mem_mb and measured_mem_mb > self.capacity.total_memory_mb:
            return GateResult("gate4_capacity", False,
                              f"冒烟实测峰值 {measured_mem_mb}MB > 机器上限 {self.capacity.total_memory_mb}MB，拒绝加入")
        if est_mem == 0:
            return GateResult("gate4_capacity", False, "资源预估未知（未声明且冒烟未测得），拒绝加入")
        if doc.resources.cpu > self.capacity.total_cpu:
            return GateResult("gate4_capacity", False,
                              f"声明 CPU {doc.resources.cpu} 核 > 机器上限 {self.capacity.total_cpu} 核")
        return GateResult("gate4_capacity", True, f"容量通过（预估内存 {est_mem}MB）", {"est_mem_mb": est_mem})

    def gate_checkpoint(self, doc: taskdoc.TaskDoc, task_dir: Path) -> GateResult:
        # 1) 跑一段然后 SIGTERM
        t_start = time.time()
        res1 = _spawn_bounded(
            doc, task_dir, "ckpt1", {"SCHED_DRY_RUN": "1"},
            timeout_sec=60, signal_after_sec=doc.smoke.signal_after_sec,
            memory_max=int(self.capacity.total_memory_mb * 1024 * 1024),
            cpu_quota=self.capacity.total_cpu,
            run_user=self.settings.run_user or None,
        )
        st1 = _read_state(task_dir, "ckpt1")
        if res1["exit_code"] != errors.EXIT_PREEMPTED and st1.get("status") != "preempted":
            return GateResult("gate5_checkpoint", False,
                              f"打断后未按 99/抢占语义退出（exit={res1['exit_code']}, status={st1.get('status')}）",
                              {"run": res1, "state": st1})
        # 运行期心跳契约：ckpt1 期间必须写过 state.json（SDK 由心跳线程保证；
        # 自研任务需在运行期产出状态），否则正式运行会在心跳超时后被杀且不可恢复
        if not _state_written_since(task_dir, "ckpt1", t_start):
            return GateResult("gate5_checkpoint", False,
                              "运行期心跳契约不满足：正式运行未写 state.json"
                              "（SDK 任务由心跳线程保证；自研任务需周期性更新状态文件）",
                              {"run": res1, "state": st1})
        rp1 = st1.get("resume_point") or {}
        if not isinstance(rp1, dict):
            return GateResult("gate5_checkpoint", False, "resume_point 不是对象", {"state": st1})
        # 2) 恢复跑（读 resume_point 继续），要求正常收敛
        res2 = _spawn_bounded(
            doc, task_dir, "ckpt2", {"SCHED_DRY_RUN": "1"},
            timeout_sec=60,
            memory_max=int(self.capacity.total_memory_mb * 1024 * 1024),
            cpu_quota=self.capacity.total_cpu,
            run_user=self.settings.run_user or None,
        )
        st2 = _read_state(task_dir, "ckpt2")
        if res2["exit_code"] != 0 or st2.get("status") not in ("success", "intentional_exit"):
            return GateResult("gate5_checkpoint", False,
                              f"恢复跑未正常收尾（exit={res2['exit_code']}, status={st2.get('status')}）",
                              {"resume": res2, "state": st2})
        return GateResult("gate5_checkpoint", True, "断点续跑通过（可抢占）",
                          {"resume_point": rp1, "resume": res2})

    def run_all(self, doc: taskdoc.TaskDoc, task_dir: Path) -> list[GateResult]:
        results: list[GateResult] = []
        results.append(self.gate_static(doc, task_dir))
        results.append(self.gate_contract(doc, task_dir))
        smoke = self.gate_smoke(doc, task_dir)
        results.append(smoke)
        measured = (smoke.artifacts.get("smoke") or {}).get("memory_peak_mb", 0) if smoke.ok else 0
        results.append(self.gate_capacity(doc, task_dir, measured_mem_mb=int(measured)))
        if not doc.contract_exempt:
            results.append(self.gate_checkpoint(doc, task_dir))
        return results


def run_gates(doc: taskdoc.TaskDoc, task_dir: Path, settings=None) -> tuple[bool, list[GateResult], dict[str, Any]]:
    gates = ContractGates(settings)
    results = gates.run_all(doc, task_dir)
    ok = all(r.ok for r in results)
    profile = {}
    for r in results:
        if r.gate == "gate4_capacity" and r.ok:
            profile["est_mem_mb"] = r.artifacts.get("est_mem_mb", 0)
        if r.gate == "gate3_smoke" and r.ok:
            smoke = r.artifacts.get("smoke") or {}
            elapsed = float(smoke.get("elapsed_sec") or 0)
            if elapsed > 0:
                # 冒烟实测平均核数 = CPU-秒 / 时长（画像 CPU 样本单位是「核数」）
                profile["est_cpu"] = round(float(smoke.get("cpu_sec") or 0) / elapsed, 3)
    return ok, results, profile