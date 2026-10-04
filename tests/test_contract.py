"""三闸准入测试。"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sched import config, contract as ctr_mod, manifest as m


def _min_doc():
    return m.TaskManifest(
        schema_version=3, name="t", description="t", run_mode="auto",
        entry=m.EntrySpec(file="run.py", interpreter="python3", args=[], cwd="."),
        priority=50, resources=m.ResourceDecl(cpu=0.1, memory_mb=16),
        error_rules=m.ErrorRules(), heartbeat=m.HeartbeatSpec(timeout_sec=60),
        runtime=m.RuntimeSpec(user=""),
    )


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_HOME", str(tmp_path))
    (tmp_path / "data" / "runtime").mkdir(parents=True)
    (tmp_path / "settings.json").write_text("{}")
    yield tmp_path


def _setup_task(tmp: Path) -> Path:
    d = tmp / "tasks" / "hello"
    d.mkdir(parents=True)
    manifest = _min_doc()
    (d / "manifest.json").write_text(manifest.model_dump_json())
    (d / "run.py").write_text("import sys; sys.exit(0)")
    return d


class TestGates:
    def test_three_gates_pass(self, env):
        task_dir = _setup_task(env)
        gates = ctr_mod.ContractGates()
        ok, results, profile = gates.run_all_gates(_min_doc(), task_dir)
        assert ok
        assert len(results) == 3
        assert all(r.ok for r in results)

    def test_gate1_rejects_missing_entry(self, env):
        task_dir = _setup_task(env)
        doc = _min_doc()
        doc.entry.file = "nonexistent.py"
        gates = ctr_mod.ContractGates()
        # 闸1（parse_manifest 内部）会因为 entry.file 不存在 raise
        # run_all_gates 调 parse_manifest → 捕获 ManifestError 并把 gate1 置 failed
        try:
            ok, results, _ = gates.run_all_gates(doc, task_dir)
        except m.ManifestError as e:
            # 闸1 捕获路径
            ok = False
        assert ok is False

    def test_gate2_rejects_bad_exit(self, env):
        task_dir = _setup_task(env)
        # 把 run.py 改成 exit 1
        (task_dir / "run.py").write_text("import sys; sys.exit(1)")
        gates = ctr_mod.ContractGates()
        ok, results, _ = gates.run_all_gates(_min_doc(), task_dir)
        assert ok is False
        # 闸2 失败
        gate2 = next(r for r in results if r.gate == "gate2_contract")
        assert gate2.ok is False
        assert "退出码非 0" in gate2.reason

    def test_gate3_rejects_declared_over_capacity(self, env):
        task_dir = _setup_task(env)
        doc = _min_doc()
        doc.resources.memory_mb = 1024 * 1024  # 夸张声明
        gates = ctr_mod.ContractGates()
        ok, results, _ = gates.run_all_gates(doc, task_dir)
        assert ok is False
        gate3 = next(r for r in results if r.gate == "gate3_capacity")
        assert gate3.ok is False


class TestParseManifest:
    def test_manifest_parses(self, env):
        task_dir = _setup_task(env)
        doc = m.load_manifest(task_dir)
        assert doc.priority == 50
        assert doc.run_mode == "auto"
        assert doc.resources.cpu == 0.1

    def test_old_task_json_rejected(self, env):
        (env / "tasks" / "legacy").mkdir(parents=True)
        (env / "tasks" / "legacy" / "task.json").write_text("{}")
        with pytest.raises(m.ManifestError):
            m.load_manifest(env / "tasks" / "legacy")

    def test_invalid_schema_version_rejected(self, env):
        (env / "tasks" / "badv").mkdir(parents=True)
        (env / "tasks" / "badv" / "manifest.json").write_text(json.dumps({"schema_version": 2,
                                                                           "entry": {"file": "run.py"}}))
        (env / "tasks" / "badv" / "run.py").write_text("exit()")
        with pytest.raises(m.ManifestError):
            m.load_manifest(env / "tasks" / "badv")

    def test_priority_out_of_range_rejected(self, env):
        (env / "tasks" / "badp").mkdir(parents=True)
        (env / "tasks" / "badp" / "manifest.json").write_text(json.dumps({
            "schema_version": 3, "name": "x", "run_mode": "auto",
            "priority": 200, "entry": {"file": "run.py", "interpreter": "python3"},
        }))
        (env / "tasks" / "badp" / "run.py").write_text("exit()")
        with pytest.raises(m.ManifestError):
            m.load_manifest(env / "tasks" / "badp")

    def test_path_escape_rejected(self, env):
        (env / "tasks" / "badpath").mkdir(parents=True)
        (env / "tasks" / "badpath" / "manifest.json").write_text(json.dumps({
            "schema_version": 3, "name": "x", "run_mode": "auto",
            "entry": {"file": "../../etc/passwd", "interpreter": "python3"},
        }))
        (env / "tasks" / "badpath" / "run.py").write_text("exit()")
        with pytest.raises(m.ManifestError):
            m.load_manifest(env / "tasks" / "badpath")
