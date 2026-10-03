import json

import pytest

from sched import taskdoc


def _write(root, name, payload, run_py="print('ok')\n"):
    d = root / name
    d.mkdir(parents=True, exist_ok=True)
    (d / "run.py").write_text(run_py)
    (d / "task.json").write_text(json.dumps(payload))
    return d


MINIMAL = {
    "schema_version": 2,
    "entry": {"file": "run.py", "interpreter": "python3", "args": [], "cwd": "."},
    "schedule": {"type": "manual"},
    "resources": {"cpu": 0.1, "memory_mb": 64},
}


def test_valid_minimal(tmp_path):
    d = _write(tmp_path, "ok_task", MINIMAL)
    doc = taskdoc.load_task_doc(d)
    assert doc.schema_version == 2
    assert doc.priority == 0


def test_invalid_task_id(tmp_path):
    _write(tmp_path, "_bad", MINIMAL)
    with pytest.raises(taskdoc.TaskDocError):
        taskdoc.load_task_doc(tmp_path / "_bad")


def test_missing_entry_file(tmp_path):
    payload = dict(MINIMAL, entry={"file": "nope.py", "interpreter": "python3", "args": [], "cwd": "."})
    _write(tmp_path, "missing", payload)
    with pytest.raises(taskdoc.TaskDocError) as e:
        taskdoc.load_task_doc(tmp_path / "missing")
    assert any("不存在" in m for m in e.value.errors)


def test_path_traversal_rejected(tmp_path):
    payload = dict(MINIMAL, entry={"file": "../run.py", "interpreter": "python3", "args": [], "cwd": "."})
    _write(tmp_path, "trav", payload)
    with pytest.raises(taskdoc.TaskDocError):
        taskdoc.load_task_doc(tmp_path / "trav")


def test_interval_requires_positive(tmp_path):
    payload = dict(MINIMAL, schedule={"type": "interval", "interval": 0})
    d = _write(tmp_path, "iv", payload)
    with pytest.raises(taskdoc.TaskDocError):
        taskdoc.load_task_doc(d)


def test_contract_exempt_manual_only(tmp_path):
    payload = dict(MINIMAL, contract_exempt=True, schedule={"type": "interval", "interval": 60})
    d = _write(tmp_path, "ex", payload)
    with pytest.raises(taskdoc.TaskDocError) as e:
        taskdoc.load_task_doc(d)
    assert any("manual" in m for m in e.value.errors)


def test_priority_range(tmp_path):
    payload = dict(MINIMAL, priority=101)
    d = _write(tmp_path, "pri", payload)
    with pytest.raises(taskdoc.TaskDocError):
        taskdoc.load_task_doc(d)


def test_v1_schema_accepted(tmp_path):
    payload = dict(MINIMAL, schema_version=1)
    d = _write(tmp_path, "v1", payload)
    doc = taskdoc.load_task_doc(d)
    assert doc.schema_version == 1