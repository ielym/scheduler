import json

from sched import config, runlog, store


def test_json_roundtrip(home):
    p = home / "data" / "x.json"
    store.save_json(p, {"a": 1, "中文": "值"})
    assert store.load_json(p) == {"a": 1, "中文": "值"}


def test_jsonl_append_iter(home):
    p = home / "data" / "x.jsonl"
    store.append_jsonl(p, {"n": 1})
    store.append_jsonl(p, {"n": 2})
    rows = list(store.iter_jsonl(p))
    assert [r["n"] for r in rows] == [1, 2]


def test_jsonl_skips_broken_line(home):
    p = home / "data" / "x.jsonl"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text('{"n":1}\nthis is broken\n{"n":3}\n', encoding="utf-8")
    rows = list(store.iter_jsonl(p))
    assert [r["n"] for r in rows] == [1, 3]


def test_runlog_append_and_query(home):
    config.ensure_layout()
    rec = {
        "job_id": "j1", "run_id": "r1", "status": "success", "scheduled_at": runlog.now_iso(),
        "queued_at": runlog.now_iso(), "finished_at": runlog.now_iso(),
    }
    runlog.append_run(rec)
    rows = runlog.query_runs(job_id="j1", status="success")
    assert len(rows) == 1
    assert rows[0]["run_id"] == "r1"


def test_query_status_filter(home):
    config.ensure_layout()
    runlog.append_run({"job_id": "j1", "run_id": "a", "status": "success"})
    runlog.append_run({"job_id": "j1", "run_id": "b", "status": "failed"})
    assert len(runlog.query_runs(job_id="j1", status="success")) == 1
    assert len(runlog.query_runs(job_id="j1")) == 2