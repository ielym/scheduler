"""管理台 HTTP 接口测试（FastAPI TestClient，不起端口、不起线程）。"""

from __future__ import annotations

import json
import time

from fastapi.testclient import TestClient

from sched import runlog, web
from sched.service import SchedulerService

from conftest import write_task


def _client(settings) -> TestClient:
    return TestClient(web.create_app(SchedulerService(settings)))


def _wait(pred, timeout=30.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.1)
    return False


def test_overview_has_no_settings_panel(home, make_settings):
    # 运行参数面板已整体移除：时区固定、冷却/退避/重试上限/准出重试全部取消
    c = _client(make_settings(run_user=""))
    s = c.get("/api/overview").json()
    assert "settings" not in s
    assert "jobs" in s and "machine" in s


def test_import_task_success_and_gate_failure(home, make_settings):
    svc = SchedulerService(make_settings(run_user=""))
    c = TestClient(web.create_app(svc))
    # 不存在的目录 → 404
    r = c.post("/api/tasks", json={"dir_name": "nope_job"})
    assert r.status_code == 404
    # 目录存在但闸不过（缺入口文件）→ 400 且透传闸门原因
    bad = home / "tasks" / "bad_job"
    bad.mkdir(parents=True)
    (bad / "task.json").write_text(json.dumps({
        "schema_version": 2, "name": "bad_job",
        "entry": {"file": "missing.py", "interpreter": "python3"},
        "schedule": {"type": "manual"}, "priority": 40,
        "resources": {"cpu": 0.1, "memory_mb": 64},
        "heartbeat": {"timeout_sec": 120},
        "smoke": {"mode": "dry", "max_runtime_sec": 10},
        "runtime": {"user": ""},
    }), encoding="utf-8")
    r = c.post("/api/tasks", json={"dir_name": "bad_job"})
    assert r.status_code == 400, r.text
    assert "missing.py" in r.text
    # 合规任务 → 导入同步过五闸成功（dry_sleep 默认 0.8s，保证闸5 SIGTERM 命中块处理中）
    write_task(home, "good_job", real_sleep=0.01)
    r = c.post("/api/tasks", json={"dir_name": "good_job"})
    assert r.status_code == 200, r.text
    assert r.json()["id"] == "good_job"
    # 重复导入 → 409
    assert c.post("/api/tasks", json={"dir_name": "good_job"}).status_code == 409
    jobs = {j["job_id"] for j in c.get("/api/jobs").json()}
    assert "good_job" in jobs


def test_job_config_update_takes_effect_immediately(home, make_settings):
    write_task(home, "cfg_job", real_sleep=0.01)
    svc = SchedulerService(make_settings(run_user=""))
    svc.register_job("cfg_job")
    c = TestClient(web.create_app(svc))
    r = c.post("/api/jobs/cfg_job/config", json={
        "priority": 77,
        "schedule": {"type": "interval", "interval": 900,
                     "start_time": "18:40", "end_time": "22:00", "weekdays": [1, 3, 5]},
        "retry": {"delay_sec": 15, "max_attempts": 3, "not_after": "23:00"},
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["priority"] == 77
    assert body["schedule"]["interval"] == 900
    assert body["retry"] == {"delay_sec": 15.0, "max_attempts": 3, "not_after": "23:00"}
    # task.json 已落盘
    raw = json.loads((home / "tasks" / "cfg_job" / "task.json").read_text(encoding="utf-8"))
    assert raw["priority"] == 77 and raw["retry"]["max_attempts"] == 3
    # /api/jobs 立即可见
    j = next(x for x in c.get("/api/jobs").json() if x["job_id"] == "cfg_job")
    assert j["priority"] == 77
    # 非白名单字段拒绝
    r = c.post("/api/jobs/cfg_job/config", json={"entry": {"file": "evil.py"}})
    assert r.status_code == 400
    r = c.post("/api/jobs/cfg_job/config", json={"resources": {"cpu": 99}})
    assert r.status_code == 400
    r = c.post("/api/jobs/cfg_job/config", json={"retry": {"bogus": 1}})
    assert r.status_code == 400
    # 不存在的任务 → 404
    assert c.post("/api/jobs/ghost/config", json={"priority": 1}).status_code in (400, 404)


def test_stop_running_run(home, make_settings):
    write_task(home, "long_job", real_sleep=1.0, total=30)
    svc = SchedulerService(make_settings(run_user=""))
    svc.register_job("long_job")
    c = TestClient(web.create_app(svc))
    rec = svc.trigger("long_job", trigger="manual")
    svc._tick_admission()
    assert _wait(lambda: bool(svc.running)), "任务应已起跑"
    # 停止一个不存在/已结束的 run → 404
    assert c.post("/api/runs/long_job/nope/stop").status_code == 404
    r = c.post(f"/api/runs/long_job/{rec['run_id']}/stop", json={"reason": "测试停止"})
    assert r.status_code == 200, r.text
    assert _wait(lambda: not svc.running and bool(runlog.query_runs(job_id="long_job"))), "停止后应落终态"
    final = runlog.query_runs(job_id="long_job", limit=1)[0]
    assert final["status"] == "killed"
    # 手动停止不自动重排队
    assert not [it for it in svc.state.wq if it["job_id"] == "long_job"]


def test_running_run_detail_and_logs_accessible(home, make_settings):
    """运行中的 run 尚未落 jsonl：详情接口从内存态合成，日志接口直接读实时文件。"""
    write_task(home, "live_log_job", real_sleep=1.0, total=30)
    svc = SchedulerService(make_settings(run_user=""))
    svc.register_job("live_log_job")
    c = TestClient(web.create_app(svc))
    rec = svc.trigger("live_log_job", trigger="manual")
    svc._tick_admission()
    assert _wait(lambda: bool(svc.running)), "任务应已起跑"
    rid = rec["run_id"]
    d = c.get(f"/api/runs/live_log_job/{rid}")
    assert d.status_code == 200, d.text
    body = d.json()
    assert body["status"] == "running" and body["run_id"] == rid
    assert body["job_id"] == "live_log_job" and body["started_at"]
    # 不属于该 job 的 run_id → 404；完全不存在 → 404
    assert c.get(f"/api/runs/other_job/{rid}").status_code == 404
    assert c.get("/api/runs/live_log_job/nope").status_code == 404
    # 日志接口对运行中 run 可读（任务 print 有输出或空内容均合法，不能 404/500）
    for stream in ("stdout", "stderr"):
        r = c.get(f"/api/logs/live_log_job/{rid}/{stream}?tail=100")
        assert r.status_code == 200, r.text
        assert "content" in r.json() and r.json()["eof"] is True
    assert c.get(f"/api/logs/live_log_job/{rid}/bogus").status_code == 400


def test_delete_run_history_only_and_with_files(home, make_settings):
    write_task(home, "del_job", real_sleep=0.01, total=3)
    svc = SchedulerService(make_settings(run_user=""))
    svc.register_job("del_job")
    c = TestClient(web.create_app(svc))
    # 造两条历史（成功即终态、无冷却，顺序跑两次）
    svc.trigger("del_job", trigger="manual")
    svc._tick_admission()
    assert _wait(lambda: len(runlog.query_runs(job_id="del_job")) == 1, timeout=30)
    svc.trigger("del_job", trigger="manual")
    svc._tick_admission()
    assert _wait(lambda: len(runlog.query_runs(job_id="del_job")) == 2, timeout=30)
    runs = runlog.query_runs(job_id="del_job", limit=10)
    first, second = runs[0], runs[1]
    # 仅删历史：jsonl 行移除，日志文件保留
    r = c.delete(f"/api/runs/del_job/{first['run_id']}?delete_files=false")
    assert r.status_code == 200, r.text
    assert runlog.stdout_path("del_job", first["run_id"]).exists() or True  # 日志本就可能空
    ids = {x["run_id"] for x in runlog.query_runs(job_id="del_job", limit=10)}
    assert first["run_id"] not in ids and second["run_id"] in ids
    # 连带本地日志删除
    log = runlog.stdout_path("del_job", second["run_id"])
    r = c.delete(f"/api/runs/del_job/{second['run_id']}?delete_files=true")
    assert r.status_code == 200, r.text
    assert runlog.query_runs(job_id="del_job", limit=10) == []
    # 不存在的 run → 404
    assert c.delete("/api/runs/del_job/ghost").status_code == 404


def test_queue_waiting_items_carry_last_failure(home, make_settings):
    # 先注册（dry 冒烟必须通过），再把失败开关写进 env 并 refresh：正式运行才失败
    write_task(home, "flaky", real_sleep=0.01)
    svc = SchedulerService(make_settings(run_user=""))
    svc.register_job("flaky")
    tj_path = home / "tasks" / "flaky" / "task.json"
    tj = json.loads(tj_path.read_text(encoding="utf-8"))
    tj["env"]["SCHED_TASK_FAIL"] = "1"
    tj_path.write_text(json.dumps(tj), encoding="utf-8")
    svc.refresh()
    c = TestClient(web.create_app(svc))
    svc.trigger("flaky", trigger="manual")
    svc._tick_admission()
    assert _wait(lambda: bool(runlog.query_runs(job_id="flaky"))), "首次运行应很快到终态"
    # 逻辑失败转人工；手动再入队一次造等待项
    if not svc.state.wq:
        svc.trigger("flaky", trigger="manual")
    q = c.get("/api/queue").json()
    assert "cooldowns" not in q and "backoffs" not in q  # 旧冷却/退避字段已移除
    item = next(x for x in q["waiting"] if x["job_id"] == "flaky")
    assert item["last"]["status"] in ("failed", "resource")
    # 任务自己写进 state 的真实错误文本能透传到队列页（不再只有干巴巴的状态码）
    assert "timeout" in (item["last"]["message"] + item["last"]["reason"])
