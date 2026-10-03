"""管理台 HTTP 接口测试（FastAPI TestClient，不起端口、不起线程）。"""

from __future__ import annotations

import time

from fastapi.testclient import TestClient

from sched import config, runlog, web
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


PUBLIC_KEYS = {
    "timezone", "cooldown_sec", "max_resource_attempts",
    "resource_backoff_base_sec", "resource_backoff_cap_sec", "auto_admit_retry_sec",
}


def test_overview_exposes_only_user_facing_settings(home, make_settings):
    c = _client(make_settings(run_user=""))
    s = c.get("/api/overview").json()["settings"]
    assert set(s) == PUBLIC_KEYS
    # 引擎内部参数不出现在接口里（前端自然也无法展示/误改）
    assert "grace_sec" not in s and "kill_grace_sec" not in s
    assert "heartbeat_timeout_sec" not in s and "hot_reload_sec" not in s


def test_settings_update_hot_reloads_running_service(home, make_settings):
    settings = make_settings(run_user="")
    svc = SchedulerService(settings)
    c = TestClient(web.create_app(svc))
    r = c.post("/api/settings", json={
        "cooldown_sec": 120,
        "resource_backoff_base_sec": 60,
        "resource_backoff_cap_sec": 600,
        "auto_admit_retry_sec": 120,
        "timezone": "UTC",
    })
    assert r.status_code == 200, r.text
    body = r.json()["settings"]
    assert body["cooldown_sec"] == 120
    assert body["resource_backoff_base_sec"] == 60
    assert body["auto_admit_retry_sec"] == 120
    # 运行中的同进程 service 立即生效，无需重启
    assert svc.settings.runner.cooldown_sec == 120
    assert svc.admission_retry_sec == 120.0
    assert str(svc.engine._tz) == "UTC"
    # 已落盘：新实例读到的也是新值
    assert config.load_settings().runner.resource_backoff_cap_sec == 600


def test_settings_update_rejects_bad_values(home, make_settings):
    c = _client(make_settings(run_user=""))
    # 起始大于封顶
    assert c.post("/api/settings", json={
        "resource_backoff_base_sec": 900,
        "resource_backoff_cap_sec": 100,
    }).status_code == 400
    # 非法时区
    assert c.post("/api/settings", json={"timezone": "Mars/Olympus"}).status_code == 400
    # 过小的重试间隔
    assert c.post("/api/settings", json={"auto_admit_retry_sec": 1}).status_code == 400
    # 负数
    assert c.post("/api/settings", json={"cooldown_sec": -5}).status_code == 400


def test_queue_waiting_items_carry_last_failure(home, make_settings):
    import json
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
    # 资源类错误会自动退避重入队；若分类为逻辑错误，则手动再入队一次造等待项
    if not svc.state.wq:
        svc.trigger("flaky", trigger="manual")
    q = c.get("/api/queue").json()
    item = next(x for x in q["waiting"] if x["job_id"] == "flaky")
    assert item["last"]["status"] in ("failed", "resource")
    # 任务自己写进 state 的真实错误文本能透传到队列页（不再只有干巴巴的状态码）
    assert "timeout" in (item["last"]["message"] + item["last"]["reason"])
