# sched — 本地子任务守护器

> 进程意图驱动 · 固定控制协议 · 全局资源协调 · 三闸准入 · FastAPI 内嵌管理台

sched 是常驻单进程的**本地子任务守护器**：

- **时机 = 意图文件**：驱动进程通过 SDK `emit_intent()` 写入不可变的 `tasks/<id>/requests/<request_id>.json`，调度器扫描后拉起、维护、终结进程。没有 interval，没有 cron。
- **三闸自动准入**：闸1 manifest schema → 闸2 dry 跑通 exit 0 → 闸3 容量 ≤ 机器上限。放入 `tasks/<id>/` 即自动开检。
- **无豁免抢占**：高优等待可抢占任何更低优先级进程（daemon 也一样），被抢进程 SIGTERM → 落断点 → exit 99 → 同意图续跑。
- **全进程断点硬契约**：daemon 与 oneshot 共用一套 SDK 断点——SIGTERM 自动落断点 exit 99，调度器重启/抢占后从断点恢复。
- **资源硬边界**：cgroup v2 memory.high = 预估×1.5（软限），memory.max = 预估×2.5（硬兜底），账本 + 资源画像（声明 + 冒烟 + 运行中动态采样）。

## 安装

```bash
# 需要 Python 3.10+ + cgroup v2 内核支持（cgroup2fs）
git clone <repo_url>
cd scheduler
pip install -e .

# 验证
python3 -m sched status    # 显示 0 个子任务，账本为空
python3 -c "import sched; print(sched.__version__)"   # 应该是 2.0.0
```

## 快速上手

### 1. 写 manifest.json

```json
// tasks/hello/manifest.json
{
  "schema_version": 3,
  "name": "hello",
  "description": "最小 hello world",
  "run_mode": "auto",
  "entry": { "file": "run.py", "interpreter": "python3", "args": [], "cwd": "." },
  "priority": 50,
  "resources": { "cpu": 0.1, "memory_mb": 64 },
  "heartbeat": { "timeout_sec": 120 },
  "runtime": {}
}
```

### 2. 写 run.py

```python
# tasks/hello/run.py —— 最简单的：
import sys
sys.exit(0)
```

### 3. 注册（三闸检查）

```bash
python3 -m sched register hello   # REGISTERED hello
```

### 4. 写一个意图文件（或用 SDK emit_intent）

```bash
mkdir -p tasks/hello/requests
cat > tasks/hello/requests/rid-001.json <<'EOF'
{
  "schema_version": 1,
  "request_id": "rid-001",
  "kind": "oneshot",
  "config": "",
  "priority": 50,
  "resources": { "cpu": 0.1, "memory_mb": 64 },
  "lifetime": {
    "completion": "terminal",
    "retry": { "max_attempts": 0, "backoff_base_sec": 5, "backoff_cap_sec": 300, "not_after": "" }
  }
}
EOF
```

### 5. 启动调度器

```bash
python3 -m sched serve --web    # 后台起调度循环 + FastAPI 管理台（http://127.0.0.1:8765）
```

serve 会扫描 `requests/` → 入队 WQ → 账本检查 → 拉起 `run.py` → exit 0 → 意图 `done`。
`data/runtime/decisions.jsonl` 审计日志里能看到完整链路。

## 数据布局（SCHED_HOME，默认 sched 包父目录）

```
SCHED_HOME/
├── tasks/<subtask_id>/
│   ├── manifest.json                    # schema_version=3 清单（唯一真源）
│   ├── run.py / main.sh                 # 业务入口（默认）
│   ├── requests/                        # 意图收件箱（append-only）
│   │   └── <request_id>.json            # 新执行 = 新文件
│   ├── cache/
│   │   ├── runs/<run_id>/state.json     # SDK 写：心跳 + 断点 + control 段
│   │   └── resume_point.json
│   └── results/
├── data/
│   ├── jobs.json                        # 注册表（enabled 开关）
│   ├── settings.json                    # 全局 Settings（可选覆盖）
│   └── runtime/
│       ├── scheduler_state.json         # WQ 快照
│       ├── intents_state.json           # 意图消费状态
│       ├── scheduler.lock               # 单实例 flock
│       ├── active.json                  # 运行中进程清单
│       ├── decisions.jsonl              # 审计日志（enqueue_intent / admit_intent / preempt / exit_decision）
│       ├── profiles.json                # 资源画像
│       └── ledger.json                  # 账本
└── runs/
    └── <subtask_id>.jsonl               # 运行历史（exit_code / start / finished / 归因）
```

## CLI

```bash
python3 -m sched list                  # 已注册子任务
python3 -m sched register <id>         # 立即跑三闸上线检查
python3 -m sched start <id>            # 启用 run_mode=manual 的子任务
python3 -m sched status                # 全局状态（WQ + intents + ledger）
python3 -m sched serve [--web]         # 常驻调度循环（可选内嵌管理台）
python3 -m sched process list [sid]    # 子任务的意图清单 + 运行进程
python3 -m sched process stop <sid> <rid>       # 终止运行中意图（SIGTERM）
python3 -m sched process no-retry <sid> <rid>   # 标记不再重试
```

**没有 `run` 子命令**——正式执行只能由调度器扫描意图文件触发。

## 启动协议（调度器注入）

SCHED_SUBTASK_ID / SCHED_REQUEST_ID / SCHED_RUN_ID / SCHED_CONFIG / SCHED_TRIGGER / SCHED_ATTEMPT / SCHED_SCHEDULED_AT / SCHED_WORKSPACE

## 退出码

| 退出码 | 含义 | 调度器动作 |
|---|---|---|
| 0 | 成功完成 | 意图 `done`，不重启 |
| **99** | **抢占/停止，断点已保存** | 同意图续跑 |
| **100 / 101** | **本机 / 外部资源错误** | 同意图 retry，受 lifetime.retry 约束 |
| **110** | 数据风险 | 可选二刷（简化重入队） |
| 其他非 0 | 兜底按 100 | 倒逼正确分类 |
| **137 / -9** | SIGKILL | 归因（cgroup OOM / 强杀） |

## SDK（sched_task_sdk）

```python
from sched_task_sdk import DaemonTask, OneshotTask, run_task, emit_intent

class DailyCrawler(DaemonTask):
    """daemon 常驻循环：被抢占后续跑。"""
    def steps(self):
        while True:
            today = datetime.now().strftime("%Y-%m-%d")
            self.save_checkpoint({"date": today})
            yield today
            time.sleep(3600)

class Migration(OneshotTask):
    """一次性迁移。"""
    def steps(self):
        for i, item in enumerate(items):
            self.save_checkpoint({"next_index": i})
            yield item

if __name__ == "__main__":
    run_task(DailyCrawler())   # 或 Migration()
```

控制 API（进程运行中）：

```python
task.request_stop("大任务已完成")        # 意图终结
task.no_retry("别再跑了")              # 退出后不自动重试
task.reschedule(300)                  # 5 分钟后再给机会
rid = task.emit_intent(config="day.json", kind="oneshot", priority=60)  # 写新意图
```

## 三闸准入

| 闸 | 检查 | 拒绝原因 |
|---|---|---|
| **闸1** 静态 | manifest schema_version=3，entry.file 存在且在任务目录内，entry.interpreter 是命令名（无路径），priority 0~100，run_mode auto/manual | ManifestError |
| **闸2** 契约 | 调度器 dry 跑一次 run.py，exit 0 | contract 检查失败 |
| **闸3** 容量 | 声明 + 冒烟实测 ≤ 机器总量 | 容量超限 |

闸失败后不自动重试——改文件后调度器热加载（30s 周期）会自动重检，或手动 `sched register <id>` 立即检查。

## cgroup v2 硬边界

- `memory.high` = 预估 × 1.5（软限节流）
- `memory.max` = 预估 × 2.5（硬兜底，超限 OOM kill → exit 137）
- CPU：`cpu.max` = 预估 CPU × 配额窗口

需要 root 权限（或预委派 `/sys/fs/cgroup/sched` 写权限）。不可用时降级运行（无硬边界，调度器会写审计日志）。

## 配置（data/settings.json，可选）

```jsonc
{
  "web": { "enabled": true, "host": "127.0.0.1", "port": 8765 },
  "preempt_exempt_sec": 60,      // 启动后抢占保护期
  "starvation_sec": 3600,        // 饿死看门狗阈值
  "hot_reload_sec": 30,          // serve 热加载周期
  "run_user": ""                 // 空=当前用户
}
```

## 目录结构

```
sched/                    # 核心包（21 模块）
  __init__.py             # v2.0.0
  __main__.py             # CLI 入口
  service.py              # 主循环：scan→admit→preempt→monitor→finalize
  runner.py               # RunSpec + SIGTERM/SIGKILL + cgroup 硬限
  scanner.py              # 扫描 requests/ + intents_state 持久化
  manifest.py             # manifest.json schema_version=3 解析
  intent.py               # 意图文件 schema_version=1 解析
  control.py              # 退出码 + state.json control 段统一决策
  contract.py             # 三闸准入
  registry.py             # jobs.json 注册表
  admission.py            # 全局队列排序 + 账本准出
  preempt.py              # 抢占选择（贪心最小覆盖）
  monitor.py              # 心跳 + 超限退让
  resources.py            # 容量 + 画像（三级 + 下行）+ 账本
  cgroup.py               # cgroup v2 生命周期
  config.py               # Settings + SCHED_HOME
  web.py                  # FastAPI 内嵌管理台
  store.py                # flock + 原子写 + jsonl 轮转
  runlog.py               # 运行历史 append-only
  persist.py              # SchedulerState 持久化
  errors.py               # 退出码常量

sched_task_sdk/           # SDK
  SchedTask / DaemonTask / OneshotTask / emit_intent / 控制 API

tests/                    # 47 个单元测试（test_scanner / test_control / test_contract / test_sdk）
skills/sched/             # SKILL（参考手册 + 场景模板）
```

## 开发

```bash
cd scheduler
pip install -e ".[dev]"   # 含 pytest / fastapi / uvicorn
pytest tests/ -q          # 47 passed in 4.3s
```
