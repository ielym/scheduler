# sched — 本地子任务调度器（v3）

> 生产者/执行分离 · SQLite 执行库 · 动态运行目标平缓扩容 · 高优先级保护与累计退让

sched 是常驻单进程的**本地子任务调度器**：

- **生产/执行分离**：每个启用任务常驻一个生产者，自行发现业务事件并发布「就绪 JSON」到 `outbox/`；调度器只控制执行进程的运行规模。生产者永不因调度压力被暂停/限额/抢占。
- **无预设上限**：不填最大运行/排队数量、不填 CPU/内存/IO 资源申请。调度器只观测执行进程的实测消耗与业务进展，动态学习运行目标（冷启动 1 个，逐步 +1）。
- **SQLite 执行库**：作业/attempt/事件/检查点/回执落盘，重启接管、进程身份核实、作业去重（相同 job_id 相同内容幂等，内容不同隔离）。
- **统一执行协议**：5 类事件（started/progress/error/done/ack），资源不足经退出码 75 表达；协作暂停 + 幂等提交。协作暂停、可重放执行、幂等提交是必需能力；断点是任务可选实践。

## 安装

```bash
# Python 3.10+（可选 cgroup v2 用于执行进程身份/观测归因）
git clone <repo_url>
cd scheduler
pip install -e .

python3 -m sched list        # 0 个任务
python3 -c "import sched; print(sched.__version__)"   # 3.0.0
```

## 快速上手

### 1. 写 `tasks/<id>/task.json`

```json
{
  "schema_version": 1, "task_id": "hello", "enabled": true, "priority": 50,
  "producer_entry": { "file": "producer.py", "interpreter": "python3", "args": [] },
  "executor_entry": { "file": "run.py", "interpreter": "python3", "args": [] },
  "workdir": ".",
  "recovery": { "replay": "outbox", "retain": "outbox 归档保留 30 天", "replay_note": "按 idempotency_key 核对归档识别遗漏" }
}
```

### 2. 写 `producer.py`（生产者，发布就绪 JSON）

```python
from sched_task_sdk import Producer, run_producer

class HelloProducer(Producer):
    def discover(self, cursor):
        if (cursor or {}).get("published"):
            return cursor, []
        return {"published": True}, [{"event_id": "once", "business_version": "1",
                                        "idempotency_key": "once", "env": {}}]

run_producer(HelloProducer(), tick_sec=60)
```

### 3. 写 `run.py`（执行者）

```python
from sched_task_sdk import ExecutorTask, run_task

class HelloExecutor(ExecutorTask):
    def execute(self):
        self.report_progress(0)
        # 业务处理...
        self.complete({"ok": True})

run_task(HelloExecutor())
```

### 4. 校验并启动调度器

```bash
python3 -m sched task validate hello
python3 -m sched serve            # 前台常驻：守护生产者 → 接收 outbox → 平缓拉起执行进程
```

`serve` 会守护生产者，接收 `outbox/` 就绪 JSON → SQLite 去重索引 → 回执 → 平缓拉起 `run.py`，事件经 `runtime/runs/<attempt_id>/events.jsonl` 上报，`runtime/status.jsonl` 记录状态。

## 数据布局（SCHED_HOME，默认 sched 包父目录）

```
SCHED_HOME/
├── config.json                      # 全局配置（control 节奏）
├── tasks/<task_id>/
│   ├── task.json                    # 任务清单（唯一真源）
│   ├── producer.py / run.py         # 生产者入口 / 执行入口
│   ├── producer_state/state.json    # 游标 + 待发布记录
│   ├── outbox/<job_id>.json         # 就绪作业 JSON
│   └── receipts/<job_id>.json       # 接收回执
└── runtime/
    ├── exec.db                      # SQLite 执行库
    ├── scheduler.lock               # 单实例 flock
    ├── snapshot.json                # serve 每轮写入的运行时快照（CLI status 优先读取）
    ├── commands.jsonl               # CLI → serve 控制命令通道
    ├── commands_ack.jsonl           # serve 对控制命令的回执
    ├── archive/  isolation/         # 归档 / 异常隔离
    ├── runs/<attempt_id>/           # events.jsonl / control.json / permit.json / worker.log
    │                               # （checkpoint.json 由任务自行按需写入，调度器不管理）
    ├── logs/<task_id>/              # 生产者日志
    └── status.jsonl                 # 状态日志
```

## CLI

```bash
sched serve                         # 常驻调度循环
sched status                        # 全局状态（生产者/动态目标/存活 attempt/队列/退让）；serve 在线显示 serve=alive
sched list                          # 已加载任务
sched task validate|enable|disable <id>
sched job list [task]               # 作业清单
sched job show <job_id>             # 作业 + attempts
sched job cancel <job_id> [--reason r]  # 显式取消（在线走命令通道+回执，离线直连撤进程树）
sched job requeue <job_id> [--reason r] # 显式重排：关闭在途 attempt 并重新排队（运行中拒绝）
sched attempt list [job]            # attempt 清单
sched attempt cancel <id> [--reason r]  # 显式取消单个 attempt
```

**没有 `run` 子命令** —— 正式执行只能由调度器扫描 outbox 触发。

## 退出码

| 退出码 | 含义 | 调度器动作 |
| --- | --- | --- |
| 0 | 成功（已发 done 事件） | SUCCEEDED |
| 0 | 未提交完成记录 | 作业回 QUEUED 重跑（attempt 记 FAILED） |
| 75 | 资源不足 | BACKOFF 到期探测 |
| 其他非 0 | 业务/未知失败 | FAILED |

## SDK（`sched_task_sdk`）

- **`Producer`**：发现游标 + 待发布记录 + 无丢失发布 + 稳定 job_id + 重放。
- **`ExecutorTask`**：进展/断点（`checkpoint`/`resume_data`）/`report_progress`/`report_shortage`/`complete`/协作暂停。

## 调度模型

- **动态运行目标**：无写死上限；冷启动 1、观察后逐步 +1；不足/退化先撤销近期增加。
- **高优保护 + 累计退让**：高优受阻逐步让低优退让，有界、有截止、可恢复。
- **变慢检测**：按任务基线识别无报错变慢（温和退让）。
- **状态机**：QUEUED → STARTING → RUNNING → PAUSING → PAUSED / BACKOFF / SUCCEEDED / FAILED / CANCELLED。
- **启动/重启**：一律冷启动重排（不接管、不核实完成、不重放断点）——终止上一实例遗留执行进程 → 删除未终结 attempt 与工件 → 全部未完结作业回 QUEUED → 调度学习态归零。生产者跨重启保留（由新实例经任务级 flock 接管）。
- **断点**：调度器不感知/不管理；断点纯属任务自身实践（`sched_task_sdk` 仅提供本地原子读写设施，落在本次 attempt 的 run_dir）。

## 开发

```bash
cd scheduler
pip install -e .
pytest tests/ -q
```

完整协议与子任务开发规范见 `skills/sched/`（SKILL.md + references）。