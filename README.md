# scheduler_v002

完全独立复刻的任务调度器，实现「任务调度重构 v04」方案的路径 B（可抢占、断点续跑）。
与现有 `scheduler` 项目**代码零复用**，仅保持协议面（task.json schema / 状态枚举 / 运行历史）对齐。

## 目录结构

```
scheduler_v002/
├── sched_v002/            调度器核心包
│   ├── taskdoc.py         schema v2 解析校验（协议 A）
│   ├── contract.py        L0 契约层：五道闸
│   ├── resources.py       L1 资源层：容量/画像三级/账本/软资源令牌桶
│   ├── admission.py       全局队列排序 + 准出扫描
│   ├── preempt.py         抢占择路（贪心最小覆盖）
│   ├── engine.py          自实现触发引擎（无 APScheduler）
│   ├── runner.py          入口调用 + cgroup + 抢占/终结 + 退出码归因（协议 B）
│   ├── service.py         组装层：主循环/注册/准出/抢占/监测/终态收敛
│   ├── monitor.py         L4 监测：阈值阶梯/心跳/超限退让
│   ├── persist.py         L3 持久化：快照 + 决策审计 + active 恢复
│   ├── cgroup.py          cgroup v2 生命周期（不可用时静默降级）
│   └── __main__.py        CLI
├── sched_task_sdk/        任务侧 SDK（SchedTask 基类）
├── tasks/sample_task/     示例任务
├── tests/                 测试套件
└── requirements.txt       仅 pydantic（+pytest 用于测试）
```

## task.json（schema v2）

较 v1 新增：`resources(cpu/memory_mb/soft)`、`error_rules`、`heartbeat`、`smoke`、
`runtime(user)`、`diff_check`、`contract_exempt`。`priority` 取值 0~100。

`contract_exempt=true` 的任务只能 `schedule.type=manual`（不可断点恢复，禁止自动触发）。

## 退出码协议

| 退出码 | 含义 |
| --- | --- |
| 0 | 成功 |
| 99 | 干净抢占（断点已保存，可恢复） |
| 100 / 101 | 资源错误（可重试，触发画像上调） |
| 110 | 数据风险（跳过项已记 pending_skipped） |
| 137 | SIGKILL —— 需归因（cgroup OOM / 调度器终结超时 / 外部 kill） |

未分类错误按「G5」一律归为资源处理，倒逼任务正确分类。

## 五道闸

注册任务时依次通过，任一未过即拒绝：

1. **静态闸**：schema v2 + 入口存在 + 路径约束 + contract_exempt 规则
2. **契约闸**：入口可执行且写合法 `cache/state.json`
3. **冒烟闸**：dry/real 冒烟，测量资源峰值
4. **容量闸**：预估 > 机器硬上限 → 硬拒绝
5. **断点闸**：跑 → SIGTERM → 验证 exit 99 + resume_point → 恢复跑成功

## 用法

```bash
cd /mnt/data01/projects/scheduler_v002
export PYTHONPATH=$PWD

python3 -m sched_v002 register sample_task   # 五道闸 + 注册
python3 -m sched_v002 list
python3 -m sched_v002 run sample_task        # 手动触发一次
python3 -m sched_v002 status
python3 -m sched_v002 serve                  # 前台常驻调度循环
```

## 测试

```bash
python3 -m pytest -q
```