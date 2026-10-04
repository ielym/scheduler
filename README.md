# sched

常驻单进程的本地任务调度器：任务放入 `tasks/` 后自动经五道契约闸准入、容量感知准出、
可抢占与断点续跑，自带零构建 Web 管理台。无外部服务依赖（不用数据库、不用 APScheduler）。

## 目录结构

```
scheduler/
├── sched/                 调度器核心包
│   ├── taskdoc.py             task.json schema 解析校验
│   ├── contract.py            契约层：五道上线闸
│   ├── resources.py           容量 / 资源画像 / 账本
│   ├── admission.py           全局队列排序 + 准出扫描
│   ├── preempt.py             抢占择路（贪心最小覆盖）
│   ├── engine.py              自实现触发引擎
│   ├── runner.py              入口调用 + cgroup + 抢占/终结 + 退出码归因
│   ├── service.py             组装层：主循环/自动准入/准出/抢占/监测/终态收敛
│   ├── monitor.py             监测：阈值阶梯/心跳/超限退让
│   ├── persist.py             持久化：快照 + 决策审计 + 崩溃恢复
│   ├── cgroup.py              cgroup v2 生命周期（不可用时显式降级）
│   ├── web.py / web_static/   内嵌 Web 管理台（FastAPI + 单页前端）
│   └── __main__.py            CLI
├── sched_task_sdk/        任务侧 SDK（SchedTask 基类）
├── tasks/sample_task/     示例任务
├── tests/                 测试套件
└── requirements.txt
```

## 安装与可移植性

代码可以 clone 到任意目录直接使用，没有任何硬编码绝对路径：

```bash
git clone https://github.com/ielym/scheduler.git
cd scheduler
pip install -r requirements.txt
# 可选：安装为命令行工具（提供 sched 命令）
pip install -e .
```

- 项目根（`tasks/`、`data/` 的父目录）默认 = `sched` 包所在目录的上一级；
- 可用环境变量 **`SCHED_HOME`** 覆盖项目根（多实例 / 测试隔离）；
- CLI 也可通过 npm 包 `@ielym/sched`（命令名 `sched`）安装。

## 用法

```bash
# 把任务目录放进 tasks/ 后，serve 会自动跑五道上线检查（≤30s 内发现），无需手动注册：
python3 -m sched register sample_task   # 可选：立即跑一次五道上线检查，不必等自动扫描
python3 -m sched list                   # 已上线任务
python3 -m sched run sample_task        # 立即运行一次（只入队，何时跑由准出决定）
python3 -m sched status                 # 任务数/等待运行/运行中/未通过检查项
python3 -m sched serve                  # 前台常驻调度循环（单实例互斥）
python3 -m sched serve --web            # 同时启动内嵌管理台（默认 127.0.0.1:8799）
```

serve 运行期间会自动发现 `tasks/` 下的新任务并后台串行跑五道闸：通过即纳入调度，
失败原因进入 `data/runtime/admission.json` 并在 status/管理台展示。闸失败**不做定时
自动重试**：由创建者在本地修改/删除任务文件，文件指纹变化后自动重新检查。对已上线
任务的修改每 `hot_reload_sec`（默认 30s）热加载生效，无需重启。

## task.json 任务协议

核心字段：`resources(cpu/memory_mb)`、`error_rules`、`heartbeat`、`smoke`、
`runtime(user)`、`contract_exempt`。`priority` 取值 0~100。

代理隧道/VPN/API 配额等**外部资源不在 task.json 申报、调度器也不做容量管理**
（跨账号/跨协议无法感知真实占用）：任务遇到连接重置/响应截断等"换个连接就好"的
瞬时故障时先就地快速重试 3~4 次（每次新连接换新 IP），连续失败才以退出码 101
快速失败。调度器**不设全局冷却/指数退避/重试上限**：资源错误后任务立即回到 WQ 排队，
其它任务释放资源后下一轮自然准出；重试节奏、尝试上限、时效截止由任务在 task.json
`retry` 段自行声明（也可运行中用 SDK `set_retry_hint()` 收紧，如半日市收盘时刻）。

`contract_exempt=true` 的任务只能 `schedule.type=manual`（不可断点恢复，禁止自动触发）。

`error_rules.resource_regex / logic_regex` 由调度器注入任务进程（环境变量
`SCHED_ERROR_RULES`），SDK 的 `classify_exception` 据此把异常归类为资源/逻辑错误；
任务代码内对 `task.resource_regex / task.logic_regex` 赋值可覆盖配置。

### 任务侧契约（上线闸强制）

- **运行期心跳**：任务运行期间必须周期性更新 `cache/runs/<run_id>/state.json`
  （SDK 任务由心跳线程自动保证；自研/bash 任务需自行周期写状态），
  否则将在 `heartbeat.timeout_sec`（默认 120s）后被判定卡死并终止。
- **断点语义**：收到 SIGTERM → 保存断点 → 退出 99（SDK 已托管）。
- **数据风险二刷**：单条数据失败调 `task.skip_item(item, reason)` 记入
  `cache/pending_skipped.jsonl` 并以退出码 110 结束；调度器自动以
  `SCHED_TRIGGER=second_pass` 重入队，任务在二刷轮用
  `task.read_pending_skipped()` 读取并补跑跳过项（重复 skip 自动去重）。

### 退出码协议

| 退出码 | 含义 |
| --- | --- |
| 0 | 成功 |
| 99 | 干净抢占（断点已保存，可恢复） |
| 100 | 本机固定资源错误（内存/CPU/磁盘，可重试，触发资源画像上调） |
| 101 | 外部资源错误（代理/隧道失效、429 限流、配额；立即重排队，节奏/上限/时效由任务 retry 段自治） |
| 110 | 数据风险（跳过项已记 pending_skipped） |
| 137 / -9 | SIGKILL —— 需归因（cgroup OOM / 调度器终结超时 / 外部 kill） |

未分类错误一律按资源错误处理（立即重排队，受任务 `retry` 段约束），倒逼任务正确分类。

### 任务自治重试（task.json `retry` 段）

```json
"retry": { "delay_sec": 0, "max_attempts": 0, "not_after": "" }
```

- `delay_sec`：自动重入队后最早再次准出的间隔（秒），默认 0 = 资源一空出来立刻再试；
- `max_attempts`：最大尝试次数（含首次）。0 = 不放弃（默认，有资源必须跑完）；
  正整数 N = 第 N 次仍失败则转人工，等待创建者处理；
- `not_after`：时效截止，过期不再自动重试（转人工）。支持 ISO8601 绝对时刻
  （`2026-10-04T15:00:00+08:00`）或每日时刻 `HH:MM`（每天此刻之后放弃）。
  任务也可在运行中用 `task.set_retry_hint(delay_sec=, max_attempts=, not_after=)`
  按当天实际情况收紧（缺省字段回落 task.json 静态声明，非法 hint 自动忽略）。
- 新计划触发（auto 到期 / 手动）的 run 计数重新从 1 开始。exit 102（SDK reschedule()+exit_skip()）只是"跳过本轮、N 秒后再给机会"，不计入 attempt。

### 五道上线闸（自动准入）

任务放入 `tasks/` 后由 serve 自动依次执行（也可 `sched register <id>` 或管理台
「导入任务」立即手动执行），任一未过即拒绝纳入调度，原因持久化并在管理台展示；
**闸失败不自动重试**：在本地修改/删除任务文件后，指纹变化即自动重新检查：

1. **静态闸**：schema + 入口存在 + 路径约束 + contract_exempt 规则
2. **契约闸**：入口可执行且写合法 `cache/state.json`
3. **冒烟闸**：dry/real 冒烟，测量资源峰值
4. **容量闸**：预估 > 机器硬上限 → 硬拒绝
5. **断点闸**：跑 → SIGTERM → 验证 exit 99 + resume_point → 恢复跑成功；
   同时校验运行期心跳契约（断点首跑期间必须写过 state.json）

## Web 管理台

`serve --web` 启动内嵌管理台（与调度主循环同进程，直接操作同一份内存态）：

- **总览**：任务/队列/资源账本水位概览，以及等待上线检查任务的闸失败原因
  （不再有"运行参数"面板——时区固定 Asia/Shanghai，冷却/退避/重试上限/闸重试
  全部取消，节奏由任务 schedule 与 retry 段自控）；
- **任务**：按子任务目录名**导入任务**（仅限本地已验证的生产级任务，导入即同步
  跑五道闸）；纯调度参数（优先级、schedule、心跳超时、retry 段）行内编辑，
  保存立即生效；管理台不提供任务文件查看/编辑；
  **讨论二 两值调度模型**：schedule.type 只有 manual（仅手动/CLI 触发）和 auto
  （自动给机会）；interval = 给机会的最小间隔秒数；精确时机（法定节假日、
  每月特定日期、收盘后等）不由调度器判定——任务进程内用 SDK `reschedule(delay_sec)`
  + `exit_skip()`（退出码 102）自行判断，调度器只读 `retry_hint.delay_sec`
  转为 `not_before` 重排队；
- **运行队列**：等待项显示触发来源（"手动运行"= 管理台「立即运行一次」或
  `sched run` 触发、"auto"= 调度器给机会）、第几次尝试、`retry.delay_sec`
  产生的最早可准出时刻与上次真实失败原因；同优先级按画像预估时长 SJF
  （短作业优先）；运行中任务可查看**实时日志**（stdout/stderr，支持 3s 自动
  刷新）与**停止**（cancel Event → SIGTERM 整组，可断点续跑，本轮不自动重排队）；
- **历史**：每条 run 可删，弹窗二次确认是"仅删历史记录"还是"同时删除本地
  详情和日志"（data/runs 下的 stdout/stderr）；
- **画像**：注册冒烟 + 运行中约每分钟动态采样 + 终态采样三层来源，P95 滑窗用于
  准出，成功 run 时长中位数用于 SJF 预估。

手动调度（manual）任务仅在「立即运行一次」或 `sched run` 时触发；auto 任务由
调度器按 interval 自动给机会，任务进程内自行判断时机。
各展开列表（运行历史、决策记录、队列）统一分页，每页 50 条。

- 默认只监听回环地址；监听非回环地址时必须配置 `auth_token`（Basic Auth）
  或在 settings 中显式打开 `allow_public_no_auth`，否则拒绝启动；
- 云主机对外访问还需在云安全组放行对应端口（默认 8799）。

## 部署要求

1. **任务运行用户**（默认 `sched-run`，可经 `data/settings.json` 的 `run_user`
   或任务级 `runtime.user` 覆盖）：
   ```bash
   useradd -r -m -s /usr/sbin/nologin sched-run
   ```
   用户不存在时上线检查会直接拒绝并给出上述指引；置空 `run_user` = 以调度器身份执行
   （仅建议测试环境使用）。
2. **cgroup v2 硬边界**：调度器需以 root 运行以写 `/sys/fs/cgroup/sched`
   （根组需委派 `+memory +cpu`）；任务进程降权到 run_user。cgroup 不可用时
   启动日志与决策日志会显式降级告警（此时仅账本预估防打爆，无内存/CPU 硬边界）。
3. **单实例**：同一 SCHED_HOME 仅允许一个 serve（`runtime/scheduler.lock` flock 互斥）。
4. **磁盘有界**：任务 stdout/stderr 落盘上限 `max_log_mb`（默认 32MB，超限截断）；
   decisions/runs 审计 jsonl 单档 16MB 自动轮转（保留 3 代）。

`data/` 为运行时数据（settings、注册表、账本、日志），已在 .gitignore 中忽略，
迁移部署时按需自行拷贝。

## 测试

```bash
python3 -m pytest -q
```
