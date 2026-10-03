# 任务调度重构 · 需求实现综合审查报告

> 审查对象：`/mnt/data01/projects/scheduler_v002`（sched_v002 核心包 + sched_task_sdk + 示例任务 + 测试套件）
> 需求基线：`/mnt/data01/projects/todo_list/1003/任务调度重构/results/任务调度重构_需求说明.md`（R1–R24）
> 审查方式：全量代码通读 + 逐条需求对照 + 针对性实验验证 + 测试套件运行（56 passed）
> 审查日期：2026-10-03

---

## 0. 结论摘要

整体实现具备清晰的五层架构（契约/资源/引擎/持久化/监测），五道闸、双轨排队、令牌桶软资源、cgroup 双水位等框架均已落地，测试套件 56 项全部通过。

**但批判性审查发现 5 个经实验实证的关键缺陷（P0），其中多个直接违反最高优先级需求（R1、R4、R12、R16、R19）**，另有多处需求偏差（P1）、死代码与安全隐患（P2）。核心问题：

| 级别 | 数量 | 概述 |
|---|---|---|
| P0 关键缺陷（已实证） | 5 | once 无限重触发、准出 over-commit、画像单位污染致永久饥饿、预估无上限钳制致永久饥饿、抢占缺口误算致 R1 失效 |
| P1 需求偏差 | 8 | G5 错误兜底未接通、渐进式提交缺失、崩溃恢复空实现、110 数据风险无二刷、软资源降额死代码、max_instances 未生效等 |
| P2 风险/安全 | 7 | 降权静默失败、环境变量注入、锁文件权限、硬编码宽限、HTTP 0.0.0.0 预留等 |
| 流程违规 | 1 | R23：代码目录不是 git 仓库，无任何远端备份 |

**总体判定：不满足"完成标准"。需求未完整、正确转化，须按第 4 章修复方案整改后复审。**

---

## 1. 需求实现验证矩阵（R1–R24）

图例：✅ 满足 ⚠️ 部分满足/有偏差 ❌ 未满足

| 需求 | 判定 | 验证结论与证据 |
|---|---|---|
| **R1** 高优任务占据全部资源并执行完毕（最高优先级） | ❌ | 抢占框架存在（[preempt.py](file:///mnt/data01/projects/scheduler_v002/sched_v002/preempt.py)、[service.py#L309-L332](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L309-L332)），但 `_tick_preempt` 把**总需求**而非**资源缺口**传给 `select_preempt_targets`。实验证实：空闲 500MB、高优需 800MB、低优占 400MB 时本应抢占成功，实际返回空集（见 3.1-Bug5）。高优任务在"部分空闲+部分被占"的常见场景下**无法抢占**，最高优先级需求落空 |
| **R2** 可抢占性一律由优先级决定，不接受任务级开关 | ⚠️ | 无用户可配的"不可抢占"开关，符合主旨。但 [preempt.py#L46](file:///mnt/data01/projects/scheduler_v002/sched_v002/preempt.py#L46) 将 `manual` 与 `contract_exempt` 运行整体排除出抢占候选：contract_exempt 排除是 R3 的必要推论（不可恢复任务被抢即损坏），可接受；manual 排除缺乏需求依据，建议以"优先级相同不参与抢占"替代整体豁免 |
| **R3** 不可恢复任务仅手动触发，禁止自动 | ✅ | [taskdoc.py#L185-L189](file:///mnt/data01/projects/scheduler_v002/sched_v002/taskdoc.py#L185-L189) 模型级校验 + [contract.py#L116-L120](file:///mnt/data01/projects/scheduler_v002/sched_v002/contract.py#L116-L120) 闸1 复核 + [service.py#L104-L110](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L104-L110) 触发引擎排除，三重防护 |
| **R4** 资源评估防打爆 | ⚠️ | 账本+画像+cgroup 三层框架完整，但存在 Bug2（同轮 over-commit，见 3.1）与 Bug3/4（画像失真），**防打爆保证在关键场景失效** |
| **R5** 借助冒烟/历史执行评估资源 | ⚠️ | 三级画像（冒烟峰值→P95 滑窗→声明值）设计正确，但 CPU 样本单位错误（把累计 CPU-秒当核数，见 Bug3），历史评估对 CPU 维度完全失真 |
| **R6** 创建时强制冒烟并预估 | ✅ | 注册必经闸3 冒烟（[contract.py#L133-L146](file:///mnt/data01/projects/scheduler_v002/sched_v002/contract.py#L133-L146)），实测峰值写入画像（[service.py#L124-L128](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L124-L128)）。小偏差：`smoke.sample_size` 字段声明但从未使用 |
| **R7** 超机器上限必须优化，优化失败拒绝 | ✅ | 闸4 容量闸对声明值与冒烟实测值双重判定（[contract.py#L148-L162](file:///mnt/data01/projects/scheduler_v002/sched_v002/contract.py#L148-L162)），超限硬拒绝。"优化"环节由任务方线下完成，调度器只拒绝，符合需求语义 |
| **R8** 资源被占但未超上限仍允许加入，待释放后执行 | ✅ | WQ 全局队列持久化等待（[persist.py#L33-L50](file:///mnt/data01/projects/scheduler_v002/sched_v002/persist.py#L33-L50)），准出扫描每 tick 重试 |
| **R9** 硬资源硬门禁 / 软资源不纳入拒绝 | ✅ | [admission.py#L58-L79](file:///mnt/data01/projects/scheduler_v002/sched_v002/admission.py#L58-L79) 内存/CPU 按账本硬判，软资源走令牌桶；注册时软资源不足不拒绝（仅运行时排队） |
| **R10** 同优先级排队等待，不得判失效 | ⚠️ | 排队主体正确（双轨排序 + 饿死看门狗 [admission.py#L25-L43](file:///mnt/data01/projects/scheduler_v002/sched_v002/admission.py#L25-L43)）。但 Bug3/Bug4 造成的永久不可准出等价于"变相失效"；且资源错误重试 5 次后永久放弃（见 R16/R17） |
| **R11** 软资源（IP 代理并发等）合理分配防超限 | ⚠️ | 令牌桶分配正确（[resources.py#L159-L193](file:///mnt/data01/projects/scheduler_v002/sched_v002/resources.py#L159-L193)）。但"软资源降额"机制（`record_soft_error`/`soft_error_rate`）是死代码，无任何调用方；退出码 101（软资源错误）与 100 走完全相同的处理，无差异化退避 |
| **R12** 禁止同时刻同时提交；禁止创建时自动运行；运行只由调度器决定 | ❌ | 注册不自动运行 ✅；但 **Bug1：`once` 类型任务到期后每个 tick 重复入队**（实验证实，见 3.1），形成无限重复提交；且 `max_instances`/`overflow` 字段只校验不使用（见 3.2-D6），interval 任务可无限制并发入队 |
| **R13** 渐进式动态提交 | ⚠️ | "见缝插针"已实现（准出全局扫描，高优等待时低优小任务可先跑）。但需求核心的**"先提交一个→健康后再提交第二个→第二个资源不足及时停止"渐进确认链完全缺失**：所有到期任务一次性入 WQ，准出即并行拉起，无"首个任务健康观察期"门禁 |
| **R14** 固定资源不足先等待并调度其他任务 | ✅ | `admission_scan` 全局扫描跳过被阻者继续准出后续任务（[admission.py#L82-L107](file:///mnt/data01/projects/scheduler_v002/sched_v002/admission.py#L82-L107)） |
| **R15** 不可控资源失败等待自然完毕，不强制终止 | ⚠️ | 软资源错误走指数退避重试（[service.py#L436-L463](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L436-L463)），不终止任务本体 ✅；但 5 次后永久放弃（`MAX_ATTEMPTS=5` → giveup 转人工），与"最终完成"存在张力 |
| **R16** 不得因排队过长终止任务 | ❌ | 无排队超时终止 ✅；但 Bug3（est_cpu=8640 核）、Bug4（est_mem 超机器上限无钳制）导致任务**永久无法准出**，效果等同终止。饿死看门狗只提升排序优先级，对"预估超容量"型饥饿无效 |
| **R17** 退让而非扑杀，任务仍须最终完成 | ⚠️ | 抢占走优雅 SIGTERM+断点（退让语义正确）；但资源错误 giveup 后任务永久离开自动调度轨道，"仍须最终完成"仅靠人工兜底 |
| **R18** 区分逻辑错误/资源错误，调度器感知资源不足；统一错误码上报 | ❌ | 退出码协议已定义（[errors.py](file:///mnt/data01/projects/scheduler_v002/sched_v002/errors.py)），但**调度器终态判定完全没有使用它**：[runner.py#L232-L251](file:///mnt/data01/projects/scheduler_v002/sched_v002/runner.py#L232-L251) 手写 if-else，`classify_exit_code`/`is_retryable_category` 是死代码；任务 `state.json` 的 `last_error.category`（需求要求的"真实错误码上报"通道）**调度器从未读取** |
| **R19** 未设置/设置不正确的错误一律按资源错误处理（G5） | ❌ | **G5 兜底链路断裂**：SDK 把未分类异常映射为 `unclassified`→退出码 1（[task.py#L159-L165](file:///mnt/data01/projects/scheduler_v002/sched_task_sdk/task.py#L159-L165)），而调度器把退出码 1 判为 `failed`（逻辑错误，[runner.py#L249-L251](file:///mnt/data01/projects/scheduler_v002/sched_v002/runner.py#L249-L251)）→ 转人工不重试。**未分类错误实际被当作逻辑错误处理，与 R19 完全相反** |
| **R20** 子任务硬性恢复能力（重试/二刷） | ⚠️ | 闸5 断点闸强制验证"打断→99→恢复跑成功"（[contract.py#L164-L193](file:///mnt/data01/projects/scheduler_v002/sched_v002/contract.py#L164-L193)），准入侧完备。但运行侧：**`_recover_interrupted` 是空实现**（[service.py#L519-L524](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L519-L524)），调度器崩溃重启后，已被 `pop_run` 移出 WQ 的 interrupted 运行**永久丢失，不会被重新入队**——恢复能力在最需要的崩溃场景失效 |
| **R21** 单条数据异常不卡顿整体（跳过/隔离） | ⚠️ | SDK 提供 `skip_item`/`dead_letter`（[task.py#L124-L130](file:///mnt/data01/projects/scheduler_v002/sched_task_sdk/task.py#L124-L130)）✅；但退出码 110（数据风险）在调度器侧被判为 `failed` 转人工，`pending_skipped` 无人消费，**二刷闭环不存在** |
| **R22** 代码层强制约束（基类继承/接口校验） | ⚠️ | schema 校验 + 契约闸（运行时行为验证）提供了较强约束。但需求点名的"强制继承固定基类"静态检查缺失：不继承 `SchedTask`、自行伪造 state.json 的脚本也能通过契约闸。行为等价但约束强度低于需求表述 |
| **R23** 修改前先推送远端 git | ❌ | `/mnt/data01/projects/scheduler_v002` **不是 git 仓库**（`git status` 报 not a repository），全部代码无版本控制、无远端备份，误改不可回退 |
| **R24** 自主执行协议（监督循环/故障自愈/状态持久化/自检） | ⚠️ | 主循环异常捕获自愈 ✅、状态持久化（原子快照+决策审计+active 恢复）✅；但故障自愈分级缺失（崩溃恢复为空实现，见 R20），完成后自检无独立证据 |

**统计：✅ 8 项 / ⚠️ 11 项 / ❌ 5 项（R1、R12、R16、R18、R19，另 R23 流程违规）。**

---

## 2. 测试现状评估

- 测试套件 56 项全部通过（`python3 -m pytest -q`，14.66s）。
- 单元覆盖较好：排序、画像、令牌桶、五道闸、窗口计算、终态收敛子流程。
- **关键盲区**（与 P0 缺陷一一对应）：
  1. `once` 到期后的行为无测试（[test_engine.py#L24-L26](file:///mnt/data01/projects/scheduler_v002/tests/test_engine.py#L24-L26) 只测未来时刻）；
  2. `admission_scan` 无"同轮多任务累计超容量"用例（现有用例单任务不超限）；
  3. 画像记录链路无"cpu 样本单位"断言——`_finalize` 传入的是 `cpu_sec`（累计秒），测试从未覆盖该真实调用路径；
  4. 无端到端抢占测试（高优入队→低优被抢→高优准出→低优恢复）；
  5. 无崩溃恢复测试（`_recover_interrupted` 空实现无人发现）；
  6. 无未分类错误（退出码 1）归因测试，G5 断裂未被捕获；
  7. 全部测试以 `run_user=""` 跳过降权，降权路径零覆盖。

---

## 3. 问题清单（批判性发现）

### 3.1 P0 关键缺陷（均已实验实证）

**Bug1｜`once` 任务到期后无限重复触发（违反 R12）**
- 位置：[engine.py#L103-L107](file:///mnt/data01/projects/scheduler_v002/sched_v002/engine.py#L103-L107)、[service.py#L196-L206](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L196-L206)
- 机理：`next_fire` 对 `once` 永远返回 `once_at` 本身，不判断是否已过期；`advance()` 重算后仍是同一过去时刻，`due()` 每 tick 命中 → **每个 tick 入队一次，无限重复执行**。
- 实证：`once_at=12:00:00`，`now=12:00:01` 时 `next_fire` 仍返回 `12:00:00`（≤now，立即 due）。

**Bug2｜准出扫描同轮 over-commit（违反 R4/R9 硬门禁）**
- 位置：[admission.py#L82-L107](file:///mnt/data01/projects/scheduler_v002/sched_v002/admission.py#L82-L107)
- 机理：`admission_scan` 循环内判定 `can_admit` 时只读账本，**从不把本轮已准出者的预估累加进去**（账本要到 `_admit` 才 `reserve`）。同一轮扫描中多个大任务可同时通过。
- 实证：容量 1000MB，两个各需 800MB 的任务同轮准出 `['r1','r2']`，预留合计 1600MB → 直接打爆。这是"防打爆"的正面击穿。

**Bug3｜画像 CPU 样本单位污染 → 任务永久饥饿（违反 R5/R10/R16）**
- 位置：[service.py#L391-L395](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L391-L395)（`_finalize` 把 `cpu_sec` 累计 CPU-秒当样本）→ [resources.py#L75-L86](file:///mnt/data01/projects/scheduler_v002/sched_v002/resources.py#L75-L86)（`p95_cpu`/`estimate` 把它当"核数"）
- 机理：样本语义混淆。一个 2 核跑 1 小时的任务样本值 = 7200，`estimate` 得出"需要 8640 核"（×1.2），`estimate_job` 又取 `max(画像, 声明)` 兜底放大 → **该任务此后永远无法通过 `can_admit`，永久饿死**。任何长任务跑一次即自杀。
- 实证：样本 (7200, 500) → est_cpu = 8640.0 核（机器 4 核）。

**Bug4｜画像上浮后无机器上限钳制 → 永久饥饿（违反 R16/R17）**
- 位置：[admission.py#L46-L55](file:///mnt/data01/projects/scheduler_v002/sched_v002/admission.py#L46-L55)
- 机理：冒烟实测 800MB 通过闸4（上限 1000MB）是合法的，但正式运行实测 900MB 入画像后 ×1.2 overshoot = 1080MB > 机器上限，`can_admit` 永久拒绝。**任务因一次正常波动被永久逐出调度**，与"退让等待、最终完成"直接冲突。
- 实证：样本 (1.0, 900)、声明 800 → est_mem = 1080MB > 1000MB。

**Bug5｜抢占缺口误算为总需求 → R1 最高优先级失效**
- 位置：[service.py#L319-L323](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L319-L323)
- 机理：`_tick_preempt` 把高优任务的**总需求** `(need_cpu, need_mem)` 传给 `select_preempt_targets`，未减去当前账本空闲量。只要"空闲 + 可抢占释放 ≥ 需求 但 可抢占释放 < 需求"（最常见的部分占用场景），`_enough()` 永远不满足，返回空集，**抢占永不发生**。
- 实证：需 800MB，低优运行占 400MB（空闲其实足够补足）→ 返回 `[]`。
- 连带问题：`select_preempt_targets` 按 `started_monotonic` 做次级排序，但 `make_running_info` 读取的 `run["_started_monotonic"]` 键**从未被写入**（恒为 0.0）；`exempt_until_monotonic` 同样从未被赋值，"豁免等待期"是纸面机制。

### 3.2 P1 需求偏差与设计缺陷

**D1｜G5 错误兜底链路断裂（违反 R18/R19，最严重的语义偏差）**
- 调度器侧 [runner.py#L232-L251](file:///mnt/data01/projects/scheduler_v002/sched_v002/runner.py#L232-L251) 的终态判定是独立 if-else，未调用 `errors.classify_exit_code`；SDK 将未分类异常退出码定为 1，调度器将 1 判为 `failed`（逻辑错误）→ `_after_finalize` 转人工。需求要求"未设置/设置不正确一律按资源错误"，实现恰好相反。
- 任务 `state.json.last_error.category` 是 SDK 已写好的精确分类通道，调度器 `_read_task_state` 读了整个 state 却**从不使用 `last_error`**。

**D2｜崩溃恢复空实现，interrupted 运行丢失（违反 R20/R16）**
- [service.py#L519-L524](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L519-L524) `_recover_interrupted` 直接 `return`。注释声称"从 WQ 兜底"，但运行在准出时已被 `pop_run` 移出 WQ（[service.py#L226-L230](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L226-L230)），崩溃后该运行既不在 WQ 也不在 running，`_reconcile` 只落了一条 interrupted 记录便再无下文。**任务在调度器崩溃后悄悄消失**。

**D3｜渐进式提交缺失（R13 部分未实现）**
- 无"首个任务健康观察期"门禁、无"第二个任务资源不足时及时停止"的联动。准出即并行拉起，资源不足场景退化为一次性全部放行（叠加 Bug2 更危险）。

**D4｜`max_instances`/`overflow` 只校验不执行（R12 连带）**
- [taskdoc.py#L72-L95](file:///mnt/data01/projects/scheduler_v002/sched_v002/taskdoc.py#L72-L95) 严格校验 1~20，但 `_tick_triggers`/`_enqueue` 从不检查同 job 在途实例数。`overflow: "skip"` 语义完全未实现，同一任务可无限并发入队。

**D5｜数据风险（110）无二刷闭环（R21 部分未实现）**
- 退出码 110 → `failed` 转人工；`pending_skipped.jsonl` 无消费方；无"二刷"重入队路径。

**D6｜软资源降额与差异化退避是死代码（R11 部分未实现）**
- `monitor.record_soft_error`/`soft_error_rate` 无调用方；101 与 100 同路径处理。

**D7｜`_close_expired_windows` 触发时机错误**
- 只在 `_tick_triggers` 有到期任务时被调用（[service.py#L196-L206](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L196-L206)）。若某 tick 无任何到期任务，过期窗口内的在途运行不会被收口，`allow_overrun=false` 保证失效。

**D8｜manual 任务完全豁免心跳监测**
- [monitor.py#L49-L53](file:///mnt/data01/projects/scheduler_v002/sched_v002/monitor.py#L49-L53)：`obs.manual` 直接跳过所有监测。contract_exempt 手动任务无心跳、无超限检查，可无限期占用资源，与 R4 防打爆冲突。

### 3.3 P2 技术风险与安全漏洞

**S1｜降权静默失败（安全）**：[runner.py#L65-L79](file:///mnt/data01/projects/scheduler_v002/sched_v002/runner.py#L65-L79) `_drop_privileges` 对"用户不存在"直接 return、对 `setuid` OSError 吞掉——**任务可能以 root 运行而调度器毫无感知**。降权失败应当是硬错误。

**S2｜task.json `env` 可注入危险变量（安全）**：`env` 透传进子进程且未做黑名单校验，任务创建者可注入 `LD_PRELOAD`、`LD_LIBRARY_PATH`、`PYTHONSTARTUP` 等。虽有降权兜底（若 S1 修复），但结合 S1 即为 root 提权通道。

**S3｜调度器环境全量透传（安全）**：[runner.py#L46](file:///mnt/data01/projects/scheduler_v002/sched_v002/runner.py#L46) `env = dict(os.environ)`，调度器进程持有的任何敏感环境变量（token、密钥）泄露给所有任务进程。应白名单透传。

**S4｜锁文件 0644 且无限增殖（运维）**：[store.py#L22](file:///mnt/data01/projects/scheduler_v002/sched_v002/store.py#L22) 每个被锁文件旁生成 `.lock`，永不清理；多用户部署时 0644 锁文件可被他人持有造成 flock 干扰。

**S5｜kill 宽限期硬编码**：[runner.py#L207-L214](file:///mnt/data01/projects/scheduler_v002/sched_v002/runner.py#L207-L214) cancel 用 10s、preempt 用 40s，配置项 `kill_grace_sec`/`grace_sec` 未被使用，配置与行为脱节。

**S6｜HTTP 配置预留 0.0.0.0 + 认证默认关闭**：[config.py#L67-L75](file:///mnt/data01/projects/scheduler_v002/sched_v002/config.py#L67-L75) 当前无 HTTP 服务，属潜在风险；一旦接入即全网暴露无认证。

**S7｜`_pid_belongs_to` 用 cmdline 子串匹配做崩溃归属判定**：[service.py#L526-L534](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L526-L534) task_dir 子串匹配可被同名路径误命中；且若崩溃后 pgid 被复用（理论上可能），会误杀无关进程组。应结合启动时间戳校验。

**其他观察**：
- `config.load_settings` 解析失败时静默重置配置（仅保留 token），可能掩盖配置错误；
- `_handle_retry` 的退避时间用 `datetime.now().astimezone()` 而排序/比较用 `runlog.now_iso()`，时区表示不一致（当前均为本地时区未出事，属隐患）；
- `ProfileStore.record` 存储的 `cpu` 键是 P95 值、`samples` 是原始值，两份数据语义重复且 `cpu`/`memory_mb` 顶层字段读取方只有 `Profile` 构造，易再次引入单位混淆；
- 无运行历史清理（`retention_days=90` 声明但未实现），jsonl 无限增长。

---

## 4. 修复方案

### 4.1 P0 修复（代码级建议）

**Fix1｜once 过期即失效**（[engine.py](file:///mnt/data01/projects/scheduler_v002/sched_v002/engine.py#L103-L107)）

```python
if stype == "once" and schedule.get("once_at"):
    try:
        fire_at = datetime.fromisoformat(str(schedule["once_at"]))
    except ValueError:
        return None
    if fire_at.tzinfo is None and now.tzinfo is not None:
        fire_at = fire_at.replace(tzinfo=now.tzinfo)
    return fire_at if fire_at > now else None   # 过期返回 None，触发引擎自动摘除
```
并在 `_tick_triggers` 中 `advance` 后若 `next_run_time` 为 None 则 `engine.remove(jid)`。

**Fix2｜准出扫描内置"本轮虚拟账本"**（[admission.py](file:///mnt/data01/projects/scheduler_v002/sched_v002/admission.py#L82-L107)）

```python
def admission_scan(...):
    res = AdmissionResult([], [], [], {})
    pending_cpu, pending_mem = 0.0, 0            # 本轮已准出的虚拟占用
    pending_soft: dict[str, int] = {}
    for it in items:
        ...
        ok, reason = can_admit(est_cpu, est_mem, soft, ledger, capacity, soft_pool,
                               extra_cpu=pending_cpu, extra_mem=pending_mem,
                               extra_soft=pending_soft)
        if ok:
            res.admitted.append(rid)
            pending_cpu += est_cpu; pending_mem += est_mem
            for k, v in soft.items(): pending_soft[k] = pending_soft.get(k, 0) + v
        ...
```
`can_admit` 增加 `extra_*` 参数并入判定。软资源同理（当前软资源在 `_admit` 有竞态兜底，硬资源没有，必须前移）。

**Fix3｜画像 CPU 改为"平均核数"**（[service.py#L391-L395](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L391-L395)）

```python
duration_sec = max(1, int((outcome.duration_ms or 0) / 1000))
avg_cpu_cores = round(outcome.cpu_usage_usec / 1_000_000 / duration_sec, 3)
self.profiles.record(rec["job_id"], avg_cpu_cores, mem_peak_mb, ...)
```
同时修正注册时的初始样本（[service.py#L126](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L126) 目前把冒烟 `cpu_sec` 也当核数传入，同病同治）。建议为样本增加单位注释与断言 `0 <= cpu <= capacity.total_cpu`。

**Fix4｜预估结果钳制在机器上限内**（[admission.py#L46-L55](file:///mnt/data01/projects/scheduler_v002/sched_v002/admission.py#L46-L55)）

```python
def estimate_job(profile, declared_cpu, declared_mem_mb, overshoot, capacity=None):
    ...
    if capacity is not None:
        est_cpu = min(est_cpu, capacity.total_cpu)
        est_mem = min(est_mem, capacity.total_memory_mb)
    return est_cpu, est_mem
```
钳制不会破坏防打爆：真正超限的运行由 cgroup `memory.max` 硬兜底 + monitor 超限退让兜住（该链路已存在）。这保证"合法准入的任务永不因画像膨胀而饿死"。

**Fix5｜抢占按真实缺口计算**（[service.py#L309-L332](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L309-L332)）

```python
need_cpu, need_mem = self._estimate_for(top)
free_cpu = self.capacity.total_cpu - self.ledger.reserved_cpu()
free_mem = self.capacity.total_memory_mb - self.ledger.reserved_mem_mb()
gap_cpu = max(0.0, need_cpu - free_cpu)
gap_mem = max(0, need_mem - free_mem)
# 软资源缺口同理：need - available
targets = preempt.select_preempt_targets(running_infos, gap_cpu, gap_mem, gap_soft)
```
同时：`_admit` 时把 `time.monotonic()` 写入 run 记录的 `_started_monotonic`；抢占后设置 `exempt_until_monotonic = now + 恢复保护期`，让纸面机制真正生效。

### 4.2 P1 修复

**Fix-D1｜接通 G5 错误归因链**：
1. `runner.execute` 终态判定改为先读任务 `state.json` 的 `last_error.category`（state_snapshot 已在 `_run_worker` 读取，把归类前移到 outcome 生成处），无 category 时回落 `errors.classify_exit_code(exit_code)`；
2. `unclassified` → 按 `RESOURCE` 处理（进入退避重试），落实 R19；
3. SDK 侧为未分类异常改用独立退出码（如 102），避免与"逻辑错误代表码 1"混用；
4. 删除或真正使用 `classify_exit_code`/`is_retryable_category`，禁止保留死协议。

**Fix-D2｜实现崩溃恢复**：`_reconcile` 落 interrupted 记录时把记录同时写入 `runtime_dir()/recover_queue.json`；`_recover_interrupted` 读取该文件，对 `contract_exempt=false` 的任务构造 `resume_point_ref=True` 的新运行入 WQ；contract_exempt 任务按 R3 不自动恢复，仅记录待人工。补充对应测试。

**Fix-D3｜渐进式提交**：在 `_tick_admission` 增加"观察期门禁"——当本轮存在因固定资源等待的项且当前无运行中任务时，一次只准出 1 项；该项运行超过 `健康观察窗口`（建议 2×sample_interval，可配置）且未出现资源错误/超限后，才放开后续准出。第二个任务准出后若 monitor 连续报超限，立即 `preempt.set()`（断点已保存前提下），对应 R13 第二条。

**Fix-D4｜落实 max_instances/overflow**：`_enqueue` 前统计 `state.wq + running` 中同 job 在途数；达到 `max_instances` 时按 `overflow` 取 skip（丢弃并记决策审计）或 queue（保留）。

**Fix-D5｜110 二刷闭环**：退出码 110 判为 `data_risk` 终态但**可重入队**：`_after_finalize` 增加分支，延迟（可配置）后以 `二刷` 标记重新入队，任务 SDK 在 `steps()` 入口读取 `pending_skipped.jsonl` 重放；连续 N 次 110 且 skipped 集合不缩小才转人工。

**Fix-D6｜软资源降额接通**：`_finalize` 中 `status==resource` 且 `exit_code==101` 时 `self.monitor.record_soft_error(资源名)`；`_tick_admission` 前检查 `soft_error_rate`，超阈值时 `soft_pool.set_capacity(name, cap * 0.5)`（下限 1），冷却期后恢复。

**Fix-D7**：把 `_close_expired_windows` 从 `_tick_triggers` 移入 `_tick_monitor`（每 tick 对所有 running 检查）。

**Fix-D8**：manual 任务保留心跳监测（仅放宽阈值，如 ×2），杜绝无限占用；或在 `trigger` 时强制要求 contract_exempt 任务声明 `max_runtime`，超时终结。

### 4.3 安全与运维修复

1. **S1**：`_drop_privileges` 失败改为抛异常终止该 run（判 `failed`，reason=`drop_privileges_failed`）；`_admit` 前预检用户存在性。
2. **S2**：`taskdoc` 增加 `env` 键黑名单（`LD_PRELOAD`、`LD_LIBRARY_PATH`、`LD_AUDIT`、`PYTHONSTARTUP`、`PYTHONPATH`、`BASH_ENV` 等），命中即校验失败。
3. **S3**：`build_env` 改为白名单基础环境（`PATH`、`HOME`、`LANG`、`TZ`、`PYTHONPATH`）+ task.json env + SCHED_*，不再全量继承 `os.environ`。
4. **S4**：锁文件 0600；提供 `prune` 命令清理孤儿 `.lock`。
5. **S5**：`_terminate` 宽限期改读 `settings.runner.kill_grace_sec`/`grace_sec`。
6. **流程（R23）**：立即在 `/mnt/data01/projects/scheduler_v002` 执行 `git init` → 全量提交 → 推送远端；此后每个修复单独提交，保证可回退。

### 4.4 测试策略优化

| 新增测试 | 覆盖缺陷 |
|---|---|
| `once` 到期后 `next_fire` 返回 None、服务不再入队 | Bug1 |
| 同轮两个大任务只准出一个（虚拟账本） | Bug2 |
| 长任务（高 cpu_sec）跑完后仍可再次准出（平均核数） | Bug3 |
| 画像膨胀后被钳制到机器上限、任务不饥饿 | Bug4 |
| 端到端抢占：部分空闲场景高优触发抢占→低优 exit 99→高优准出→低优恢复入队 | Bug5 |
| 未分类异常（无 error_rules）→ 按资源重试（G5） | D1 |
| 杀掉调度器主进程→重启→interrupted 任务自动恢复入队 | D2 |
| 同 job 超 `max_instances` 时 skip/queue 行为 | D4 |
| 降权失败 → run 判 failed 而非 root 执行 | S1 |
| env 注入 `LD_PRELOAD` 被注册校验拒绝 | S2 |

另建议：引入 `pytest -m slow` 的端到端场景（真实信号、真实超时）；在 CI 中以非 root + 无 cgroup 与 root + 有 cgroup 两种环境各跑一遍，覆盖降级路径。

---

## 5. 风险评估报告

| 风险 | 等级 | 触发条件 | 影响 | 缓解状态 |
|---|---|---|---|---|
| once 任务无限重复执行 | **高** | 任何 `type=once` 任务到期后 | 重复副作用（数据污染、外部 API 重复调用）、队列膨胀 | 未缓解，须 Fix1 |
| 内存超卖打爆机器 | **高** | 多个大任务同轮到期 | OOM 连锁、非调度任务受累，违背重构初衷 | 未缓解，须 Fix2 |
| 长任务一次运行后永久饥饿 | **高** | 任何 CPU 时长可观的任务 | 任务静默消失于调度，无人察觉 | 未缓解，须 Fix3/Fix4 |
| 高优任务无法抢占 | **高** | 部分资源空闲的常见场景 | R1 最高优先级需求失效 | 未缓解，须 Fix5 |
| 调度器崩溃丢任务 | **高** | 进程被杀/机器重启 | 已准出任务永久丢失，无告警 | 未缓解，须 Fix-D2 |
| 未分类错误被当逻辑错误转人工 | **中** | 任务未正确设置 error_rules（常态） | 可重试错误不重试，运维负担激增，与 R19 相反 | 未缓解，须 Fix-D1 |
| 任务以 root 运行 | **中** | `sched-run` 用户不存在/降权失败 | 任务可读写调度器及全机数据 | 未缓解，须 S1 |
| 环境变量注入/泄露 | **中** | 恶意或粗心任务创建者 | 提权、窃取调度器机密 | 未缓解，须 S2/S3 |
| 无 git 备份 | **中** | 误改/磁盘故障 | 全部成果不可恢复 | 未缓解，立即建仓推送 |
| 单条数据异常拖垮任务 | **低** | 任务未用 skip_item | 有 SDK 支持但闭环缺失 | 部分缓解，须 Fix-D5 |
| jsonl/锁文件无限增长 | **低** | 长期运行 | 磁盘耗尽、查询变慢 | 未缓解，须实现 retention |

---

## 6. 复审建议

1. 先执行 4.3 的 git 建仓推送（R23 前置），再逐项修复；
2. 每个 Fix 独立提交并附对应回归测试；
3. 全部修复后重跑本报告的验证实验（第 3.1 节五个实验脚本）+ 完整测试套件 + 端到端抢占/崩溃恢复场景；
4. 以本报告第 1 章矩阵为复审清单，全部需求转为 ✅ 或带书面豁免理由的 ⚠️ 后方可宣告完成。
