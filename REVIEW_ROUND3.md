# 任务调度重构 · 需求实现综合审查报告（第三轮）

> 审查对象：`/mnt/data01/projects/scheduler_v002`（sched_v002 核心包 + sched_task_sdk + 示例任务 + 测试套件，HEAD=9e16a18）
> 需求基线：`/mnt/data01/projects/todo_list/1003/任务调度重构/results/任务调度重构_需求说明.md`（R1–R24）
> 审查方式：全量代码通读 + 逐条需求对照 + 针对性实验实证 + 测试套件运行（97 passed）+ 运行时数据检查
> 审查日期：2026-10-03
> 与前两轮关系：第一轮（REVIEW.md）5 个 P0、第二轮（REVIEW_ROUND2.md）7 项修复（F1–F7）经代码与回归测试核实均已正确落地；本报告为对当前 HEAD 的**独立第三轮复审**，所有结论基于本轮重新取证。

---

## 0. 结论摘要

前两轮发现的全部关键缺陷（once 无限重触发、准出 over-commit、画像单位污染、预估无钳制、抢占缺口误算、优先级反转、G5 兜底断裂、退避占位、多实例串扰、外部内存感知、闸门降权、stop 双终态）**均已修复且各有回归测试锁定**。五道闸、双轨排队、令牌桶软资源、cgroup 双水位、渐进式提交、崩溃恢复等主体框架落地质量良好，测试从 56 → 81 → 97 项递增。

**但本轮批判性复审仍发现 2 个 P0 级问题（其一违反最高优先级需求 R1，其二同时违反 R16/R17/R20 与总目标）、6 个 P1 级偏差/失效、9 个 P2 风险，且 R23 流程违规三轮未决。** 其中 3 项经本轮实验直接实证（manual 豁免抢占路径、SIGKILL 退出码归因死分支、默认 run_user 开箱即坏），另有 1 项在真实运行时数据中获得物证（测试数据污染生产 data/ 目录）。

| 级别 | 数量 | 概述 |
|---|---|---|
| P0 关键缺陷（含实证） | 2 | ① manual 触发任务完全豁免抢占 → 低优 manual 可无限期阻塞高优（R1 失效）；② 资源错误 5 次重试后 giveup 永久放弃 → 任务被"扑杀"（R16/R17/R20 失效） |
| P1 需求偏差/功能失效 | 6 | serve 无注册热更新、refresh() 重置 interval 触发时间地雷、退出码 137 归因死分支（-9 不匹配）、error_rules 配置面断裂、run_user 默认值开箱即坏、运行期心跳契约未验证 |
| P2 风险/安全/数据卫生 | 9 | 测试污染生产 data/（物证）、stdout 无上限可打爆磁盘、日志无轮转、无单实例锁、dry 冒烟低估、cgroup 降级依赖 root、HTTP 死配置、110 二刷任务侧无支撑、_handle_data_risk 字段残留 |
| P3 小问题 | 6 | HOME 透传、soft_error_rate 命名、参数硬编码、skip 无去重、preempt 只服务最高优、死配置字段若干 |
| 流程违规 | 1 | R23：git 无远端、从未推送（三轮未决）；data/ 未入 .gitignore |

**总体判定：不满足"完成标准"。R1（最高优先级）存在结构性反例，R16/R17 存在明确违反，须按第 4 章修复方案整改后复审。**

---

## 1. 需求实现验证矩阵（R1–R24）

图例：✅ 满足 ⚠️ 部分满足/有偏差 ❌ 未满足

| 需求 | 判定 | 验证结论与证据 |
|---|---|---|
| **R1** 高优任务占据全部资源并执行完毕（最高优先级，无商量余地） | ❌ | 缺口式抢占、优先级下限、豁免期保护均已落地（[service.py#L419-L452](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L419-L452)、[preempt.py#L36-L72](file:///mnt/data01/projects/scheduler_v002/sched_v002/preempt.py#L36-L72)）。但 [preempt.py#L48](file:///mnt/data01/projects/scheduler_v002/sched_v002/preempt.py#L48) 将 `manual` 触发的运行整体排除出抢占候选（且 [test_preempt.py#L33-L36](file:///mnt/data01/projects/scheduler_v002/tests/test_preempt.py#L33-L36) 固化为预期）：低优 manual 任务健康运行时，高优任务无法抢占、只能等其自然结束。若 manual 任务跑数小时，R1「无任何商量余地」落空。monitor 侧 D8 修复只放宽了超限阈值（×2），未触及抢占豁免。详见 3.1-P0① |
| **R2** 可抢占性一律由优先级统一决定，不接受任务级开关 | ⚠️ | 无用户可配开关 ✅；但 manual 豁免本质是按触发方式划分的抢占豁免标记，与 R2「一律由优先级统一决定」冲突（contract_exempt 豁免是 R3 的必要推论，可接受；manual 豁免无需求依据）。非 contract_exempt 的 manual 任务同样通过闸5 验证了可恢复性，被抢后可断点续跑，豁免既无必要也无依据 |
| **R3** 不可恢复任务仅手动触发、禁止自动 | ✅ | schema 模型校验（[taskdoc.py#L189-L193](file:///mnt/data01/projects/scheduler_v002/sched_v002/taskdoc.py#L189-L193)）+ 闸1 复核 + refresh 从触发引擎摘除 + 崩溃恢复不自动重入队（[service.py#L743-L746](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L743-L746)），四重防护。CLI `run` 仅入队不直跑，符合"运行只能由调度器决定"的补充条款 |
| **R4** 资源评估机制防打爆 | ⚠️ | 内存/CPU 维度三层防护（账本虚拟占用防同轮 over-commit + cgroup 双水位 + monitor 超限退让）已闭环。但**磁盘维度完全不受控**：任务 stdout/stderr 重定向落盘无大小上限（`max_log_mb` 死配置，[runner.py#L193](file:///mnt/data01/projects/scheduler_v002/sched_v002/runner.py#L193) 直接 `open(stdout,"wb")`；已定义的 `TRUNCATE_HEAD` 常量无调用方）——狂打日志的任务可耗尽磁盘，机器同样被打爆。cgroup 不可用时（非 root 部署）硬边界静默失效，仅剩账本预估 |
| **R5** 借助冒烟/历史执行评估资源 | ✅ | 三级画像（冒烟峰值 → P95 滑窗 → 声明值）× overshoot 上浮 + 运行后实样回流（[service.py#L544-L551](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L544-L551)），CPU 样本为平均核数。`test_finalize_records_avg_cpu_cores` 锁定 |
| **R6** 创建/加入时完成本地冒烟并预估 | ⚠️ | 注册必经闸3 冒烟、实测峰值入画像 ✅。偏差：默认 `smoke.mode="dry"`，dry 路径的资源占用系统性低于真实运行（sample_task dry sleep 1.0s vs real 0.3s 即为例证），gate4 用 dry 峰值判容量会低估；`smoke.sample_size` 声明未使用 |
| **R7** 预估超机器上限必须优化，优化失败拒绝 | ✅ | 闸4 对声明值与冒烟实测双重硬拒（[contract.py#L171-L185](file:///mnt/data01/projects/scheduler_v002/sched_v002/contract.py#L171-L185)）。运行期画像上浮超限时钳制在机器上限内（防永久饥饿），由 cgroup memory.max 硬兜底 + monitor 超限退让接住，注释明确——闭环成立 |
| **R8** 当前资源被占但未超上限仍允许加入 | ✅ | 注册闸只对比机器硬上限不看当前占用；运行时占用导致的等待只发生在准出侧且不失效 |
| **R9** 硬资源硬门禁 / 软资源不纳入拒绝 | ✅ | 内存/CPU 超限注册硬拒；软资源未配置/不足仅运行时排队（blocked_soft 不失效），注册不受软资源影响 |
| **R10** 同优先级排队等待，不得判失效 | ⚠️ | 双轨排序 + 饿死看门狗（提权钳制 101）+ WQ 持久化等待主体成立 ✅。但 P0②（5 次资源重试后 giveup）与 P1（serve 无热更新：注册的新任务在 serve 重启前永不入队）共同构成"任务实际无法得到执行"的旁路 |
| **R11** 软资源合理分配避免超限 | ✅ | 令牌桶准入 + 101 错误计入错误率触发降额（时间换空间）+ 冷却恢复，`test_soft_degrade_and_restore`、`test_finalize_records_soft_error_on_101` 锁定 |
| **R12** 禁止同时刻同时提交；运行只由调度器决定 | ✅ | once 到期摘除（Bug1 修复）+ max_instances/overflow 生效（D4/D6 修复）+ 注册不自动运行 + 触发只入队、spawn 仅由准出决定 |
| **R13** 渐进式动态提交 | ⚠️ | staged 门禁已实现（冷启动+有固定资源等待者时一次只准出 1 项，健康观察期后放开；观察项以资源错误/超限终结即重置门禁），"见缝插针"由全局扫描承担 ✅。偏差：门禁只在 `blocked_fixed` 非空时激活（资源充足时一轮全放——可接受的优化解读）；"健康"仅时间维度 + 未被杀，无资源指标核验；"第二个任务严重资源不足及时停止"由 monitor 超限退让承担而非 staged 机制本身 |
| **R14** 固定资源不足先等待并调度其他任务 | ✅ | `admission_scan` 全局扫描跳过被阻者继续准出（见缝插针）+ MemAvailable 感知非调度占用退让（F5 修复），`test_f5_*` 锁定 |
| **R15** 不可控资源失败等自然完毕，不强制终止 | ⚠️ | 101 软资源错误 → 退避重试，不强杀任务本体 ✅；SDK 心跳线程保证网络重试期间不被心跳误杀 ✅。但 5 次后 giveup 转人工（同 P0②） |
| **R16** 不得因排队时间过长终止任务 | ❌ | 无排队超时终止 ✅；但 `_handle_retry` 的 `MAX_ATTEMPTS=5` giveup（[service.py#L637-L643](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L637-L643)）实质是"因退避等待时长超限而永久终止任务"：5 轮指数退避累计约 75 分钟，长于它的资源紧张期（如高优任务占用 2 小时）会把排队任务永久踢出调度。退避等待是排队的一种形态，giveup 直接违反 R16 |
| **R17** 兜底为退让而非扑杀，任务仍须最终完成 | ❌ | 抢占/超限/窗口收口全部走优雅 SIGTERM+断点+重入队（退让语义正确）✅；但 giveup 把任务从自动调度轨道永久移除（decisions 记 `giveup`，不再入队），这是需求明文禁止的"扑杀"。"任务仍须最终完成执行、仅在空闲资源出现后继续"未成立 |
| **R18** 区分逻辑/资源错误；统一错误码上报 | ⚠️ | 退出码协议（0/99/100/101/110/137）+ state.json `last_error.category` 精确通道 + G5 兜底链路已接通 ✅。但 137 归因是死分支：Popen 被信号杀死返回 **-9/-15** 而非 137（实验实证，见 3.2-P1③），`exit_code == EXIT_SIGKILL` 永不匹配 → cgroup OOM 的精确归因（"资源错误"）与外部 SIGKILL 的"failed 转人工"归因双双失效，全部落入 unclassified→resource。OOM 感知（R18 核心）受损，仅靠凑巧可重试掩盖 |
| **R19** 未设置/不正确的错误一律按资源处理；分类规则由创建者自定义 | ⚠️ | G5 闭环正确（SDK unclassified→exit 1→调度器按 resource 重试）✅。但 `error_rules` 配置面断裂：task.json 的 `error_rules.resource_regex/logic_regex` 解析后从未传入任务进程（build_env 不带、SDK 只读代码内 `self.resource_regex`）——创建者按文档在 task.json 定义分类规则完全无效，R19「创建者自行定义」只剩改代码一条路（见 3.2-P1④） |
| **R20** 资源不足不得导致数据/任务失败；必须支持恢复 | ⚠️ | 闸5 断点闸 + 崩溃恢复重入队 + 抢占豁免期 + timeout/interrupted 恢复链路完整 ✅。但 giveup（P0②）破坏"资源不足不得导致任务失败"；心跳超时（STATUS_KILLED）转人工不重试对"长步无心跳"的非 SDK 契约任务构成误杀转人工（P1⑥，R20 边缘场景） |
| **R21** 单条数据异常不卡顿（跳过/隔离） | ⚠️ | `skip_item`/`dead_letter` + 110 二刷调度侧闭环（最多 3 轮转人工）✅。缺口：任务侧无 pending_skipped 消费/去重 API，`SCHED_TRIGGER=second_pass` 语义未文档化，无端到端测试验证"110 → 二刷 → pending_skipped 被处理"全链路；`_handle_data_risk` 重入队残留旧 error/stdout_log/wait_ms 字段 |
| **R22** 代码层强制约束 | ⚠️ | 五道闸（schema + 契约行为 + 冒烟 + 容量 + 断点）提供强约束 ✅；env 黑名单、路径穿越防护、优先级/时长校验齐备 ✅。缺口：运行期心跳契约未纳入闸门验证（闸2 只验证 probe 阶段写一次 state.json），bash 类契约任务运行期不写心跳将在 120s 后被 heartbeat_kill 且不重试；`error_rules` 未接线（同 R19） |
| **R23** 修改前推送远端 git | ❌ | `git remote -v` 为空：仓库有 4 个本地 commit，**从未配置远端、从未推送**，误改不可回退。三轮审查均提出，至今未落实 |
| **R24** 自主执行协议 | ⚠️ | 长时监督循环（tick 异常自捕获）、状态持久化（原子快照 + flock）、决策审计（decisions.jsonl）、完成后自检（两轮审查报告 + 回归测试）均体现 ✅；阻塞清单由 REVIEW 系列报告承担 ✅。R23 未决即协议完整性缺口 |

**统计：✅ 8 项 / ⚠️ 12 项 / ❌ 4 项（R1、R16、R17、R23）。**

---

## 2. 前两轮修复项验证结果（全部确认落地）

| 修复项 | 验证证据 |
|---|---|
| Bug1 once 无限重触发 | [engine.py#L110-L111](file:///mnt/data01/projects/scheduler_v002/sched_v002/engine.py#L110-L111) 过期返回 None + [service.py#L238-L240](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L238-L240) 触发后摘除；`test_once_expired_next_fire_none`、`test_once_fires_once_then_removed` |
| Bug2 准出同轮 over-commit | `admission_scan` 维护 pending_* 虚拟账本（[admission.py#L124-L140](file:///mnt/data01/projects/scheduler_v002/sched_v002/admission.py#L124-L140)）；`test_admission_scan_same_round_no_overcommit` |
| Bug3 画像 CPU 单位 | `_finalize` 记平均核数；`test_finalize_records_avg_cpu_cores` |
| Bug4 预估钳制 | `estimate_job` 钳制在机器上限；`test_estimate_job_clamped_to_capacity` |
| Bug5 抢占缺口误算 | `_tick_preempt` 按 gap 计算并校验豁免期；`test_preempt_uses_gap_not_total_need` |
| F1 优先级反转 | `select_preempt_targets` 以 `waiter_priority` 为严格下限；`test_f1_*` 三项 |
| F2 G5 兜底 | runner 读 `state_category` 精确分类；`test_g5_unclassified_treated_as_resource` |
| F3 退避占位 | `_tick_admission` 过滤 cooldown/backoff；`test_f3_backoff_items_skipped_in_admission_scan` |
| F4 多实例隔离 | 实例级 `cache/runs/<run_id>/state.json` + 任务级 resume_point；`test_f4_concurrent_instances_isolated_state` |
| F5 外部内存感知 | `external_mem_margin_mb` + MemAvailable 判定；`test_f5_*` 三项 |
| F6 闸门降权 + env 黑名单 | 闸门与运行同等降权、降权失败上抛、黑名单双重过滤；`test_f6_*` 三项 |
| F7 stop 单终态定点 | stop 发 cancel → worker 收敛；信号先于退出码归因；`test_f7_*` 两项 |

---

## 3. 本轮新发现问题详述

### 3.1 P0 级（违反最高优先级需求 / 总目标）

#### P0① manual 触发任务完全豁免抢占 → R1 失效

- **位置**：[preempt.py#L48](file:///mnt/data01/projects/scheduler_v002/sched_v002/preempt.py#L48)
  ```python
  candidates = [r for r in running if not r.manual and not r.contract_exempt
                and r.priority < waiter_priority]
  ```
- **违反需求**：R1（高优任务必须占据全部资源并执行完毕，「无任何商量余地」）、R2（可抢占性一律由优先级统一决定）。
- **场景**：优先级 10 的任务以 manual 触发运行（健康心跳 + 内存未超预估值 ×2 + 观察者以 `manual=True` 入场）需 4 小时完成；期间优先级 90 的任务入队且固定资源不足。`_tick_preempt` 找到 blocked 等待者 → `select_preempt_targets` 候选为空 → 高优任务等待 4 小时。monitor 的 D8 修复（超限 ×2 阈值、心跳 ×2 放宽）只约束"异常"manual 任务，健康 manual 任务不受任何约束。
- **非契约矛盾的佐证**：非 contract_exempt 的 manual 任务注册时与自动任务同样通过闸5（跑 → SIGTERM → exit 99 → resume_point → 恢复跑成功），即其**具备被抢占后恢复的全部能力**，豁免既无必要亦无需求依据。test_preempt.py#L33-L36 将其固化为预期是方向性错误（与第二轮对 Bug1 测试的批评同理）。
- **连带不一致**：同一 manual 标记在 monitor 侧是"放宽不豁免"（×2 阈值），在 preempt 侧是"完全豁免"——两套语义并存，说明豁免是残留设计而非有意权衡。
- **修复方案**：见 4.1-A。

#### P0② 资源错误 5 次重试后 giveup 永久放弃 → R16/R17/R20 失效

- **位置**：[service.py#L637-L643](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L637-L643)
  ```python
  if attempts >= MAX_ATTEMPTS:
      persist.append_decision({... "act": "giveup", ... "reason": "资源错误已达最大重试，转人工"})
      return
  ```
- **违反需求**：R16（不得因排队时间过长而终止任务——指数退避累计约 75 分钟（300+600+1200+2400s），超过它的资源占用期即触发永久放弃）、R17（兜底为退让而非扑杀，任务仍须最终完成——giveup 后任务永久离开自动调度）、R20（资源不足不得导致任务失败）、第 1 节总目标（所有已提交任务最终都能被正确执行）。
- **关键矛盾**：需求场景恰恰是"资源有限且被高优/非调度任务长期占用"——高优任务跑 2 小时是最常见的合法场景，低优资源错误任务在退避中累计 attempt，高优结束后资源空闲，但任务若已完成 5 次退避则已被永久放弃。资源真正不足的场景恰是需求要求"等待资源释放后继续执行"的场景，giveup 与需求的兜底哲学正面冲突。
- **G5 交互**：unclassified 兜底使崩溃型逻辑 bug（任务未自报分类）也走 5 次重试后 giveup——按 R19 语义按资源处理是正确方向，但同样不应扑杀；无限重试的运维风险应由决策审计 + 可配置上限（默认无限）承担，而非硬编码 5 次。
- **修复方案**：见 4.1-B。

### 3.2 P1 级（需求偏差 / 功能失效）

#### P1① serve 常驻进程无注册/配置热更新

- **位置**：[service.py#L212-L220](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L212-L220)（`_loop` 只跑 4 个 tick，无 refresh）；`refresh()` 仅在 `start()` 与 CLI 单命令进程中调用。
- **影响**：`sched-v002 register X` 写 jobs.json 后，运行中的 serve 进程 `self.entries`/触发引擎均不感知——新任务（含其 schedule/priority/资源声明）在 serve 重启前**永不被触发**；修改已注册任务的 task.json（调优先级、改窗口）同样不生效。违反总目标"所有已提交任务最终都能被正确执行"与 R10。
- **修复方案**：见 4.2-C。

#### P1② `refresh()` 无条件重置 interval 触发时间（热更新地雷）

- **位置**：[engine.py#L122-L123](file:///mnt/data01/projects/scheduler_v002/sched_v002/engine.py#L122-L123)（`upsert` 每次 `next_fire(schedule, now)`，纯 interval 即 `now + interval`）+ [service.py#L125-L136](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L125-L136)（refresh 对全部任务无条件 upsert）。
- **影响**：当前因 serve 进程不 refresh 而未爆发；一旦按 P1① 加热更新（每 30s refresh），interval=3600 的任务在注册频率高于每小时时**永不触发**（触发时间被反复重置为 now+3600）。这是修 P1① 时必然踩中的地雷，须一并修复。
- **修复方案**：见 4.2-C。

#### P1③ 退出码 137 归因死分支（实验实证）

- **实证**：本轮实验 `os.killpg(p.pid, SIGKILL)` 后 `Popen.returncode == -9`，而 [runner.py#L269](file:///mnt/data01/projects/scheduler_v002/sched_v002/sched_v002/runner.py#L269) 比较 `exit_code == errors.EXIT_SIGKILL`（137）。不经 shell 直接 exec 时，信号死亡以负数返回码呈现，137 永不出现。
- **影响**：① cgroup OOM（memory.events oom_kill>0 且 rc=-9）精确归因失效，落入 unclassified→resource——行为凑巧可重试，但"cgroup OOM"信息丢失、R18 调度器对资源不足的感知受损；② 外部 `kill -9`（rc=-9，非调度器信号、非 OOM）应判 failed 转人工，实际被判 resource 无限重试 5 次后才 giveup；③ `test_f2_negative_exit_code_is_unclassified` 把 -15→unclassified 固化为预期，掩盖了该分支从未被正确设计的事实。
- **修复方案**：见 4.2-D。

#### P1④ `error_rules` 配置面断裂（R19 创建者自定义规则无效）

- **位置**：[taskdoc.py#L120-L123](file:///mnt/data01/projects/scheduler_v002/sched_v002/taskdoc.py#L120-L123) 解析 `error_rules` 后，sched_v002 全代码库无任何消费者；[runner.py#L45-L72](file:///mnt/data01/projects/scheduler_v002/sched_v002/runner.py#L45-L72) `build_env` 不透传；[task.py#L45-L46](file:///mnt/data01/projects/scheduler_v002/sched_task_sdk/task.py#L45-L46) SDK 只读代码内 `self.resource_regex = []`，不读 task.json。
- **影响**：任务创建者按 schema 在 task.json 声明的 `error_rules.resource_regex/logic_regex` 完全无效；R19「错误分类规则由任务创建者自行定义」只剩"改任务代码"一条路。schema 字段成为误导性死配置（作者以为配置生效，实际不生效——比没有该字段更糟）。
- **修复方案**：见 4.2-E。

#### P1⑤ 默认 `run_user="sched-run"` 开箱即坏（实验实证）

- **实证**：本机 `id sched-run` 报 no such user；实验以 `preexec_fn=_drop_privileges('sched-run')` 拉起进程 → `SubprocessError: Exception occurred in preexec_fn`。
- **影响**：默认 `config.load_settings()` 生成 `run_user="sched-run"`（[config.py#L113](file:///mnt/data01/projects/scheduler_v002/sched_v002/config.py#L113)），在未 `useradd sched-run` 的机器上：注册五道闸的闸2（契约 probe）直接以 SubprocessError 失败 → **任何任务无法注册**。README 无 useradd 部署说明，`ensure_layout()` 也不检查。测试全部以 `run_user=""` 绕过，掩盖了该问题。
- **修复方案**：见 4.2-F。

#### P1⑥ 运行期心跳契约未纳入闸门验证（非 SDK 契约任务陷阱）

- **位置**：闸2 只验证 probe 阶段写一次合法 state.json（[contract.py#L143-L153](file:///mnt/data01/projects/scheduler_v002/sched_v002/contract.py#L143-L153)）；闸5 验证断点。运行期"持续心跳"无任何闸门覆盖。
- **影响**：bash/非 Python 契约任务通过闸2 后，正式运行若不持续更新 state.json（心跳由 SDK 后台线程承担，bash 无此机制），120s 后被 `heartbeat_kill` → STATUS_KILLED → `_after_finalize` 转人工不重试。任务未失败、未卡死（例如 bash sleep 180），仅因心跳契约盲区被杀且不可恢复——R20/R22 的边缘违反。
- **修复方案**：见 4.2-G。

### 3.3 P2 级（技术风险 / 安全 / 数据卫生）

| # | 问题 | 位置/证据 | 风险 |
|---|---|---|---|
| P2① | **测试污染生产 data/ 目录（物证）** | 真实 `data/runtime/active.json` 残留 pytest-23 的 ok2 running 记录（pid 768883，task_dir 指向 /tmp/pytest-of-root/...）；`data/runs/b.jsonl` 残留 pytest-22 的 b 任务终态记录。机制：worker 为 daemon 线程，测试不 join；monkeypatch teardown 恢复 SCHED_HOME 后，`_finalize`/`_on_started` 的异步持久化写入 DEFAULT_HOME | 生产 serve 重启时 `_reconcile` 将消费残留 active.json（虽被 `_pid_belongs_to` 防误杀 + 未注册 job recover_skip 兜住）；.gitignore 未含 `data/`，污染可能入库；测试卫生缺陷 |
| P2② | **stdout/stderr 无大小上限** | [runner.py#L193](file:///mnt/data01/projects/scheduler_v002/sched_v002/sched_v002/runner.py#L193) `open(stdout,"wb")`；`TRUNCATE_HEAD` 常量（L28）无调用方；`max_log_mb` 死配置 | 任务狂打日志耗尽磁盘 → 整机故障（R4 的"资源"应涵盖磁盘）；cgroup 不限磁盘 |
| P2③ | **日志/审计无轮转** | decisions.jsonl、runs/*.jsonl append-only；`retention_days` 死配置 | 长期运行磁盘无限增长 |
| P2④ | **无单实例锁** | serve 启动无 runtime pid/flock 互斥；store flock 只保文件原子性 | 误启两个 serve → 同一 WQ 双跑任务、账本双记、cgroup 名冲突（`Cgroup.__init__` mkdir exist_ok 静默复用旧 cgroup） |
| P2⑤ | **dry 冒烟系统性低估资源** | gate3 默认 dry 模式；sample dry/real sleep 比即例证；`smoke.sample_size` 死字段 | gate4 低估 → 首次真实运行靠 cgroup 硬限兜底 + OOM 重试修正画像，造成一轮不必要的 OOM/退让（R6 偏差） |
| P2⑥ | **cgroup 静默降级依赖 root 部署** | `Cgroup` 不可用时 available=False 全 no-op；调度器须 root 才能写 /sys/fs/cgroup；部署模型（root 调度器 + 降权任务）未文档化 | 非 root 运行时 R4 防打爆只剩账本预估，无硬边界；且无告警 |
| P2⑦ | **HTTP 死配置** | HttpConfig(0.0.0.0:8787) + AuthConfig(enabled=False) 存在但无 HTTP server 实现 | 未来接线时若沿用默认（全网卡绑定 + 鉴权默认关）即成暴露面；现为死代码 |
| P2⑧ | **110 二刷任务侧无支撑** | SDK 无 pending_skipped 消费/去重 API；`SCHED_TRIGGER=second_pass` 语义未文档化；无端到端测试 | 任务作者无法按文档实现二刷；同一 item 重试多刷重复记录 |
| P2⑨ | **`_handle_data_risk` 字段残留** | [service.py#L618-L630](file:///mnt/data01/projects/scheduler_v002/sched_v002/service.py#L618-L630) 未清 error/stdout_log/stderr_log/wait_ms/duration_ms | 二刷 run 记录携带上轮失败痕迹，审计噪声 |

### 3.4 P3 级（小问题）

1. HOME 从调度器环境透传给降权用户（`build_env` 白名单含 HOME）——降权任务拿到 /root 家目录（无权写则行为异常），应改设目标用户 home。
2. `monitor.soft_error_rate` 返回计数而非率，命名误导。
3. `STARVATION_SEC`、`preempt_exempt_sec`、`data_risk_max_reschedules`、soft_degrade 系列参数硬编码，不可经 settings 配置。
4. `skip_item`/`dead_letter` 无去重。
5. `_tick_preempt` 只服务最高优 blocked 等待者；次优者的可满足缺口本轮不处理（正确性无碍，效率次优）。
6. 死配置字段：`wait_max_sec`、`max_workers`、`decay`、`diff_check`、`SchedulerState.latch`、`smoke.sample_size`、`max_log_mb`、`retention_days`、HttpConfig/AuthConfig 全套。
7. `_reconcile` 不清理旧 cgroup 目录（run_id 唯一不复用 → 目录累积）；pid 已死但 pgid 存活时孤儿进程组不清理。
8. `Capacity` 在构造时快照总内存/CPU，长期运行不刷新（热插拔/容器场景失真）。

### 3.5 流程合规

- **R23（三轮未决）**：`git remote -v` 为空，4 个本地 commit 从未推送远端。任何"误改回退"承诺均无实现基础。修复：配置远端（内网 GitLab/自托管均可）并 `git push -u origin main`；将推送纳入改动前置检查清单。
- **data/ 未入 .gitignore**：`git status` 显示 `?? data/`，与 P2① 叠加存在误提交运行时数据风险。

---

## 4. 修复方案

### 4.1 P0 修复（必须）

**A. 移除 manual 抢占豁免（R1/R2）**

```python
# preempt.py — 仅保留 contract_exempt 排除（R3 的必要推论）
candidates = [r for r in running if not r.contract_exempt
              and r.priority < waiter_priority]
```
- manual 任务与非 manual 任务在被抢占后走完全相同的恢复路径（闸5 已验证其可恢复性），删除豁免不引入新风险。
- 保留：豁免期（`_exempt_until_monotonic`）、心跳/超限的 manual ×2 放宽（这是对"人工在场"的合理照顾，且不豁免）。
- 若确需保护刚启动的 manual 任务，可给 manual 一个有限时长（如 5 分钟）的启动豁免，而非全程豁免。
- **测试**：新增「低优 manual 运行 + 高优等待 → 抢占成功且 manual 以 resume_point_ref 重入队」；改造 `test_select_skips_manual` 为「contract_exempt 仍跳过、manual 不再跳过」。

**B. 资源重试永不扑杀（R16/R17/R20）**

```python
# service.py — _handle_retry 改造
def _handle_retry(self, rec, category):
    attempts = int(rec.get("attempt") or 1)
    backoff = min(300 * (2 ** min(attempts - 1, 10)), 3600)  # 封顶 1h，attempt 封防溢出
    # 删除 MAX_ATTEMPTS giveup：资源类错误无限退避重入队（R17 退让哲学）
    ...
```
- 保留 attempt 递增仅作审计字段；决策日志继续记录每轮 retry_enqueue，可供人工观察。
- 转人工的合法触发改为：①任务自报 logic（已有）；②数据风险 3 轮上限（已有）；③新增可选 escape hatch——`settings.runner.max_resource_attempts`（默认 0=无限，运维显式配置才封顶）。这样 R19 G5「宁可重试」语义与运维止损需求两全。
- 需同步修订 `test_handle_retry_gives_up_after_max`：改为「默认无限重试；配置 max_resource_attempts=N 时 N 次后转人工」双分支断言。

### 4.2 P1 修复（应当）

**C. serve 热更新 + upsert 保序（P1①②一并）**

```python
# engine.py — 未到期的下次触发不被刷新重置
def upsert(self, job_id, schedule):
    nxt = next_fire(schedule, datetime.now(self._tz))
    cur = self._next.get(job_id)
    if cur is not None and nxt is not None and cur > datetime.now(self._tz) \
       and cur <= nxt:          # 已有更近且未到期的触发 → 保留
        return
    self._next[job_id] = nxt

# service.py — 主循环低频热更新（mtime 触发或固定 30s）
def _loop(self):
    last_refresh = 0.0
    while not self._stopping.wait(TICK_SEC):
        if time.monotonic() - last_refresh > 30:
            self.refresh(); last_refresh = time.monotonic()
        ...
```
- refresh 自身已有"移除失效任务 + upsert 新任务"的完整语义，配合 upsert 保序即安全。
- **测试**：`upsert` 两次调用不改未到期的 interval next_fire；refresh 后新注册任务出现在 engine._next。

**D. 信号退出码归因（P1③）**

```python
# runner.py — 终态判定覆盖负数返回码
elif exit_code == -signal.SIGKILL or exit_code == EXIT_SIGKILL:  # -9 或经 shell 的 137
    if oom: ...
    elif scheduler_signaled: ...
    else: status, reason = runlog.STATUS_FAILED, f"被外部信号杀死（rc={exit_code}）"
```
- 其余负数（-15 等）维持 unclassified→resource（G5）路径不变。
- **测试**：真实 spawn 子进程后 `os.killpg(SIGKILL)` → 断言 rc=-9 走归因分支（cgroup 不可用时 oom=False → FAILED；伪造 oom=True → RESOURCE）。删除/修正 `test_f2_negative_exit_code_is_unclassified` 中对 -15 的固化（-15 应保持 unclassified，但需新增 -9 的独立断言）。

**E. error_rules 接线（P1④）**

```python
# runner.py build_env — 注入分类规则（JSON 串）
env["SCHED_ERROR_RULES"] = json.dumps({"resource_regex": ..., "logic_regex": ...})

# sched_task_sdk/task.py __init__ — 从环境解析并合并
raw = os.environ.get("SCHED_ERROR_RULES")
if raw:
    rules = json.loads(raw)
    self.resource_regex = list(rules.get("resource_regex") or [])
    self.logic_regex = list(rules.get("logic_regex") or [])
```
- RunSpec 增加 error_rules 字段，`_admit` 组装时从 doc 传入。
- **测试**：task.json 配 `logic_regex: ["my-biz-error"]`，任务抛 "my-biz-error xxx" → FAILED 转人工；未配置时维持 G5。

**F. run_user 开箱可用（P1⑤）**

- `ensure_layout()` 或 `cmd_register` 前置检查：`settings.run_user` 非空且 `pwd.getpwnam` 失败时，输出明确指引（`useradd -r -s /usr/sbin/nologin sched-run`）并以友好错误退出，而非 SubprocessError 深处爆炸。
- README「部署」章节补齐：调度器以 root 运行（cgroup 需要）、任务降权用户创建、SCHED_HOME 布局。
- **测试**：mock `pwd.getpwnam` 抛 KeyError → register 返回带指引的错误信息。

**G. 心跳契约入闸（P1⑥）**

- 闸5（或新增闸6）断言：断点首跑期间（signal_after_sec 窗口内）state.json mtime 至少更新一次（或 `updated_at` 变化），证明任务具备运行期心跳能力；纯 sleep 的 bash 任务在此被拒并得到明确报错。
- 短期替代（低成本）：README/SDK 文档显式声明运行期心跳契约；`heartbeat.timeout_sec` 允许任务级放大（已有）+ heartbeat_kill 后按 interrupted 语义重入队一次（防误杀即丢）。
- **测试**：无心跳任务在闸5 被拒并提示心跳契约。

### 4.3 P2 修复（建议排期）

1. **测试隔离（P2①）**：`home` fixture 增加 finalizer——`svc.stop()` + join 全部 worker 线程（带超时）后再退出 mock 作用域；.gitignore 追加 `data/`；清理现存污染（删除 data/ 下 pytest 残留）。
2. **stdout 上限（P2②）**：runner 落盘改为带 max_log_mb 的写封装（超限截断 + 写 `TRUNCATE_HEAD` 标记，常量已备）；顺带实现 stderr 同策略。
3. **日志轮转（P2③）**：append_jsonl 增加按大小/日期轮转（简单 split 即可，retention_days 清理旧档）。
4. **单实例锁（P2④）**：serve 启动时对 `runtime/scheduler.lock` flock 独占，失败即报"已有实例运行"退出。
5. **冒烟真实化（P2⑤）**：文档引导高风险任务 `smoke.mode="real"`；`sample_size` 实现为多次采样取峰值或删字段。
6. **降级告警（P2⑥）**：serve 启动日志显式输出 cgroup 可用性；不可用时 decisions.jsonl 记一条 `cgroup_degraded` 告警。
7. **HTTP 处置（P2⑦）**：短期删除 HttpConfig/AuthConfig 死代码（或标注预留）；若保留，默认绑定改 127.0.0.1。
8. **二刷支撑（P2⑧）**：SDK 增加 `read_pending_skipped()`（带去重游标）+ 示例任务演示 110 全链路 + 端到端测试（任务 skip 一条 → exit 110 → second_pass 触发 → 补跑成功）。
9. **字段清理（P2⑨）**：`_handle_data_risk` 与 `_handle_retry`/`_requeue_resume` 统一走 `_new_run_record(rec, trigger=...)` 工厂，显式清空 error/logs/times/pid 字段。

### 4.4 P3 清单（backlog）

HOME 按目标用户设置、`soft_error_rate` 更名 `soft_error_count`、硬编码参数入 Settings、skip 去重、preempt 次优者批量处理、死配置字段清理（删除或实现）、cgroup 目录回收与孤儿进程组清理、Capacity 定期刷新。

### 4.5 流程整改（R23）

```bash
git remote add origin <远端URL> && git push -u origin main
echo "data/" >> .gitignore && git add .gitignore && git commit -m "chore: ignore runtime data dir"
```
后续改动前置检查：`git status` 干净 + 已 push 后再动代码。

### 4.6 测试策略优化

1. **新增 P0 回归**：manual 可被抢占且恢复；资源重试第 6 次仍重入队（默认无上限）。
2. **新增 P1 回归**：upsert 保序；refresh 热更新生效；rc=-9 三态归因（OOM/调度器信号/外部）；error_rules 经环境注入生效；run_user 不存在的友好报错；无心跳任务被闸门拒绝。
3. **端到端补全**：110 数据风险全链路（skip → 110 → second_pass → pending_skipped 消费 → success）；双 serve 互斥；日志超限截断。
4. **混沌场景**：外部进程吃内存下准出退让（已有 F5 单元级，补集成级）；cgroup 不可用降级路径。
5. **测试卫生**：所有启动 worker 的测试必须在 fixture finalizer join 线程（修 P2① 根因）；`-15` 归因测试拆分为 -9/-15 两类各自断言。
6. **测试有效性**：现存 `test_select_skips_manual` 固化错误预期（同第二轮 test_preempt 选错方向的问题）——修 A 项时必须同步改测试，防止回归测试反向锁定缺陷。

---

## 5. 总体结论

| 维度 | 结论 |
|---|---|
| 架构与主体功能 | 五层架构清晰、五道闸/双轨排队/令牌桶/cgroup 双水位/渐进提交/崩溃恢复均已落地，前两轮 12 项关键修复全部有效，97 项测试通过 |
| 需求符合度 | ✅ 8 / ⚠️ 12 / ❌ 4：R1（manual 豁免抢占）、R16+R17（giveup 扑杀）、R23（无远端）为硬缺口 |
| 技术风险 | 磁盘不受控（stdout/日志）、双实例并发、非 root 无硬边界、测试污染生产数据 |
| 安全 | env 黑名单/路径穿越/降权上抛已闭环；HTTP 死配置与 HOME 透传为小缺口；无网络暴露面（当前无 HTTP 实现） |
| 判定 | **不满足完成标准**。须完成 4.1（A、B）+ 4.5（R23）并复审通过后方可宣称达标；4.2 建议同批整改，4.3/4.4 可排期 |

> 本报告为第三轮独立复审证据，修复后请以第四轮复审闭环验证矩阵全绿。

---

## 6. 修复完成记录（2026-10-03）

第三轮全部问题已按第 4 章方案修复完毕，本地提交 `b4a2504`：

- **P0-A/B**：manual 不再豁免抢占（仅 contract_exempt 保留）；资源错误默认无限退避重试，`max_resource_attempts>0` 才封顶转人工。R1/R16/R17/R20 硬缺口关闭。
- **P1-C~G**：upsert 保序 + serve 热加载（`hot_reload_sec`）；-9 三态归因；error_rules 经 `SCHED_ERROR_RULES` 注入并由 SDK 读取；run_user 前置校验含 useradd 指引；闸5 心跳契约 + 心跳误杀重入队一次。
- **P2/P3**：stdout 截断、jsonl 轮转、单实例 flock + cmd_serve 友好报错、cgroup 降级告警、二刷支撑、重入队字段工厂；死配置清理、调度参数入 Settings、`_reconcile` 清理遗留 cgroup、`Capacity.refresh()`。
- **P2① 根治**：worker 在 `running.pop` 之后才落盘终态，按 running 追踪线程会漏——fixture finalizer 改为 join 全部新建线程；生产 `data/` 污染已清除并验证全量跑后保持干净。
- **测试**：更新 4 处固化错误预期的旧测试，新增 `tests/test_round3_fixes.py` 17 项回归；全量 **115 passed**。
- **R23 遗留**：`.gitignore` 已加 `data/`，本地已提交；但仓库未配置远端，**推送远端需由运维提供 remote URL 后执行 `git push`**。
